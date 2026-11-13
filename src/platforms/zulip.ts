import type {
  ChannelDescriptor,
  ContentBlock,
  ContextInjection,
  TextContent,
} from '@animalabs/mcpl-core';
import { dmKey, parseKey, streamIdOfKey, streamKey, type ChannelKey, type KeyedMessage } from '../channel-key.js';
import { StreamDirectory } from '../stream-directory.js';
import type {
  ChangeEntry,
  ChannelHistoryPage,
  ChannelHistoryQuery,
  MessageChangeEvent,
  OnIncomingMessage,
  OnMessageChange,
  OnReaction,
  OnSystemEvent,
  PlatformAdapter,
  PublishResult,
  ReactionEvent,
  RoutingHints,
  SentRecord,
} from './adapter.js';
import { ZulipEventLoop, type ZulipMessageChange } from './zulip-events.js';
import { cleanMarkdown } from '../content.js';
import { namePosted, NothingToSend, SendRefused, sendMessage, ZULIP_MAX_MESSAGE_LENGTH, type Destination } from '../outgoing.js';
import { messageLineHead } from '../message-line.js';
import { agentLineTimeFormatter } from '../timezone.js';
import { uploadBlocks, type UploadPolicy, type Uploader } from '../uploads.js';
import { isRefusal, mayHaveBeenApplied, ZulipRefused, type ZulipApi } from '../zulip-api.js';
import {
  dmChannelIdFor,
  dmCounterparts,
  dmDescriptor,
  editedTrailer,
  fetchHistory,
  keyOfMessage,
  normalizeMessage,
  looksLikeBotEmail,
  toIncoming,
  type ZulipIdentity,
  type ZulipMessage,
  type ZulipRawMessage,
} from '../history.js';

export interface ZulipChannelAddress {
  stream_name: string;
  stream_id: number;
}

export function zulipChannelId(streamName: string): string {
  return `zulip:${streamName}`;
}

const SNIPPET_MAX = 80;

export function snippetOf(text: string): string | null {
  const one = text.replace(/\s+/g, ' ').trim();
  if (!one) return null;
  return one.length > SNIPPET_MAX ? `${one.slice(0, SNIPPET_MAX - 1)}…` : one;
}

function quotedMessageIds(content: string): Set<number> {
  const ids = new Set<number>();
  let pending: number | null = null;
  let fence: string | null = null;
  for (const line of content.split(/\r?\n/)) {
    const marker = /^ {0,3}(`{3,}|~{3,})(.*)$/.exec(line);
    if (fence !== null) {
      if (marker && marker[1][0] === fence[0] && marker[1].length >= fence.length && !marker[2].trim()) fence = null;
      continue;
    }
    if (marker) {
      if (pending !== null && marker[2].trim() === 'quote') ids.add(pending);
      if (ids.size === 2) return ids;
      fence = marker[1];
      pending = null;
      continue;
    }
    if (!line.trim()) continue;
    const header = /^(?:@_\*\*[^\r\n]*\*\* )?\[[^\]\n]*\]\s*\([^\s)]*\/near\/([1-9]\d*)\)\s*[:：][ \t]*$/.exec(line);
    const id = header ? Number(header[1]) : NaN;
    pending = Number.isSafeInteger(id) ? id : null;
  }
  return ids;
}

type QuoteLookup = { authorId: number } | null | 'budget-skipped';
type QuoteVerdict = 'reply' | 'not-reply' | 'ineligible' | 'unverified' | 'budget-skipped';

interface ZulipStream {
  stream_id?: number;
  name: string;
  subscriber_count?: number;
  invite_only?: boolean;
}

interface SeenMessage {
  key: ChannelKey;
  authorId: number;
  authorName: string;
  authorEmail: string;
  isDm: boolean;
  topic: string;
  snippet: string | null;
  mentioned: boolean;
  replyToSelf: boolean;
  quoteTargets: ReadonlySet<number>;
}

const LOCATE_TIMEOUT_MS = 15_000;
const QUOTE_TIMEOUT_MS = 3_000;
const HISTORY_QUOTE_GET_CAP = 8;
const FAILED_LOOKUP_TTL_MS = 3 * 60_000;
const SELF_DELETE_TTL_MS = 10 * 60_000;
const SELF_DELETE_CAP = 500;
const STREAM_REFRESH_HOLDOFF_MS = 60_000;
const MOVED_AWAY_TTL_MS = 60_000;

const QUOTE_GET_BURST = 30;
const QUOTE_GET_REFILL_PER_MS = 30 / 60_000;

export interface FilterView {
  streamAllowed(streamName: string): boolean;
  dmAllowed(sender: { id: number; email: string }): boolean;
}

const ALLOW_ALL: FilterView = { streamAllowed: () => true, dmAllowed: () => true };

export interface ZulipAdapterOptions {
  backscrollDefault?: number;
  backscrollLimits?: ReadonlyMap<string, number>;
  filters?: FilterView;
  dmDiscoveryLimit?: number;
  directory?: StreamDirectory;
  maxMessageLength?: number;
  uploader?: Uploader;
  uploadPolicy?: UploadPolicy;
  formatTime?: (d: Date) => string;
}

export const DEFAULT_BACKSCROLL = 500;
const DEFAULT_DM_DISCOVERY_LIMIT = 300;

export class ZulipAdapter implements PlatformAdapter {
  readonly type = 'zulip';
  onSent: ((sent: SentRecord) => void) | null = null;

  private eventLoop: ZulipEventLoop | null = null;
  private readonly identity: ZulipIdentity;
  private readonly backscrollDefault: number;
  private readonly backscrollLimits: ReadonlyMap<string, number>;
  private backscrollFormerSaid = new Map<string, string | undefined>();
  private readonly filters: FilterView;
  private readonly dmDiscoveryLimit: number;
  private readonly maxMessageLength: number;
  private readonly uploader: Uploader | null;
  private readonly uploadPolicy: UploadPolicy | null;
  private readonly formatTime: (d: Date) => string;
  readonly directory: StreamDirectory;
  private knownDms = new Map<string, ChannelDescriptor>();
  private seen = new Map<number, SeenMessage>();
  private static readonly SEEN_CAP = 2000;
  private static quoteBudget = { tokens: QUOTE_GET_BURST, updatedAt: performance.now() };

  private static takeQuoteToken(): boolean {
    const budget = ZulipAdapter.quoteBudget;
    const now = performance.now();
    budget.tokens = Math.min(QUOTE_GET_BURST, budget.tokens + Math.max(0, now - budget.updatedAt) * QUOTE_GET_REFILL_PER_MS);
    budget.updatedAt = now;
    if (budget.tokens < 1) return false;
    budget.tokens--;
    return true;
  }

  private failedQuoteLookups = new Map<number, number>();
  /** Zulip's delete event names no actor, so the echo of the bot's own
   *  deletion is recognised here and dropped. */
  private selfDeleted = new Map<number, number>();
  private readonly listReadFor = new Map<string, number>();
  private listRead: Promise<boolean> | null = null;
  /** Zulip reports a move to a stream the bot cannot see as a delete event. */
  private movedAway = new Map<number, number>();
  private readonly changeOf = new WeakMap<ChangeEntry, ZulipMessageChange>();
  private placing = 0;
  private placed: Promise<void> = Promise.resolve();

  constructor(
    private api: ZulipApi,
    identity: ZulipIdentity,
    options: ZulipAdapterOptions = {},
  ) {
    this.identity = identity;
    this.backscrollDefault = options.backscrollDefault ?? DEFAULT_BACKSCROLL;
    this.backscrollLimits = options.backscrollLimits ?? new Map();
    this.filters = options.filters ?? ALLOW_ALL;
    this.dmDiscoveryLimit = options.dmDiscoveryLimit ?? DEFAULT_DM_DISCOVERY_LIMIT;
    this.maxMessageLength = options.maxMessageLength ?? ZULIP_MAX_MESSAGE_LENGTH;
    this.uploader = options.uploader ?? null;
    this.uploadPolicy = options.uploadPolicy ?? null;
    this.formatTime = options.formatTime ?? agentLineTimeFormatter();
    this.directory = options.directory ?? new StreamDirectory();
    if (this.backscrollLimits.size > 0) {
      void this.directory.whenFilled().then(() => this.logFormerBackscrollNames());
      this.directory.onChange((change) => {
        if (change.renamed.length > 0 && this.directory.isFilled) this.logFormerBackscrollNames();
      });
    }
    if (this.uploader && !this.uploadPolicy) throw new Error('ZulipAdapter: uploader needs an uploadPolicy');
  }

  private logFormerBackscrollNames(): void {
    for (const name of this.backscrollLimits.keys()) {
      const former = this.directory.formerOf(name);
      const said = former ? `${former.id}:${former.current}` : undefined;
      if (this.backscrollFormerSaid.get(name) === said) continue;
      this.backscrollFormerSaid.set(name, said);
      if (former) console.error(`[zulip-mcp] ZULIP_BACKSCROLL_CHANNELS names #${name}, which Zulip stream ${former.id} had before it was renamed #${former.current}; the default cap applies to it`);
    }
  }

  backscrollLimitFor(streamName: string): number {
    return this.backscrollLimits.get(streamName) ?? this.backscrollDefault;
  }

  private streamAllowed(streamId: number): boolean {
    const name = this.directory.nameOf(streamId);
    return name !== undefined && this.filters.streamAllowed(name);
  }

  get selfUserId(): number | null {
    return this.identity.selfUserId;
  }

  async discoverChannels(): Promise<ChannelDescriptor[]> {
    const channels: ChannelDescriptor[] = [];
    try {
      const result = await this.api.request<{ streams?: ZulipStream[] }>('GET', '/streams', 'listing streams', {
        include_public: true,
        include_subscribed: true,
      });
      if (!Array.isArray(result.streams)) throw new Error('the answer carried no stream list');
      const streams = result.streams;
      this.directory.list(streams);
      for (const stream of streams) {
        if (typeof stream.stream_id !== 'number' || !this.streamAllowed(stream.stream_id)) continue;
        const descriptor = this.streamDescriptor(stream.stream_id, {
          subscriber_count: stream.subscriber_count,
          is_public: !stream.invite_only,
        });
        if (descriptor) channels.push(descriptor);
      }
    } catch (error) {
      console.error('Failed to discover Zulip streams:', error);
    }
    channels.push(...(await this.discoverDmChannels()));
    return channels;
  }

  streamDescriptor(streamId: number, metadata: Record<string, unknown> = {}): ChannelDescriptor | null {
    const name = this.directory.nameOf(streamId);
    if (name === undefined) return null;
    const address: ZulipChannelAddress = { stream_name: name, stream_id: streamId };
    const descriptor: ChannelDescriptor = {
      id: zulipChannelId(name),
      type: 'zulip',
      label: `#${name}`,
      direction: 'bidirectional',
      address,
      metadata,
      capabilities: {
        history: {
          maxMessages: this.backscrollLimitFor(name),
          supportsBeforeMessage: true,
          supportsSinceLastSeen: true,
        },
      },
    };
    return descriptor;
  }

  async describeChannels(keys: ChannelKey[]): Promise<ChannelDescriptor[]> {
    const out: ChannelDescriptor[] = [];
    for (const key of keys) {
      const streamId = streamIdOfKey(key);
      if (streamId === null || !(await this.knownStream(streamId)) || !this.streamAllowed(streamId)) continue;
      const descriptor = this.streamDescriptor(streamId);
      if (descriptor) out.push(descriptor);
    }
    return out;
  }

  private async discoverDmChannels(): Promise<ChannelDescriptor[]> {
    if (this.dmDiscoveryLimit <= 0) return [];
    try {
      const result = await this.api.request<{ messages?: ZulipRawMessage[] }>('GET', '/messages', 'recent direct messages', {
        anchor: 'newest',
        num_before: this.dmDiscoveryLimit,
        num_after: 0,
        narrow: [['is', 'dm']],
        apply_markdown: false,
        include_anchor: true,
      });
      for (const raw of result.messages ?? []) {
        const m = normalizeMessage(raw);
        if (!m.isDm) continue;
        if (!this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail }) && m.authorId !== this.identity.selfUserId) continue;
        this.describeDm(m);
      }
    } catch (error) {
      console.error('Failed to discover Zulip DM conversations:', (error as Error).message);
    }
    return [...this.knownDms.values()];
  }

  private remember(key: ChannelKey, m: ZulipMessage): void {
    this.failedQuoteLookups.delete(m.id);
    this.movedAway.delete(m.id);
    if (!this.seen.has(m.id) && this.seen.size >= ZulipAdapter.SEEN_CAP) {
      const oldest = this.seen.keys().next().value;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    this.seen.set(m.id, {
      key,
      authorId: m.authorId,
      authorName: m.authorName,
      authorEmail: m.authorEmail,
      isDm: m.isDm,
      topic: m.topic,
      snippet: snippetOf(m.cleanContent),
      mentioned: m.mentioned,
      replyToSelf: m.replyToSelf,
      quoteTargets: quotedMessageIds(m.rawContent),
    });
  }

  private quoteLookupFailed(messageId: number): boolean {
    const retryAfter = this.failedQuoteLookups.get(messageId);
    if (retryAfter !== undefined && Date.now() < retryAfter) return true;
    this.failedQuoteLookups.delete(messageId);
    return false;
  }

  private rememberFailedQuoteLookup(messageId: number): void {
    if (this.failedQuoteLookups.size >= ZulipAdapter.SEEN_CAP) {
      const oldest = this.failedQuoteLookups.keys().next().value;
      if (oldest !== undefined) this.failedQuoteLookups.delete(oldest);
    }
    this.failedQuoteLookups.set(messageId, Date.now() + FAILED_LOOKUP_TTL_MS);
  }

  private async locate(messageId: number, purpose = 'a reaction', timeoutMs = LOCATE_TIMEOUT_MS, quoteLookup = false): Promise<SeenMessage | null> {
    const cached = this.seen.get(messageId);
    if (cached) return cached;
    try {
      const result = await this.api.requestAnswer<{ message?: ZulipRawMessage }>(
        'GET', `/messages/${messageId}`, { apply_markdown: false }, { timeoutMs },
      );
      // This endpoint uses BAD_REQUEST for both missing and inaccessible ids.
      // Older (pre-5.0) servers omit code; only then use the legacy text.
      if (quoteLookup && result.result === 'error' &&
          (result.code === 'BAD_REQUEST' || (result.code === undefined && result.msg === 'Invalid message(s)'))) return null;
      if (isRefusal(result)) throw new ZulipRefused(`message ${messageId}`, result);
      const raw = result.message;
      if (!raw) throw new Error('response contained no message');
      if (raw.id !== messageId) throw new Error('response contained a different message id');
      if (!Number.isSafeInteger(raw.sender_id) || raw.sender_id <= 0) throw new Error('response contained no valid sender_id');
      const m = normalizeMessage(raw);
      const key = await this.keyFor(m, raw);
      if (key === null) {
        if (quoteLookup) throw new Error(`the stream of message ${messageId} is not in the stream list yet`);
        return null;
      }
      this.remember(key, m);
      return this.seen.get(messageId) ?? null;
    } catch (err) {
      console.error(`[zulip-mcp] could not resolve message ${messageId} for ${purpose}:`, (err as Error).message);
      if (quoteLookup) throw err;
      return null;
    }
  }

  private async resolveQuoteTarget(id: number, remainingMs: number): Promise<QuoteLookup> {
    if (!ZulipAdapter.takeQuoteToken()) return 'budget-skipped';
    try {
      const quoted = await this.locate(id, 'a quote reply', remainingMs, true);
      if (!quoted) this.rememberFailedQuoteLookup(id);
      return quoted;
    } catch {
      return 'budget-skipped';
    }
  }

  private async classifyQuoteReply(
    m: Pick<ZulipMessage, 'mentioned' | 'isDm' | 'authorId' | 'authorEmail' | 'rawContent'>,
    resolve = async (id: number, remainingMs: number): Promise<QuoteLookup> => {
      const known = this.seen.get(id);
      if (known) return known;
      if (this.quoteLookupFailed(id)) return null;
      if (remainingMs <= 0) return 'budget-skipped';
      return this.resolveQuoteTarget(id, remainingMs);
    },
    deadline = performance.now() + QUOTE_TIMEOUT_MS,
  ): Promise<QuoteVerdict> {
    const targets = quotedMessageIds(m.rawContent);
    if (targets.size === 0) return 'not-reply';
    if (m.mentioned || m.isDm || this.identity.selfUserId === null || m.authorId === this.identity.selfUserId || looksLikeBotEmail(m.authorEmail)) return 'ineligible';
    let verdict: QuoteVerdict = 'not-reply';
    for (const id of targets) {
      const quoted = await resolve(id, deadline - performance.now());
      if (quoted === 'budget-skipped') verdict = 'budget-skipped';
      else if (quoted === null) {
        if (verdict !== 'budget-skipped') verdict = 'unverified';
      } else if (quoted.authorId === this.identity.selfUserId) return 'reply';
    }
    return verdict;
  }

  private replyToSelfForVerdict(verdict: QuoteVerdict, targets: ReadonlySet<number>, previous: SeenMessage | null | undefined): boolean {
    return verdict === 'reply' ||
      ((verdict === 'budget-skipped' || verdict === 'unverified') && previous?.replyToSelf === true &&
        targets.size === previous.quoteTargets.size && [...targets].every((id) => previous.quoteTargets.has(id)));
  }

  private changeAllowed(where: SeenMessage): boolean {
    const parsed = parseKey(where.key);
    if (parsed.kind === 'dm') {
      if (this.identity.selfUserId !== null && where.authorId === this.identity.selfUserId) {
        return parsed.userIds.some((id) => this.filters.dmAllowed({ id, email: '' }));
      }
      return this.filters.dmAllowed({ id: where.authorId, email: where.authorEmail });
    }
    return this.streamAllowed(parsed.streamId);
  }

  private keyNow(m: ZulipMessage, raw: Pick<ZulipRawMessage, 'display_recipient'>): ChannelKey | null {
    if (m.isDm) return keyOfMessage(m, this.identity.selfUserId);
    if (m.streamId !== null) return streamKey(m.streamId);
    const id = typeof raw.display_recipient === 'string' ? this.directory.idOf(raw.display_recipient) : undefined;
    return id !== undefined ? streamKey(id) : null;
  }

  private changeKeyNow(change: ZulipMessageChange): ChannelKey | null {
    if (change.kind === 'edit') {
      const cached = this.seen.get(change.messageId);
      if (cached) return cached.key;
    } else {
      for (const id of change.messageIds) {
        const cached = this.seen.get(id);
        if (cached) return cached.key;
      }
    }
    return change.streamId !== null ? streamKey(change.streamId) : null;
  }

  private inOrder(current: () => boolean, place: () => Promise<void>): void {
    this.placing++;
    this.placed = this.placed
      .then(() => (current() ? place() : undefined))
      .catch((err) => { console.error('[zulip-mcp] placing an event failed:', (err as Error).message); })
      .finally(() => { this.placing--; });
  }

  private async keyFor(m: ZulipMessage, raw: Pick<ZulipRawMessage, 'display_recipient'>): Promise<ChannelKey | null> {
    if (m.isDm) return keyOfMessage(m, this.identity.selfUserId);
    let streamId = m.streamId;
    if (streamId === null) {
      const name = typeof raw.display_recipient === 'string' ? raw.display_recipient : null;
      if (name !== null && this.directory.idOf(name) === undefined) await this.refreshStreams(`#${name}`);
      streamId = name !== null ? this.directory.idOf(name) ?? null : null;
      if (streamId === null) {
        console.error(`[zulip-mcp] dropping message ${m.id}: it carries no stream id, and no stream is named ${name === null ? '(none)' : `#${name}`}`);
        return null;
      }
    }
    if (!(await this.knownStream(streamId))) {
      console.error(`[zulip-mcp] dropping message ${m.id}: stream ${streamId} is not in the stream list`);
      return null;
    }
    return streamKey(streamId);
  }

  /**
   * A deleted message can't be fetched, so a deletion is described from the
   * cache, or else by the stream the event names alone; a deleted DM the
   * cache never saw can't be placed.
   */
  private async onZulipChange(change: ZulipMessageChange, located?: SeenMessage, only?: readonly number[]): Promise<MessageChangeEvent | null> {
    const self = this.identity.selfUserId;
    if (change.kind === 'edit') {
      // A GET reads the message as it is now: after a cross-stream move, the
      // stream it went to, while the event names the one it came from.
      const known = located ? null : this.seen.get(change.messageId) ?? null;
      const first = located ?? known ?? await this.locate(change.messageId, 'an edit');
      const id = first ? change.messageId : change.messageIds.find((m) => this.seen.has(m)) ?? change.messageId;
      const cached = first ? known : this.seen.get(id) ?? null;
      const where = first ?? cached;
      if (!where) return null;
      const crossStream = change.newStreamId !== null && !where.isDm;
      if (crossStream && !cached && change.streamId === null) {
        console.error(`[zulip-mcp] dropping a move of message ${id}: the event names no origin stream`);
        return null;
      }
      const fromKey = crossStream && change.streamId !== null ? streamKey(change.streamId) : where.key;
      const placed: SeenMessage = { ...where, key: fromKey };
      if (!this.changeAllowed(placed)) return null;
      const raw = id === change.messageId ? change.content : null;
      const flags = id === change.messageId ? change.flags : [];
      const content = raw !== null ? cleanMarkdown(raw) : null;
      const previousContent = raw !== null && change.origContent !== null ? cleanMarkdown(change.origContent) : null;
      const topic = change.topic ?? where.topic;
      const movedToKey = crossStream && (await this.knownStream(Number(change.newStreamId))) ? streamKey(Number(change.newStreamId)) : null;
      const mentioned = content !== null ? flags.includes('mentioned') : (where.mentioned || flags.includes('mentioned'));
      const replyVerdict = raw !== null
        ? await this.classifyQuoteReply({ ...where, rawContent: raw, mentioned })
        : null;
      const quoteTargets = raw !== null ? quotedMessageIds(raw) : where.quoteTargets;
      const replyToSelf = replyVerdict !== null ? this.replyToSelfForVerdict(replyVerdict, quoteTargets, where) : where.replyToSelf;
      this.seen.set(id, {
        ...where,
        topic,
        mentioned,
        replyToSelf,
        quoteTargets,
        ...(content !== null ? { snippet: snippetOf(content) } : {}),
        ...(movedToKey !== null ? { key: movedToKey } : {}),
      });
      if (change.topic !== null || movedToKey !== null) {
        for (const other of change.messageIds) {
          if (other === id) continue;
          const co = this.seen.get(other);
          if (co) this.seen.set(other, { ...co, topic, ...(movedToKey !== null ? { key: movedToKey } : {}) });
        }
      }
      if (crossStream && movedToKey === null) {
        const now = Date.now();
        for (const [old, at] of this.movedAway) if (now - at > MOVED_AWAY_TTL_MS) this.movedAway.delete(old);
        for (const moved of change.messageIds) this.movedAway.set(moved, now);
      } else if (movedToKey !== null) {
        for (const moved of change.messageIds) this.movedAway.delete(moved);
      }
      return {
        kind: content !== null ? 'edit' : 'move',
        key: fromKey,
        messageId: String(id),
        messageIds: change.messageIds.map(String),
        authorId: String(where.authorId),
        authorName: where.authorName,
        authorEmail: where.authorEmail,
        actorId: change.actorId !== null ? String(change.actorId) : null,
        topic,
        previousTopic: change.topic !== null ? change.origTopic : null,
        movedToKey,
        leftChannel: crossStream,
        byBot: self !== null && change.actorId === self,
        content,
        previousContent,
        mentioned,
        replyToSelf,
        ...(replyVerdict !== null ? { replyToSelfVerified: replyVerdict === 'reply' || replyVerdict === 'not-reply' } : {}),
        previouslyReplyToSelf: cached ? cached.replyToSelf : null,
        // The cache holds the pre-change verdict; a GET only the post-change one.
        previouslyMentioned: cached ? cached.mentioned : null,
        vanished: false,
        isDM: where.isDm,
        onOwnMessage: self !== null && where.authorId === self,
        timestamp: new Date(change.editedAt * 1000),
      };
    }

    // The bot's own deletions (rollback, the delete tool) echo back without
    // an actor: drop what this adapter deleted itself.
    const ids = change.messageIds.filter((id) => (only === undefined || only.includes(id)) && !this.wasSelfDeleted(id));
    if (ids.length === 0) return null;
    const placedId = ids.find((id) => this.seen.has(id)) ?? ids[0];
    const cached = this.seen.get(placedId) ?? null;
    const now = Date.now();
    const vanished = ids.some((id) => {
      const at = this.movedAway.get(id);
      return at !== undefined && now - at <= MOVED_AWAY_TTL_MS;
    });
    for (const id of ids) { this.seen.delete(id); this.movedAway.delete(id); }
    if (cached) {
      if (!this.changeAllowed(cached)) return null;
      return {
        kind: 'delete',
        key: cached.key,
        messageId: String(placedId),
        messageIds: ids.map(String),
        authorId: String(cached.authorId),
        authorName: cached.authorName,
        authorEmail: cached.authorEmail,
        actorId: null,
        topic: change.topic ?? cached.topic,
        previousTopic: null,
        movedToKey: null,
        leftChannel: false,
        byBot: false,
        content: null,
        previousContent: cached.snippet,
        mentioned: cached.mentioned,
        replyToSelf: cached.replyToSelf,
        previouslyReplyToSelf: cached.replyToSelf,
        previouslyMentioned: cached.mentioned,
        vanished,
        isDM: cached.isDm,
        onOwnMessage: self !== null && cached.authorId === self,
        timestamp: new Date(),
      };
    }
    if (change.messageType === 'stream' && change.streamId !== null) {
      if (!(await this.knownStream(change.streamId)) || !this.streamAllowed(change.streamId)) return null;
      return {
        kind: 'delete',
        key: streamKey(change.streamId),
        messageId: String(placedId),
        messageIds: ids.map(String),
        authorId: null,
        authorName: null,
        authorEmail: null,
        actorId: null,
        topic: change.topic ?? '',
        previousTopic: null,
        movedToKey: null,
        leftChannel: false,
        byBot: false,
        content: null,
        previousContent: null,
        mentioned: false,
        previouslyMentioned: null,
        vanished,
        isDM: false,
        onOwnMessage: false,
        timestamp: new Date(),
      };
    }
    return null;
  }

  private describeDm(m: ZulipMessage): ChannelDescriptor {
    const counterparts = dmCounterparts(m.recipients, this.identity.selfUserId);
    const descriptor = dmDescriptor(counterparts, this.backscrollDefault);
    this.knownDms.set(descriptor.id, descriptor);
    return descriptor;
  }

  async publish(
    target: ChannelKey | { streamName: string },
    _descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult> {
    const textContent = content
      .filter((c): c is TextContent => c.type === 'text')
      .map(c => c.text)
      .join('\n');
    // Image and audio blocks ride along as uploads, linked after the text
    // (Zulip's own attachment form). A send that fails after them leaves
    // unreferenced uploads, which Zulip garbage-collects.
    const uploader = this.uploader;
    const uploadPolicy = this.uploadPolicy;
    const upload = uploader && uploadPolicy ? () => uploadBlocks(uploader, content, uploadPolicy) : undefined;

    const parsed = typeof target === 'string' ? parseKey(target) : null;
    let destination: Destination;
    let channelId: string;
    let what: string;
    if (parsed?.kind === 'dm') {
      destination = { type: 'private', to: parsed.userIds };
      channelId = dmChannelIdFor(parsed.userIds);
      what = `direct message to ${channelId}`;
    } else {
      const topic =
        (typeof hints?.metadata?.topic === 'string' ? hints.metadata.topic : undefined) ??
        hints?.threadId ??
        'mcpl';
      const streamName = parsed?.kind === 'stream' ? this.directory.nameOf(parsed.streamId) ?? String(parsed.streamId) : (target as { streamName: string }).streamName;
      destination = { type: 'stream', to: parsed?.kind === 'stream' ? parsed.streamId : streamName, topic };
      channelId = zulipChannelId(streamName);
      what = `message to #${streamName}`;
    }

    const posted: number[] = [];
    try {
      const { chunks } = await sendMessage(this.api, destination, textContent, {
        limit: this.maxMessageLength,
        unlinkable: 'as-typed',
        ...(upload ? { upload } : {}),
        onChunk: (chunk) => {
          posted.push(chunk.id);
          this.onSent?.({ messageId: String(chunk.id), channelId, content: chunk.content });
        },
      });
      const messageIds = chunks.map((c) => String(c.id));
      return { delivered: true, messageId: messageIds[messageIds.length - 1], messageIds };
    } catch (err) {
      if (err instanceof NothingToSend) return { delivered: false };
      if (err instanceof SendRefused) throw namePosted(new ZulipRefused(what, err.answer), posted);
      throw err instanceof Error ? namePosted(err, posted) : err;
    }
  }

  async acknowledge(_key: ChannelKey | null, messageId: string, value?: string): Promise<string> {
    const name = (value ?? '').trim().replace(/^:|:$/g, '') || 'eyes';
    await this.api.request('POST', `/messages/${Number(messageId)}/reactions`, `acknowledging message ${messageId}`, { emoji_name: name, reaction_type: 'unicode_emoji' });
    return `:${name}:`;
  }

  async deleteMessage(_channelId: string, messageId: string): Promise<void> {
    const id = Number(messageId);
    this.noteSelfDeleted(id);
    try {
      await this.api.request('DELETE', `/messages/${id}`, `deleting message ${messageId}`);
    } catch (err) {
      if (!mayHaveBeenApplied(err)) this.selfDeleted.delete(id);
      throw err;
    }
  }

  noteSelfDeleted(messageId: number): void {
    if (!Number.isFinite(messageId)) return;
    const now = Date.now();
    for (const [id, at] of this.selfDeleted) {
      if (now - at > SELF_DELETE_TTL_MS) this.selfDeleted.delete(id);
    }
    this.selfDeleted.delete(messageId);
    if (this.selfDeleted.size >= SELF_DELETE_CAP) {
      const oldest = this.selfDeleted.keys().next().value;
      if (oldest !== undefined) this.selfDeleted.delete(oldest);
    }
    this.selfDeleted.set(messageId, now);
  }

  forgetSelfDeleted(messageId: number): void {
    this.selfDeleted.delete(messageId);
  }

  whereSeen(messageId: string): { key: ChannelKey; topic: string } | null {
    const id = Number(messageId);
    const seen = this.seen.get(id);
    if (!seen || seen.isDm || this.movedAway.has(id)) return null;
    return { key: seen.key, topic: seen.topic };
  }

  private wasSelfDeleted(messageId: number): boolean {
    const at = this.selfDeleted.get(messageId);
    if (at === undefined) return false;
    this.selfDeleted.delete(messageId);
    return Date.now() - at <= SELF_DELETE_TTL_MS;
  }

  private async knownStream(streamId: number): Promise<boolean> {
    if (this.directory.nameOf(streamId) !== undefined) return true;
    await this.refreshStreams(`stream ${streamId}`);
    return this.directory.nameOf(streamId) !== undefined;
  }

  async refreshStreams(why = 'a name the directory does not hold', force = false): Promise<void> {
    const now = Date.now();
    if (!force && this.listRead === null && now - (this.listReadFor.get(why) ?? -Infinity) < STREAM_REFRESH_HOLDOFF_MS) return;
    const started = this.listRead === null;
    this.listRead ??= this.readStreamList(why).finally(() => { this.listRead = null; });
    if (!(await this.listRead) || !started) return;
    for (const [what, at] of this.listReadFor) if (now - at >= STREAM_REFRESH_HOLDOFF_MS) this.listReadFor.delete(what);
    this.listReadFor.set(why, now);
  }

  private async readStreamList(why: string): Promise<boolean> {
    try {
      const result = await this.api.request<{ streams?: unknown[] }>(
        'GET', '/streams', 'refreshing the stream list', { include_public: true, include_subscribed: true }, { timeoutMs: LOCATE_TIMEOUT_MS },
      );
      if (!Array.isArray(result.streams)) throw new Error('the answer carried no stream list');
      this.directory.list(result.streams);
      return true;
    } catch (err) {
      console.error(`[zulip-mcp] could not refresh the stream list for ${why}:`, (err as Error).message);
      return false;
    }
  }

  /**
   * Zulip clients stop showing a typing start that is never stopped once the
   * server's expiry period passes, so a missed stop lingers but does not stick.
   */
  async sendTyping(
    key: ChannelKey,
    _descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void> {
    const send = (params: Record<string, unknown>) => this.api.requestAnswer('POST', '/typing', params);
    const parsed = parseKey(key);
    try {
      let result: Awaited<ReturnType<typeof send>>;
      if (parsed.kind === 'dm') {
        result = await send({ type: 'direct', to: parsed.userIds, op });
      } else {
        const topic = typeof metadata?.topic === 'string' ? metadata.topic : 'mcpl';
        result = await send({ type: 'stream', stream_id: parsed.streamId, topic, op });
      }
      if (result.result && result.result !== 'success') {
        console.error(`[zulip-mcp] typing.send(${op}) non-success: ${result.result} ${result.msg ?? ''}`);
      }
    } catch (err) {
      console.error(`[zulip-mcp] typing.send(${op}) failed:`, (err as Error).message);
    }
  }

  private allowed(m: ZulipMessage): boolean {
    if (this.identity.selfUserId !== null && m.authorId === this.identity.selfUserId) return true;
    if (m.isDm) return this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail });
    return m.streamId === null || this.streamAllowed(m.streamId);
  }

  async fetchContext(
    key: ChannelKey,
    _descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<Omit<ContextInjection, 'namespace'> | null> {
    const parsed = parseKey(key);
    if (parsed.kind === 'stream' && !this.streamAllowed(parsed.streamId)) return null;
    const streamName = parsed.kind === 'stream' ? this.directory.nameOf(parsed.streamId) ?? '' : null;
    const page = await fetchHistory(this.api, parsed.kind === 'dm'
      ? { dmUserIds: parsed.userIds, limit: historySize }
      : { streamId: parsed.streamId, limit: historySize });
    const messages = page.messages.filter((m) => this.allowed(m));
    if (messages.length === 0) return null;

    const formatted = messages.map((m) => messageLineHead({
      id: m.id,
      time: Number.isNaN(m.timestamp.getTime()) ? '' : this.formatTime(m.timestamp),
      stream: parsed.kind === 'dm' ? null : m.streamNameShown ?? streamName,
      topic: m.topic,
      author: m.authorName,
      mentioned: m.mentioned,
    }) + m.cleanContent + editedTrailer(m)).join('\n');

    const label = parsed.kind === 'dm' ? `the direct-message conversation ${dmChannelIdFor(parsed.userIds)}` : `Zulip #${streamName}`;
    return {
      position: 'beforeUser',
      content: `Recent messages from ${label}:\n${formatted}`,
    };
  }

  async fetchHistory(key: ChannelKey, query: ChannelHistoryQuery): Promise<ChannelHistoryPage> {
    const parsed = parseKey(key);
    if (parsed.kind === 'stream' && !this.streamAllowed(parsed.streamId)) {
      return { messages: [], scannedThrough: null, reachedNewest: true };
    }
    const page = await fetchHistory(this.api, {
      ...(parsed.kind === 'dm' ? { dmUserIds: parsed.userIds } : { streamId: parsed.streamId }),
      limit: query.limit,
      before: query.beforeMessageId !== undefined ? Number(query.beforeMessageId) : undefined,
      after: query.afterMessageId !== undefined ? Number(query.afterMessageId) : undefined,
      timeoutMs: query.timeoutMs,
    });
    const pageById = new Map(page.messages.map((m) => [m.id, m]));
    let getsLeft = HISTORY_QUOTE_GET_CAP;
    let pageRemainingMs = QUOTE_TIMEOUT_MS;
    const unverifiedMessageIds: string[] = [];
    const resolve = async (id: number, remainingMs: number): Promise<QuoteLookup> => {
      const known = pageById.get(id) ?? this.seen.get(id);
      if (known) return known;
      if (this.quoteLookupFailed(id)) return null;
      remainingMs = Math.min(remainingMs, pageRemainingMs, query.quoteLookupBudget?.remainingMs ?? Infinity);
      if (getsLeft <= 0 || remainingMs <= 0) return 'budget-skipped';
      getsLeft--;
      const started = performance.now();
      try {
        return await this.resolveQuoteTarget(id, remainingMs);
      } finally {
        const waited = Math.max(0, performance.now() - started);
        pageRemainingMs = Math.max(0, pageRemainingMs - waited);
        if (query.quoteLookupBudget) query.quoteLookupBudget.remainingMs = Math.max(0, query.quoteLookupBudget.remainingMs - waited);
      }
    };
    for (const m of page.messages) {
      if (!this.allowed(m)) continue;
      const verdict = await this.classifyQuoteReply(m, resolve, Infinity);
      m.replyToSelf = this.replyToSelfForVerdict(verdict, quotedMessageIds(m.rawContent), this.seen.get(m.id));
      if (verdict === 'budget-skipped') unverifiedMessageIds.push(String(m.id));
    }
    for (const m of page.messages) this.remember(key, m);
    const messages = page.messages
      .filter((m) => this.identity.selfUserId === null || m.authorId !== this.identity.selfUserId)
      .filter((m) => this.allowed(m))
      .map((m) => toIncoming(key, m, this.identity, { backscroll: true }));
    return {
      messages,
      scannedThrough: page.messages.length > 0 ? page.messages[page.messages.length - 1].id : null,
      reachedNewest: page.foundNewest,
      ...(unverifiedMessageIds.length > 0 ? { unverifiedMessageIds } : {}),
    };
  }

  /**
   * Even with `all_public_streams` on the queue, a bot that isn't subscribed
   * gets a public stream's edits and reactions only for messages that
   * mention it, and from a private stream at most the messages that mention
   * it. So opening a channel subscribes the bot. The call answers
   * `already_subscribed` when there is nothing to do.
   */
  async ensureSubscribed(key: ChannelKey): Promise<void> {
    const streamId = streamIdOfKey(key);
    if (streamId === null) return;
    // Zulip's subscribe call takes names only.
    const streamName = this.directory.nameOf(streamId);
    if (streamName === undefined) throw new Error(`stream ${streamId} is not in the stream list`);
    const result = await this.api.request<{ unauthorized?: unknown; subscribed?: Record<string, unknown> }>(
      'POST', '/users/me/subscriptions', `subscribing to #${streamName}`, { subscriptions: [{ name: streamName }] },
    );
    const unauthorized: unknown[] = Array.isArray(result.unauthorized) ? result.unauthorized : [];
    if (unauthorized.includes(streamName)) {
      throw new Error(`not authorized to subscribe to #${streamName} (private stream; the bot must be invited)`);
    }
    const fresh = result.subscribed && Object.keys(result.subscribed).length > 0;
    if (fresh) console.error(`[zulip-mcp] subscribed to #${streamName} (stream ${streamId})`);
  }

  startEvents(
    onMessage: OnIncomingMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnReaction,
    onMessageChange?: OnMessageChange,
  ): void {
    const loop = this.eventLoop = new ZulipEventLoop({ directory: this.directory });
    const current = () => this.eventLoop === loop;
    const changeHandler = onMessageChange
      ? (change: ZulipMessageChange) => {
          const byBot = change.kind === 'edit' && this.identity.selfUserId !== null && change.actorId === this.identity.selfUserId;
          if (byBot && change.topic === null && change.newStreamId === null) return;
          const kind = change.kind === 'delete' ? 'delete' : change.topic !== null || change.newStreamId !== null ? 'move' : 'edit';
          const enter = (key: ChannelKey, located?: SeenMessage) => {
            const entry: ChangeEntry = { key, messageIds: change.messageIds, kind, ready: (only) => this.onZulipChange(change, located, only) };
            this.changeOf.set(entry, change);
            onMessageChange(entry);
          };
          const key = this.changeKeyNow(change);
          if (key !== null && (change.streamId !== null || this.placing === 0)) return enter(key);
          this.inOrder(current, async () => {
            const cachedKey = this.changeKeyNow(change);
            const located = cachedKey === null && change.kind === 'edit' ? await this.locate(change.messageId, 'an edit') : null;
            const placedKey = cachedKey ?? located?.key ?? null;
            if (placedKey !== null && current()) enter(placedKey, located ?? undefined);
          });
        }
      : undefined;
    const reactionHandler = onReaction
      ? (ev: import('./zulip-events.js').ZulipReactionEvent) => {
          if (this.identity.selfUserId !== null && ev.user_id === this.identity.selfUserId) return;
          const at = (where: SeenMessage, timestamp: Date): ReactionEvent => ({
            action: ev.op,
            key: where.key,
            messageId: String(ev.message_id),
            emoji: ev.emoji_name,
            emojiCode: ev.emoji_code || undefined,
            emojiType: ev.reaction_type || undefined,
            reactorId: String(ev.user_id),
            reactorName: ev.user?.full_name ?? `user ${ev.user_id}`,
            onOwnMessage: this.identity.selfUserId !== null && where.authorId === this.identity.selfUserId,
            messageSnippet: where.snippet,
            timestamp,
          });
          const emit = (where: SeenMessage | null) => {
            if (!where || !current() || (where.isDm && where.authorId !== this.identity.selfUserId && !this.changeAllowed(where))) return;
            const arrived = at(where, new Date());
            onReaction({ ...arrived, placed: () => at(this.seen.get(ev.message_id) ?? where, arrived.timestamp) });
          };
          const cached = this.seen.get(ev.message_id);
          if (cached && this.placing === 0) return emit(cached);
          this.inOrder(current, async () => emit(await this.locate(ev.message_id)));
        }
      : undefined;
    loop.start(this.api, (_streamName, msg, flags) => {
      const raw = { ...(msg as ZulipRawMessage), flags };
      const m = normalizeMessage(raw);
      const key = this.keyNow(m, raw);
      if (key !== null && (m.isDm || m.streamId !== null || this.placing === 0)) return this.handOn(raw, m, key, onMessage);
      this.inOrder(current, async () => {
        const placed = await this.keyFor(m, raw);
        if (placed !== null && current()) this.handOn(raw, m, placed, onMessage);
      });
    }, onSystemEvent, reactionHandler, changeHandler).catch(error => {
      console.error('Zulip event loop failed:', error);
    });
  }

  private handOn(raw: ZulipRawMessage, m: ZulipMessage, key: ChannelKey, onMessage: OnIncomingMessage): void {
    this.remember(key, m);
    if (this.identity.selfUserId !== null && m.authorId === this.identity.selfUserId) return;
    if (m.isDm) {
      if (!this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail })) {
        console.error(`[zulip-mcp] dropping DM from ${m.authorEmail} (${m.authorId}): not in the dmUsers allowlist`);
        return;
      }
      const descriptor = this.describeDm(m);
      onMessage({
        key,
        messageId: String(m.id),
        ready: async (changes) => {
          const now = this.applyChanges(raw, m, changes);
          return now === null ? null : { message: toIncoming(key, now, this.identity), newChannel: descriptor };
        },
      });
      return;
    }
    onMessage({
      key,
      messageId: String(m.id),
      ready: async (changes) => {
        const now = this.applyChanges(raw, m, changes);
        return now === null ? null : this.readyStreamMessage(now, key);
      },
    });
  }

  private applyChanges(raw: ZulipRawMessage, m: ZulipMessage, entries: readonly ChangeEntry[] | undefined): ZulipMessage | null {
    if (!entries || entries.length === 0) return m;
    let edited = raw;
    for (const entry of entries) {
      const change = this.changeOf.get(entry);
      if (change?.kind === 'delete') {
        this.seen.delete(m.id);
        this.movedAway.delete(m.id);
        return null;
      }
      if (change?.kind === 'edit' && change.content !== null) edited = { ...edited, content: change.content, flags: change.flags, last_edit_timestamp: change.editedAt };
    }
    if (edited === raw) return m;
    const fresh = normalizeMessage(edited);
    const now: ZulipMessage = {
      ...m,
      rawContent: fresh.rawContent,
      cleanContent: fresh.cleanContent,
      mentioned: fresh.mentioned,
      wildcardMentioned: fresh.wildcardMentioned,
      attachments: fresh.attachments,
      editedAt: fresh.editedAt,
    };
    const cached = this.seen.get(m.id);
    if (cached) this.seen.set(m.id, { ...cached, snippet: snippetOf(now.cleanContent), mentioned: now.mentioned, quoteTargets: quotedMessageIds(now.rawContent) });
    return now;
  }

  private async readyStreamMessage(m: ZulipMessage, key: ChannelKey): Promise<{ message: KeyedMessage; newChannel?: ChannelDescriptor } | null> {
    const streamId = streamIdOfKey(key)!;
    if (!(await this.knownStream(streamId))) {
      console.error(`[zulip-mcp] dropping message ${m.id}: stream ${streamId} is not in the stream list`);
      return null;
    }
    if (!this.streamAllowed(streamId)) return null;
    m.replyToSelf = await this.classifyQuoteReply(m) === 'reply';
    const cached = this.seen.get(m.id);
    if (cached) cached.replyToSelf = m.replyToSelf;
    // A stream the bot joined after startup is unknown to the host, so its
    // messages carry their descriptor (#20).
    const newChannel = this.streamDescriptor(streamId);
    return { message: toIncoming(key, m, this.identity), ...(newChannel ? { newChannel } : {}) };
  }

  stopEvents(): void {
    this.eventLoop?.stop();
    this.eventLoop = null;
  }
}
