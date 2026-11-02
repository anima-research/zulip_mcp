/**
 * ZulipAdapter — Zulip implementation of PlatformAdapter.
 *
 * Channel ids:
 *   zulip:{stream_name}      a stream; threads map to topics (incoming carries
 *                            the topic as threadId; publishes route to the
 *                            topic of the newest message
 *                            ChannelManager.noteConversation took there,
 *                            else 'mcpl')
 *   zulip:dm:{ids}           a direct-message conversation, keyed by the
 *                            other parties' sorted user ids (see history.ts)
 *
 * DM conversations are discovered from recent DM history at startup and
 * described on the fly when a message from a new conversation arrives.
 */

import type {
  ChannelDescriptor,
  ContentBlock,
  ContextInjection,
  IncomingChannelMessage,
  TextContent,
} from '@animalabs/mcpl-core';
import type {
  ChannelHistoryPage,
  ChannelHistoryQuery,
  MessageChangeEvent,
  OnIncomingMessage,
  OnMessageChange,
  OnReaction,
  OnSystemEvent,
  PlatformAdapter,
  PublishResult,
  RoutingHints,
} from './adapter.js';
import { ZulipEventLoop, type ZulipMessageChange } from './zulip-events.js';
import { chunkMessage, cleanMarkdown } from '../content.js';
import { messageLineHead } from '../message-line.js';
import { agentLineTimeFormatter } from '../timezone.js';
import { uploadBlocks, withAttachmentLinks, type UploadPolicy, type Uploader } from '../uploads.js';
import {
  assertApiSuccess,
  channelIdOf,
  dmCounterparts,
  dmDescriptor,
  editedTrailer,
  fetchHistory,
  normalizeMessage,
  looksLikeBotEmail,
  parseDmChannelId,
  toIncoming,
  type ZulipIdentity,
  type ZulipMessage,
  type ZulipRawMessage,
} from '../history.js';

/** The address every stream descriptor carries. */
export interface ZulipChannelAddress {
  stream_name: string;
  /** Absent only when a stream was described from an event that carried no
   *  id; the name addresses every send, and typing degrades to a no-op. */
  stream_id?: number;
}

export function zulipChannelId(streamName: string): string {
  return `zulip:${streamName}`;
}

/** The stream behind a `zulip:{stream_name}` channel id. */
export function streamNameOf(channelId: string): string {
  return channelId.slice('zulip:'.length);
}

const SNIPPET_MAX = 80;

/** One line of a message body, whitespace collapsed, capped; null when empty. */
export function snippetOf(text: string): string | null {
  const one = text.replace(/\s+/g, ' ').trim();
  if (!one) return null;
  return one.length > SNIPPET_MAX ? `${one.slice(0, SNIPPET_MAX - 1)}…` : one;
}

function isDmChannelIdLocal(channelId: string): boolean {
  return parseDmChannelId(channelId) !== null;
}

function addressOf(descriptor: ChannelDescriptor | undefined): Partial<ZulipChannelAddress> {
  const address = descriptor?.address;
  return typeof address === 'object' && address !== null ? (address as Partial<ZulipChannelAddress>) : {};
}

/** Only a link immediately heading a quote fence is a reply target.
 *  Ignore fenced examples; keep only the first two distinct targets. */
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
type QuoteVerdict = 'reply' | 'not-reply' | 'unverified' | 'budget-skipped';

/** A recently seen message, as the cache places reactions and changes. */
interface SeenMessage {
  channelId: string;
  authorId: number;
  authorName: string;
  authorEmail: string;
  isDm: boolean;
  topic: string;
  snippet: string | null;
  /** Zulip's mention verdict. */
  mentioned: boolean;
  replyToSelf: boolean;
}

/** How long a GET placing a change may take before the change is dropped
 *  rather than wedging every change behind it. */
const LOCATE_TIMEOUT_MS = 15_000;
/** Wait budget for one incoming stream message's quote targets. */
const QUOTE_TIMEOUT_MS = 3_000;
/** Maximum uncached quote-target GETs shared by a history page. */
const HISTORY_QUOTE_GET_CAP = 8;
/** Avoid repeatedly GETting inaccessible/deleted targets; retry after three minutes. */
const FAILED_LOOKUP_TTL_MS = 3 * 60_000;
/** How long the bot's own deletion is remembered so its echo is dropped. */
const SELF_DELETE_TTL_MS = 10 * 60_000;
const SELF_DELETE_CAP = 500;
/** How long a stream id that the list did not resolve is not asked about again. */
const STREAM_REFRESH_HOLDOFF_MS = 60_000;
/** How long "moved to a stream the bot cannot see" explains a following delete event. */
const MOVED_AWAY_TTL_MS = 60_000;

class LookupTimeoutError extends Error {}

/** Bound the wait only: zulip-js does not pass an AbortSignal to fetch, so
 *  the underlying request can outlive this timer. */
function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new LookupTimeoutError(`${what} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => { if (timer) clearTimeout(timer); });
}

/** The live filters the adapter consults — the plane, or a stand-in. */
export interface FilterView {
  streamAllowed(streamName: string): boolean;
  dmAllowed(sender: { id: number; email: string }): boolean;
}

const ALLOW_ALL: FilterView = { streamAllowed: () => true, dmAllowed: () => true };

export interface ZulipAdapterOptions {
  /** Backscroll cap advertised per channel and enforced on channels/open. */
  backscrollDefault?: number;
  /** Per-stream overrides of the backscroll cap. */
  backscrollLimits?: ReadonlyMap<string, number>;
  /** Stream allowlist + DM allowlist, read live on every event. */
  filters?: FilterView;
  /** How many recent DMs to scan for conversations at startup. */
  dmDiscoveryLimit?: number;
  /** The realm's max message length; longer publishes are split. */
  maxMessageLength?: number;
  /** Uploads for the media blocks of a publish (image/audio with inline data). Unset: such blocks are dropped. */
  uploader?: Uploader;
  /** Size and count limits for those uploads. Required with `uploader`. */
  uploadPolicy?: UploadPolicy;
  /** Line time for the recent history injected before inference (default: the agent line time from env). */
  formatTime?: (d: Date) => string;
}

export const DEFAULT_BACKSCROLL = 500;
const DEFAULT_DM_DISCOVERY_LIMIT = 300;

export class ZulipAdapter implements PlatformAdapter {
  readonly type = 'zulip';

  private eventLoop: ZulipEventLoop | null = null;
  private readonly identity: ZulipIdentity;
  private readonly backscrollDefault: number;
  private readonly backscrollLimits: ReadonlyMap<string, number>;
  private readonly filters: FilterView;
  private readonly dmDiscoveryLimit: number;
  private readonly maxMessageLength: number | undefined;
  private readonly uploader: Uploader | null;
  private readonly uploadPolicy: UploadPolicy | null;
  private readonly formatTime: (d: Date) => string;
  /** DM conversations already described to the server, by channel id. */
  private knownDms = new Map<string, ChannelDescriptor>();
  /** Recently seen messages, so a reaction or an edit can be placed without
   *  a round trip: id → channel, author, topic, snippet. Bounded; oldest
   *  evicted. A deletion can only be placed from here — the message is gone. */
  private seen = new Map<number, SeenMessage>();
  private static readonly SEEN_CAP = 2000;
  /** Failed quote-target GETs only; id → retry-after timestamp. Bounded like seen. */
  private failedQuoteLookups = new Map<number, number>();
  /** Stream names by id, from discovery (refreshed on a miss): how a
   *  deletion or a cross-stream move names a stream the cache does not know. */
  private streamNamesById = new Map<number, string>();
  /** Messages this adapter deleted itself (rollback, the delete tool), by
   *  id → when: Zulip's delete event names no actor, so the echo of the
   *  bot's own deletion is recognised here and dropped. Bounded and expiring
   *  — a failed delete never echoes. */
  private selfDeleted = new Map<number, number>();
  /** When the stream list was last refreshed for an unknown id: a stream
   *  the bot cannot see never resolves, and must not cost a refresh per event. */
  private streamsRefreshedAt = 0;
  /** Messages just moved to a stream the bot cannot see, by id → when: the
   *  delete event Zulip sends for lost visibility is a vanishing, not a deletion. */
  private movedAway = new Map<number, number>();

  constructor(
    private zulipClient: any,
    selfUserId: number | null,
    sessionId: string,
    options: ZulipAdapterOptions = {},
  ) {
    this.identity = { selfUserId, sessionId };
    this.backscrollDefault = options.backscrollDefault ?? DEFAULT_BACKSCROLL;
    this.backscrollLimits = options.backscrollLimits ?? new Map();
    this.filters = options.filters ?? ALLOW_ALL;
    this.dmDiscoveryLimit = options.dmDiscoveryLimit ?? DEFAULT_DM_DISCOVERY_LIMIT;
    this.maxMessageLength = options.maxMessageLength;
    this.uploader = options.uploader ?? null;
    this.uploadPolicy = options.uploadPolicy ?? null;
    this.formatTime = options.formatTime ?? agentLineTimeFormatter();
    if (this.uploader && !this.uploadPolicy) throw new Error('ZulipAdapter: uploader needs an uploadPolicy');
  }

  /** The history cap for a stream (descriptor `capabilities.history.maxMessages`). */
  backscrollLimitFor(streamName: string): number {
    return this.backscrollLimits.get(streamName) ?? this.backscrollDefault;
  }

  get selfUserId(): number | null {
    return this.identity.selfUserId;
  }

  async discoverChannels(): Promise<ChannelDescriptor[]> {
    const channels: ChannelDescriptor[] = [];
    try {
      const result = await this.zulipClient.streams.retrieve({
        include_public: true,
        include_subscribed: true,
      });
      const streams = result.streams || [];
      for (const stream of streams) {
        if (typeof stream.stream_id === 'number' && typeof stream.name === 'string') {
          this.streamNamesById.set(stream.stream_id, stream.name);
        }
        if (!this.filters.streamAllowed(stream.name)) continue;
        channels.push(this.streamDescriptor(stream.name, stream.stream_id, {
          subscriber_count: stream.subscriber_count,
          is_public: !stream.invite_only,
        }));
      }
    } catch (error) {
      console.error('Failed to discover Zulip streams:', error);
    }
    channels.push(...(await this.discoverDmChannels()));
    return channels;
  }

  /**
   * The descriptor for a stream. Built from discovery and, for a stream the
   * bot joined after startup, from the first message that arrives on it —
   * both must describe the same channel, so they share this (#20).
   */
  private streamDescriptor(name: string, streamId: number | undefined, metadata: Record<string, unknown> = {}): ChannelDescriptor {
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

  /**
   * Descriptors for the named stream channels, without enumerating the realm.
   * The stream id comes from `get_stream_id` so the descriptor is the same
   * shape discovery builds; a name Zulip does not resolve is omitted, and one
   * outside the allowlist is never described.
   */
  async describeChannels(channelIds: string[]): Promise<ChannelDescriptor[]> {
    const out: ChannelDescriptor[] = [];
    for (const channelId of channelIds) {
      if (isDmChannelIdLocal(channelId)) continue;
      const name = streamNameOf(channelId);
      if (!name || !this.filters.streamAllowed(name)) continue;
      let streamId: number | undefined;
      try {
        const result = await this.zulipClient.streams.getStreamId({ stream: name });
        if (result?.result === 'success' && typeof result.stream_id === 'number') streamId = result.stream_id;
      } catch (error) {
        console.error(`[zulip-mcp] could not resolve the stream id for #${name}:`, (error as Error).message);
      }
      out.push(this.streamDescriptor(name, streamId));
    }
    return out;
  }

  /**
   * DM conversations the bot has been part of recently. Zulip has no
   * "list my DM conversations" call; the recent DM history is the source.
   */
  private async discoverDmChannels(): Promise<ChannelDescriptor[]> {
    if (this.dmDiscoveryLimit <= 0) return [];
    try {
      const result = await this.zulipClient.messages.retrieve({
        anchor: 'newest',
        num_before: this.dmDiscoveryLimit,
        num_after: 0,
        narrow: [['is', 'dm']],
        apply_markdown: false,
        include_anchor: true,
      });
      assertApiSuccess(result, 'recent direct messages');
      for (const raw of (result?.messages ?? []) as ZulipRawMessage[]) {
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

  private remember(channelId: string, m: ZulipMessage): void {
    this.failedQuoteLookups.delete(m.id);
    if (this.seen.size >= ZulipAdapter.SEEN_CAP) {
      const oldest = this.seen.keys().next().value;
      if (oldest !== undefined) this.seen.delete(oldest);
    }
    this.seen.set(m.id, {
      channelId,
      authorId: m.authorId,
      authorName: m.authorName,
      authorEmail: m.authorEmail,
      isDm: m.isDm,
      topic: m.topic,
      snippet: snippetOf(m.cleanContent),
      mentioned: m.mentioned,
      replyToSelf: m.replyToSelf,
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

  /** Where a message lives — from the recent-messages cache, else one GET. */
  private async locate(messageId: number, purpose = 'a reaction', timeoutMs = LOCATE_TIMEOUT_MS, throwOnTimeout = false): Promise<SeenMessage | null> {
    const cached = this.seen.get(messageId);
    if (cached) return cached;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await withTimeout<any>(
        this.zulipClient.messages.getById({ message_id: messageId, apply_markdown: false }),
        timeoutMs,
        `GET message ${messageId}`,
      );
      assertApiSuccess(result, `message ${messageId}`);
      const raw = result?.message as ZulipRawMessage | undefined;
      if (!raw) throw new Error('response contained no message');
      const m = normalizeMessage(raw);
      const channelId = channelIdOf(m, this.identity.selfUserId);
      this.remember(channelId, m);
      return this.seen.get(messageId) ?? null;
    } catch (err) {
      console.error(`[zulip-mcp] could not resolve message ${messageId} for ${purpose}:`, (err as Error).message);
      if (throwOnTimeout && err instanceof LookupTimeoutError) throw err;
      return null;
    }
  }

  /** Cache definite failures, not deadlines: a deferred pass needs a fresh
   *  chance to resolve a target whose earlier request merely timed out. */
  private async resolveQuoteTarget(id: number, remainingMs: number): Promise<QuoteLookup> {
    try {
      const quoted = await this.locate(id, 'a quote reply', remainingMs, true);
      if (!quoted) this.rememberFailedQuoteLookup(id);
      return quoted;
    } catch (err) {
      if (err instanceof LookupTimeoutError) return 'budget-skipped';
      throw err;
    }
  }

  /** Verify at most two quote targets; all readers use the same classifier.
   *  Cached authors remain usable even after the lookup budget is exhausted. */
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
    if (m.mentioned || m.isDm || this.identity.selfUserId === null || m.authorId === this.identity.selfUserId || looksLikeBotEmail(m.authorEmail)) return 'unverified';
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

  /**
   * The allowlists as the live event path applies them to a placed message.
   * A DM is judged by its sender; the bot's own DM message has no sender to
   * judge and is left to the server's per-conversation check.
   */
  private changeAllowed(where: SeenMessage): boolean {
    if (where.isDm) {
      if (this.identity.selfUserId !== null && where.authorId === this.identity.selfUserId) {
        // The bot's own DM message: judged by whom it was sent to.
        const counterparts = parseDmChannelId(where.channelId) ?? [];
        return counterparts.some((id) => this.filters.dmAllowed({ id, email: '' }));
      }
      return this.filters.dmAllowed({ id: where.authorId, email: where.authorEmail });
    }
    return this.filters.streamAllowed(streamNameOf(where.channelId));
  }

  /**
   * An edit, move or deletion from the event queue, placed and cleaned for
   * the server. The bot's own edits are its own doing; a change someone
   * else made to the bot's message is news. A deletion is placed from the
   * cache alone (the message is gone), falling back to the stream the event
   * names — a deleted DM the cache never saw cannot be placed and is dropped.
   */
  private async onZulipChange(change: ZulipMessageChange, emit: OnMessageChange): Promise<void> {
    const self = this.identity.selfUserId;
    if (change.kind === 'edit') {
      if (self !== null && change.actorId === self) return;
      const cached = this.seen.get(change.messageId) ?? null;
      // A cache miss GETs the message as it reads NOW. For a content edit
      // that is the right place; for a cross-stream move it is the stream
      // the message went to, and the agent only ever saw it in the one it
      // came from — which the event names.
      const where = cached ?? await this.locate(change.messageId, 'an edit');
      if (!where) return;
      const crossStream = change.newStreamId !== null && !where.isDm;
      const fromStream = crossStream && change.streamId !== null ? await this.streamNameById(change.streamId) : null;
      // A cross-stream move of an uncached message whose origin cannot be
      // named has nowhere honest to go: the GET only knows the destination.
      if (crossStream && !cached && fromStream === null) {
        console.error(`[zulip-mcp] dropping a move of message ${change.messageId}: origin stream ${change.streamId} unknown`);
        return;
      }
      const fromChannelId = fromStream !== null ? zulipChannelId(fromStream) : where.channelId;
      const placed: SeenMessage = { ...where, channelId: fromChannelId };
      if (!this.changeAllowed(placed)) return;
      const content = change.content !== null ? cleanMarkdown(change.content) : null;
      const previousContent = change.origContent !== null ? cleanMarkdown(change.origContent) : null;
      const topic = change.topic ?? where.topic;
      const movedToStream = crossStream ? await this.streamNameById(Number(change.newStreamId)) : null;
      const movedToChannelId = movedToStream !== null ? zulipChannelId(movedToStream) : null;
      // Zulip's verdict on the new content replaces the cached one only when
      // the content changed; a move keeps the mention it had (the event's
      // flags are not relied on to be present for a move).
      const mentioned = content !== null ? change.flags.includes('mentioned') : (where.mentioned || change.flags.includes('mentioned'));
      const replyVerdict = change.content !== null
        ? await this.classifyQuoteReply({ ...where, rawContent: change.content, mentioned })
        : null;
      const replyToSelf = replyVerdict !== null ? replyVerdict === 'reply' : where.replyToSelf;
      // Keep the cache current: a later reaction or deletion reads the message
      // as it now is. A move re-seats every co-moved message, not only the one
      // acted on.
      this.seen.set(change.messageId, {
        ...where,
        topic,
        mentioned,
        replyToSelf,
        ...(content !== null ? { snippet: snippetOf(content) } : {}),
        ...(movedToChannelId !== null ? { channelId: movedToChannelId } : {}),
      });
      if (change.topic !== null || movedToChannelId !== null) {
        for (const id of change.messageIds) {
          if (id === change.messageId) continue;
          const co = this.seen.get(id);
          if (co) this.seen.set(id, { ...co, topic, ...(movedToChannelId !== null ? { channelId: movedToChannelId } : {}) });
        }
      }
      if (crossStream && movedToStream === null) {
        const now = Date.now();
        for (const [id, at] of this.movedAway) if (now - at > MOVED_AWAY_TTL_MS) this.movedAway.delete(id);
        for (const id of change.messageIds) this.movedAway.set(id, now);
      }
      emit({
        kind: content !== null ? 'edit' : 'move',
        channelId: fromChannelId,
        messageId: String(change.messageId),
        messageIds: change.messageIds.map(String),
        authorId: String(where.authorId),
        authorName: where.authorName,
        authorEmail: where.authorEmail,
        actorId: change.actorId !== null ? String(change.actorId) : null,
        topic,
        previousTopic: change.topic !== null ? change.origTopic : null,
        movedToChannelId,
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
      });
      return;
    }

    // The bot's own deletions (rollback, the delete tool) echo back without
    // an actor: drop what this adapter deleted itself.
    const ids = change.messageIds.filter((id) => !this.wasSelfDeleted(id));
    if (ids.length === 0) return;
    // A bulk deletion is reported under an id the cache can vouch for, so the
    // line never shows one message's id with another's author and quote.
    const placedId = ids.find((id) => this.seen.has(id)) ?? ids[0];
    const cached = this.seen.get(placedId) ?? null;
    const now = Date.now();
    const vanished = ids.some((id) => {
      const at = this.movedAway.get(id);
      return at !== undefined && now - at <= MOVED_AWAY_TTL_MS;
    });
    for (const id of ids) { this.seen.delete(id); this.movedAway.delete(id); }
    let event: MessageChangeEvent | null = null;
    if (cached) {
      if (!this.changeAllowed(cached)) return;
      event = {
        kind: 'delete',
        channelId: cached.channelId,
        messageId: String(placedId),
        messageIds: ids.map(String),
        authorId: String(cached.authorId),
        authorName: cached.authorName,
        authorEmail: cached.authorEmail,
        actorId: null,
        topic: change.topic ?? cached.topic,
        previousTopic: null,
        movedToChannelId: null,
        content: null,
        previousContent: cached.snippet,
        // A deleted mention is the one the agent was about to answer.
        mentioned: cached.mentioned,
        replyToSelf: cached.replyToSelf,
        previouslyReplyToSelf: cached.replyToSelf,
        previouslyMentioned: cached.mentioned,
        vanished,
        isDM: cached.isDm,
        onOwnMessage: self !== null && cached.authorId === self,
        timestamp: new Date(),
      };
    } else if (change.messageType === 'stream' && change.streamId !== null) {
      const streamName = await this.streamNameById(change.streamId);
      if (streamName === null || !this.filters.streamAllowed(streamName)) return;
      event = {
        kind: 'delete',
        channelId: zulipChannelId(streamName),
        messageId: String(placedId),
        messageIds: ids.map(String),
        authorId: null,
        authorName: null,
        authorEmail: null,
        actorId: null,
        topic: change.topic ?? '',
        previousTopic: null,
        movedToChannelId: null,
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
    if (event) emit(event);
  }

  /** The descriptor for a DM's conversation, remembered once described. */
  private describeDm(m: ZulipMessage): { descriptor: ChannelDescriptor; isNew: boolean } {
    const counterparts = dmCounterparts(m.recipients, this.identity.selfUserId);
    const descriptor = dmDescriptor(counterparts, this.backscrollDefault);
    const isNew = !this.knownDms.has(descriptor.id);
    this.knownDms.set(descriptor.id, descriptor);
    return { descriptor, isNew };
  }

  async publish(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult> {
    let textContent = content
      .filter((c): c is TextContent => c.type === 'text')
      .map(c => c.text)
      .join('\n');
    // Image and audio blocks ride along as uploads, linked after the text
    // (Zulip's own attachment form). Uploads happen before the send, so a
    // failed upload fails the publish whole; a send that fails after them
    // leaves unreferenced uploads, which Zulip garbage-collects.
    if (this.uploader && this.uploadPolicy) {
      const uploaded = await uploadBlocks(this.uploader, content, this.uploadPolicy);
      textContent = withAttachmentLinks(textContent, uploaded);
    }
    if (!textContent) return { delivered: false };

    const dmIds = parseDmChannelId(channelId);
    let target: Record<string, unknown>;
    let what: string;
    if (dmIds) {
      target = { type: 'private', to: dmIds };
      what = `direct message to ${channelId}`;
    } else {
      const streamName = streamNameOf(channelId);
      // Route to the topic of the newest message noteConversation took on
      // this channel (in-thread answers); fall back to the 'mcpl' topic when
      // the agent initiates the conversation.
      const topic =
        (typeof hints?.metadata?.topic === 'string' ? hints.metadata.topic : undefined) ??
        hints?.threadId ??
        'mcpl';
      target = { type: 'stream', to: streamName, topic };
      what = `message to #${streamName}`;
    }

    // Zulip rejects anything over the realm's max_message_length; a long
    // reply goes out as several messages rather than failing whole.
    const messageIds: string[] = [];
    for (const chunk of chunkMessage(textContent, this.maxMessageLength)) {
      const result = await this.zulipClient.messages.send({ ...target, content: chunk });
      assertApiSuccess(result, what);
      messageIds.push(String(result.id));
    }
    return { delivered: true, messageId: messageIds[messageIds.length - 1], messageIds };
  }

  /** Mark a message as seen: a reaction, 👀 by default. `value` may be an emoji name (':eyes:' / 'eyes'). */
  async acknowledge(_channelId: string, messageId: string, value?: string): Promise<string> {
    const name = (value ?? '').trim().replace(/^:|:$/g, '') || 'eyes';
    const result = await this.zulipClient.reactions.add({ message_id: Number(messageId), emoji_name: name, reaction_type: 'unicode_emoji' });
    assertApiSuccess(result, `acknowledging message ${messageId}`);
    return `:${name}:`;
  }

  async deleteMessage(_channelId: string, messageId: string): Promise<void> {
    const id = Number(messageId);
    this.noteSelfDeleted(id);
    try {
      const result = await this.zulipClient.messages.deleteById({ message_id: id });
      assertApiSuccess(result, `deleting message ${messageId}`);
    } catch (err) {
      this.selfDeleted.delete(id);
      throw err;
    }
  }

  /** A message the bot deleted through another path (the delete tool): its
   *  delete event is the bot's own doing and must not surface. */
  noteSelfDeleted(messageId: number): void {
    if (!Number.isFinite(messageId)) return;
    const now = Date.now();
    for (const [id, at] of this.selfDeleted) {
      if (now - at > SELF_DELETE_TTL_MS) this.selfDeleted.delete(id);
    }
    if (this.selfDeleted.size >= SELF_DELETE_CAP) {
      const oldest = this.selfDeleted.keys().next().value;
      if (oldest !== undefined) this.selfDeleted.delete(oldest);
    }
    this.selfDeleted.set(messageId, now);
  }

  forgetSelfDeleted(messageId: number): void {
    this.selfDeleted.delete(messageId);
  }

  private wasSelfDeleted(messageId: number): boolean {
    const at = this.selfDeleted.get(messageId);
    if (at === undefined) return false;
    this.selfDeleted.delete(messageId);
    return Date.now() - at <= SELF_DELETE_TTL_MS;
  }

  /** A stream's name by id — from discovery, else one refresh of the list
   *  (a stream created after startup). */
  private async streamNameById(streamId: number): Promise<string | null> {
    const known = this.streamNamesById.get(streamId);
    if (known !== undefined) return known;
    if (Date.now() - this.streamsRefreshedAt < STREAM_REFRESH_HOLDOFF_MS) return null;
    this.streamsRefreshedAt = Date.now();
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await withTimeout<any>(
        this.zulipClient.streams.retrieve({ include_public: true, include_subscribed: true }),
        LOCATE_TIMEOUT_MS,
        'stream list refresh',
      );
      for (const stream of (result?.streams ?? []) as { stream_id?: unknown; name?: unknown }[]) {
        if (typeof stream.stream_id === 'number' && typeof stream.name === 'string') {
          this.streamNamesById.set(stream.stream_id, stream.name);
        }
      }
    } catch (err) {
      console.error(`[zulip-mcp] could not refresh the stream list for stream ${streamId}:`, (err as Error).message);
    }
    return this.streamNamesById.get(streamId) ?? null;
  }

  /**
   * Best-effort typing indicator.
   *
   * Stream typing goes to `metadata.topic`. ChannelManager.sendTyping fills
   * it in when the host names none (the topic of the newest message
   * noteConversation took) and stops a run in the topic it started in;
   * falls back to 'mcpl' only when there is still no topic.
   *
   * `op` is passed through: 'start' while the host refreshes (every ~7s
   * during inference), 'stop' when the turn ends. Zulip expires a start that
   * is never stopped on its own, so a missed stop lingers but does not stick.
   *
   * Note: zulip-js's `typing.send` unconditionally dereferences `params.to.length`,
   * so we must pass `to: []` even for the stream form — otherwise the library
   * throws a TypeError before the HTTP request is made. The Zulip server ignores
   * `to` when `type:'stream'` is set.
   */
  async sendTyping(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void> {
    const send = this.zulipClient.typing.send as (p: unknown) => Promise<{ result?: string; msg?: string }>;
    const dmIds = parseDmChannelId(channelId);
    try {
      let result: { result?: string; msg?: string };
      if (dmIds) {
        result = await send({ type: 'direct', to: dmIds, op });
      } else {
        const streamId = addressOf(descriptor).stream_id;
        if (!streamId) {
          console.error(`[zulip-mcp] sendTyping: no stream_id for ${channelId} (descriptor=${descriptor ? 'present' : 'missing'})`);
          return;
        }
        const topic = typeof metadata?.topic === 'string' ? metadata.topic : 'mcpl';
        result = await send({ type: 'stream', stream_id: streamId, topic, op, to: [] });
      }
      if (result?.result && result.result !== 'success') {
        console.error(`[zulip-mcp] typing.send(${op}) non-success: ${result.result} ${result.msg ?? ''}`);
      }
    } catch (err) {
      // Best-effort — swallow errors so typing never breaks the agent.
      console.error(`[zulip-mcp] typing.send(${op}) failed:`, (err as Error).message);
    }
  }

  /**
   * The allowlists, applied to what a history read may hand on: a message
   * from a stream outside `streams`, or a DM from a sender outside `dmUsers`,
   * is withheld here the same way the live event path withholds it — so a
   * narrowed allowlist is enforced on catch-up, gap recovery, backscroll and
   * context injection, not only on live delivery. The bot's own messages
   * pass (they are its own doing); callers decide whether to show them.
   */
  private allowed(m: ZulipMessage): boolean {
    if (this.identity.selfUserId !== null && m.authorId === this.identity.selfUserId) return true;
    if (m.isDm) return this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail });
    return m.streamName === null || this.filters.streamAllowed(m.streamName);
  }

  async fetchContext(
    channelId: string,
    _descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<ContextInjection | null> {
    const dmIds = parseDmChannelId(channelId);
    if (!dmIds && !this.filters.streamAllowed(streamNameOf(channelId))) return null;
    const page = await fetchHistory(this.zulipClient, dmIds
      ? { dmUserIds: dmIds, limit: historySize }
      : { streamName: streamNameOf(channelId), limit: historySize });
    const messages = page.messages.filter((m) => this.allowed(m));
    if (messages.length === 0) return null;

    // The shared line shape, so injected history reads like live delivery
    // and every line carries an id the agent can fetch_around.
    const formatted = messages.map((m) => messageLineHead({
      id: m.id,
      time: Number.isNaN(m.timestamp.getTime()) ? '' : this.formatTime(m.timestamp),
      stream: dmIds ? null : (m.streamName ?? streamNameOf(channelId)),
      topic: m.topic,
      author: m.authorName,
      mentioned: m.mentioned,
    }) + m.cleanContent + editedTrailer(m)).join('\n');

    const label = dmIds ? `the direct-message conversation ${channelId}` : `Zulip #${streamNameOf(channelId)}`;
    return {
      namespace: channelId,
      position: 'beforeUser',
      content: `Recent messages from ${label}:\n${formatted}`,
    };
  }

  /**
   * History as incoming-shaped messages, oldest first, the bot's own
   * messages excluded (they are already in the agent's own record as its
   * turns) and disallowed senders withheld. Marked `backscroll: true` so
   * consumers can tell replayed history from live delivery. The page's
   * `scannedThrough` covers the excluded rows too, so a pager never mistakes
   * a page of nothing but the bot's own messages for the end of history.
   */
  async fetchHistory(channelId: string, query: ChannelHistoryQuery): Promise<ChannelHistoryPage> {
    const dmIds = parseDmChannelId(channelId);
    if (!dmIds && !this.filters.streamAllowed(streamNameOf(channelId))) {
      return { messages: [], scannedThrough: null, reachedNewest: true };
    }
    const page = await fetchHistory(this.zulipClient, {
      ...(dmIds ? { dmUserIds: dmIds } : { streamName: streamNameOf(channelId) }),
      limit: query.limit,
      before: query.beforeMessageId !== undefined ? Number(query.beforeMessageId) : undefined,
      after: query.afterMessageId !== undefined ? Number(query.afterMessageId) : undefined,
    });
    // Page authors take precedence over older cached rows, including targets
    // later in the page and pages larger than the seen cache. Uncached targets
    // share a GET cap and wait budget for this page, never for other channels.
    const pageById = new Map(page.messages.map((m) => [m.id, m]));
    let getsLeft = HISTORY_QUOTE_GET_CAP;
    const deadline = Math.min(performance.now() + QUOTE_TIMEOUT_MS, query.quoteLookupDeadline ?? Infinity);
    const unverifiedMessageIds: string[] = [];
    const resolve = async (id: number, remainingMs: number): Promise<QuoteLookup> => {
      const known = pageById.get(id) ?? this.seen.get(id);
      if (known) return known;
      if (this.quoteLookupFailed(id)) return null;
      if (getsLeft <= 0 || remainingMs <= 0) return 'budget-skipped';
      getsLeft--;
      return this.resolveQuoteTarget(id, remainingMs);
    };
    for (const m of page.messages) {
      if (!this.allowed(m)) continue;
      const verdict = await this.classifyQuoteReply(m, resolve, deadline);
      m.replyToSelf = verdict === 'reply';
      if (verdict === 'budget-skipped') unverifiedMessageIds.push(String(m.id));
    }
    for (const m of page.messages) this.remember(channelId, m);
    const messages = page.messages
      .filter((m) => this.identity.selfUserId === null || m.authorId !== this.identity.selfUserId)
      .filter((m) => this.allowed(m))
      .map((m) => toIncoming(channelId, m, this.identity, { backscroll: true }));
    return {
      messages,
      scannedThrough: page.messages.length > 0 ? page.messages[page.messages.length - 1].id : null,
      reachedNewest: page.foundNewest,
      ...(unverifiedMessageIds.length > 0 ? { unverifiedMessageIds } : {}),
    };
  }

  /**
   * Zulip delivers stream events only to subscribers — even with
   * `all_public_streams` on the queue — so opening a channel must also
   * subscribe the bot, or the host would be listening to silence.
   * Idempotent; subscription persists server-side. DMs need nothing.
   *
   * Asks Zulip every time rather than remembering: the `unlisten` tool
   * removes subscriptions behind this adapter's back, and a remembered
   * "subscribed" would let a later open succeed onto silence. The add call
   * is cheap and answers `already_subscribed` when there is nothing to do.
   *
   * Throws when the subscription did not happen: an API error, a transport
   * failure, or — Zulip's way of refusing a private stream — a `success`
   * result that lists the stream under `unauthorized`. The caller (the open
   * lifecycle) turns that into a failed open rather than a silent one.
   */
  async ensureSubscribed(channelId: string): Promise<void> {
    if (parseDmChannelId(channelId)) return;
    const streamName = streamNameOf(channelId);
    const result = await this.zulipClient.users.me.subscriptions.add({
      subscriptions: [{ name: streamName }],
    });
    assertApiSuccess(result, `subscribing to #${streamName}`);
    const unauthorized: string[] = Array.isArray(result?.unauthorized) ? result.unauthorized : [];
    if (unauthorized.includes(streamName)) {
      throw new Error(`not authorized to subscribe to #${streamName} (private stream; the bot must be invited)`);
    }
    const fresh = result.subscribed && Object.keys(result.subscribed).length > 0;
    if (fresh) console.error(`[zulip-mcp] subscribed to #${streamName} for channel ${channelId}`);
  }

  startEvents(
    onMessage: OnIncomingMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnReaction,
    onMessageChange?: OnMessageChange,
  ): void {
    const loop = this.eventLoop = new ZulipEventLoop();
    // Changes are placed asynchronously (a cache miss costs one GET) but must
    // reach the server in queue order: an edit followed by a deletion of the
    // same message reads backwards otherwise. Every await in the chain is
    // bounded (LOCATE_TIMEOUT_MS), so one hung GET delays, never wedges.
    let changeChain: Promise<void> = Promise.resolve();
    const changeHandler = onMessageChange
      ? (change: ZulipMessageChange) => {
          changeChain = changeChain
            .then(() => {
              if (this.eventLoop !== loop) return;
              return this.onZulipChange(change, (event) => {
                if (this.eventLoop === loop) onMessageChange(event);
              });
            })
            .catch((err) => {
              console.error('[zulip-mcp] message change handling failed:', (err as Error).message);
            });
        }
      : undefined;
    const reactionHandler = onReaction
      ? (ev: import('./zulip-events.js').ZulipReactionEvent) => {
          // The bot's own reactions are its own doing; only others' are news.
          if (this.identity.selfUserId !== null && ev.user_id === this.identity.selfUserId) return;
          void this.locate(ev.message_id).then((where) => {
            if (!where || this.eventLoop !== loop) return;
            if (!isDmChannelIdLocal(where.channelId) && !this.filters.streamAllowed(streamNameOf(where.channelId))) return;
            onReaction({
              action: ev.op,
              channelId: where.channelId,
              messageId: String(ev.message_id),
              emoji: ev.emoji_name,
              emojiCode: ev.emoji_code || undefined,
              emojiType: ev.reaction_type || undefined,
              reactorId: String(ev.user_id),
              reactorName: ev.user?.full_name ?? `user ${ev.user_id}`,
              onOwnMessage: this.identity.selfUserId !== null && where.authorId === this.identity.selfUserId,
              messageSnippet: where.snippet,
              timestamp: new Date(),
            });
          });
        }
      : undefined;
    loop.start(this.zulipClient, async (_streamName, msg, flags) => {
      const m = normalizeMessage({ ...(msg as ZulipRawMessage), flags });
      // Remember our own messages too: reactions to them are the ones that
      // matter most ("someone reacted to your message").
      this.remember(m.isDm ? dmDescriptor(dmCounterparts(m.recipients, this.identity.selfUserId), 0).id : channelIdOf(m, this.identity.selfUserId), m);
      if (this.identity.selfUserId !== null && msg.sender_id === this.identity.selfUserId) return;
      if (m.isDm) {
        if (!this.filters.dmAllowed({ id: m.authorId, email: m.authorEmail })) {
          console.error(`[zulip-mcp] dropping DM from ${m.authorEmail} (${m.authorId}): not in the dmUsers allowlist`);
          return;
        }
        const { descriptor, isNew } = this.describeDm(m);
        onMessage(toIncoming(descriptor.id, m, this.identity), isNew ? descriptor : undefined);
        return;
      }
      if (m.streamName !== null && !this.filters.streamAllowed(m.streamName)) return;
      m.replyToSelf = await this.classifyQuoteReply(m) === 'reply';
      if (this.eventLoop !== loop) return;
      const cached = this.seen.get(m.id);
      if (cached) cached.replyToSelf = m.replyToSelf;
      // A stream the bot joined after startup is unknown to the host; the
      // message proves it is reachable, so it travels with its descriptor
      // (#20). Sent with EVERY message, never suppressed as "already
      // described": the registry that decides what is new is the server's,
      // it is cleared on reconnect, and a descriptor the host refused or
      // never confirmed must be offered again rather than held back by
      // adapter-side memory.
      const channelId = channelIdOf(m, this.identity.selfUserId);
      const streamId = typeof msg.stream_id === 'number' ? msg.stream_id : undefined;
      // Remembered so a later reaction or edit on this stream can be placed.
      if (m.streamName !== null && streamId !== undefined) this.streamNamesById.set(streamId, m.streamName);
      const descriptor = m.streamName !== null ? this.streamDescriptor(m.streamName, streamId) : undefined;
      onMessage(toIncoming(channelId, m, this.identity), descriptor);
    }, onSystemEvent, reactionHandler, changeHandler).catch(error => {
      console.error('Zulip event loop failed:', error);
    });
  }

  stopEvents(): void {
    this.eventLoop?.stop();
    this.eventLoop = null;
  }
}
