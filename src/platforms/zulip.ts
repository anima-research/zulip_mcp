
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
  parseDmChannelId,
  toIncoming,
  type ZulipIdentity,
  type ZulipMessage,
  type ZulipRawMessage,
} from '../history.js';

export interface ZulipChannelAddress {
  stream_name: string;
  /** Sends address a stream by name; only typing needs its id. */
  stream_id?: number;
}

export function zulipChannelId(streamName: string): string {
  return `zulip:${streamName}`;
}

export function streamNameOf(channelId: string): string {
  return channelId.slice('zulip:'.length);
}

const SNIPPET_MAX = 80;

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

interface SeenMessage {
  channelId: string;
  authorId: number;
  authorName: string;
  authorEmail: string;
  isDm: boolean;
  topic: string;
  snippet: string | null;
  mentioned: boolean;
}

const LOCATE_TIMEOUT_MS = 15_000;
const SELF_DELETE_TTL_MS = 10 * 60_000;
const SELF_DELETE_CAP = 500;
const STREAM_REFRESH_HOLDOFF_MS = 60_000;
const MOVED_AWAY_TTL_MS = 60_000;

function withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => { if (timer) clearTimeout(timer); });
}

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
  maxMessageLength?: number;
  uploader?: Uploader;
  uploadPolicy?: UploadPolicy;
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
  private knownDms = new Map<string, ChannelDescriptor>();
  private seen = new Map<number, SeenMessage>();
  private static readonly SEEN_CAP = 2000;
  private streamNamesById = new Map<number, string>();
  /** Zulip's delete event names no actor, so the echo of the bot's own
   *  deletion is recognised here and dropped. */
  private selfDeleted = new Map<number, number>();
  private streamsRefreshedAt = 0;
  /** Zulip reports a move to a stream the bot cannot see as a delete event. */
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
    });
  }

  private async locate(messageId: number, purpose = 'a reaction'): Promise<SeenMessage | null> {
    const cached = this.seen.get(messageId);
    if (cached) return cached;
    try {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      const result = await withTimeout<any>(
        this.zulipClient.messages.getById({ message_id: messageId, apply_markdown: false }),
        LOCATE_TIMEOUT_MS,
        `GET message ${messageId}`,
      );
      assertApiSuccess(result, `message ${messageId}`);
      const raw = result?.message as ZulipRawMessage | undefined;
      if (!raw) return null;
      const m = normalizeMessage(raw);
      const channelId = channelIdOf(m, this.identity.selfUserId);
      this.remember(channelId, m);
      return this.seen.get(messageId) ?? null;
    } catch (err) {
      console.error(`[zulip-mcp] could not resolve message ${messageId} for ${purpose}:`, (err as Error).message);
      return null;
    }
  }

  private changeAllowed(where: SeenMessage): boolean {
    if (where.isDm) {
      if (this.identity.selfUserId !== null && where.authorId === this.identity.selfUserId) {
        const counterparts = parseDmChannelId(where.channelId) ?? [];
        return counterparts.some((id) => this.filters.dmAllowed({ id, email: '' }));
      }
      return this.filters.dmAllowed({ id: where.authorId, email: where.authorEmail });
    }
    return this.filters.streamAllowed(streamNameOf(where.channelId));
  }

  /**
   * A deleted message can't be fetched, so a deletion is placed from the
   * cache, falling back to the stream the event names; a deleted DM the cache
   * never saw cannot be placed.
   */
  private async onZulipChange(change: ZulipMessageChange, emit: OnMessageChange): Promise<void> {
    const self = this.identity.selfUserId;
    if (change.kind === 'edit') {
      if (self !== null && change.actorId === self) return;
      const cached = this.seen.get(change.messageId) ?? null;
      // A GET reads the message as it is now: after a cross-stream move, the
      // stream it went to, while the event names the one it came from.
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
      const mentioned = content !== null ? change.flags.includes('mentioned') : (where.mentioned || change.flags.includes('mentioned'));
      this.seen.set(change.messageId, {
        ...where,
        topic,
        mentioned,
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
        mentioned: cached.mentioned,
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
    // (Zulip's own attachment form). A send that fails after them leaves
    // unreferenced uploads, which Zulip garbage-collects.
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
   * Zulip clients stop showing a typing start that is never stopped once the
   * server's expiry period passes, so a missed stop lingers but does not stick.
   *
   * zulip-js's `typing.send` dereferences `params.to.length` unconditionally,
   * so the stream form must pass `to: []` or the library throws a TypeError
   * before the request is made; since Zulip 8.0 the server ignores `to` for
   * `type:'stream'`.
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
      console.error(`[zulip-mcp] typing.send(${op}) failed:`, (err as Error).message);
    }
  }

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
    for (const m of page.messages) this.remember(channelId, m);
    const messages = page.messages
      .filter((m) => this.identity.selfUserId === null || m.authorId !== this.identity.selfUserId)
      .filter((m) => this.allowed(m))
      .map((m) => toIncoming(channelId, m, this.identity, { backscroll: true }));
    return {
      messages,
      scannedThrough: page.messages.length > 0 ? page.messages[page.messages.length - 1].id : null,
      reachedNewest: page.foundNewest,
    };
  }

  /**
   * Zulip sends a private stream's messages only to its subscribers. A public
   * stream's reach an unsubscribed bot's `all_public_streams` queue without
   * their flags, so without `mentioned`, and a guest bot's not at all unless
   * the stream is web-public. A message's edits and reactions go only to those
   * who received it. So opening a channel subscribes the bot. The call answers
   * `already_subscribed` when there is nothing to do.
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
    this.eventLoop = new ZulipEventLoop();
    let changeChain: Promise<void> = Promise.resolve();
    const changeHandler = onMessageChange
      ? (change: ZulipMessageChange) => {
          changeChain = changeChain
            .then(() => this.onZulipChange(change, onMessageChange))
            .catch((err) => {
              console.error('[zulip-mcp] message change handling failed:', (err as Error).message);
            });
        }
      : undefined;
    const reactionHandler = onReaction
      ? (ev: import('./zulip-events.js').ZulipReactionEvent) => {
          if (this.identity.selfUserId !== null && ev.user_id === this.identity.selfUserId) return;
          void this.locate(ev.message_id).then((where) => {
            if (!where) return;
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
    this.eventLoop.start(this.zulipClient, (_streamName, msg, flags) => {
      const m = normalizeMessage({ ...(msg as ZulipRawMessage), flags });
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
      // A stream the bot joined after startup is unknown to the host, so its
      // messages carry their descriptor (#20).
      const channelId = channelIdOf(m, this.identity.selfUserId);
      const streamId = typeof msg.stream_id === 'number' ? msg.stream_id : undefined;
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
