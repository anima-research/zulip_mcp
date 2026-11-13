import { ERR_UNKNOWN_CHANNEL } from '@animalabs/mcpl-core';
import type {
  ChannelDescriptor,
  ChannelsChangedParams,
  ChannelsCloseParams,
  ChannelsIncomingResult,
  ChannelsListResult,
  ChannelsPublishParams,
  ChannelsPublishResult,
  ChannelsRegisterResult,
  IncomingChannelMessage,
} from '@animalabs/mcpl-core';
import { toWire, type ChannelKey, type KeyedMessage } from './channel-key.js';
import type { ChannelIds } from './channel-ids.js';
import type { CapabilityGrant } from './grant.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import type { PlatformAdapter, PlatformSystemEvent, RoutingHints } from './platforms/adapter.js';

const DEFAULT_BATCH_WINDOW_MS = 500;

export interface HostClient {
  registerChannels(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult | undefined>;
  channelsChanged(params: ChannelsChangedParams, timeoutMs?: number): Promise<ChannelsRegisterResult | undefined>;
  sendIncoming(messages: IncomingChannelMessage[]): Promise<ChannelsIncomingResult | undefined>;
}

/** Pre-0.5 hosts answered `channels/register` with a flat list of accepted ids. */
type RegisterResultCompat = Partial<ChannelsRegisterResult> & { registered?: string[] };

const MAX_PENDING_ANNOUNCEMENTS = 100;

const MAX_BATCH_ATTEMPTS = 2;

interface Queued {
  message: KeyedMessage;
  attempts: number;
}

export interface RegisterAdditionalResult {
  announced: string[];
  local: string[];
  refused: string[];
  reason?: string;
}

export interface ChannelManagerHooks {
  onDelivered?: (key: ChannelKey, accepted: KeyedMessage[], rejected: KeyedMessage[]) => void;
  deliverable?: (message: KeyedMessage) => boolean;
  onWithheld?: (key: ChannelKey, messages: KeyedMessage[]) => void;
  onGivenUp?: (key: ChannelKey, messages: KeyedMessage[]) => void;
}

export class ChannelManager {
  private allChannels = new Map<string, ChannelDescriptor>();
  private openChannels = new Set<string>();
  private batchBuffer = new Map<ChannelKey, Queued[]>();
  private batchTimer: ReturnType<typeof setTimeout> | null = null;
  private batchWindowMs: number;
  private flushing: Promise<void> | null = null;
  private idleWaiters: (() => void)[] = [];
  private lastIncoming = new Map<ChannelKey, { hints: RoutingHints; from?: number }>();
  private typingTopic = new Map<ChannelKey, string | undefined>();

  /**
   * There is no ungated construction: absence of a capability is denial
   * (§5.4), so a default would be a default-allow.
   */
  constructor(
    private host: HostClient,
    private adapters: Map<string, PlatformAdapter>,
    private grant: CapabilityGrant,
    readonly ids: ChannelIds,
    batchWindowMs?: number,
    private hooks: ChannelManagerHooks = {},
  ) {
    this.batchWindowMs = batchWindowMs ?? DEFAULT_BATCH_WINDOW_MS;
  }

  /**
   * §14.5 authorizes each descriptor independently and the Request form
   * answers itemwise, so a descriptor is recorded only once the host has
   * accepted it.
   */
  async registerChannels(): Promise<void> {
    if (!this.grant.has('channels.register')) {
      console.error('channels.register not granted; skipping channel registration');
      return;
    }

    const channels: ChannelDescriptor[] = [];
    const discovered = new Map<string, ChannelDescriptor>();

    for (const adapter of this.adapters.values()) {
      try {
        for (const descriptor of await adapter.discoverChannels()) {
          channels.push(descriptor);
          discovered.set(descriptor.id, descriptor);
        }
      } catch (error) {
        console.error(`Failed to discover ${adapter.type} channels:`, error);
      }
    }

    if (channels.length === 0) return;

    try {
      const result = await this.host.registerChannels(channels);
      const accepted = this.acceptedIds(result, channels);
      for (const id of accepted) {
        const descriptor = discovered.get(id);
        if (descriptor) this.record(descriptor);
      }
      const rejected = channels.length - accepted.size;
      console.error(
        `Registered ${accepted.size} channels with host` +
          (rejected > 0 ? ` (${rejected} rejected)` : ''),
      );
    } catch (error) {
      console.error('Failed to register channels:', error);
    }
  }

  /**
   * An announcement that never landed (transport error, timeout, a host that
   * treats `channels/changed` as a Notification and answers nothing) is not a
   * refusal: the host may already know the channel from the message that
   * arrived from it (#20). Only an itemized `accepted: false` keeps a
   * descriptor out.
   */
  async registerAdditional(
    descriptors: ChannelDescriptor[],
    opts: { retryBacklog?: boolean; retryRefused?: boolean; timeoutMs?: number } = {},
  ): Promise<RegisterAdditionalResult> {
    const added: ChannelDescriptor[] = [];
    const refusedNow: string[] = [];
    const before = new Map<string, 'confirmed' | 'pending' | 'refused' | 'new'>();
    for (const d of descriptors) {
      const key = this.ids.keyOfDescriptor(d);
      const held = key === null ? undefined : this.ids.heldIdOf(key);
      const holder = this.ids.holderOf(d.id);
      if ((held !== undefined && held !== d.id) || (holder !== undefined && holder !== key)) continue;
      if (this.announcing.has(d.id)) continue;
      if (this.refused.has(d.id)) {
        if (!opts.retryRefused) {
          refusedNow.push(d.id);
          continue;
        }
        before.set(d.id, 'refused');
        added.push(d);
        continue;
      }
      if (this.allChannels.has(d.id) && !this.unannounced.has(d.id)) {
        this.record(d);
        continue;
      }
      before.set(d.id, this.unannounced.has(d.id) ? 'pending' : 'new');
      added.push(d);
    }
    if (opts.retryBacklog) {
      const queued = new Set(added.map((d) => d.id));
      for (const [id, d] of this.unannounced) {
        if (queued.has(id) || this.announcing.has(id)) continue;
        const key = this.ids.keyOfDescriptor(d);
        if (key !== null && this.ids.freshIdOf(key) !== id) continue;
        before.set(id, 'pending');
        added.push(d);
      }
    }
    if (added.length === 0) return { announced: [], local: [], refused: refusedNow };
    if (!this.grant.has('channels.register')) {
      console.error(`channels.register not granted; ${added.length} new channel(s) stay unregistered`);
      return { announced: [], local: [], refused: refusedNow, reason: 'channels.register is not in the grant' };
    }

    // Recorded before the announcement goes out: a host that opens a channel
    // from inside its own `channels/changed` handler would otherwise be told
    // `Unknown channel` for the very descriptor it is processing.
    for (const d of added) {
      this.record(d);
      this.announcing.add(d.id);
    }
    try {
      const result = await this.host.channelsChanged({ added }, opts.timeoutMs);
      const verdicts = this.readVerdicts(result, added);
      const announced: string[] = [];
      const local: string[] = [];
      for (const d of added) {
        const was = before.get(d.id) ?? 'new';
        switch (verdicts.get(d.id)) {
          case 'accepted':
            this.unannounced.delete(d.id);
            this.refused.delete(d.id);
            announced.push(d.id);
            break;
          case 'refused':
            this.unannounced.delete(d.id);
            if (was === 'confirmed') {
              console.error(`host refused ${d.id}, which it had already accepted; keeping it registered`);
              announced.push(d.id);
              break;
            }
            this.refused.add(d.id);
            refusedNow.push(d.id);
            this.unrecord(d.id);
            break;
          default:
            // Not in the itemization at all: a host that answers only for
            // what it changed has said nothing about this one. Silence is
            // not a refusal — and it is not permission either, so a channel
            // that was refused before stays refused.
            if (was === 'refused') {
              this.restoreRefusal(d.id);
              refusedNow.push(d.id);
              break;
            }
            this.queueUnannounced(d);
            local.push(d.id);
        }
      }
      if (refusedNow.length > 0) console.error(`host refused ${refusedNow.length} channel(s): ${refusedNow.join(', ')}`);
      return { announced, local, refused: refusedNow };
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      console.error(`Failed to announce ${added.length} channel(s) (${reason}); they stay usable here and are re-announced on the next refresh`);
      const local: string[] = [];
      for (const d of added) {
        if ((before.get(d.id) ?? 'new') === 'refused') {
          this.restoreRefusal(d.id);
          refusedNow.push(d.id);
          continue;
        }
        this.queueUnannounced(d);
        local.push(d.id);
      }
      return { announced: [], local, refused: refusedNow, reason };
    } finally {
      for (const d of added) this.announcing.delete(d.id);
    }
  }

  async announceChanges(removed: string[], added: ChannelDescriptor[], timeoutMs?: number): Promise<Map<string, 'accepted' | 'refused'> | 'unanswered'> {
    if (!this.grant.has('channels.register')) return 'unanswered';
    if (added.some((d) => this.announcing.has(d.id))) return 'unanswered';
    const addedIds = added.map((d) => d.id);
    const doubtful = this.ids.inDoubt().filter((id) => !addedIds.includes(id));
    const keys = removed.map((id) => this.ids.holderOf(id)).filter((key): key is ChannelKey => key !== undefined);
    const before = added.map((d) => ({ id: d.id, descriptor: this.allChannels.get(d.id), open: this.openChannels.has(d.id) }));
    for (const d of added) {
      this.allChannels.set(d.id, d);
      if (removed.includes(d.id)) this.openChannels.delete(d.id);
      this.announcing.add(d.id);
    }
    try {
      const result = await this.host.channelsChanged({ removed: [...removed, ...doubtful], added }, timeoutMs);
      this.ids.answered(keys, doubtful);
      const verdicts = this.readVerdicts(result, added);
      const out = new Map<string, 'accepted' | 'refused'>();
      for (const d of added) {
        if (verdicts.get(d.id) === 'refused') {
          this.refused.add(d.id);
          this.allChannels.delete(d.id);
          this.openChannels.delete(d.id);
          out.set(d.id, 'refused');
        } else {
          this.refused.delete(d.id);
          out.set(d.id, 'accepted');
        }
      }
      return out;
    } catch (error) {
      console.error(`[channels] changing ${removed.join(', ') || 'nothing'} for ${added.map((d) => d.id).join(', ') || 'nothing'} got no answer (${(error as Error).message}); offered again at the next pass`);
      for (const { id, descriptor, open } of before) {
        if (descriptor) this.allChannels.set(id, descriptor);
        else this.allChannels.delete(id);
        if (open) this.openChannels.add(id);
      }
      this.ids.unanswered(keys, addedIds);
      return 'unanswered';
    } finally {
      for (const d of added) this.announcing.delete(d.id);
    }
  }

  confirmAnnounced(channelId: string): void {
    if (this.unannounced.delete(channelId)) {
      console.error(`[channels] ${channelId} confirmed by the host acting on it; dropping it from the announcement backlog`);
    }
    this.refused.delete(channelId);
  }

  private restoreRefusal(channelId: string): void {
    this.refused.add(channelId);
    this.unannounced.delete(channelId);
    this.unrecord(channelId);
  }

  private record(descriptor: ChannelDescriptor): void {
    this.allChannels.set(descriptor.id, descriptor);
    const key = this.ids.keyOfDescriptor(descriptor);
    if (key !== null) this.ids.noteHeld(key, descriptor.id);
  }

  private unrecord(channelId: string): void {
    this.allChannels.delete(channelId);
    this.openChannels.delete(channelId);
    this.ids.forget(channelId);
  }

  retireId(channelId: string): boolean {
    const wasOpen = this.openChannels.has(channelId);
    this.unannounced.delete(channelId);
    this.refused.delete(channelId);
    this.unrecord(channelId);
    return wasOpen;
  }

  private queueUnannounced(d: ChannelDescriptor): void {
    this.unannounced.delete(d.id);
    this.unannounced.set(d.id, d);
    while (this.unannounced.size > MAX_PENDING_ANNOUNCEMENTS) {
      const oldest = this.unannounced.keys().next().value as string;
      this.unannounced.delete(oldest);
      console.error(`${MAX_PENDING_ANNOUNCEMENTS} channels are waiting to be announced; ${oldest} will not be retried again (it stays usable here)`);
    }
  }

  /** §14.5: an itemized `accepted: false` is a refusal. */
  private readVerdicts(
    result: RegisterResultCompat | undefined,
    submitted: ChannelDescriptor[],
  ): Map<string, 'accepted' | 'refused' | 'unstated'> {
    const verdicts = new Map<string, 'accepted' | 'refused' | 'unstated'>();
    if (result && Array.isArray(result.results)) {
      for (const d of submitted) verdicts.set(d.id, 'unstated');
      for (const r of result.results) {
        if (r && typeof r.id === 'string') verdicts.set(r.id, r.accepted ? 'accepted' : 'refused');
      }
      return verdicts;
    }
    if (result && Array.isArray(result.registered)) {
      const accepted = new Set(result.registered);
      for (const d of submitted) verdicts.set(d.id, accepted.has(d.id) ? 'accepted' : 'unstated');
      return verdicts;
    }
    for (const d of submitted) verdicts.set(d.id, 'accepted');
    return verdicts;
  }

  private readonly unannounced = new Map<string, ChannelDescriptor>();
  private readonly refused = new Set<string>();
  private readonly announcing = new Set<string>();

  private acceptedIds(
    result: RegisterResultCompat | undefined,
    submitted: ChannelDescriptor[],
  ): Set<string> {
    if (result && Array.isArray(result.results)) {
      return new Set(result.results.filter((r) => r?.accepted).map((r) => r.id));
    }
    if (result && Array.isArray(result.registered)) {
      return new Set(result.registered);
    }
    return new Set(submitted.map((d) => d.id));
  }

  /**
   * Requires `channels.lifecycle` (§14.1); a denied capability behaves as if
   * never advertised (§5.4), so this answers with an error rather than acting.
   */
  openChannel(params: { channelId?: string; type: string; address?: unknown }): { channel: ChannelDescriptor } {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    const descriptor = this.findChannel(params);
    this.openChannels.add(descriptor.id);
    return { channel: descriptor };
  }

  findChannel(params: { channelId?: string; type: string; address?: unknown }): ChannelDescriptor {
    if (params.channelId) {
      const descriptor = this.allChannels.get(params.channelId);
      if (!descriptor) {
        throw new McplRpcError(
          ERR_UNKNOWN_CHANNEL,
          `Unknown channel: ${params.channelId} — not visible to the bot, filtered out, or not registered ` +
            `with the host (refresh_channels re-announces what the bot can see)`,
          {
            channelId: params.channelId,
          },
        );
      }
      return descriptor;
    }
    const wanted = isRecord(params.address) ? params.address : null;
    for (const descriptor of this.allChannels.values()) {
      if (descriptor.type !== params.type) continue;
      const have = isRecord(descriptor.address) ? descriptor.address : {};
      if (!wanted || Object.entries(wanted).every(([k, v]) => have[k] === v)) return descriptor;
    }
    throw new McplRpcError(ERR_UNKNOWN_CHANNEL, `No channel found matching type=${params.type}`, {
      type: params.type,
      address: params.address,
    });
  }

  markOpen(channelId: string): void {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    this.openChannels.add(channelId);
  }

  isOpen(channelId: string): boolean {
    return this.openChannels.has(channelId);
  }

  isKeyOpen(key: ChannelKey): boolean {
    const channelId = this.ids.channelIdOf(key);
    return channelId !== null && this.openChannels.has(channelId);
  }

  keyOfHeld(channelId: string): ChannelKey | null {
    const answer = this.ids.resolve(channelId);
    return answer && 'key' in answer ? answer.key : null;
  }

  closeChannel(params: ChannelsCloseParams): { closed: boolean } {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    const existed = this.openChannels.delete(params.channelId);
    return { closed: existed };
  }

  /** §14.1 keys `channels/list` in either direction on `channels.register`. */
  listChannels(): ChannelsListResult {
    if (!this.grant.has('channels.register')) throw capabilityDenied('channels.register');
    return { channels: Array.from(this.allChannels.values()) };
  }

  onIncomingMessage(key: ChannelKey, message: KeyedMessage): void {
    if (!this.isKeyOpen(key)) return;
    this.noteConversation(key, message);
    this.enqueue(key, message);
  }

  noteConversation(key: ChannelKey, message: Pick<KeyedMessage, 'messageId' | 'threadId' | 'metadata'>): boolean {
    const meta = isRecord(message.metadata) ? message.metadata : undefined;
    if (meta?.reaction === true || typeof meta?.change === 'string') return false;
    const id = Number(message.messageId);
    const from = this.lastIncoming.get(key)?.from;
    if (Number.isFinite(id) && from !== undefined && id < from) return false;
    this.lastIncoming.set(key, {
      hints: { threadId: message.threadId, metadata: meta },
      ...(Number.isFinite(id) ? { from: id } : {}),
    });
    return true;
  }

  followMove(key: ChannelKey, messageIds: string[], topic: string): void {
    const last = this.lastIncoming.get(key);
    if (last?.from === undefined || !messageIds.some((id) => Number(id) === last.from)) return;
    this.lastIncoming.set(key, { hints: { threadId: topic || undefined, metadata: { ...last.hints.metadata, topic } }, from: last.from });
  }

  hintsOf(key: ChannelKey): { from?: number; topic?: string } | undefined {
    const last = this.lastIncoming.get(key);
    return last && { from: last.from, topic: last.hints.threadId };
  }

  broadcastSystemEvent(platformType: string, event: PlatformSystemEvent): void {
    const prefix = `${platformType}:`;
    const targets = Array.from(this.openChannels).filter(id => id.startsWith(prefix));

    if (targets.length === 0) {
      console.error(`[system:${platformType}] ${event.kind}: ${event.text} (no open channels to notify)`);
      return;
    }

    const timestamp = new Date().toISOString();
    for (const channelId of targets) {
      const key = this.keyOfHeld(channelId);
      if (key !== null) this.systemEventTo(key, platformType, event, timestamp);
    }
  }

  systemEventTo(key: ChannelKey, platformType: string, event: PlatformSystemEvent, timestamp = new Date().toISOString()): void {
    if (!this.isKeyOpen(key)) return;
    this.enqueue(key, {
      key,
      messageId: `system:${platformType}:${event.kind}:${Date.now()}`,
      author: { id: 'system', name: `${platformType} connection` },
      timestamp,
      content: [{ type: 'text', text: event.text }],
      metadata: { ...event.metadata, system: true, kind: event.kind },
    });
  }

  notifyChannel(key: ChannelKey, text: string, metadata: Record<string, unknown>): void {
    if (!this.isKeyOpen(key)) return;
    this.enqueue(key, {
      key,
      messageId: `system:${String(metadata.kind ?? 'notice')}:${Date.now()}`,
      author: { id: 'system', name: 'zulip connection' },
      timestamp: new Date().toISOString(),
      content: [{ type: 'text', text }],
      metadata: { ...metadata, system: true },
    });
  }

  async publish(params: ChannelsPublishParams): Promise<ChannelsPublishResult> {
    if (!this.grant.has('channels.publish')) throw capabilityDenied('channels.publish');

    const channelId = params.channelId;
    const adapter = this.adapterFor(channelId);
    if (!adapter) {
      throw new McplRpcError(ERR_UNKNOWN_CHANNEL, `Unknown channel format: ${channelId}`, { channelId });
    }
    const target = await this.resolveTarget(channelId);
    if (typeof target !== 'string') return adapter.publish(target, undefined, params.content, undefined);
    return adapter.publish(
      target,
      this.allChannels.get(this.ids.channelIdOf(target) ?? channelId),
      params.content,
      this.lastIncoming.get(target)?.hints,
    );
  }

  private async resolveTarget(channelId: string): Promise<ChannelKey | { streamName: string }> {
    const answer = await this.ids.keyOf(channelId);
    if ('refusal' in answer) throw new McplRpcError(ERR_UNKNOWN_CHANNEL, answer.refusal, { channelId });
    return 'key' in answer ? answer.key : { streamName: answer.unknownName };
  }

  async sendTyping(
    channelId: string,
    metadata?: Record<string, unknown>,
    op: 'start' | 'stop' = 'start',
  ): Promise<void> {
    if (!this.grant.has('channels.typing')) throw capabilityDenied('channels.typing');
    const adapter = this.adapterFor(channelId);
    if (!adapter?.sendTyping) return;
    const key = await this.resolveTarget(channelId);
    if (typeof key !== 'string') {
      console.error(`[zulip-mcp] typing on ${channelId}: no Zulip stream is named #${key.streamName}, so nothing was sent`);
      return;
    }
    const descriptor = this.allChannels.get(this.ids.channelIdOf(key) ?? channelId);
    // The host may send typing refreshes and the stop with the channel alone,
    // so a run is stopped in the topic it started in.
    const hostTopic = metadata?.topic;
    const named = typeof hostTopic === 'string';
    const at = (topic: string | undefined) => (topic === undefined ? metadata : { ...metadata, topic });
    const current = named ? hostTopic : this.lastIncoming.get(key)?.hints.threadId;
    const running = this.typingTopic.has(key);
    const started = this.typingTopic.get(key);
    if (op === 'stop') {
      this.typingTopic.delete(key);
      if (running && (!named || started !== current)) await adapter.sendTyping(key, descriptor, at(started), 'stop');
      if (named || !running) await adapter.sendTyping(key, descriptor, at(current), 'stop');
      return;
    }
    if (running && started !== current) await adapter.sendTyping(key, descriptor, at(started), 'stop');
    this.typingTopic.set(key, current);
    await adapter.sendTyping(key, descriptor, at(current), 'start');
  }

  getOpenChannels(): Set<string> {
    return this.openChannels;
  }

  getChannel(id: string): ChannelDescriptor | undefined {
    return this.allChannels.get(id);
  }

  adapterFor(channelId: string): PlatformAdapter | undefined {
    const prefix = channelId.split(':', 1)[0];
    return this.adapters.get(prefix);
  }

  firstAdapterType(): string {
    const first = this.adapters.values().next().value as PlatformAdapter | undefined;
    return first?.type ?? 'unknown';
  }

  async flush(): Promise<void> {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    while (this.flushing) await this.flushing;
    const flushing: Promise<void> = this.flushBatch().finally(() => {
      if (this.flushing === flushing) this.flushing = null;
      this.wakeIdleWaiters();
    });
    this.flushing = flushing;
    await flushing;
  }

  isIdle(): boolean {
    return this.batchTimer === null && this.flushing === null;
  }

  idle(): Promise<void> {
    if (this.isIdle()) return Promise.resolve();
    return new Promise((resolve) => this.idleWaiters.push(resolve));
  }

  pendingCount(): number {
    let n = 0;
    for (const q of this.batchBuffer.values()) n += q.length;
    return n;
  }

  destroy(): void {
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }
    this.wakeIdleWaiters();
  }

  reset(): void {
    this.destroy();
    this.allChannels.clear();
    this.openChannels.clear();
    this.batchBuffer.clear();
    this.lastIncoming.clear();
    this.typingTopic.clear();
    this.unannounced.clear();
    this.refused.clear();
    this.announcing.clear();
    this.ids.reset();
  }

  private enqueue(key: ChannelKey, message: KeyedMessage, attempts = 0): void {
    let buffer = this.batchBuffer.get(key);
    if (!buffer) {
      buffer = [];
      this.batchBuffer.set(key, buffer);
    }
    buffer.push({ message, attempts });

    this.scheduleBatchFlush();
  }

  private scheduleBatchFlush(): void {
    if (this.batchTimer) return;
    this.batchTimer = setTimeout(() => {
      this.batchTimer = null;
      if (this.flushing) {
        this.scheduleBatchFlush();
        return;
      }
      this.flushing = this.flushBatch().finally(() => {
        this.flushing = null;
        this.wakeIdleWaiters();
      });
    }, this.batchWindowMs);
  }

  private wakeIdleWaiters(): void {
    if (this.idleWaiters.length === 0 || !this.isIdle()) return;
    const waiters = this.idleWaiters;
    this.idleWaiters = [];
    for (const resolve of waiters) resolve();
  }

  private async flushBatch(): Promise<void> {
    const taken = new Map(this.batchBuffer);
    this.batchBuffer.clear();
    let queued: Queued[] = [];
    for (const items of taken.values()) queued.push(...items);
    if (queued.length === 0) return;

    const wire = new Map<KeyedMessage, IncomingChannelMessage>();
    const withheld: KeyedMessage[] = [];
    queued = queued.filter((q) => {
      const channelId = this.ids.channelIdOf(q.message.key);
      if (channelId === null || !this.openChannels.has(channelId) || (this.hooks.deliverable && !this.hooks.deliverable(q.message))) {
        withheld.push(q.message);
        return false;
      }
      wire.set(q.message, toWire(q.message, channelId));
      return true;
    });
    for (const [key, messages] of groupByKey(withheld)) this.report(this.hooks.onWithheld, key, messages);
    if (queued.length === 0) return;
    const allMessages = queued.map((q) => q.message);

    if (!this.grant.has('channels.incoming')) {
      console.error(
        `channels.incoming not granted; dropping ${allMessages.length} inbound message(s)`,
      );
      for (const [key, messages] of groupByKey(allMessages)) this.report(this.hooks.onWithheld, key, messages);
      return;
    }

    let result: ChannelsIncomingResult | undefined;
    try {
      result = await this.host.sendIncoming(allMessages.map((m) => wire.get(m)!));
    } catch (error) {
      const retry = queued.filter((q) => q.attempts + 1 < MAX_BATCH_ATTEMPTS);
      const givenUp = queued.filter((q) => q.attempts + 1 >= MAX_BATCH_ATTEMPTS).map((q) => q.message);
      console.error(
        `Failed to send ${allMessages.length} incoming message(s) to host` +
          (retry.length > 0 ? `; retrying ${retry.length}` : '') +
          (givenUp.length > 0 ? `; giving up on ${givenUp.length} (held below the watermark for a later replay)` : '') +
          ':',
        (error as Error).message ?? error,
      );
      for (const [key, messages] of groupByKey(givenUp)) this.report(this.hooks.onGivenUp, key, messages);
      const requeued = new Map<ChannelKey, Queued[]>();
      for (const q of retry) {
        const list = requeued.get(q.message.key) ?? [];
        list.push({ message: q.message, attempts: q.attempts + 1 });
        requeued.set(q.message.key, list);
      }
      for (const [key, later] of this.batchBuffer) {
        const list = requeued.get(key) ?? [];
        list.push(...later);
        requeued.set(key, list);
      }
      this.batchBuffer = requeued;
      if (retry.length > 0) this.scheduleBatchFlush();
      return;
    }

    const accepted = this.acceptedMessageIds(result, allMessages);
    const byKey = new Map<ChannelKey, { accepted: KeyedMessage[]; rejected: KeyedMessage[] }>();
    let rejected = 0;
    for (const m of allMessages) {
      const entry = byKey.get(m.key) ?? { accepted: [], rejected: [] };
      if (accepted.has(m.messageId)) entry.accepted.push(m);
      else {
        entry.rejected.push(m);
        rejected++;
      }
      byKey.set(m.key, entry);
    }
    if (rejected > 0) {
      console.error(`Host rejected ${rejected} of ${allMessages.length} incoming message(s); they are not delivered`);
    }
    for (const [key, entry] of byKey) {
      try {
        this.hooks.onDelivered?.(key, entry.accepted, entry.rejected);
      } catch (error) {
        console.error('onDelivered hook failed:', (error as Error).message ?? error);
      }
    }
  }

  private report(
    hook: ((key: ChannelKey, messages: KeyedMessage[]) => void) | undefined,
    key: ChannelKey,
    messages: KeyedMessage[],
  ): void {
    if (!hook || messages.length === 0) return;
    try {
      hook(key, messages);
    } catch (error) {
      console.error('channel manager hook failed:', (error as Error).message ?? error);
    }
  }

  /** Itemized `channels/incoming` result (§14.5); a host that does not
   *  itemize has accepted the batch as a whole. */
  private acceptedMessageIds(
    result: ChannelsIncomingResult | undefined,
    sent: KeyedMessage[],
  ): Set<string> {
    if (result && Array.isArray(result.results)) {
      return new Set(result.results.filter((r) => r?.accepted).map((r) => r.messageId));
    }
    return new Set(sent.map((m) => m.messageId));
  }
}

function groupByKey(messages: KeyedMessage[]): Map<ChannelKey, KeyedMessage[]> {
  const out = new Map<ChannelKey, KeyedMessage[]>();
  for (const m of messages) {
    const list = out.get(m.key) ?? [];
    list.push(m);
    out.set(m.key, list);
  }
  return out;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}
