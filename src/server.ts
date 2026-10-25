import {
  ERR_CHANNEL_OPEN_FAILED,
  ERR_UNKNOWN_CHANNEL,
  ManifestTracker,
  McplConnection,
  method,
  type ChannelsChangedParams,
  type ChannelsCloseParams,
  type ChannelsIncomingResult,
  type ChannelsOpenParams,
  type ChannelsOpenResult,
  type ChannelsPublishParams,
  type ChannelsRegisterResult,
  type ChannelDescriptor,
  type ContextBeforeInferenceParams,
  type FeatureSetsUpdateParams,
  type IncomingChannelMessage,
  type InitializeCapabilities,
  type JsonRpcNotification,
  type JsonRpcRequest,
  type McplInitializeParams,
  type McplInitializeResult,
  type PushEventParams,
  type StateRollbackParams,
  type StateRollbackResult,
  type TextContent,
} from '@animalabs/mcpl-core';
import { ChannelManager, type HostClient, type RegisterAdditionalResult } from './channels.js';
import { ContextProvider } from './context.js';
import { DeliveryState, attributeMessage, renderMissedBlock, selectMissed, viewOf, DEFAULT_MISSED_BLOCK_MAX_CHARS } from './delivery.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import { MESSAGING_FEATURE_SET, buildServerCapabilities, featureSetForTool } from './feature-sets.js';
import { isDmChannelId } from './history.js';
import type { FiltersPlane } from './filters.js';
import { buildAttachmentBlocks, type AttachmentSource, type InlineOptions } from './attachments.js';
import type { AttachmentRef } from './content.js';
import { agentLineTimeFormatter } from './timezone.js';
import { CapabilityGrant } from './grant.js';
import { StateTracker } from './state.js';
import type { MessageChangeEvent, PlatformAdapter, PlatformSystemEvent, ReactionEvent } from './platforms/adapter.js';
import { messageLineHead } from './message-line.js';
import { CHAT_TAGS } from '@animalabs/mcpl-core';
import type { ReactionSummary } from './history.js';
import { toolDefinitions } from './tools.js';
import { withToolClasses } from './tool-classes.js';
import { toToolCallResult, type ToolCallResult, type ZulipToolRuntime } from './tool-runtime.js';

/** A host that reconciles before it answers `channels/changed` cannot be
 *  served while an announcement is outstanding, since this server answers
 *  one request at a time. */
const ANNOUNCE_TIMEOUT_MS = 5_000;

const KNOWN_MCP_PROTOCOL_VERSIONS = new Set(['2024-11-05', '2025-03-26', '2025-06-18']);
const FALLBACK_MCP_PROTOCOL_VERSION = '2024-11-05';

const MISSED_VICINITY = 7;
const CATCHUP_HARD_CAP = 10_000;
export const DEFAULT_CATCHUP_LIMIT = 3000;
const HISTORY_ON_OPEN_CAP = 500;
const PRE_LIVE_BUFFER_CAP = 5000;
const SHUTDOWN_INFLIGHT_GRACE_MS = 5000;

export interface ZulipMcplServerOptions {
  serverInfo: { name: string; version: string };
  mcplEnabled?: boolean;
  batchWindowMs?: number;
  contextHistorySize?: number;
  stateDir?: string | null;
  sessionId?: string;
  catchupLimit?: number;
  formatTime?: (d: Date) => string;
  filters?: FiltersPlane;
  attachments?: { source: AttachmentSource; inline: InlineOptions };
  missedBlockMaxChars?: number;
  announceTimeoutMs?: number;
  attributeDelivery?: boolean;
}

export class ZulipMcplServer {
  private conn: McplConnection | null = null;
  private mcplActive = false;

  readonly adapters: Map<string, PlatformAdapter>;
  readonly grant: CapabilityGrant;
  readonly manifestTracker: ManifestTracker;
  readonly channelManager: ChannelManager;
  readonly contextProvider: ContextProvider;
  readonly delivery: DeliveryState;
  readonly stateTracker = new StateTracker();

  private detachManifest: (() => void) | null = null;
  private eventsStarted = false;
  private sweepDone = false;
  private live = false;
  private preLive: { message: IncomingChannelMessage; newChannel?: ChannelDescriptor }[] = [];
  private preLiveDropped = 0;
  private sweepAnchors = new Map<string, number>();
  private inFlight = new Set<Promise<void>>();
  private healing = false;
  private readonly catchupLimit: number;
  private readonly missedBlockMaxChars: number;
  private readonly formatTime: (d: Date) => string;
  private changeSeq = 0;
  private readonly attributeDelivery: boolean;
  private readonly announceTimeoutMs: number;
  private readonly filters: FiltersPlane | null;

  constructor(
    private readonly adapter: PlatformAdapter,
    private readonly tools: ZulipToolRuntime,
    private readonly options: ZulipMcplServerOptions,
  ) {
    this.adapters = new Map([[adapter.type, adapter]]);
    this.catchupLimit = Math.min(CATCHUP_HARD_CAP, Math.max(0, options.catchupLimit ?? DEFAULT_CATCHUP_LIMIT));
    this.missedBlockMaxChars = Math.max(1000, options.missedBlockMaxChars ?? DEFAULT_MISSED_BLOCK_MAX_CHARS);
    this.formatTime = options.formatTime ?? agentLineTimeFormatter();
    this.attributeDelivery = options.attributeDelivery !== false;
    this.announceTimeoutMs = options.announceTimeoutMs ?? ANNOUNCE_TIMEOUT_MS;
    this.filters = options.filters ?? null;
    this.delivery = new DeliveryState(options.stateDir ?? null, options.sessionId ?? 'default');
    this.filters?.onChange((next, prev) => {
      const before = new Set(prev.streams ?? []);
      const widened = !next.streams || (next.streams ?? []).some((name) => prev.streams && !before.has(name));
      if (widened) void this.applyFilterChange();
      void this.healUndelivered();
    });

    const manifest = buildServerCapabilities({ typing: typeof adapter.sendTyping === 'function' });

    // The manifest is what `initialize` presents (§5.1) and what `mcpl/manifest`
    // returns (§17.4).
    this.manifestTracker = new ManifestTracker(manifest);

    // The effective capability grant for this connection (§5.4). It starts
    // empty: until the initial policy exchange completes, every
    // capability-dependent behavior is unavailable (§5.3).
    this.grant = new CapabilityGrant(
      typeof manifest.featureSets === 'object' ? manifest.featureSets : {},
    );

    const host: HostClient = {
      registerChannels: (channels) => this.registerChannelsWithHost(channels),
      channelsChanged: (params, timeoutMs) => this.channelsChangedWithHost(params, timeoutMs),
      sendIncoming: (messages) => this.sendIncomingToHost(messages),
    };
    this.channelManager = new ChannelManager(host, this.adapters, this.grant, options.batchWindowMs, {
      onDelivered: (channelId, accepted, rejected) => this.onDelivered(channelId, accepted, rejected),
      deliverable: (message) => this.mayDeliver(message),
      onWithheld: (channelId, messages) => this.onWithheld(channelId, messages),
      onGivenUp: (channelId, messages) => this.onGivenUp(channelId, messages),
    });
    this.contextProvider = new ContextProvider(this.channelManager, this.grant, options.contextHistorySize, {
      excludeChannel: (channelId) => this.isMuted(channelId) || !this.isAllowed(channelId),
    });
    this.tools.onSent = (sent) => this.stateTracker.recordSent(sent.messageId, sent.channelId, sent.content);
    // A stream `listen` just subscribed the bot to is registered at once, so
    // the host can open it without waiting for a message or a refresh (#20).
    this.tools.onSubscribed = (streams) => this.registerSubscribed(streams);
    // A deletion made through the tool surface echoes back as a delete event
    // without an actor; the adapter recognises its own.
    this.tools.onDeleted = (messageId) => this.adapter.noteSelfDeleted?.(Number(messageId));
    this.tools.onDeleteFailed = (messageId) => this.adapter.forgetSelfDeleted?.(Number(messageId));
  }

  get mcplMode(): boolean {
    return this.mcplActive;
  }

  get isLive(): boolean {
    return this.live;
  }

  async serve(conn: McplConnection): Promise<void> {
    this.conn = conn;
    this.beginConnection();

    try {
      await this.run(conn);
    } catch (err) {
      if ((err as Error).name !== 'ConnectionClosedError') {
        console.error('[zulip-mcp] Connection error:', err);
      }
    }

    await this.quiesce();
    this.detachManifest?.();
    this.detachManifest = null;
    this.conn = null;
  }

  private beginConnection(): void {
    this.grant.reset();
    this.channelManager.reset();
    this.live = false;
    this.sweepDone = false;
    this.preLive = [];
    this.preLiveDropped = 0;
    this.sweepAnchors = new Map(this.delivery.watermarkedChannels().map((id) => [id, this.delivery.watermark(id)!]));
  }

  private async run(conn: McplConnection): Promise<void> {
    const ok = await this.handleInitialize(conn);
    if (!ok) return;

    if (this.mcplActive) {
      // Seeded from the handshake so a fresh connection does not fire a
      // redundant `mcpl/manifestChanged` (§17.10).
      this.detachManifest = this.manifestTracker.attach(conn);

      // §5.3: registration waits for the initial policy exchange, not for a
      // timer. Until `featureSets/update` arrives the grant is empty and
      // `channels.register` is denied, so registering earlier would be acting
      // on a capability nobody has granted yet. A host that never sends it
      // leaves this server inert by design — absence is denial.
      // Runs concurrently with the loop: the policy arrives as a Request the
      // loop must read and answer, and channels/register is itself a
      // server→host Request the host answers only once policy is settled.
      void this.grant.whenReady().then(async () => {
        if (this.conn !== conn) return;
        try {
          await this.channelManager.registerChannels();
        } catch (error) {
          console.error('Failed to register channels after initial policy:', error);
        }
        try {
          await this.runReconnectSweep();
        } catch (error) {
          console.error('[zulip-mcp] Reconnect catch-up sweep failed:', error);
        }
        await this.goLive();
      });

      // The event queue is registered at once — Zulip delivers nothing that
      // predates the queue, so every second of delay here is a second of
      // messages that no sweep can recover for a never-watermarked channel.
      this.startEvents();
    }

    while (!conn.isClosed) {
      const msg = await conn.nextMessage();
      if (msg.type === 'request') {
        await this.handleRequest(conn, msg.request);
      } else {
        await this.handleNotification(conn, msg.notification);
      }
    }
  }

  async shutdown(): Promise<void> {
    await this.quiesce();
  }

  private async quiesce(): Promise<void> {
    if (this.eventsStarted) {
      this.adapter.stopEvents();
      this.eventsStarted = false;
    }
    if (this.inFlight.size > 0) {
      let timer: ReturnType<typeof setTimeout> | undefined;
      const grace = new Promise<void>((resolve) => {
        timer = setTimeout(resolve, SHUTDOWN_INFLIGHT_GRACE_MS);
        (timer as { unref?: () => void }).unref?.();
      });
      await Promise.race([Promise.allSettled([...this.inFlight]), grace]);
      clearTimeout(timer);
      if (this.inFlight.size > 0) {
        console.error(`[zulip-mcp] ${this.inFlight.size} delivery(ies) still in flight at shutdown; left for the next catch-up sweep`);
      }
    }
    try {
      await this.channelManager.flush();
    } catch (error) {
      console.error('[zulip-mcp] final flush failed:', (error as Error).message ?? error);
    }
    this.channelManager.destroy();
    this.delivery.save();
  }

  private async goLive(): Promise<void> {
    if (this.live) return;
    const boundary = new Map(this.delivery.watermarkedChannels().map((id) => [id, this.delivery.watermark(id)!]));
    while (this.preLive.length > 0) {
      const held = this.preLive;
      this.preLive = [];
      for (const { message, newChannel } of held) {
        const watermark = boundary.get(message.channelId);
        const id = Number(message.messageId);
        if (watermark !== undefined && Number.isFinite(id) && id <= watermark) continue;
        try {
          await this.route(message, newChannel);
        } catch (err) {
          console.error('[zulip-mcp] delivery of a held message failed:', (err as Error).message);
        }
      }
    }
    this.live = true;
    if (this.preLiveDropped > 0) {
      console.error(`[zulip-mcp] ${this.preLiveDropped} live message(s) exceeded the pre-live buffer; held below the watermark for replay`);
      this.preLiveDropped = 0;
    }
    void this.healUndelivered();
  }

  private async handleInitialize(conn: McplConnection): Promise<boolean> {
    const msg = await conn.nextMessage();
    if (msg.type !== 'request' || msg.request.method !== method.INITIALIZE) {
      console.error('[zulip-mcp] Expected initialize request, got:', msg);
      conn.close();
      return false;
    }

    const params = msg.request.params as McplInitializeParams | undefined;
    const clientMcpl = params?.capabilities?.experimental?.mcpl;
    this.mcplActive = clientMcpl !== undefined && this.options.mcplEnabled !== false;

    const requested = params?.protocolVersion;
    const protocolVersion =
      typeof requested === 'string' && KNOWN_MCP_PROTOCOL_VERSIONS.has(requested)
        ? requested
        : FALLBACK_MCP_PROTOCOL_VERSION;

    const capabilities: InitializeCapabilities = {
      tools: {},
      resources: {},
      ...(this.mcplActive ? { experimental: { mcpl: this.manifestTracker.snapshot() } } : {}),
    };

    const result: McplInitializeResult = {
      protocolVersion,
      capabilities,
      serverInfo: this.options.serverInfo,
    };
    conn.sendResponse(msg.request.id, result);

    // The client's `notifications/initialized` (or, from a lax client, its
    // first request) follows.
    const next = await conn.nextMessage();
    if (next.type === 'notification' && next.notification.method === 'notifications/initialized') {
      console.error(`[zulip-mcp] Client initialized (${this.mcplActive ? 'MCPL' : 'MCP'} mode)`);
    } else if (next.type === 'request') {
      await this.handleRequest(conn, next.request);
    } else {
      await this.handleNotification(conn, next.notification);
    }
    return true;
  }

  private async handleRequest(conn: McplConnection, req: JsonRpcRequest): Promise<void> {
    const params = (req.params ?? {}) as Record<string, unknown>;
    try {
      switch (req.method) {
        case 'ping':
          conn.sendResponse(req.id, {});
          break;

        case 'tools/list':
          // MCPL RFC-008: each tool carries its class in _meta['mcpl/class'].
          conn.sendResponse(req.id, { tools: withToolClasses(toolDefinitions) });
          break;

        case 'tools/call': {
          const name = String(params.name ?? '');
          const args = (params.arguments ?? {}) as Record<string, unknown>;
          conn.sendResponse(req.id, await this.callTool(name, args));
          break;
        }

        case 'resources/list':
          conn.sendResponse(req.id, { resources: this.tools.listResources() });
          break;

        case 'resources/read': {
          const uri = String(params.uri ?? '');
          try {
            conn.sendResponse(req.id, await this.tools.readResource(uri));
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            throw new McplRpcError(-32602, `Failed to read resource: ${message}`);
          }
          break;
        }

        case 'prompts/list':
          conn.sendResponse(req.id, { prompts: [] });
          break;

        case method.FEATURE_SETS_UPDATE: {
          // §6.7: featureSets/update is a Request carrying the effective grant,
          // and its response is a degradation receipt — what this server WILL
          // DO under the grant it was given. It is testimony about
          // consequences, never a claim of entitlement, and it asks for
          // nothing. Only this form can establish a ready state.
          this.requireMcpl();
          const receipt = this.grant.apply(params as unknown as FeatureSetsUpdateParams, 'request');
          conn.sendResponse(req.id, receipt);
          void this.healUndelivered();
          break;
        }

        case method.MCPL_MANIFEST:
          // §17.4: the complete current manifest, never a delta, in the same
          // shape initialize carries. Not gated on any capability path.
          this.requireMcpl();
          conn.sendResponse(req.id, this.manifestTracker.handleManifestRequest());
          break;

        case method.CONTEXT_BEFORE_INFERENCE: {
          this.requireMcpl();
          const result = await this.contextProvider.handleBeforeInference(
            params as unknown as ContextBeforeInferenceParams,
          );
          conn.sendResponse(req.id, result);
          break;
        }

        case method.CHANNELS_LIST:
          this.requireMcpl();
          conn.sendResponse(req.id, this.channelManager.listChannels());
          break;

        case method.CHANNELS_OPEN: {
          this.requireMcpl();
          const open = params as unknown as ChannelsOpenParams;
          conn.sendResponse(req.id, await this.handleChannelOpen(open));
          break;
        }

        case method.CHANNELS_CLOSE: {
          this.requireMcpl();
          const close = params as unknown as ChannelsCloseParams;
          conn.sendResponse(req.id, this.handleChannelClose(close));
          break;
        }

        case method.CHANNELS_PUBLISH: {
          this.requireMcpl();
          const publish = params as unknown as ChannelsPublishParams;
          const result = await this.channelManager.publish(publish);
          if (result.delivered) {
            // No checkpoint is minted here: the
            // publish result has no field to carry one (§14.3), so a host can
            // only learn checkpoints from tool results.
            const text = publish.content
              .filter((b): b is TextContent => b.type === 'text')
              .map((b) => b.text)
              .join('\n');
            for (const id of (result as { messageIds?: string[] }).messageIds ?? (result.messageId ? [result.messageId] : [])) {
              this.stateTracker.recordSent(id, publish.channelId, text);
            }
          }
          conn.sendResponse(req.id, { delivered: result.delivered, ...(result.messageId ? { messageId: result.messageId } : {}) });
          break;
        }

        case method.CHANNELS_ACKNOWLEDGE: {
          this.requireMcpl();
          if (!this.grant.has('channels.acknowledge')) throw capabilityDenied('channels.acknowledge');
          const ack = params as { channelId?: string; messageId?: string; intent?: string; value?: string };
          if (!ack.channelId || !ack.messageId) throw new McplRpcError(-32602, 'channels/acknowledge requires channelId and messageId');
          if (!this.adapter.acknowledge) {
            conn.sendResponse(req.id, { acknowledged: false, reason: 'this surface has no acknowledgment representation' });
            break;
          }
          try {
            const representation = await this.adapter.acknowledge(ack.channelId, ack.messageId, ack.value);
            conn.sendResponse(req.id, { acknowledged: true, representation });
          } catch (error) {
            conn.sendResponse(req.id, { acknowledged: false, reason: (error as Error).message });
          }
          break;
        }

        case method.STATE_ROLLBACK: {
          this.requireMcpl();
          conn.sendResponse(req.id, await this.handleRollback(params as unknown as StateRollbackParams));
          break;
        }

        case method.CHANNELS_TYPING: {
          this.requireMcpl();
          await this.typing(params);
          conn.sendResponse(req.id, {});
          break;
        }

        default:
          conn.sendError(req.id, -32601, `Method not found: ${req.method}`);
      }
    } catch (err) {
      if (err instanceof McplRpcError) {
        conn.sendError(req.id, err.code, err.message, err.data);
        return;
      }
      const e = err as Error;
      console.error(`[zulip-mcp] handleRequest error: method=${req.method}`, e.stack ?? e.message);
      conn.sendError(req.id, -32603, e.message ?? String(err));
    }
  }

  private async handleNotification(conn: McplConnection, notif: JsonRpcNotification): Promise<void> {
    const params = (notif.params ?? {}) as Record<string, unknown>;
    try {
      switch (notif.method) {
        case 'notifications/initialized':
        case 'notifications/cancelled':
        case 'notifications/roots/list_changed':
          break;

        case method.FEATURE_SETS_UPDATE:
          // §6.7 Notification form: only `disabled` reductions apply. Grant-bearing
          // updates (including the §5.3 initial policy) arrive as a Request.
          if (this.mcplActive) this.grant.apply(params as unknown as FeatureSetsUpdateParams, 'notification');
          break;

        case method.CHANNELS_TYPING:
        case 'notifications/typing':
          if (this.mcplActive) await this.typing(params);
          break;

        case method.CHANNELS_OUTGOING_CHUNK:
        case method.CHANNELS_OUTGOING_COMPLETE:
          // Advisory stream of what the host is about to publish. Delivery is
          // NEVER a side effect of a lifecycle event (§14.5): the only send
          // path is channels/publish. Nothing to finalize here.
          break;

        default:
          break;
      }
    } catch (err) {
      // Notifications cannot be answered; a failing one is logged, never fatal.
      const e = err as Error;
      console.error(`[zulip-mcp] notification ${notif.method} failed:`, e.message ?? String(err));
    }
    void conn;
  }

  private async handleChannelOpen(params: ChannelsOpenParams): Promise<ChannelsOpenResult> {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    const descriptor = await this.findChannelRediscovering(params);
    // Opening a channel is proof the host knows it — the only proof available
    // when the answer to its announcement cannot reach us (#20 review).
    this.channelManager.confirmAnnounced(descriptor.id);
    const result: ChannelsOpenResult = { channel: descriptor };

    if (this.adapter.ensureSubscribed) {
      try {
        await this.adapter.ensureSubscribed(descriptor.id);
      } catch (err) {
        throw new McplRpcError(
          ERR_CHANNEL_OPEN_FAILED,
          `Cannot open ${descriptor.id}: ${(err as Error).message}`,
          { channelId: descriptor.id },
        );
      }
    }

    const requested = Math.max(0, Math.floor(params.history?.limit ?? 0));
    if (requested > 0 && this.adapter.fetchHistory && (this.isMuted(descriptor.id) || !this.isAllowed(descriptor.id))) {
      result.history = [];
      result.historyTruncated = false;
    } else if (requested > 0 && this.adapter.fetchHistory) {
      const cap = Math.min(HISTORY_ON_OPEN_CAP, historyCapOf(descriptor));
      const limit = Math.min(requested, cap);
      const watermark = this.delivery.watermark(descriptor.id);
      const afterMessageId =
        params.history?.sinceLastSeen && watermark !== undefined ? String(watermark) : undefined;
      const beforeMessageId = afterMessageId === undefined ? params.history?.beforeMessageId : undefined;
      const page = await this.adapter.fetchHistory(descriptor.id, { limit, beforeMessageId, afterMessageId });
      // Not attributed: the host hands this back as the channel_open tool
      // result, JSON that already shows author, threadId and metadata.
      result.history = this.projectHistoryReactions(page.messages);
      result.historyTruncated = requested > limit;
      for (const m of page.messages) this.accepted(descriptor.id, Number(m.messageId));
      if (beforeMessageId === undefined) {
        for (let i = page.messages.length - 1; i >= 0; i--) {
          if (this.channelManager.noteConversation(descriptor.id, page.messages[i])) break;
        }
      }
    }

    this.channelManager.markOpen(descriptor.id);
    this.delivery.markOpen(descriptor.id);
    this.delivery.save();
    return result;
  }

  private async findChannelRediscovering(params: ChannelsOpenParams): Promise<ChannelDescriptor> {
    try {
      return this.channelManager.findChannel(params);
    } catch (err) {
      if (!params.channelId || !(err instanceof McplRpcError) || err.code !== ERR_UNKNOWN_CHANNEL) throw err;
      const { added } = await this.applyFilterChange();
      if (added.length > 0) {
        console.error(`[zulip-mcp] channels/open ${params.channelId}: re-discovery registered ${added.join(', ')}`);
      }
      return this.channelManager.findChannel(params);
    }
  }

  private attributed(message: IncomingChannelMessage): IncomingChannelMessage {
    return this.attributeDelivery ? attributeMessage(message, this.formatTime) : message;
  }

  private handleChannelClose(params: ChannelsCloseParams): { closed: boolean } {
    // agent-framework reconciles a channel it does not want with `channels/close`.
    this.channelManager.confirmAnnounced(params.channelId);
    const result = this.channelManager.closeChannel(params);
    this.delivery.markClosed(params.channelId);
    this.delivery.save();
    return result;
  }

  private isMuted(channelId: string): boolean {
    if (!this.filters || isDmChannelId(channelId) || !channelId.startsWith('zulip:')) return false;
    return this.filters.streamMuted(channelId.slice('zulip:'.length));
  }

  private isAllowed(channelId: string, message?: IncomingChannelMessage): boolean {
    if (!this.filters || !channelId.startsWith('zulip:')) return true;
    if (!isDmChannelId(channelId)) return this.filters.streamAllowed(channelId.slice('zulip:'.length));
    const sender = message ? senderOf(message) : null;
    if (sender) return this.filters.dmAllowed(sender);
    const meta = this.channelManager.getChannel(channelId)?.metadata as { participants?: unknown } | undefined;
    const participants = Array.isArray(meta?.participants) ? (meta!.participants as { id?: unknown; email?: unknown }[]) : [];
    if (participants.length === 0) return true;
    return participants.some((p) => this.filters!.dmAllowed({ id: Number(p.id), email: typeof p.email === 'string' ? p.email : '' }));
  }

  private mayDeliver(message: IncomingChannelMessage): boolean {
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return false;
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    if (meta.system === true) return true;
    return !this.isMuted(message.channelId) && this.isAllowed(message.channelId, message);
  }

  private accepted(channelId: string, id: number): boolean {
    if (!Number.isFinite(id) || id <= 0) return false;
    const released = this.delivery.release(channelId, id);
    return this.delivery.advance(channelId, id) || released;
  }

  private onDelivered(channelId: string, accepted: IncomingChannelMessage[], rejected: IncomingChannelMessage[]): void {
    let moved = false;
    let high = 0;
    for (const m of accepted) {
      const id = numericId(m);
      if (id === null) continue;
      moved = this.delivery.release(channelId, id) || moved;
      high = Math.max(high, id);
    }
    for (const m of rejected) {
      const id = numericId(m);
      if (id !== null) this.delivery.hold(channelId, id, 'rejected');
    }
    if (high > 0) moved = this.delivery.advance(channelId, high) || moved;
    if (moved) this.delivery.save();
    if (accepted.length > 0) void this.healUndelivered();
  }

  private onGivenUp(channelId: string, messages: IncomingChannelMessage[]): void {
    for (const m of messages) {
      const id = numericId(m);
      if (id !== null) this.delivery.hold(channelId, id, 'failed');
    }
  }

  private onWithheld(channelId: string, messages: IncomingChannelMessage[]): void {
    let moved = false;
    for (const m of messages) {
      const id = numericId(m);
      if (id === null) continue;
      if (this.isMuted(channelId) || !this.isAllowed(channelId, m)) moved = this.delivery.release(channelId, id) || moved;
      else this.delivery.hold(channelId, id, 'failed');
    }
    if (moved) this.delivery.save();
  }

  private deliverToOpen(channelId: string, message: IncomingChannelMessage): boolean {
    if (!this.channelManager.isOpen(channelId)) return false;
    const id = numericId(message);
    if (id !== null) this.delivery.hold(channelId, id, 'queued');
    this.channelManager.onIncomingMessage(channelId, message);
    return true;
  }

  private async onIncoming(message: IncomingChannelMessage, newChannel?: ChannelDescriptor): Promise<void> {
    if (!this.live) {
      if (this.preLive.length >= PRE_LIVE_BUFFER_CAP) {
        const dropped = this.preLive.shift()!;
        const id = numericId(dropped.message);
        if (id !== null) this.delivery.hold(dropped.message.channelId, id, 'failed');
        this.preLiveDropped++;
      }
      this.preLive.push({ message, newChannel });
      return;
    }
    await this.route(message, newChannel);
  }

  private async route(message: IncomingChannelMessage, newChannel?: ChannelDescriptor): Promise<void> {
    const channelId = message.channelId;
    const id = Number(message.messageId);
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const addressed = meta.mentioned === true || meta.isDM === true;

    if (this.isMuted(channelId) || !this.isAllowed(channelId, message)) return;

    // §6.7: a disabled zulip.messaging stops its traffic at once, incoming
    // and push alike.
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;

    // A channel the host has never seen (#20) is registered first, so the host
    // can open it and route a reply back to it.
    if (newChannel && !this.channelManager.getChannel(channelId)) {
      await this.channelManager.registerAdditional([newChannel]);
    }

    const refs = Array.isArray(meta.attachments) ? (meta.attachments as AttachmentRef[]) : [];
    if (refs.length > 0 && this.options.attachments) {
      const blocks = await buildAttachmentBlocks(refs, this.options.attachments.source, this.options.attachments.inline);
      if (blocks.length > 0) message = { ...message, content: [...message.content, ...blocks] };
    }

    // A host that renders only the content blocks shows the model only the
    // body, so who said it, where and when go into it.
    const bodyText = viewOf(message).text;
    message = this.attributed(message);

    if (meta.isDM === true && this.delivery.watermark(channelId) === undefined) {
      const authorId = message.author.id;
      const note = `<system>Direct message from ${message.author.name} (user id ${authorId}). ` +
        `To reply, use send_dm(["${authorId}"]) or publish to channel ${channelId}. ` +
        `DMs always reach you; there is nothing to subscribe to.</system>`;
      message = { ...message, content: [{ type: 'text', text: note }, ...message.content] };
    }

    if (this.deliverToOpen(channelId, message)) return;

    if (addressed) {
      this.delivery.hold(channelId, id, 'queued');
      const delivered = await this.pushEvent(message, `zulip_msg_${message.messageId}`);
      if (delivered) {
        this.channelManager.noteConversation(channelId, message);
        if (this.accepted(channelId, id)) this.delivery.save();
        void this.healUndelivered();
      } else {
        this.delivery.hold(channelId, id, 'failed');
      }
      return;
    }

    if (this.delivery.countMissed(channelId, { id, text: bodyText })) this.delivery.save();
  }

  private async healUndelivered(): Promise<void> {
    if (this.healing || !this.live || !this.conn || !this.mcplActive) return;
    const channels = this.delivery.heldChannels();
    if (channels.length === 0) return;
    this.healing = true;
    try {
      for (const channelId of channels) {
        if (this.isMuted(channelId) || !this.isAllowed(channelId)) {
          this.delivery.releaseAll(channelId);
          continue;
        }
        const ids = this.delivery.heldIds(channelId, { replayable: true });
        if (ids.length === 0) continue;
        if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) continue;
        this.delivery.markReplayed(channelId, ids);
        try {
          if (this.channelManager.isOpen(channelId)) {
            await this.replayHeld(channelId, ids);
          } else {
            const watermark = this.delivery.watermark(channelId);
            await this.catchUpClosedChannel(channelId, this.delivery.wasOpen(channelId), watermark ?? ids[0] - 1);
          }
        } catch (err) {
          console.error(`[zulip-mcp] replay of undelivered messages on ${channelId} failed:`, (err as Error).message);
        }
      }
      this.delivery.save();
    } finally {
      this.healing = false;
    }
  }

  private async replayHeld(channelId: string, ids: number[]): Promise<void> {
    if (!this.adapter.fetchHistory || ids.length === 0) return;
    const wanted = new Set(ids);
    const fetched = await this.fetchAfter(channelId, ids[0] - 1, ids[ids.length - 1]);
    const found = new Set<number>();
    const msgs = fetched.messages.filter((m) => {
      const id = numericId(m);
      if (id === null || !wanted.has(id)) return false;
      found.add(id);
      return true;
    });
    const replayed = this.replayOnOpen(channelId, msgs);
    const scanned = fetched.scannedThrough ?? 0;
    for (const id of ids) if (!found.has(id) && (id <= scanned || fetched.complete)) this.delivery.release(channelId, id);
    if (replayed > 0) console.error(`[zulip-mcp] ${channelId}: replaying ${replayed} undelivered message(s) from history`);
  }

  private replayOnOpen(channelId: string, msgs: IncomingChannelMessage[]): number {
    let n = 0;
    for (const m of this.projectHistoryReactions(msgs)) {
      const queued = this.deliverToOpen(channelId, this.attributed({
        ...m,
        tags: [...(m.tags ?? []), 'zulip:missed'],
        metadata: { ...(m.metadata as Record<string, unknown>), backscroll: undefined, recovered: true },
      }));
      if (queued) n++;
    }
    return n;
  }

  /**
   * Requires the `pushEvents` capability and an active zulip.messaging. The
   * origin carries the MCPL channel id, which the host registers and routes
   * replies to.
   */
  private async pushEvent(message: IncomingChannelMessage, eventId: string, extraOrigin: Record<string, unknown> = {}): Promise<boolean> {
    const conn = this.conn;
    if (!conn || !this.mcplActive) return false;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      console.error(`[zulip-mcp] pushEvents not granted; dropping addressed message ${message.messageId} on closed ${message.channelId}`);
      return false;
    }
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const missed = this.delivery.tally(message.channelId);
    const params: PushEventParams = {
      featureSet: MESSAGING_FEATURE_SET,
      eventId,
      timestamp: message.timestamp,
      origin: {
        source: 'zulip',
        mcplChannelId: message.channelId,
        messageId: message.messageId,
        stream: !isDmChannelId(message.channelId) && message.channelId.startsWith('zulip:')
          ? message.channelId.slice('zulip:'.length)
          : undefined,
        topic: meta.topic,
        authorId: message.author.id,
        authorName: message.author.name,
        isMention: meta.mentioned === true,
        isDM: meta.isDM === true,
        // agent-framework stores `origin` (not message metadata) as the
        // stored message's metadata on this path, so the attribution stamp
        // rides here too, or a host strategy cannot tell the payload already
        // names its author.
        ...(meta.attributed === true ? { attributed: true, attributionHeader: meta.attributionHeader } : {}),
        ...(missed ? { missedMessages: missed.messages, missedCharacters: missed.characters } : {}),
        ...extraOrigin,
      },
      tags: message.tags,
      payload: { content: message.content },
    };
    try {
      const result = (await conn.sendRequest(method.PUSH_EVENT, params)) as { accepted?: boolean; reason?: string } | undefined;
      if (result && result.accepted === false) {
        console.error(`[zulip-mcp] push/event ${eventId} not accepted by host${result.reason ? `: ${result.reason}` : ''}`);
        return false;
      }
      return true;
    } catch (err) {
      console.error('[zulip-mcp] push/event failed:', (err as Error).message);
      return false;
    }
  }

  private async runReconnectSweep(): Promise<void> {
    if (this.sweepDone) return;
    this.sweepDone = true;
    if (!this.conn || !this.mcplActive || !this.adapter.fetchHistory || this.catchupLimit === 0) return;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;

    const candidates = new Set<string>([
      ...this.sweepAnchors.keys(),
      ...this.delivery.lastOpenChannels(),
    ]);
    let delivered = 0;
    for (const channelId of candidates) {
      if (await this.catchUpClosedChannel(channelId, this.delivery.wasOpen(channelId), this.sweepAnchors.get(channelId))) delivered++;
    }

    await this.backfillMissedTallies();
    this.delivery.save();
    if (delivered > 0) {
      console.error(`[zulip-mcp] Reconnect catch-up: delivered missed messages from ${delivered} channel(s)`);
    }
  }

  private async fetchAfter(
    channelId: string,
    afterId: number,
    throughId?: number,
  ): Promise<{ messages: IncomingChannelMessage[]; truncated: boolean; complete: boolean; scannedThrough: number | null }> {
    const out: IncomingChannelMessage[] = [];
    let cursor = afterId;
    let complete = false;
    while (out.length < this.catchupLimit) {
      const want = this.catchupLimit - out.length;
      const page = await this.adapter.fetchHistory!(channelId, { limit: want, afterMessageId: String(cursor) });
      if (page.scannedThrough === null) {
        complete = true;
        break;
      }
      out.push(...page.messages);
      if (page.scannedThrough <= cursor) break;
      cursor = page.scannedThrough;
      if (page.reachedNewest || (throughId !== undefined && cursor >= throughId)) {
        complete = true;
        break;
      }
    }
    return { messages: out, truncated: !complete, complete, scannedThrough: cursor > afterId ? cursor : null };
  }

  private async catchUpClosedChannel(channelId: string, keepAllHint: boolean, afterId?: number): Promise<boolean> {
    if (!this.adapter.fetchHistory || this.catchupLimit === 0) return false;
    if (!this.channelManager.getChannel(channelId)) return false;
    if (this.isMuted(channelId) || !this.isAllowed(channelId)) return false;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return false;
    const after = afterId ?? this.delivery.watermark(channelId);
    if (after === undefined) return false;
    let fetched: Awaited<ReturnType<ZulipMcplServer['fetchAfter']>>;
    try {
      fetched = await this.fetchAfter(channelId, after);
    } catch (err) {
      console.error(`[zulip-mcp] catch-up: history fetch failed for ${channelId}:`, (err as Error).message);
      return false;
    }
    if (fetched.scannedThrough === null) return false;
    const newestId = fetched.scannedThrough;
    const msgs = fetched.messages;
    const views = msgs.map(viewOf);
    const keepAll = keepAllHint || isDmChannelId(channelId);
    const kept = selectMissed(views, { keepAll, vicinity: MISSED_VICINITY });
    if (kept.length === 0) {
      this.delivery.advanceThrough(channelId, newestId);
      return false;
    }
    const mentionCount = views.filter((v) => v.mentioned).length;
    const streamName = isDmChannelId(channelId)
      ? (this.channelManager.getChannel(channelId)?.label ?? channelId)
      : channelId.slice('zulip:'.length);
    const block = renderMissedBlock(kept, {
      streamName,
      channelId,
      reason: keepAll ? 'backscroll' : 'mention',
      count: keepAll ? kept.length : mentionCount,
      formatTime: this.formatTime,
      maxChars: this.missedBlockMaxChars,
      moreBeyond: fetched.truncated,
      newestScannedId: newestId,
    });
    const synthetic: IncomingChannelMessage = {
      channelId,
      messageId: String(newestId),
      author: { id: 'system', name: 'zulip catch-up' },
      timestamp: new Date().toISOString(),
      content: [{ type: 'text', text: block } satisfies TextContent],
      tags: ['zulip:missed', ...(mentionCount > 0 ? ['chat:mention'] : ['chat:ambient'])],
      metadata: { missed: true, topic: undefined, mentioned: mentionCount > 0, isDM: isDmChannelId(channelId) },
    };
    const ok = await this.pushEvent(synthetic, `zulip_missed_${channelId}_${newestId}`, {
      missed: true,
      reason: keepAll ? 'backscroll' : 'mention',
      messages: kept.length,
      ...(fetched.truncated ? { truncated: true } : {}),
    });
    if (ok) {
      this.delivery.advanceThrough(channelId, newestId);
      for (let i = msgs.length - 1; i >= 0; i--) {
        if (!keepAll && !views[i].mentioned) continue;
        if (this.channelManager.noteConversation(channelId, msgs[i])) break;
      }
    }
    return ok;
  }

  private async backfillMissedTallies(): Promise<void> {
    if (!this.adapter.fetchHistory) return;
    for (const channelId of this.delivery.talliedChannels()) {
      if (this.isMuted(channelId) || !this.isAllowed(channelId)) continue;
      const tally = this.delivery.tally(channelId)!;
      if (!tally.talliedThrough) continue;
      let fetched: Awaited<ReturnType<ZulipMcplServer['fetchAfter']>>;
      try {
        fetched = await this.fetchAfter(channelId, tally.talliedThrough);
      } catch {
        continue;
      }
      if (fetched.scannedThrough === null) continue;
      const views = fetched.messages.map(viewOf);
      const ambient = views.filter((v) => !v.mentioned);
      this.delivery.backfillMissed(channelId, ambient, fetched.scannedThrough);
    }
  }

  /**
   * A Zulip event queue's replacement starts from "now", so the gap is healed
   * from history before the gap marker is delivered.
   */
  private async onSystemEvent(event: PlatformSystemEvent): Promise<void> {
    let recovered = 0;
    let closedCaughtUp = 0;
    // §6.7 holds for replayed traffic as much as for live: nothing is
    // recovered into a feature set the host has disabled.
    if (event.kind === 'gap' && this.adapter.fetchHistory && this.live && this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      for (const channelId of this.channelManager.getOpenChannels()) {
        if (this.isMuted(channelId) || !this.isAllowed(channelId)) continue;
        const watermark = this.delivery.watermark(channelId);
        if (watermark === undefined) continue;
        try {
          const msgs = (await this.fetchAfter(channelId, watermark)).messages;
          recovered += this.replayOnOpen(channelId, msgs);
        } catch (err) {
          console.error(`[zulip-mcp] gap recovery failed for ${channelId}:`, (err as Error).message);
        }
      }
      for (const channelId of this.delivery.watermarkedChannels()) {
        if (this.channelManager.isOpen(channelId)) continue;
        if (await this.catchUpClosedChannel(channelId, false)) closedCaughtUp++;
      }
      if (recovered > 0 || closedCaughtUp > 0) this.delivery.save();
    }
    const notes = [
      recovered > 0 ? `${recovered} message(s) on open channels were recovered from history and delivered above.` : null,
      closedCaughtUp > 0 ? `Mentions on ${closedCaughtUp} closed channel(s) were delivered as catch-up events.` : null,
    ].filter(Boolean);
    const text = notes.length > 0 ? `${event.text} ${notes.join(' ')}` : event.text;
    this.channelManager.broadcastSystemEvent(this.adapter.type, {
      ...event,
      text,
      metadata: { ...event.metadata, ...(event.kind === 'gap' ? { recoveredMessages: recovered, closedChannelsCaughtUp: closedCaughtUp } : {}) },
    });
  }

  private async handleRollback(params: StateRollbackParams): Promise<StateRollbackResult> {
    if (params.featureSet !== MESSAGING_FEATURE_SET) {
      return { checkpoint: params.checkpoint, success: false, reason: `Feature set '${params.featureSet}' does not support rollback` };
    }
    const toDelete = this.stateTracker.rollback(params.checkpoint);
    if (toDelete === null) {
      return { checkpoint: params.checkpoint, success: false, reason: 'Checkpoint not found' };
    }
    let deleted = 0;
    for (const msg of toDelete) {
      if (!this.adapter.deleteMessage) break;
      try {
        await this.adapter.deleteMessage(msg.channelId, msg.messageId);
        deleted++;
      } catch {
        // Best-effort — the message may already be gone, or past the realm's delete window.
      }
    }
    return {
      checkpoint: params.checkpoint,
      success: true,
      ...(deleted < toDelete.length ? { reason: `Rolled back (${deleted}/${toDelete.length} messages deleted)` } : {}),
    };
  }

  private requireMcpl(): void {
    if (!this.mcplActive) {
      throw new McplRpcError(-32601, 'MCPL is not negotiated on this connection');
    }
  }

  private async typing(params: Record<string, unknown>): Promise<void> {
    const channelId = typeof params.channelId === 'string' ? params.channelId : undefined;
    if (!channelId) throw new McplRpcError(-32602, 'channels/typing requires channelId');
    const metadata =
      typeof params.metadata === 'object' && params.metadata !== null
        ? (params.metadata as Record<string, unknown>)
        : undefined;
    const op = params.op === 'stop' ? 'stop' : 'start';
    await this.channelManager.sendTyping(channelId, metadata, op);
  }

  /**
   * In MCPL mode `tools` must be in the effective grant (§14.1 / §6.2), and a
   * tool owned by a feature set the host disabled is unavailable with it
   * (§6.7). Plain MCP clients have no grant to consult.
   */
  private async callTool(name: string, args: Record<string, unknown>): Promise<ToolCallResult> {
    if (this.mcplActive) {
      if (!this.grant.has('tools')) throw capabilityDenied('tools');
      const owner = featureSetForTool(name);
      if (owner && !this.grant.isFeatureSetActive(owner)) {
        return {
          content: [{ type: 'text' as const, text: `Feature set '${owner}' is not enabled` }],
          isError: true,
        };
      }
    }
    try {
      const own = await this.serverTool(name, args);
      const result = toToolCallResult(own !== undefined ? own : await this.tools.handleToolCall(name, args));
      // §8: a tool of the rollback-capable feature set mints a checkpoint and
      // hands it back in `state`, the only way a host ever learns one.
      if (this.mcplActive && featureSetForTool(name) === MESSAGING_FEATURE_SET) {
        const checkpoint = this.stateTracker.createCheckpoint();
        const parent = this.stateTracker.getCheckpointState()?.parent ?? null;
        return { ...result, state: { featureSet: MESSAGING_FEATURE_SET, checkpoint, parent } };
      }
      return result;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      return { content: [{ type: 'text' as const, text: `Error: ${message}` }], isError: true };
    }
  }

  async applyFilterChange(opts: { retryRefused?: boolean } = {}): Promise<{ visible: number; added: string[]; local: string[]; refused: string[]; reason?: string }> {
    if (!this.conn || !this.mcplActive) {
      return { visible: 0, added: [], local: [], refused: [], reason: 'not connected to an MCPL host; channels are registered in MCPL mode only' };
    }
    const descriptors = await this.adapter.discoverChannels();
    const result = await this.channelManager.registerAdditional(descriptors, { retryBacklog: true, retryRefused: opts.retryRefused });
    if (result.announced.length > 0) console.error(`[zulip-mcp] registered ${result.announced.length} newly visible channel(s): ${result.announced.join(', ')}`);
    return { visible: descriptors.length, added: result.announced, local: result.local, refused: result.refused, reason: result.reason };
  }

  /**
   * Runs off the request loop, so the announcement cannot deadlock against a
   * host that reconciles before it answers.
   */
  private async ensureChannelKnown(channelId: string): Promise<void> {
    if (this.channelManager.getChannel(channelId)) return;
    if (!this.conn || !this.mcplActive || !this.adapter.describeChannels) return;
    try {
      const [descriptor] = await this.adapter.describeChannels([channelId]);
      if (descriptor) await this.channelManager.registerAdditional([descriptor]);
    } catch (error) {
      console.error(`[zulip-mcp] could not register ${channelId} from a message change:`, (error as Error).message);
    }
  }

  private async registerSubscribed(streams: string[]): Promise<RegisterAdditionalResult> {
    const empty: RegisterAdditionalResult = { announced: [], local: [], refused: [] };
    if (!this.conn || !this.mcplActive) {
      return { ...empty, reason: 'not connected to an MCPL host; channels are registered in MCPL mode only' };
    }
    if (!this.adapter.describeChannels) return empty;
    try {
      const descriptors = await this.adapter.describeChannels(streams.map((name) => `zulip:${name}`));
      const allowed = descriptors.filter((d) => this.isAllowed(d.id));
      if (allowed.length === 0) return empty;
      return await this.channelManager.registerAdditional(allowed, { retryBacklog: true });
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      console.error(`[zulip-mcp] registering ${streams.join(', ')} after listen failed:`, error);
      return { ...empty, reason };
    }
  }

  private requireFilters(): FiltersPlane {
    if (!this.filters) throw new Error('No filters plane is configured on this server.');
    return this.filters;
  }

  private streamArg(value: unknown): string {
    const raw = String(value ?? '').trim().replace(/^#/, '');
    if (!raw) throw new Error('channel is required');
    const name = raw.startsWith('zulip:') ? raw.slice('zulip:'.length) : raw;
    if (isDmChannelId(`zulip:${name}`) || name.startsWith('dm:')) throw new Error('DM conversations cannot be muted or filtered by stream; use the dmUsers allowlist.');
    return name;
  }

  private async serverTool(name: string, args: Record<string, unknown>): Promise<unknown> {
    switch (name) {
      case 'filters_get': {
        const plane = this.requireFilters();
        const f = plane.current();
        return {
          streams: f.streams ?? null,
          dmUsers: f.dmUsers ?? null,
          mutedStreams: f.mutedStreams ?? [],
          reactionChannels: f.reactionChannels ?? [],
          plane: plane.planeStatus(),
          reactionSuppression: plane.suppressionStatus(),
          note: 'null = unrestricted. Filters gate delivery only; the bot must also be able to see a stream. ' +
            'suppressedReactionEmojis is operator-owned and reported as a count/digest, never the entries.',
        };
      }
      case 'filters_update': {
        const plane = this.requireFilters();
        const addStreams = Array.isArray(args.addStreams) ? args.addStreams.map((s) => this.streamArg(s)) : [];
        const removeStreams = Array.isArray(args.removeStreams) ? args.removeStreams.map((s) => this.streamArg(s)) : [];
        const setDmUsers = Array.isArray(args.setDmUsers) ? args.setDmUsers.map(String) : undefined;
        if (addStreams.length === 0 && removeStreams.length === 0 && setDmUsers === undefined) {
          throw new Error('Nothing to change: pass addStreams, removeStreams, and/or setDmUsers.');
        }
        let materialized = false;
        const result = plane.update((f) => {
          let streams = f.streams ? [...f.streams] : null;
          if (removeStreams.length > 0 && streams === null) {
            streams = this.channelManager.listChannels().channels
              .filter((c) => !isDmChannelId(c.id))
              .map((c) => c.id.slice('zulip:'.length));
            materialized = true;
          }
          if (streams !== null) {
            for (const name of addStreams) if (!streams.includes(name)) streams.push(name);
            streams = streams.filter((name) => !removeStreams.includes(name));
            if (streams.length === 0) {
              throw new Error(
                'Refusing to remove the last allowed stream: an empty allowlist means unrestricted, which would ' +
                  'deliver EVERY stream. Add another stream first, or mute streams to hear nothing from them.',
              );
            }
          }
          return {
            ...f,
            ...(streams !== null ? { streams } : {}),
            ...(setDmUsers !== undefined ? { dmUsers: setDmUsers } : {}),
          };
        });
        if (!result.ok) throw new Error(`filters_update refused: ${result.reason}`);
        const { added } = await this.applyFilterChange();
        return {
          streams: result.filters.streams ?? null,
          dmUsers: result.filters.dmUsers ?? null,
          registered: added,
          note: [
            materialized ? 'The stream allowlist was unrestricted; it was materialized as the full current list before removing.' : null,
            setDmUsers !== undefined && (result.filters.dmUsers ?? null) === null ? 'dmUsers is now UNRESTRICTED (anyone may DM the bot).' : null,
            'Applied immediately and persisted.',
          ].filter(Boolean).join(' '),
        };
      }
      case 'mute_channel':
      case 'unmute_channel': {
        const plane = this.requireFilters();
        const stream = this.streamArg(args.channel);
        const mute = name === 'mute_channel';
        const result = plane.update((f) => {
          const muted = new Set(f.mutedStreams ?? []);
          if (mute) muted.add(stream);
          else muted.delete(stream);
          return { ...f, mutedStreams: [...muted] };
        });
        if (!result.ok) throw new Error(`${name} refused: ${result.reason}`);
        return {
          channelId: `zulip:${stream}`,
          muted: mute,
          mutedStreams: result.filters.mutedStreams ?? [],
          note: mute
            ? 'Nothing from this stream reaches you now — not even mentions — and nothing is tallied. Persisted; reverse with unmute_channel.'
            : 'Messages from this stream reach you again by the usual rules (mentions always; ambient when the channel is open).',
        };
      }
      case 'set_reaction_visibility': {
        const plane = this.requireFilters();
        const channelId = this.channelIdArg(args.channel);
        const visible = args.visible === true;
        const result = plane.update((f) => {
          const set = new Set(f.reactionChannels ?? []);
          if (visible) set.add(channelId);
          else set.delete(channelId);
          return { ...f, reactionChannels: [...set] };
        });
        if (!result.ok) throw new Error(`set_reaction_visibility refused: ${result.reason}`);
        return {
          channelId,
          visible,
          note: visible
            ? 'Reaction visibility ON: reactions in this channel now appear in your context as they happen (they never wake you). Persisted.'
            : 'Reaction visibility OFF for this channel. Persisted.',
        };
      }
      case 'refresh_channels': {
        const { visible, added, local, refused, reason } = await this.applyFilterChange({ retryRefused: true });
        const notes: string[] = [];
        if (added.length > 0) notes.push(`Registered ${added.length} newly visible channel(s).`);
        if (local.length > 0) notes.push(`${local.length} channel(s) could not be announced to the host (${reason ?? 'no answer'}); they are usable here and will be re-announced.`);
        if (refused.length > 0) notes.push(`The host refused ${refused.length} channel(s): ${refused.join(', ')}.`);
        if (notes.length === 0) {
          notes.push(reason
            ? `No channels were announced: ${reason}.`
            : 'No new channels — the host already knows about every visible channel this server can see. A stream outside the filters allowlist is not visible here and is not counted.');
        }
        return { visible, added, ...(local.length > 0 ? { pendingAnnouncement: local } : {}), ...(refused.length > 0 ? { refused } : {}), note: notes.join(' ') };
      }
      case 'channel_missed': {
        const channelId = this.channelIdArg(args.channel);
        const tally = this.delivery.tally(channelId);
        if (!tally) {
          return {
            channelId,
            tracked: false,
            open: this.channelManager.isOpen(channelId),
            note: this.channelManager.isOpen(channelId)
              ? 'This channel is open: everything is delivered, nothing is missed.'
              : 'Not tracked: the host has not closed this channel since delivery began, so there is no baseline to count from.',
          };
        }
        return {
          channelId,
          tracked: true,
          missedMessages: tally.messages,
          missedCharacters: tally.characters,
          sinceMessageId: tally.anchorId || null,
          talliedThroughMessageId: tally.talliedThrough || null,
          note: 'Ambient messages dropped since the host closed this channel. Mentions were delivered and are not counted.',
        };
      }
      default:
        return undefined;
    }
  }

  private channelIdArg(value: unknown): string {
    const raw = String(value ?? '').trim();
    if (!raw) throw new Error('channel is required');
    return raw.startsWith('zulip:') ? raw : `zulip:${raw.replace(/^#/, '')}`;
  }

  private startEvents(): void {
    if (this.eventsStarted) return;
    this.eventsStarted = true;
    this.adapter.startEvents(
      (message, newChannel) => {
        const delivery = this.onIncoming(message, newChannel).catch((err) => {
          console.error('[zulip-mcp] inbound delivery failed:', (err as Error).message);
        });
        this.inFlight.add(delivery);
        void delivery.finally(() => this.inFlight.delete(delivery));
      },
      (event) => {
        void this.onSystemEvent(event).catch((err) => {
          console.error('[zulip-mcp] system event handling failed:', (err as Error).message);
        });
      },
      (reaction) => {
        void this.onReaction(reaction).catch((err) => {
          console.error('[zulip-mcp] reaction handling failed:', (err as Error).message);
        });
      },
      (change) => {
        void this.onMessageChange(change).catch((err) => {
          console.error('[zulip-mcp] message change handling failed:', (err as Error).message);
        });
      },
    );
  }

  private async onReaction(ev: ReactionEvent): Promise<void> {
    if (!this.filters || !this.filters.reactionsVisible(ev.channelId)) return;
    if (this.isMuted(ev.channelId) || !this.isAllowed(ev.channelId)) return;
    if (this.filters.reactionSuppressed(ev.emoji, ev.emojiCode, ev.emojiType)) return;
    if (!this.mcplActive || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const verb = ev.action === 'add' ? 'reacted' : 'removed a reaction';
    const target = ev.onOwnMessage ? 'your message' : `message ${ev.messageId}`;
    const quoted = ev.messageSnippet ? ` — "${ev.messageSnippet}"` : '';
    const line = `[reaction] ${ev.reactorName} ${verb} :${ev.emoji}: on ${target}${quoted}`;
    const message: IncomingChannelMessage = {
      channelId: ev.channelId,
      messageId: `reaction:${ev.action}:${ev.messageId}:${ev.reactorId}:${ev.timestamp.getTime()}`,
      author: { id: ev.reactorId, name: ev.reactorName },
      timestamp: ev.timestamp.toISOString(),
      content: [{ type: 'text', text: line }],
      tags: [ev.action === 'add' ? CHAT_TAGS.reaction : CHAT_TAGS.reactionRemove],
      metadata: {
        reaction: true,
        action: ev.action,
        emoji: ev.emoji,
        ...(ev.emojiCode ? { emojiCode: ev.emojiCode } : {}),
        targetMessageId: ev.messageId,
        onOwnMessage: ev.onOwnMessage,
        isDM: false,
        mentioned: false,
      },
    };
    if (this.channelManager.isOpen(ev.channelId)) {
      this.channelManager.onIncomingMessage(ev.channelId, message);
    } else {
      await this.pushEvent(message, `zulip_reaction_${ev.action}_${ev.messageId}_${ev.reactorId}_${ev.timestamp.getTime()}`, {
        reaction: true,
        action: ev.action,
        onOwnMessage: ev.onOwnMessage,
      });
    }
  }

  private async onMessageChange(ev: MessageChangeEvent): Promise<void> {
    if (this.isMuted(ev.channelId) || !this.isAllowed(ev.channelId)) return;
    if (!this.mcplActive || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const addressed = ev.mentioned || ev.isDM;
    await this.ensureChannelKnown(ev.channelId);
    const open = this.channelManager.isOpen(ev.channelId);
    if (!open && !addressed) return;
    if (open && !addressed && !ev.onOwnMessage) {
      const watermark = this.delivery.watermark(ev.channelId) ?? 0;
      const held = new Set(this.delivery.heldIds(ev.channelId));
      const ids = ev.messageIds.map(Number).filter((n) => Number.isFinite(n));
      if (!ids.some((n) => n <= watermark || held.has(n))) return;
    }

    const head = messageLineHead({
      id: ev.messageId,
      time: Number.isNaN(ev.timestamp.getTime()) ? '' : this.formatTime(ev.timestamp),
      stream: ev.isDM ? null : (ev.channelId.startsWith('zulip:') ? ev.channelId.slice('zulip:'.length) : ev.channelId),
      topic: ev.topic,
      author: ev.authorName ?? 'unknown author',
      mentioned: ev.mentioned,
    });
    const others = ev.messageIds.length > 1 ? ` (${ev.messageIds.length} messages)` : '';
    const byOther = ev.actorId !== null && ev.actorId !== ev.authorId ? ` [by user ${ev.actorId}]` : '';
    const wasQuoted = ev.previousContent ? ` — was: "${ev.previousContent}"` : '';
    const movedTo = ev.movedToChannelId ? ` (now in ${ev.movedToChannelId})` : '';
    const fromTopic = ev.previousTopic !== null ? ` [moved from topic "${ev.previousTopic}"${movedTo}]` : (movedTo ? ` [moved${movedTo}]` : '');
    let line: string;
    if (ev.kind === 'edit') {
      line = `[edited] ${head}${ev.content ?? ''}${fromTopic}${byOther}`;
    } else if (ev.kind === 'move') {
      line = `[moved] ${head}topic changed${ev.previousTopic !== null ? ` from "${ev.previousTopic}"` : ''}${movedTo}${others}${byOther}`;
    } else if (ev.vanished) {
      line = `[deleted] ${head}no longer visible to the bot (moved to a stream it cannot see)${others}${wasQuoted}`;
    } else {
      line = `[deleted] ${head}message deleted${others}${wasQuoted}`;
    }
    const tag = ev.kind === 'delete' ? CHAT_TAGS.deleted : CHAT_TAGS.edited;
    // Zulip's edit time has one-second resolution and the host dedupes
    // pushes by eventId: a counter keeps two edits within a second apart.
    const stamp = `${ev.timestamp.getTime()}.${++this.changeSeq}`;
    const message: IncomingChannelMessage = {
      channelId: ev.channelId,
      messageId: `${ev.kind}:${ev.messageId}:${stamp}`,
      author: { id: ev.authorId ?? 'unknown', name: ev.authorName ?? 'unknown author' },
      timestamp: ev.timestamp.toISOString(),
      content: [{ type: 'text', text: line }],
      // The same addressing tag a message carries, so a debounced or
      // tag-keyed policy sees an ambient change as ambient; the host folds
      // chat:mention / chat:dm into chat:addressed.
      tags: [
        tag,
        ...(ev.kind === 'move' ? ['zulip:moved'] : []),
        ...(ev.isDM ? [CHAT_TAGS.dm, CHAT_TAGS.private] : ev.mentioned ? [CHAT_TAGS.mention] : [CHAT_TAGS.ambient]),
      ],
      metadata: {
        change: ev.kind,
        targetMessageId: ev.messageId,
        targetMessageIds: ev.messageIds,
        ...(ev.actorId !== null ? { actorId: ev.actorId } : {}),
        ...(ev.authorEmail !== null ? { senderEmail: ev.authorEmail } : {}),
        topic: ev.topic,
        ...(ev.previousTopic !== null ? { previousTopic: ev.previousTopic } : {}),
        ...(ev.movedToChannelId !== null ? { movedToChannelId: ev.movedToChannelId } : {}),
        ...(ev.previousContent !== null ? { previousContent: ev.previousContent } : {}),
        mentioned: ev.mentioned,
        ...(ev.previouslyMentioned !== null ? { previouslyMentioned: ev.previouslyMentioned } : {}),
        ...(ev.vanished ? { vanished: true } : {}),
        isDM: ev.isDM,
        onOwnMessage: ev.onOwnMessage,
        attributed: true,
        attributionHeader: `[${ev.kind === 'delete' ? 'deleted' : ev.kind === 'move' ? 'moved' : 'edited'}] ${head}`,
      },
    };
    if (open) {
      this.channelManager.onIncomingMessage(ev.channelId, message);
    } else {
      await this.pushEvent(message, `zulip_${ev.kind}_${ev.messageId}_${stamp}`, {
        change: ev.kind,
        targetMessageId: ev.messageId,
      });
    }
  }

  private projectHistoryReactions(messages: IncomingChannelMessage[]): IncomingChannelMessage[] {
    if (!this.filters) return messages;
    const plane = this.filters;
    return messages.map((m) => {
      const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
      const reactions = Array.isArray(meta.reactions) ? (meta.reactions as ReactionSummary[]) : null;
      if (!reactions) return m;
      return { ...m, metadata: { ...meta, reactions: reactions.filter((r) => !plane.reactionSuppressed(r.name, r.code, r.type)) } };
    });
  }

  private async registerChannelsWithHost(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_REGISTER, { channels })) as ChannelsRegisterResult | undefined;
  }

  private async channelsChangedWithHost(params: ChannelsChangedParams, timeoutMs = this.announceTimeoutMs): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_CHANGED, params, timeoutMs)) as ChannelsRegisterResult | undefined;
  }

  private async sendIncomingToHost(messages: IncomingChannelMessage[]): Promise<ChannelsIncomingResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_INCOMING, { messages })) as ChannelsIncomingResult | undefined;
  }
}

function numericId(m: IncomingChannelMessage): number | null {
  const n = Number(m.messageId);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function senderOf(m: IncomingChannelMessage): { id: number; email: string } | null {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  if (meta.system === true || meta.reaction === true || meta.missed === true || typeof meta.change === 'string') return null;
  const id = Number(m.author?.id);
  if (!Number.isFinite(id)) return null;
  return { id, email: typeof meta.senderEmail === 'string' ? meta.senderEmail : '' };
}

function historyCapOf(descriptor: ChannelDescriptor): number {
  const max = descriptor.capabilities?.history?.maxMessages;
  return typeof max === 'number' && max > 0 ? max : HISTORY_ON_OPEN_CAP;
}
