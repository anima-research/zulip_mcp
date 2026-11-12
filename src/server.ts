/**
 * ZulipMcplServer — the JSON-RPC main loop over an `McplConnection`.
 *
 * Speaks plain MCP to any client (initialize, tools/*, resources/*) and MCPL
 * 0.5 to hosts that advertise `experimental.mcpl` in their initialize
 * capabilities: featureSets/update (the §5.3 policy exchange), mcpl/manifest,
 * channels/*, push/event, and context/beforeInference.
 *
 * Delivery model (per message the adapter hands over):
 *   - channel open by the host  → channels/incoming (batched)
 *   - channel closed, addressed → push/event with the closed-channel origin,
 *                                  carrying the missed-ambient tally so the
 *                                  host can show what staying out has cost
 *   - channel closed, ambient   → dropped and tallied (`channel_missed`)
 *
 * Every forward the host accepts advances a persisted per-channel
 * watermark; what is on its way to the host, or was offered and not
 * accepted, holds the watermark below it (the undelivered floor). What the
 * host has not accepted is offered again — by a
 * live replay from history once the host answers, by the catch-up sweep on
 * the next connection, or by the gap recovery a Zulip event-queue expiry
 * triggers. Nothing is lost to a later acceptance.
 *
 * One connection at a time. `serve()` resolves when the peer disconnects;
 * every connection starts from a fresh grant, registration and sweep.
 */

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
import { isDmKey, streamIdOfKey, streamKey, toWire, type ChannelKey, type KeyedMessage } from './channel-key.js';
import { ChannelIds } from './channel-ids.js';
import { ChannelManager, type HostClient, type RegisterAdditionalResult } from './channels.js';
import { StreamDirectory, type DirectoryChange } from './stream-directory.js';
import { ContextProvider } from './context.js';
import { Lanes, type Lane } from './lanes.js';
import { DeliveryState, attributeMessage, renderMissedBlock, selectMissed, viewOf, isAddressed, DEFAULT_MISSED_BLOCK_MAX_CHARS } from './delivery.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import { MESSAGING_FEATURE_SET, buildServerCapabilities, featureSetForTool } from './feature-sets.js';
import { isDmChannelId, parseDmChannelId } from './history.js';
import { LONG_TIMEOUT_MS } from './zulip-api.js';
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

/** MCP protocol revisions this server answers with verbatim. Anything else
 *  is answered with the oldest, which every client can speak. */
/** How long an announcement waits for the host. A host that takes
 *  `channels/changed` as a notification never answers it, and the channel's
 *  messages wait this long behind it. */
const ANNOUNCE_TIMEOUT_MS = 5_000;

const REMOVED_LINE = '';

const KNOWN_MCP_PROTOCOL_VERSIONS = new Set(['2024-11-05', '2025-03-26', '2025-06-18']);
const FALLBACK_MCP_PROTOCOL_VERSION = '2024-11-05';

/** Messages kept around each addressed message in a catch-up block for a closed channel. */
const MISSED_VICINITY = 7;
/** Hard ceiling on what one channel's catch-up fetches. */
const CATCHUP_HARD_CAP = 10_000;
export const DEFAULT_CATCHUP_LIMIT = 3000;
const HISTORY_ON_OPEN_CAP = 500;
/** Live events received before the catch-up sweep has run are held, so the
 *  sweep's "everything after the watermark" is not pre-empted by a live
 *  delivery that would jump the watermark over the offline gap. Beyond the
 *  cap the oldest are dropped from the buffer but stay held below the
 *  watermark, so the replay after go-live recovers them from history. */
const PRE_LIVE_BUFFER_CAP = 5000;
/** How long shutdown waits for deliveries still building their content
 *  (attachment fetches) before flushing what has been queued. */
const SHUTDOWN_INFLIGHT_GRACE_MS = 5000;
const ATTACHMENT_DEADLINE_MS = 15_000;

export interface ZulipMcplServerOptions {
  /** Server name/version reported in `initialize`. */
  serverInfo: { name: string; version: string };
  /** `false` forces plain-MCP mode even for hosts that advertise MCPL. */
  mcplEnabled?: boolean;
  /** channels/incoming batching window. */
  batchWindowMs?: number;
  /** Messages injected per open channel on context/beforeInference. */
  contextHistorySize?: number;
  /** Where delivery state (watermarks, tallies) persists. null = in-memory. */
  stateDir?: string | null;
  /** Session id the delivery state file is keyed by. */
  sessionId?: string;
  /** Per-channel ceiling for the reconnect sweep and gap recovery. */
  catchupLimit?: number;
  /** Renders timestamps in agent-visible catch-up lines. Default: AGENT_TIMEZONE / AGENT_TIMESTAMP_STYLE. */
  formatTime?: (d: Date) => string;
  /** The filters plane (stream/DM allowlists, mutes, reaction policy). Optional: without it nothing is filtered. */
  filters?: FiltersPlane;
  /** Where attachment bytes come from, and how much of them to inline on live delivery. `deadline` gives one bound for all of a message's fetches together; 15 s from the first when left out. */
  attachments?: { source: AttachmentSource; inline: InlineOptions; deadline?: () => AbortSignal };
  /** Size cap (characters) on one `<missed>` catch-up block; the oldest lines are elided. */
  missedBlockMaxChars?: number;
  /** How long an announcement waits for the host (default 5s). */
  announceTimeoutMs?: number;
  /** Render `[time id=N] [#stream > topic] Author: ` into delivered bodies (live, push, recovered replays; default true). `false` for a host that renders the structured fields itself. */
  attributeDelivery?: boolean;
  directory?: StreamDirectory;
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
  readonly directory: StreamDirectory;
  readonly ids: ChannelIds;
  readonly stateTracker = new StateTracker();

  private detachManifest: (() => void) | null = null;
  private eventsStarted = false;
  private sweepDone = false;
  /** Live delivery is gated until registration and the catch-up sweep are done. */
  private live = false;
  private preLive: { message: KeyedMessage; newChannel?: ChannelDescriptor }[] = [];
  private preLiveDropped = 0;
  /** Live-verified replies not yet accepted: history can fail closed under a
   *  tighter/shared lookup budget without erasing the verdict held for retry.
   *  Oldest-first eviction, capped per channel like the pre-live buffer. */
  private pendingReplies = new Map<ChannelKey, Set<number>>();
  /** Commit scan-only advancement after buffered live verdicts are reconciled. */
  private preLiveScanned = new Map<ChannelKey, number>();
  /** Watermarks as they stood when the connection began: what the sweep
   *  catches up from, whatever moves them meanwhile (a channel_open with
   *  backscroll in the first seconds must not hide the offline gap). */
  private sweepAnchors = new Map<ChannelKey, number>();
  private lanes: Lanes<null> | null = null;
  private requestTail: Promise<void> = Promise.resolve();
  private requestsWaiting = 0;
  private readonly pushesOut = new Set<Promise<unknown>>();
  private readonly lifecycleOnChain = new Map<string, number>();
  private readonly heldReadWaiting = new WeakSet<Lane<null>>();
  private readonly catchupLimit: number;
  private readonly missedBlockMaxChars: number;
  private readonly formatTime: (d: Date) => string;
  /** Distinguishes change markers minted within the same second. */
  private changeSeq = 0;
  private readonly attributeDelivery: boolean;
  private readonly announceTimeoutMs: number;
  private readonly filters: FiltersPlane | null;
  private renamedUnannounced = new Map<ChannelKey, { from: string; to: string }>();
  private toldAgent = new Map<ChannelKey, string>();
  private reconciling = false;
  private reconcileAgain = false;

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
    this.directory = options.directory ?? new StreamDirectory();
    this.ids = new ChannelIds(this.directory, (why) => this.adapter.refreshStreams?.(why) ?? Promise.resolve());
    this.delivery = new DeliveryState(options.stateDir ?? null, options.sessionId ?? 'default', this.directory);
    this.directory.onChange((change) => this.onDirectoryChange(change));
    this.delivery.onResolved = (keys) => {
      if (this.sweepDone) return;
      for (const key of keys) if (!this.sweepAnchors.has(key)) this.sweepAnchors.set(key, this.delivery.watermark(key)!);
    };
    // A widened stream allowlist means channels the host has never seen:
    // make them known. A narrowed one is enforced on every delivery surface
    // (`isAllowed`); the host keeps its descriptors (a reopen would
    // re-announce nothing). Either way, what is held for a channel that may
    // no longer reach the agent is let go.
    this.filters?.onChange((next, prev) => {
      const before = new Set(prev.streams ?? []);
      const widened = !next.streams || (next.streams ?? []).some((name) => prev.streams && !before.has(name));
      if (widened) void this.applyFilterChange();
      this.enterHeldReads();
      if (this.directory.isFilled) this.logUnmatchedFilters();
    });
    void this.directory.whenFilled().then(() => this.logUnmatchedFilters());

    // Derived from the adapter rather than restated, so `channels.typing`
    // cannot be advertised when the adapter does not implement it (§6.4).
    const manifest = buildServerCapabilities({ typing: typeof adapter.sendTyping === 'function' });

    // The manifest is what `initialize` presents (§5.1) and what `mcpl/manifest`
    // returns (§17.4). Building it through the tracker stamps the canonical
    // content digest (§17.2) onto the same snapshot both paths serve.
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
    this.channelManager = new ChannelManager(host, this.adapters, this.grant, this.ids, options.batchWindowMs, {
      // The only place an open channel's watermark moves: on the host's
      // itemized acceptance of a channels/incoming batch.
      onDelivered: (key, accepted, rejected) => this.answerOn(this.lanes?.lane(key), () => this.onDelivered(key, accepted, rejected)),
      // Policy is rechecked when the batch goes out, not only when it was
      // queued: a mute, a narrowed allowlist or a disabled feature set that
      // landed during the batch window applies to what is still waiting.
      deliverable: (message) => this.mayDeliver(message),
      onWithheld: (key, messages) => this.answerOn(this.lanes?.lane(key), () => this.onWithheld(key, messages)),
      onGivenUp: (key, messages) => this.answerOn(this.lanes?.lane(key), () => this.onGivenUp(key, messages)),
    });
    this.contextProvider = new ContextProvider(this.channelManager, this.grant, options.contextHistorySize, {
      excludeChannel: (key) => this.isMuted(key) || !this.isAllowed(key),
    });
    // Sends made through the tool surface and through publish are part of the rollback record.
    this.tools.onSent = (sent) => this.stateTracker.recordSent(sent.messageId, sent.channelId, sent.content);
    this.adapter.onSent = (sent) => this.stateTracker.recordSent(sent.messageId, sent.channelId, sent.content);
    // A stream `listen` just subscribed the bot to is registered at once, so
    // the host can open it without waiting for a message or a refresh (#20).
    // Only those streams: a realm enumeration here would cost a full stream
    // list plus a DM-history fetch to announce what the caller already named.
    this.tools.onSubscribed = (streams) => this.registerSubscribed(streams);
    // A deletion made through the tool surface echoes back as a delete event
    // without an actor; the adapter recognises its own.
    this.tools.onDeleted = (messageId) => this.adapter.noteSelfDeleted?.(Number(messageId));
    this.tools.onDeleteFailed = (messageId) => this.adapter.forgetSelfDeleted?.(Number(messageId));
  }

  /** True when the connected peer negotiated MCPL. */
  get mcplMode(): boolean {
    return this.mcplActive;
  }

  /** True once registration and the catch-up sweep are done and live
   *  delivery is flowing (before that, inbound events are held). */
  get isLive(): boolean {
    return this.live;
  }

  // ── Serve loop ──

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

    // The peer is gone: stop events, let what is in flight settle, try the
    // last flush (it fails harmlessly on a closed connection), persist.
    await this.quiesce();
    this.detachManifest?.();
    this.detachManifest = null;
    this.conn = null;
  }

  /**
   * Every connection is a fresh session over the persisted state: an empty
   * grant until this peer's §5.3 policy arrives, nothing registered or open,
   * no live delivery until this peer's sweep has run. Nothing of a previous
   * peer's grant, registration or sweep carries over.
   */
  private beginConnection(): void {
    this.commitScannedCatchUp();
    this.directory.restart(this.delivery.directoryCopy());
    this.grant.reset();
    this.channelManager.reset();
    this.live = false;
    this.sweepDone = false;
    this.preLive = [];
    this.preLiveDropped = 0;
    this.pendingReplies.clear();
    this.preLiveScanned.clear();
    this.toldAgent.clear();
    this.lanes = new Lanes(null, { maxWaitingMessages: Infinity, onOverflow: () => {} });
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
      //
      // Runs concurrently with the loop: the policy arrives as a Request the
      // loop must read and answer, and channels/register is itself a
      // server→host Request the host answers only once policy is settled.
      // The catch-up sweep follows registration so its pushes land on
      // registered channels inside the granted window; live delivery opens
      // only after the sweep, so nothing received meanwhile can advance a
      // watermark over the offline gap the sweep is about to fetch.
      void this.grant.whenReady().then(async () => {
        if (conn.isClosed) return;
        try {
          await this.channelManager.registerChannels();
        } catch (error) {
          console.error('Failed to register channels after initial policy:', error);
        }
        if (conn.isClosed) return;
        const renamed = await this.pushRenamedUnannounced(conn);
        if (conn.isClosed) return;
        void this.reconcileHostIds();
        try {
          await this.runReconnectSweep(conn);
        } catch (error) {
          console.error('[zulip-mcp] Reconnect catch-up sweep failed:', error);
        }
        if (conn.isClosed) return;
        this.closeRenamed(renamed);
        await this.goLive(conn);
      });

      // The event queue is registered at once — Zulip delivers nothing that
      // predates the queue, so every second of delay here is a second of
      // messages that no sweep can recover for a never-watermarked channel.
      this.startEvents();
    }

    this.requestTail = Promise.resolve();
    try {
      while (!conn.isClosed) {
        const msg = await conn.nextMessage();
        if (msg.type === 'request' && this.enterHostQueue(conn, msg.request)) continue;
        const lifecycleId = msg.type === 'request' ? lifecycleChannelId(msg.request) : null;
        if (lifecycleId !== null) this.lifecycleOnChain.set(lifecycleId, (this.lifecycleOnChain.get(lifecycleId) ?? 0) + 1);
        this.requestsWaiting++;
        this.requestTail = this.requestTail.then(async () => {
          try {
            if (conn.isClosed) return;
            await (msg.type === 'request'
              ? this.handleRequest(conn, msg.request)
              : this.handleNotification(conn, msg.notification));
          } finally {
            this.requestsWaiting--;
            if (lifecycleId !== null) {
              const left = (this.lifecycleOnChain.get(lifecycleId) ?? 1) - 1;
              if (left > 0) this.lifecycleOnChain.set(lifecycleId, left);
              else this.lifecycleOnChain.delete(lifecycleId);
            }
          }
        });
      }
    } finally {
      await this.requestTail;
    }
  }

  /**
   * Settles once nothing is under way: no delivery job runs or waits, no
   * request waits to be served, no batch waits for its window or is on the
   * wire, no push waits for its answer, and no replay runs. For tests and
   * diagnostics.
   */
  async quiet(): Promise<void> {
    while (!this.isQuiet()) {
      await Promise.all([
        this.lanes?.quiet(),
        this.requestTail,
        Promise.allSettled(this.pushesOut),
        this.channelManager.idle(),
      ]);
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  private isQuiet(): boolean {
    return (this.lanes?.isQuiet() ?? true) && this.requestsWaiting === 0 && this.pushesOut.size === 0 && this.channelManager.isIdle();
  }

  private enterHostQueue(conn: McplConnection, req: JsonRpcRequest): boolean {
    const channelId = lifecycleChannelId(req);
    if (channelId === null || !this.mcplActive || !this.lanes || this.lifecycleOnChain.has(channelId)) return false;
    if (!this.grant.has('channels.lifecycle')) return false;
    const answer = this.ids.resolve(channelId);
    if (!answer || !('key' in answer)) return false;
    const key = answer.key;
    if (req.method === method.CHANNELS_CLOSE) {
      const close = req.params as unknown as ChannelsCloseParams;
      this.serveInLane(conn, req, key, () => this.handleChannelClose(close));
      return true;
    }
    const open = req.params as unknown as ChannelsOpenParams;
    let descriptor: ChannelDescriptor;
    try {
      descriptor = this.channelManager.findChannel(open);
    } catch {
      return false;
    }
    if (this.ids.keyOfDescriptor(descriptor) !== key) return false;
    this.serveInLane(conn, req, key, () => this.openChannel(open, descriptor, key));
    return true;
  }

  private serveInLane(conn: McplConnection, req: JsonRpcRequest, key: ChannelKey, serve: () => unknown): void {
    this.lanes?.lane(key).host(async () => {
      if (conn.isClosed) return;
      try {
        conn.sendResponse(req.id, await serve());
      } catch (err) {
        this.answerError(conn, req, err);
      }
    });
  }

  /**
   * Stop platform event delivery, wait (bounded) for deliveries still
   * building their content, push out anything still batched, and persist
   * delivery state. A flush over a connection that is already gone fails
   * harmlessly: those watermarks stay put and the next sweep re-fetches.
   * Idempotent.
   */
  async shutdown(): Promise<void> {
    await this.quiesce();
  }

  private async quiesce(): Promise<void> {
    if (this.eventsStarted) {
      this.adapter.stopEvents();
      this.eventsStarted = false;
    }
    const stillRunning = (await this.lanes?.end(SHUTDOWN_INFLIGHT_GRACE_MS)) ?? 0;
    if (stillRunning > 0) {
      console.error(`[zulip-mcp] ${stillRunning} delivery(ies) still in flight at shutdown; left for the next catch-up sweep`);
    }
    try {
      await this.channelManager.flush();
    } catch (error) {
      console.error('[zulip-mcp] final flush failed:', (error as Error).message ?? error);
    }
    this.channelManager.destroy();
    this.delivery.failQueued();
    this.commitScannedCatchUp();
    this.delivery.save();
  }

  /**
   * Release the pre-live buffer through the normal routing, then open live
   * delivery. Held messages the sweep already covered — id at or below the
   * channel's accepted/scanned boundary when the sweep finished — are skipped
   * unless their buffered reply verdict still awaits acceptance;
   * the boundary is taken once, before draining, because draining itself
   * moves watermarks. Delivery stays gated until the buffer is empty, so a
   * live message arriving mid-drain (while a held one waits on an
   * attachment fetch) joins the queue behind the held ones instead of
   * overtaking them and burying them under its own acceptance.
   */
  private async goLive(conn: McplConnection): Promise<void> {
    if (this.live) return;
    const boundary = new Map(this.delivery.watermarkedChannels().map((id) => [id, this.delivery.watermark(id)!]));
    for (const [channelId, through] of this.preLiveScanned) {
      boundary.set(channelId, Math.max(boundary.get(channelId) ?? 0, through));
    }
    while (this.preLive.length > 0) {
      const { message, newChannel } = this.preLive.shift()!;
      const watermark = boundary.get(message.key);
      const id = Number(message.messageId);
      if (watermark !== undefined && Number.isFinite(id) && id <= watermark) {
        const replyToSelf = (message.metadata as Record<string, unknown> | undefined)?.replyToSelf === true;
        // Only acceptance (or the original watermark) deduplicates a
        // live-verified reply that history could not verify.
        const pendingReply = replyToSelf && this.pendingReplies.get(message.key)?.has(id);
        if (!pendingReply || id <= (this.sweepAnchors.get(message.key) ?? 0)) {
          continue;
        }
      }
      try {
        this.delivery.hold(message.key, id, 'queued');
        await this.route(message, newChannel);
      } catch (err) {
        console.error('[zulip-mcp] delivery of a held message failed:', (err as Error).message);
      } finally {
        // A policy return or construction error may settle without queueing.
        // Transfer any still-pending reply to existing replay ownership.
        if (this.pendingReplies.get(message.key)?.has(id) &&
            !this.delivery.heldIds(message.key).includes(id)) {
          this.delivery.hold(message.key, id, 'failed');
        }
      }
      if (conn.isClosed) return;
    }
    this.commitScannedCatchUp();
    this.delivery.save();
    this.live = true;
    if (this.preLiveDropped > 0) {
      console.error(`[zulip-mcp] ${this.preLiveDropped} live message(s) exceeded the pre-live buffer; held below the watermark for replay`);
      this.preLiveDropped = 0;
    }
    this.enterHeldReads();
  }

  private commitScannedCatchUp(): void {
    for (const [key, scannedThrough] of this.preLiveScanned) {
      // Pending verdicts outlive buffer membership: routing can settle
      // without acceptance (temporary policy disable or an early error).
      let through = scannedThrough;
      const held = new Set(this.delivery.heldIds(key));
      for (const id of this.pendingReplies.get(key) ?? []) {
        if (!held.has(id)) through = Math.min(through, id - 1);
      }
      // A refused buffered reply must keep its hold below the scanned range.
      for (const id of held) {
        if (id <= through && !this.pendingReplies.get(key)?.has(id)) {
          this.delivery.release(key, id);
        }
      }
      this.delivery.advance(key, through);
    }
    this.preLiveScanned.clear();
  }

  // ── Handshake ──

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
    this.tools.channelIds = this.mcplActive ? this.ids : null;
    this.delivery.writes = this.mcplActive;

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
    // first request) follows. Anything that is not the initialized
    // notification is handed to the main loop untouched.
    if (!this.mcplActive && !this.directory.isFilled && this.adapter.refreshStreams) {
      void this.adapter.refreshStreams('a client without MCPL', true).then(() => {
        if (!this.directory.isFilled) this.directory.listUnanswered();
      });
    }

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

  // ── Requests ──

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
          // A feature set enabled again: what was withheld under the
          // reduction is offered again.
          this.enterHeldReads();
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
          const { descriptor, key } = await this.openTarget(open);
          this.serveInLane(conn, req, key, () => this.openChannel(open, descriptor, key));
          break;
        }

        case method.CHANNELS_CLOSE: {
          this.requireMcpl();
          const close = params as unknown as ChannelsCloseParams;
          const answer = typeof close.channelId === 'string' ? this.ids.resolve(close.channelId) : null;
          if (answer && 'key' in answer) this.serveInLane(conn, req, answer.key, () => this.handleChannelClose(close));
          else conn.sendResponse(req.id, this.handleChannelClose(close));
          break;
        }

        case method.CHANNELS_PUBLISH: {
          this.requireMcpl();
          const publish = params as unknown as ChannelsPublishParams;
          // No checkpoint is minted here: the publish result has no field to
          // carry one (§14.6), so a host can only learn checkpoints from tool
          // results — see callTool.
          const result = await this.channelManager.publish(publish);
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
          const ackKey = await this.ids.keyOf(ack.channelId);
          if ('refusal' in ackKey) throw new McplRpcError(ERR_UNKNOWN_CHANNEL, ackKey.refusal, { channelId: ack.channelId });
          try {
            const representation = await this.adapter.acknowledge('key' in ackKey ? ackKey.key : null, ack.messageId, ack.value);
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
      this.answerError(conn, req, err);
    }
  }

  private answerError(conn: McplConnection, req: JsonRpcRequest, err: unknown): void {
    if (err instanceof McplRpcError) {
      conn.sendError(req.id, err.code, err.message, err.data);
      return;
    }
    const e = err as Error;
    console.error(`[zulip-mcp] handleRequest error: method=${req.method}`, e.stack ?? e.message);
    conn.sendError(req.id, -32603, e.message ?? String(err));
  }

  // ── Notifications ──

  private async handleNotification(conn: McplConnection, notif: JsonRpcNotification): Promise<void> {
    const params = (notif.params ?? {}) as Record<string, unknown>;
    try {
      switch (notif.method) {
        case 'notifications/initialized':
        case 'notifications/cancelled':
        case 'notifications/roots/list_changed':
          break;

        case method.FEATURE_SETS_UPDATE:
          // §6.7 Notification form: descriptive metadata only. Grant-bearing
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

  // ── Channel lifecycle ──

  private async openTarget(params: ChannelsOpenParams): Promise<{ descriptor: ChannelDescriptor; key: ChannelKey }> {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    if (params.channelId) {
      const answer = await this.ids.keyOf(params.channelId);
      if ('refusal' in answer) throw new McplRpcError(ERR_UNKNOWN_CHANNEL, answer.refusal, { channelId: params.channelId });
    }
    const descriptor = await this.findChannelRediscovering(params);
    const key = this.ids.keyOfDescriptor(descriptor);
    if (key === null) throw new McplRpcError(ERR_UNKNOWN_CHANNEL, `Unknown channel: ${descriptor.id}`, { channelId: descriptor.id });
    return { descriptor, key };
  }

  private async openChannel(params: ChannelsOpenParams, descriptor: ChannelDescriptor, key: ChannelKey): Promise<ChannelsOpenResult> {
    if (!this.grant.has('channels.lifecycle')) throw capabilityDenied('channels.lifecycle');
    // Opening a channel is proof the host knows it — the only proof available
    // when the answer to its announcement cannot reach us (#20 review).
    this.channelManager.confirmAnnounced(descriptor.id);
    const result: ChannelsOpenResult = { channel: descriptor };

    // Zulip only delivers stream events to subscribers; an open channel the
    // bot is not subscribed to would be listening to silence. So the
    // subscription comes FIRST, and a refusal (private stream, permission,
    // rate limit) fails the open: the host records the operation as failed
    // and the agent's tool result says why, instead of an open channel that
    // hears nothing.
    if (this.adapter.ensureSubscribed) {
      try {
        await this.adapter.ensureSubscribed(key);
      } catch (err) {
        throw new McplRpcError(
          ERR_CHANNEL_OPEN_FAILED,
          `Cannot open ${descriptor.id}: ${(err as Error).message}`,
          { channelId: descriptor.id },
        );
      }
    }

    // Requested history is fetched BEFORE the lifecycle is committed, so a
    // failed open cannot leave the channel open while the host records the
    // operation as failed. A muted stream, or one outside the allowlist,
    // yields none: nothing from it reaches the agent, backscroll included.
    const requested = Math.max(0, Math.floor(params.history?.limit ?? 0));
    let page: { messages: KeyedMessage[]; older: boolean; sinceLastSeen: boolean } | null = null;
    if (requested > 0 && this.adapter.fetchHistory && (this.isMuted(key) || !this.isAllowed(key))) {
      result.history = [];
      result.historyTruncated = false;
    } else if (requested > 0 && this.adapter.fetchHistory) {
      const cap = Math.min(HISTORY_ON_OPEN_CAP, historyCapOf(descriptor));
      const limit = Math.min(requested, cap);
      const watermark = this.delivery.watermark(key);
      // Since the last seen message wins: a request that also names a
      // message to page back from reads forward from the watermark.
      const afterMessageId =
        params.history?.sinceLastSeen && watermark !== undefined ? String(watermark) : undefined;
      const beforeMessageId = afterMessageId === undefined ? params.history?.beforeMessageId : undefined;
      const firstWaiting = this.firstWaiting(key);
      const readBefore = afterMessageId === undefined && Number.isFinite(firstWaiting) && !(Number(beforeMessageId) < firstWaiting)
        ? String(firstWaiting)
        : beforeMessageId;
      const read = await this.adapter.fetchHistory(key, { limit, beforeMessageId: readBefore, afterMessageId });
      const below = Math.min(firstWaiting, this.firstWaiting(key));
      const messages = read.messages.filter((m) => !(Number(m.messageId) >= below));
      // Not attributed: the host hands this back as the channel_open tool
      // result, JSON that already shows author, threadId and metadata.
      result.history = this.projectHistoryReactions(messages).map((m) => toWire(m, descriptor.id));
      result.historyTruncated = requested > limit;
      page = { messages, older: beforeMessageId !== undefined, sinceLastSeen: afterMessageId !== undefined };
    }

    this.channelManager.markOpen(descriptor.id);
    if (page) {
      const newest = Math.max(...page.messages.map((m) => Number(m.messageId)).filter(Number.isFinite));
      if (Number.isFinite(newest) && (page.sinceLastSeen || this.placeOf(key) === undefined)) this.decide(key, newest);
      const place = this.placeOf(key);
      // Handed to the host in this very response: forwarded, up to the lane's place.
      for (const m of page.messages) if (place !== undefined && Number(m.messageId) <= place) this.accepted(key, Number(m.messageId));
      // This history goes to the agent, which answers it through
      // channels/publish, so its newest message gives the topic. A page
      // fetched before a given message is older history, read for context,
      // and moves nothing.
      if (!page.older) {
        for (let i = page.messages.length - 1; i >= 0; i--) {
          if (this.channelManager.noteConversation(key, page.messages[i])) break;
        }
      }
    }
    this.delivery.markOpen(key);
    this.delivery.save();
    return result;
  }

  private firstWaiting(key: ChannelKey): number {
    return Math.min(Infinity, ...(this.lanes?.lane(key).waitingMessages() ?? []).filter(Number.isFinite));
  }

  /**
   * `findChannel`, but an unknown exact `channelId` gets one re-discovery
   * before the open fails: a stream created (or made visible) after startup
   * is not in the registry until something announces it, and the agent
   * asking to open it is exactly that something. Only a miss re-discovers,
   * so opening a known channel costs no extra round trip.
   */
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

  /** The delivered form of a message: attributed unless the host asked for bare bodies. */
  private attributed(message: KeyedMessage): KeyedMessage {
    return this.attributeDelivery ? attributeMessage(message, message.streamNameShown ?? this.streamNameOf(message.key), this.formatTime) : message;
  }

  private label(key: ChannelKey): string {
    return this.ids.channelIdOf(key) ?? this.ids.freshIdOf(key) ?? key;
  }

  private streamNameOf(key: ChannelKey): string | null {
    const streamId = streamIdOfKey(key);
    return streamId === null ? null : this.directory.nameOf(streamId) ?? null;
  }

  private handleChannelClose(params: ChannelsCloseParams): { closed: boolean } {
    // As with open: the host named a channel, so it has it. agent-framework
    // reconciles a channel it does not want with `channels/close`.
    this.channelManager.confirmAnnounced(params.channelId);
    const result = this.channelManager.closeChannel(params);
    const key = this.channelManager.keyOfHeld(params.channelId);
    if (key !== null && this.ids.channelIdOf(key) === params.channelId) {
      this.delivery.markClosed(key);
      this.delivery.save();
    }
    return result;
  }

  // ── Inbound delivery ──

  /** A muted stream, by its current name: nothing from it reaches the agent on any surface. */
  private isMuted(key: ChannelKey): boolean {
    const name = this.streamNameOf(key);
    return this.filters !== null && name !== null && this.filters.streamMuted(name);
  }

  /**
   * The allowlists, on every surface: a stream outside `streams`, or a DM
   * conversation none of whose parties may DM the bot, delivers nothing —
   * live, replayed, swept, injected or backscrolled. With a message in hand
   * a DM is judged by its sender; without one (a per-channel surface) by the
   * conversation's registered participants, and a conversation this server
   * has not described is left to the per-message check in the adapter.
   */
  private isAllowed(key: ChannelKey, message?: KeyedMessage): boolean {
    if (!this.filters) return true;
    if (!isDmKey(key)) {
      const name = this.streamNameOf(key);
      return name !== null && this.filters.streamAllowed(name);
    }
    const sender = message ? senderOf(message) : null;
    if (sender) return this.filters.dmAllowed(sender);
    const meta = this.channelManager.getChannel(this.ids.channelIdOf(key) ?? '')?.metadata as { participants?: unknown } | undefined;
    const participants = Array.isArray(meta?.participants) ? (meta!.participants as { id?: unknown; email?: unknown }[]) : [];
    if (participants.length === 0) return true;
    return participants.some((p) => this.filters!.dmAllowed({ id: Number(p.id), email: typeof p.email === 'string' ? p.email : '' }));
  }

  /**
   * Send-time policy for a queued message (see the ChannelManager hooks). A
   * system marker (a delivery gap, degraded polling) is about the
   * connection, not the stream it rides on: it passes mute and allowlist,
   * and only a disabled feature set withholds it.
   */
  private mayDeliver(message: KeyedMessage): boolean {
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return false;
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    if (meta.system === true) return true;
    return !this.isMuted(message.key) && this.isAllowed(message.key, message);
  }

  private forgetPendingReply(key: ChannelKey, id: number): void {
    const pending = this.pendingReplies.get(key);
    pending?.delete(id);
    if (pending?.size === 0) this.pendingReplies.delete(key);
  }

  /** The host accepted (or was handed) a message: its hold is released and the watermark may advance. */
  private accepted(key: ChannelKey, id: number): boolean {
    if (!Number.isFinite(id) || id <= 0) return false;
    this.forgetPendingReply(key, id);
    const released = this.delivery.release(key, id);
    return this.delivery.advance(key, id) || released;
  }

  /**
   * The host answered a channels/incoming batch. Accepted messages release
   * their holds and the watermark advances towards the highest of them —
   * never past a message still held: one the host rejected here, one
   * queued behind, or one given up on earlier. A rejected message stays
   * below the watermark until something delivers it again.
   */
  private onDelivered(key: ChannelKey, accepted: KeyedMessage[], rejected: KeyedMessage[]): void {
    let moved = false;
    let high = 0;
    for (const m of accepted) {
      const id = numericId(m);
      if (id === null) continue;
      this.forgetPendingReply(key, id);
      moved = this.delivery.release(key, id) || moved;
      high = Math.max(high, id);
    }
    for (const m of rejected) {
      const id = numericId(m);
      if (id !== null) this.notAccepted(key, id, 'rejected', m);
    }
    if (high > 0) moved = this.delivery.advance(key, high) || moved;
    if (moved) this.delivery.save();
    if (accepted.length > 0) this.enterHeldReads();
  }

  /** A batch outlived its transport retry: the host never answered. Held for replay. */
  private onGivenUp(key: ChannelKey, messages: KeyedMessage[]): void {
    for (const m of messages) {
      const id = numericId(m);
      if (id !== null) this.notAccepted(key, id, 'failed', m);
    }
  }

  private notAccepted(key: ChannelKey, id: number, status: 'failed' | 'rejected', message?: KeyedMessage): void {
    if (this.isMuted(key) || !this.isAllowed(key, message)) {
      this.forgetPendingReply(key, id);
      this.letGo(key, id);
    } else {
      this.delivery.hold(key, id, status);
    }
  }

  /**
   * Policy withheld queued messages. A mute or a narrowed allowlist means
   * they may never reach the agent: let them go. A disabled feature set
   * means "not now": they stay held, replayed once it is enabled again.
   */
  private onWithheld(key: ChannelKey, messages: KeyedMessage[]): void {
    let moved = false;
    for (const m of messages) {
      const id = numericId(m);
      if (id === null) continue;
      if (this.isMuted(key) || !this.isAllowed(key, m)) {
        this.forgetPendingReply(key, id);
        moved = this.delivery.release(key, id) || moved;
      } else this.delivery.hold(key, id, 'failed');
    }
    if (moved) this.delivery.save();
  }

  /** Queue a message for an open channel, holding its id until the host accepts it. */
  private deliverToOpen(key: ChannelKey, message: KeyedMessage): boolean {
    if (!this.channelManager.isKeyOpen(key)) return false;
    const id = numericId(message);
    if (id !== null) this.delivery.hold(key, id, 'queued');
    this.channelManager.onIncomingMessage(key, message);
    return true;
  }

  /**
   * One message from the adapter. Before live delivery opens (registration
   * and the sweep pending) it is held, in order, bounded — the oldest beyond
   * the cap leave the buffer but stay held below the watermark, and the
   * replay after go-live fetches them back. Live, it enters its channel's
   * lane, held until it is decided.
   */
  private onIncoming(message: KeyedMessage, newChannel?: ChannelDescriptor): void {
    const id = numericId(message);
    if ((message.metadata as Record<string, unknown> | undefined)?.replyToSelf === true && id !== null &&
        !this.isMuted(message.key) && this.isAllowed(message.key, message)) {
      let pending = this.pendingReplies.get(message.key);
      if (!pending) this.pendingReplies.set(message.key, pending = new Set());
      if (!pending.has(id) && pending.size >= PRE_LIVE_BUFFER_CAP) {
        const oldest = pending.values().next().value;
        if (oldest !== undefined) pending.delete(oldest);
      }
      pending.add(id);
    }
    if (!this.live) {
      if (this.preLive.length >= PRE_LIVE_BUFFER_CAP) {
        const dropped = this.preLive.shift()!;
        const id = numericId(dropped.message);
        if (id !== null) this.delivery.hold(dropped.message.key, id, 'failed');
        this.preLiveDropped++;
      }
      this.preLive.push({ message, newChannel });
      return;
    }
    if (this.lanes) this.enterMessage(this.lanes, message, newChannel);
  }

  private enterMessage(lanes: Lanes<null>, message: KeyedMessage, newChannel: ChannelDescriptor | undefined): void {
    const lane = lanes.lane(message.key);
    if (lane.isEnded) return;
    const id = numericId(message);
    const place = this.placeOf(message.key);
    if (id !== null && place !== undefined && id <= place) return;
    if (id !== null) this.delivery.hold(message.key, id, 'queued');
    lane.enter(() => this.route(message, newChannel, lane), { kind: 'message', ...(id !== null ? { id } : {}) });
  }

  private letGo(key: ChannelKey, id: number): void {
    if (this.delivery.release(key, id)) this.delivery.save();
  }

  private placeOf(key: ChannelKey): number | undefined {
    const own = this.lanes?.lane(key).place;
    const watermark = this.delivery.watermark(key);
    if (own === undefined) return watermark;
    return watermark === undefined ? own : Math.max(own, watermark);
  }

  private decide(key: ChannelKey, id: number): void {
    if (Number.isFinite(id)) this.lanes?.lane(key).decide(id);
  }

  /**
   * Route one message. See the delivery model in the file header. A forward
   * advances the watermark once the host has accepted it; an ambient message
   * dropped on a closed channel does not, so the next sweep can still find
   * it if the host opens the channel meanwhile.
   */
  private async route(message: KeyedMessage, newChannel?: ChannelDescriptor, lane?: Lane<null>): Promise<void> {
    const key = message.key;
    const id = Number(message.messageId);
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const addressed = meta.mentioned === true || meta.isDM === true || meta.replyToSelf === true;

    // Muted stream, or outside the allowlist: nothing reaches the agent —
    // ambient AND mentions — and nothing is tallied, before any other
    // routing. Rechecked here (not only at the event) for what was held.
    if (this.isMuted(key) || !this.isAllowed(key, message)) {
      this.decide(key, id);
      return this.letGo(key, id);
    }

    // §6.7: a disabled zulip.messaging stops its traffic at once — incoming
    // and push alike. Held, not let go: the host asked not to hear it now,
    // which is not the same as having heard it.
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      this.decide(key, id);
      return this.delivery.hold(key, id, 'failed');
    }

    // A channel the host has never seen — a DM from someone new, or a stream
    // the bot was added to after startup (#20): make it a registered channel
    // first, so the host can open it and route a reply back to it. The
    // message that arrived is itself the proof that the channel is reachable.
    if (newChannel && this.ids.heldIdOf(key) === undefined) {
      if (lane) {
        void this.channelManager.registerAdditional([newChannel]);
        const entryId = numericId(message);
        lane.enterFirst(() => this.route(message, undefined, lane), { kind: 'message', ...(entryId !== null ? { id: entryId } : {}) });
        return;
      }
      await this.channelManager.registerAdditional([newChannel]);
    }

    // Live delivery shows what was shared: images downsampled to model-max,
    // small text files inline. The reference note stays so anything not
    // inlined can still be fetched.
    const refs = Array.isArray(meta.attachments) ? (meta.attachments as AttachmentRef[]) : [];
    if (refs.length > 0 && this.options.attachments) {
      const { source, inline, deadline = () => AbortSignal.timeout(ATTACHMENT_DEADLINE_MS) } = this.options.attachments;
      const blocks = await buildAttachmentBlocks(refs, source, inline, deadline());
      if (blocks.length > 0) message = { ...message, content: [...message.content, ...blocks] };
    }

    // Who said it, where and when, in the body the model reads. The
    // structured fields stay; this is what a host that renders only the
    // content blocks shows the model (the Discord surface does the same).
    // The missed tally below counts the body as written, not the header.
    const bodyText = viewOf(message).text;
    message = this.attributed(message);

    // The first message of a DM conversation carries an explicit reply
    // affordance: DMs have no subscription semantics, and the agent should
    // not have to discover the send path by trial.
    if (meta.isDM === true && this.delivery.watermark(key) === undefined) {
      const authorId = message.author.id;
      const note = `<system>Direct message from ${message.author.name} (user id ${authorId}). ` +
        `To reply, use send_dm(["${authorId}"]) or publish to channel ${this.ids.channelIdOf(key)}. ` +
        `DMs always reach you; there is nothing to subscribe to.</system>`;
      message = { ...message, content: [{ type: 'text', text: note }, ...message.content] };
    }

    this.decide(key, id);
    if (this.isMuted(key) || !this.isAllowed(key, message)) return this.letGo(key, id);

    // The watermark moves in onDelivered, once the host has accepted it.
    if (this.deliverToOpen(key, message)) return;

    if (addressed) {
      const answered = (delivered: boolean) => {
        if (delivered) {
          // The agent's answer goes back through channels/publish, so it goes
          // to this message's topic, as an answer on an open channel does.
          this.channelManager.noteConversation(key, message);
          if (this.accepted(key, id)) this.delivery.save();
          this.enterHeldReads();
        } else {
          this.notAccepted(key, id, 'failed', message);
        }
      };
      const sent = this.pushEvent(message, `zulip_msg_${message.messageId}`);
      if (lane) void sent.then((delivered) => this.answerOn(lane, () => answered(delivered)));
      else answered(await sent);
      return;
    }

    this.letGo(key, id);
    if (this.delivery.countMissed(key, { id, text: bodyText })) this.delivery.save();
  }

  private enterHeldReads(): void {
    if (!this.live || !this.lanes || !this.conn || !this.mcplActive) return;
    for (const key of this.delivery.heldChannels()) {
      const letGo = this.isMuted(key) || !this.isAllowed(key);
      if (!letGo && this.delivery.heldIds(key, { replayable: true }).length === 0) continue;
      const lane = this.lanes.lane(key);
      if (lane.isEnded || this.heldReadWaiting.has(lane)) continue;
      this.heldReadWaiting.add(lane);
      lane.enter((ctx) => {
        this.heldReadWaiting.delete(lane);
        if (!ctx.ended) return this.readHeld(key, lane);
      }, { kind: 'other', turn: true });
    }
  }

  private async readHeld(key: ChannelKey, lane: Lane<null>): Promise<void> {
    if (this.isMuted(key) || !this.isAllowed(key)) {
      for (const id of this.pendingReplies.get(key) ?? []) this.forgetPendingReply(key, id);
      this.delivery.releaseAll(key);
      this.delivery.save();
      return;
    }
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const place = this.placeOf(key);
    const ids = this.delivery.heldIds(key, { replayable: true }).filter((id) => place !== undefined && id <= place);
    if (ids.length === 0) return;
    try {
      if (this.channelManager.isKeyOpen(key)) {
        this.delivery.markReplayed(key, ids);
        await this.replayHeld(key, ids, lane);
      } else {
        const watermark = this.delivery.watermark(key);
        const after = (this.delivery.floor(key) ?? ids[0]) < ids[0] ? ids[0] - 1 : watermark ?? ids[0] - 1;
        await this.catchUpClosedChannel(key, this.delivery.wasOpen(key), after, undefined,
          (through) => this.delivery.markReplayed(key, ids.filter((id) => id <= through)), { through: place, lane });
      }
    } catch (err) {
      console.error(`[zulip-mcp] replay of undelivered messages on ${this.label(key)} failed:`, (err as Error).message);
    }
    this.delivery.save();
  }

  private async replayHeld(key: ChannelKey, ids: number[], lane: Lane<null>): Promise<void> {
    if (!this.adapter.fetchHistory || ids.length === 0) return;
    const wanted = new Set(ids);
    const fetched = await this.fetchAfter(key, ids[0] - 1, ids[ids.length - 1]);
    if (lane.isEnded) return;
    const found = new Set<number>();
    const msgs = fetched.messages.filter((m) => {
      const id = numericId(m);
      if (id === null || !wanted.has(id)) return false;
      found.add(id);
      return true;
    });
    // A held id the scan covered but did not find is gone from history (or
    // no longer visible to the agent): nothing left to deliver.
    const scanned = fetched.scannedThrough ?? 0;
    for (const id of ids) {
      if (!found.has(id) && (id <= scanned || fetched.complete)) {
        this.forgetPendingReply(key, id);
        this.delivery.release(key, id);
      }
    }
    const rows = this.projectHistoryReactions(msgs);
    for (const m of rows) {
      if (lane.isEnded) return;
      await this.route(recovered(m), undefined, lane);
    }
    if (rows.length > 0) console.error(`[zulip-mcp] ${this.label(key)}: replaying ${rows.length} undelivered message(s) from history`);
  }

  private answerOn(lane: Lane<null> | undefined, job: () => void): void {
    if (lane && !lane.isEnded) lane.host(job);
    else job();
  }

  /** Queue history back to an open channel as recovered delivery. Returns how many. */
  private replayOnOpen(key: ChannelKey, msgs: KeyedMessage[]): number {
    let n = 0;
    for (const m of this.projectHistoryReactions(msgs)) {
      // Watermarks move in onDelivered, once the host accepts the replay.
      // Attributed like live delivery: a recovered message is read the same way.
      const queued = this.deliverToOpen(key, this.attributed(recovered(m)));
      if (queued) n++;
    }
    return n;
  }

  /**
   * Addressed message on a closed channel → push/event. Requires the
   * `pushEvents` capability and an active zulip.messaging. The origin carries
   * the MCPL channel id (what the host registers and routes replies to) and
   * the missed-ambient tally, so the host's closed-channel invitation can
   * show what staying out has cost.
   */
  private async pushEvent(message: KeyedMessage, eventId: string, extraOrigin: Record<string, unknown> = {}, beforeOffer?: () => void): Promise<boolean> {
    const conn = this.conn;
    if (!conn || !this.mcplActive) return false;
    const channelId = this.ids.channelIdOf(message.key);
    if (channelId === null) return false;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      console.error(`[zulip-mcp] pushEvents not granted; dropping addressed message ${message.messageId} on closed ${channelId}`);
      return false;
    }
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const missed = this.delivery.tally(message.key);
    const params: PushEventParams = {
      featureSet: MESSAGING_FEATURE_SET,
      eventId,
      timestamp: message.timestamp,
      origin: {
        source: 'zulip',
        mcplChannelId: channelId,
        messageId: message.messageId,
        stream: message.streamNameShown ?? this.streamNameOf(message.key) ?? undefined,
        topic: meta.topic,
        authorId: message.author.id,
        authorName: message.author.name,
        isMention: meta.mentioned === true || meta.replyToSelf === true,
        isExplicitMention: meta.mentioned === true,
        isReplyToBot: meta.replyToSelf === true,
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
      // Local policy exits above are not offers. Once attempted, a refusal
      // or ambiguous transport failure must still consume the replay.
      beforeOffer?.();
      const answer = conn.sendRequest(method.PUSH_EVENT, params);
      this.pushesOut.add(answer);
      const settled = () => this.pushesOut.delete(answer);
      answer.then(settled, settled);
      const result = (await answer) as { accepted?: boolean; reason?: string } | undefined;
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

  /**
   * On (re)connect, deliver what arrived while the server was offline.
   * Channels the host had open get their full missed backscroll; every other
   * watermarked channel gets each addressed message with its vicinity. Deferred
   * quote verification can split a channel into multiple `<missed>` pushes.
   * Runs at most once per connection.
   */
  private async runReconnectSweep(conn: McplConnection): Promise<void> {
    if (this.sweepDone) return;
    this.sweepDone = true;
    if (!this.conn || !this.mcplActive || !this.adapter.fetchHistory || this.catchupLimit === 0) return;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;

    const candidates = new Set<ChannelKey>([
      ...this.sweepAnchors.keys(),
      ...this.delivery.lastOpenChannels(),
    ]);
    let delivered = 0;
    for (const key of candidates) {
      if (conn.isClosed) return;
      // From the watermark as it stood when this connection began: a
      // channel_open with backscroll answered meanwhile has moved the live
      // one past the offline gap this sweep exists to deliver.
      if (await this.catchUpClosedChannel(key, this.delivery.wasOpen(key), this.sweepAnchors.get(key), undefined, undefined, { conn })) delivered++;
    }

    if (conn.isClosed) return;
    await this.backfillMissedTallies(conn);
    this.delivery.save();
    if (delivered > 0) {
      console.error(`[zulip-mcp] Reconnect catch-up: delivered missed messages from ${delivered} channel(s)`);
    }
  }

  /**
   * Everything after `afterId` on a channel (through `throughId` when
   * given), oldest first, paginated up to the catch-up ceiling. The cursor
   * advances on what each page SCANNED, not on what it handed back: a page
   * shorter than asked, or empty of anything for the agent, is not "no
   * more" — the adapter clamps to the platform's page size and withholds
   * the bot's own messages and disallowed senders after fetching. The walk
   * ends when the platform reports the newest message reached, when the
   * page is empty, or at the bound.
   */
  private async fetchAfter(
    key: ChannelKey,
    afterId: number,
    throughId?: number,
    quoteLookupBudget?: { remainingMs: number },
  ): Promise<{ messages: KeyedMessage[]; truncated: boolean; complete: boolean; scannedThrough: number | null; unverifiedMessageIds: number[] }> {
    const out: KeyedMessage[] = [];
    const unverified = new Set<number>();
    let cursor = afterId;
    let complete = false;
    while (out.length < this.catchupLimit) {
      const want = this.catchupLimit - out.length;
      const page = await this.adapter.fetchHistory!(key, { limit: want, afterMessageId: String(cursor), quoteLookupBudget, timeoutMs: LONG_TIMEOUT_MS });
      if (page.scannedThrough === null) {
        complete = true;
        break;
      }
      for (const id of page.unverifiedMessageIds ?? []) {
        const numeric = Number(id);
        if (Number.isFinite(numeric) && !this.pendingReplies.get(key)?.has(numeric)) unverified.add(numeric);
      }
      out.push(...page.messages.map((message) => {
        if (!this.pendingReplies.get(key)?.has(Number(message.messageId))) return message;
        return {
          ...message,
          tags: [...new Set([...(message.tags ?? []).filter((tag) => tag !== CHAT_TAGS.ambient), CHAT_TAGS.reply])],
          metadata: { ...(message.metadata as Record<string, unknown>), replyToSelf: true, isMention: true, isReplyToBot: true },
        };
      }));
      if (page.scannedThrough <= cursor) break;
      cursor = page.scannedThrough;
      if (page.reachedNewest || (throughId !== undefined && cursor >= throughId)) {
        complete = true;
        break;
      }
    }
    // Not complete: the ceiling (or a bound) stopped the walk with more beyond the newest id scanned.
    return { messages: out, truncated: !complete, complete, scannedThrough: cursor > afterId ? cursor : null, unverifiedMessageIds: [...unverified].sort((a, b) => a - b) };
  }

  private advanceCatchUp(key: ChannelKey, through: number, after: number): void {
    // An accepted/empty catch-up scan releases every hold in its range,
    // including ids deleted from history. Their pending verdicts go too.
    for (const id of this.delivery.heldIds(key)) {
      if (id > after && id <= through) this.forgetPendingReply(key, id);
    }
    if (this.live) this.delivery.advanceThrough(key, through, after);
    else this.preLiveScanned.set(key, Math.max(this.preLiveScanned.get(key) ?? 0, through));
  }

  /**
   * Deliver what a closed (or not-yet-reopened) channel accumulated past its
   * watermark as `<missed>` push events: the full backscroll when
   * `keepAll` (the host had it open, or it is a DM), else each addressed message with
   * its vicinity. Advances the watermark past everything scanned once the
   * host has accepted the push. Returns true when something was delivered.
   */
  private async catchUpClosedChannel(
    key: ChannelKey,
    keepAllHint: boolean,
    afterId?: number,
    retryWindow?: { through: number; truncated: boolean; budget: { remainingMs: number } },
    onAttempt?: (through: number) => void,
    opts: { through?: number; lane?: Lane<null>; conn?: McplConnection | null } = {},
  ): Promise<boolean> {
    const conn = opts.conn === undefined ? this.conn : opts.conn;
    if (!this.adapter.fetchHistory || this.catchupLimit === 0) return false;
    if (this.ids.heldIdOf(key) === undefined) return false;
    const channelId = this.ids.channelIdOf(key)!;
    if (this.isMuted(key) || !this.isAllowed(key)) return false;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return false;
    const after = afterId ?? this.delivery.watermark(key);
    if (after === undefined) return false;
    // At most two quote-wait allowances for the entire call, not per row or
    // page. Reserve the second allowance for one pass over ALL deferred rows.
    const budget = retryWindow?.budget ?? { remainingMs: 6_000 };
    const passBudget = { remainingMs: retryWindow ? budget.remainingMs : Math.min(budget.remainingMs, 3_000) };
    const startedWith = passBudget.remainingMs;
    let fetched: Awaited<ReturnType<ZulipMcplServer['fetchAfter']>>;
    try {
      fetched = await this.fetchAfter(key, after, retryWindow?.through ?? opts.through, passBudget);
    } catch (err) {
      console.error(`[zulip-mcp] catch-up: history fetch failed for ${this.label(key)}:`, (err as Error).message);
      onAttempt?.(Infinity); // A history failure counts as the held read's attempt, unlike a local policy abort.
      return false;
    } finally {
      budget.remainingMs -= startedWith - passBudget.remainingMs;
    }
    if (conn?.isClosed) return false;
    if (fetched.scannedThrough === null) {
      onAttempt?.(Infinity);
      return false;
    }
    // The one retry stays inside the first walk's ceiling. Any quote still
    // unverified on that pass fails closed rather than starting another walk.
    const window = retryWindow ?? {
      through: Math.min(fetched.scannedThrough, opts.through ?? Infinity),
      truncated: fetched.truncated,
      budget,
    };
    fetched.truncated ||= window.truncated;
    let newestId = Math.min(fetched.scannedThrough, window.through);
    let msgs = fetched.messages.filter((message) => Number(message.messageId) > after && Number(message.messageId) <= newestId);
    const blocked = retryWindow ? undefined : fetched.unverifiedMessageIds.find((id) => id > after && id <= newestId);
    const retry = (cursor: number) => this.catchUpClosedChannel(key, keepAllHint, cursor, window, onAttempt, { ...opts, conn });
    if (blocked !== undefined) {
      newestId = Math.min(newestId, blocked - 1);
      msgs = msgs.filter((message) => Number(message.messageId) < blocked);
      if (newestId <= after) return retry(after);
    }
    const views = msgs.map(viewOf);
    const keepAll = keepAllHint || isDmKey(key);
    const kept = selectMissed(views, { keepAll, vicinity: MISSED_VICINITY });
    if (kept.length === 0) {
      onAttempt?.(Infinity);
      // Nothing to deliver. Commit this scan after pre-live verdicts are reconciled.
      this.advanceCatchUp(key, newestId, after);
      return blocked !== undefined ? retry(newestId) : false;
    }
    const addressedCount = views.filter(isAddressed).length;
    const mentionCount = views.filter((v) => v.mentioned).length;
    const replyCount = views.filter((v) => v.replyToSelf).length;
    const reason = keepAll ? 'backscroll' : addressedCount === mentionCount ? 'mention' : 'addressed';
    const streamName = isDmKey(key)
      ? (this.channelManager.getChannel(channelId)?.label ?? channelId)
      : this.streamNameOf(key) ?? channelId;
    const block = renderMissedBlock(kept, {
      streamName,
      channelId,
      reason,
      count: keepAll ? kept.length : addressedCount,
      formatTime: this.formatTime,
      maxChars: this.missedBlockMaxChars,
      moreBeyond: fetched.truncated || blocked !== undefined,
      newestScannedId: newestId,
    });
    const synthetic: KeyedMessage = {
      key,
      messageId: String(newestId),
      author: { id: 'system', name: 'zulip catch-up' },
      timestamp: new Date().toISOString(),
      content: [{ type: 'text', text: block } satisfies TextContent],
      tags: [
        'zulip:missed',
        ...(isDmKey(key) ? [CHAT_TAGS.dm, CHAT_TAGS.private] : [
          ...(mentionCount > 0 ? [CHAT_TAGS.mention] : []),
          ...(replyCount > 0 ? [CHAT_TAGS.reply] : []),
          ...(addressedCount === 0 ? [CHAT_TAGS.ambient] : []),
        ]),
      ],
      metadata: { missed: true, topic: undefined, mentioned: mentionCount > 0, replyToSelf: replyCount > 0, isDM: isDmKey(key) },
    };
    let offered = false;
    const firstId = kept[0].id;
    const lane = opts.lane;
    if (lane) this.delivery.hold(key, firstId, 'queued');
    const sent = this.pushEvent(synthetic, `zulip_missed_${channelId}_${newestId}`, {
      missed: true,
      reason,
      messages: kept.length,
      ...(fetched.truncated || blocked !== undefined ? { truncated: true } : {}),
    }, () => {
      offered = true;
      onAttempt?.(newestId);
    });
    const answered = (ok: boolean) => {
      if (ok) {
        for (const view of kept) this.forgetPendingReply(key, view.id);
        // Advance past everything scanned, not just what was delivered, so a
        // mention-only channel does not re-surface its non-mention tail; the
        // block covered every held id in the range, so their holds go too.
        this.advanceCatchUp(key, newestId, after);
        // The agent answers the block through channels/publish too, so the
        // conversation is where its newest message that addresses the agent
        // is (in a backscroll, its newest message), as live pushes or an open
        // channel's deliveries would have left it.
        for (let i = msgs.length - 1; i >= 0; i--) {
          if (!keepAll && !isAddressed(views[i])) continue;
          if (this.channelManager.noteConversation(key, msgs[i])) break;
        }
      } else if (offered && (this.isMuted(key) || !this.isAllowed(key))) {
        for (const id of this.pendingReplies.get(key) ?? []) this.forgetPendingReply(key, id);
        this.delivery.releaseAll(key);
      } else if (offered) {
        this.delivery.hold(key, firstId, 'rejected');
        this.delivery.refuse(key, after, newestId);
      } else if (lane) {
        this.delivery.hold(key, firstId, 'failed');
      }
      this.delivery.save();
    };
    if (lane) {
      void sent.then((ok) => this.answerOn(lane, () => answered(ok)));
      if (blocked !== undefined) await retry(newestId);
      return true;
    }
    const ok = await sent;
    answered(ok);
    if (blocked !== undefined) {
      // A refused prefix must be re-offered too; an accepted prefix need not.
      const retried = await retry(ok ? newestId : after);
      return ok || retried;
    }
    return ok;
  }

  /** Count the ambient that arrived on tallied channels during downtime. */
  private async backfillMissedTallies(conn: McplConnection): Promise<void> {
    if (!this.adapter.fetchHistory) return;
    for (const key of this.delivery.talliedChannels()) {
      if (this.isMuted(key) || !this.isAllowed(key)) continue;
      const tally = this.delivery.tally(key)!;
      if (!tally.talliedThrough) continue;
      let fetched: Awaited<ReturnType<ZulipMcplServer['fetchAfter']>>;
      try {
        fetched = await this.fetchAfter(key, tally.talliedThrough);
      } catch {
        continue;
      }
      if (conn.isClosed) return;
      if (fetched.scannedThrough === null) continue;
      const views = fetched.messages.map(viewOf);
      // Only ambient counts as missed: addressed messages are delivered by the sweep.
      const ambient = views.filter((v) => !isAddressed(v));
      this.delivery.backfillMissed(key, ambient, fetched.scannedThrough);
    }
  }

  /**
   * A Zulip event queue died and its replacement starts from "now". Heal
   * the gap from history (watermark → now) before the gap marker itself is
   * delivered, so the agent gets the messages, not advice to go looking for
   * them: open channels are replayed through channels/incoming; every other
   * watermarked channel gets its addressed messages as a `<missed>` push, exactly as
   * after a restart.
   */
  private async onSystemEvent(event: PlatformSystemEvent): Promise<void> {
    let recovered = 0;
    let closedCaughtUp = 0;
    // §6.7 holds for replayed traffic as much as for live: nothing is
    // recovered into a feature set the host has disabled.
    if (event.kind === 'gap' && this.adapter.fetchHistory && this.live && this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      for (const channelId of [...this.channelManager.getOpenChannels()]) {
        const key = this.channelManager.keyOfHeld(channelId);
        if (key === null || this.ids.channelIdOf(key) !== channelId) continue;
        if (this.isMuted(key) || !this.isAllowed(key)) continue;
        const watermark = this.delivery.watermark(key);
        if (watermark === undefined) continue;
        try {
          const msgs = (await this.fetchAfter(key, watermark)).messages;
          recovered += this.replayOnOpen(key, msgs);
        } catch (err) {
          console.error(`[zulip-mcp] gap recovery failed for ${channelId}:`, (err as Error).message);
        }
      }
      for (const key of this.delivery.watermarkedChannels()) {
        if (this.channelManager.isKeyOpen(key)) continue;
        if (await this.catchUpClosedChannel(key, false)) closedCaughtUp++;
      }
      if (recovered > 0 || closedCaughtUp > 0) this.delivery.save();
    }
    const notes = [
      recovered > 0 ? `${recovered} message(s) on open channels were recovered from history and delivered above.` : null,
      closedCaughtUp > 0 ? `Addressed messages on ${closedCaughtUp} closed channel(s) were delivered as catch-up events.` : null,
    ].filter(Boolean);
    const text = notes.length > 0 ? `${event.text} ${notes.join(' ')}` : event.text;
    this.channelManager.broadcastSystemEvent(this.adapter.type, {
      ...event,
      text,
      metadata: { ...event.metadata, ...(event.kind === 'gap' ? { recoveredMessages: recovered, closedChannelsCaughtUp: closedCaughtUp } : {}) },
    });
  }

  private onDirectoryChange(change: DirectoryChange): void {
    if (!this.mcplActive) return;
    this.rewriteFilters(change.renamed);
    for (const { id, from, to } of change.renamed) {
      const key = streamKey(id);
      console.error(`[zulip-mcp] Zulip stream ${id} was renamed #${from} -> #${to} (zulip:${from} -> zulip:${to})`);
      if (this.ids.heldIdOf(key) === undefined && this.delivery.wasOpen(key)) {
        const first = this.renamedUnannounced.get(key)?.from ?? from;
        if (first === to) this.renamedUnannounced.delete(key);
        else this.renamedUnannounced.set(key, { from: first, to });
      }
    }
    if (change.renamed.length > 0 || change.removed.length > 0) {
      this.delivery.save();
      void this.reconcileHostIds();
    }
  }

  private logUnmatchedFilters(): void {
    if (!this.filters) return;
    const f = this.filters.current();
    const reactionStreams = (f.reactionChannels ?? []).filter((id) => id.startsWith('zulip:') && !isDmChannelId(id)).map((id) => id.slice('zulip:'.length));
    for (const [list, names] of [['streams', f.streams], ['mutedStreams', f.mutedStreams], ['reactionChannels', reactionStreams]] as const) {
      for (const name of names ?? []) {
        if (this.directory.idOf(name) !== undefined) continue;
        const former = this.directory.formerOf(name);
        console.error(`[zulip-mcp] filters: ${list} names #${name}, which matches no stream${former ? `; Zulip stream ${former.id} is #${former.current} now` : ''}`);
      }
    }
  }

  private rewriteFilters(renamed: { from: string; to: string }[]): void {
    if (!this.filters || renamed.length === 0) return;
    const names = new Map(renamed.map(({ from, to }) => [from, to]));
    const ids = new Map(renamed.map(({ from, to }) => [`zulip:${from}`, `zulip:${to}`]));
    const mapped = (list: string[] | undefined, map: Map<string, string>) => list?.map((x) => map.get(x) ?? x);
    this.filters.tick();
    const f = this.filters.current();
    const held = new Set([...(f.streams ?? []), ...(f.mutedStreams ?? []), ...(f.reactionChannels ?? [])]);
    const touched = renamed.filter(({ from }) => held.has(from) || held.has(`zulip:${from}`));
    if (touched.length === 0) return;
    const result = this.filters.update((cur) => ({
      ...cur,
      ...(cur.streams ? { streams: mapped(cur.streams, names) } : {}),
      ...(cur.mutedStreams ? { mutedStreams: mapped(cur.mutedStreams, names) } : {}),
      ...(cur.reactionChannels ? { reactionChannels: mapped(cur.reactionChannels, ids) } : {}),
    }));
    for (const { from, to } of touched) {
      if (result.ok) console.error(`[zulip-mcp] filters: #${from} is now #${to}`);
      else {
        const holder = this.directory.idOf(from);
        const now = holder === undefined ? 'the old name now matches no stream' : `the old name now matches Zulip stream ${holder}, which took it`;
        console.error(`[zulip-mcp] filters: could not rewrite #${from} as #${to} (${result.reason}); ${now}`);
      }
    }
  }

  private async reconcileHostIds(): Promise<void> {
    if (!this.conn || !this.mcplActive) return;
    if (this.reconciling) {
      this.reconcileAgain = true;
      return;
    }
    this.reconciling = true;
    try {
      do {
        this.reconcileAgain = false;
        await this.changeAtHost();
      } while (this.reconcileAgain);
    } catch (err) {
      console.error('[zulip-mcp] bringing the host\'s channel ids to the stream names failed:', (err as Error).message);
    } finally {
      this.reconciling = false;
    }
  }

  private async changeAtHost(): Promise<void> {
    const renames: { key: ChannelKey; held: string; fresh: string; descriptor: ChannelDescriptor }[] = [];
    const removals: { key: ChannelKey; held: string }[] = [];
    for (const { key, held, fresh } of this.ids.stale()) {
      if (streamIdOfKey(key) === null) continue;
      if (fresh === null) {
        removals.push({ key, held });
        continue;
      }
      const [descriptor] = (await this.adapter.describeChannels?.([key])) ?? [];
      if (descriptor && descriptor.id === fresh) renames.push({ key, held, fresh, descriptor });
    }
    if (renames.length === 0 && removals.length === 0 && this.ids.inDoubt().length === 0) return;
    let told = false;
    for (const { key, held, fresh } of renames) {
      if (held === fresh || !this.channelManager.isOpen(held) || this.toldAgent.get(key) === fresh) continue;
      const from = held.slice('zulip:'.length);
      this.channelManager.notifyChannel(key,
        `Zulip stream #${from} was renamed #${fresh.slice('zulip:'.length)}. Its channel is now ${fresh}, and ${this.oldIdNow(key, from)}. ` +
          `Open ${fresh} to keep hearing it, unless it is already open. What you had read stays read.`,
        { kind: 'stream-renamed', from: held, to: fresh });
      this.toldAgent.set(key, fresh);
      told = true;
    }
    for (const { key, held } of removals) {
      if (!this.channelManager.isOpen(held) || this.toldAgent.get(key) === REMOVED_LINE) continue;
      this.channelManager.notifyChannel(key,
        `Zulip stream #${held.slice('zulip:'.length)} was archived, or this bot lost access to it. This channel is gone.`,
        { kind: 'stream-removed', channelId: held });
      this.toldAgent.set(key, REMOVED_LINE);
      told = true;
    }
    if (told) await this.channelManager.flush();
    const removed = [...renames, ...removals].map(({ held }) => held);
    const openBefore = new Set(removed.filter((id) => this.channelManager.isOpen(id)));
    const answer = await this.channelManager.announceChanges(removed, renames.map(({ descriptor }) => descriptor), this.announceTimeoutMs);
    if (answer === 'unanswered') return;
    for (const { key } of [...renames, ...removals]) this.toldAgent.delete(key);
    const added = new Set(renames.map(({ fresh }) => fresh));
    const wasOpen = new Map<ChannelKey, boolean>();
    for (const { key, held } of [...renames, ...removals]) {
      if (added.has(held)) {
        wasOpen.set(key, openBefore.has(held));
        this.ids.forget(held);
      } else {
        wasOpen.set(key, this.channelManager.retireId(held));
      }
    }
    for (const { key, held, fresh } of renames) {
      const verdict = answer.get(fresh);
      if (verdict === 'accepted') this.ids.move(key, fresh);
      if (wasOpen.get(key) && !this.channelManager.isKeyOpen(key)) this.delivery.markClosed(key);
      console.error(`[zulip-mcp] stream ${streamIdOfKey(key)}: the host ${verdict === 'accepted' ? `now holds ${fresh}` : `refused ${fresh}`} in place of ${held}`);
    }
    for (const { key, held } of removals) console.error(`[zulip-mcp] stream ${streamIdOfKey(key)} is gone: the host no longer holds ${held}`);
    this.delivery.save();
  }

  private oldIdNow(key: ChannelKey, oldName: string): string {
    const holder = this.directory.idOf(oldName);
    return holder !== undefined && holder !== streamIdOfKey(key)
      ? `zulip:${oldName} now belongs to another stream, which took the name #${oldName}`
      : `zulip:${oldName} is gone`;
  }

  private async pushRenamedUnannounced(conn: McplConnection): Promise<ChannelKey[]> {
    const pushedFor: ChannelKey[] = [];
    for (const [key, { from, to }] of this.renamedUnannounced) {
      if (conn.isClosed) break;
      this.renamedUnannounced.delete(key);
      if (!this.delivery.wasOpen(key) || !this.isAllowed(key)) continue;
      const fresh = this.ids.channelIdOf(key);
      if (fresh === null) continue;
      const text = `Zulip stream #${from} was renamed #${to} since its channel was last open. Its channel is now ${fresh}, and ${this.oldIdNow(key, from)}. ` +
        `Open ${fresh} to keep hearing it. What you had read stays read.`;
      const pushed = await this.pushEvent({
        key,
        messageId: `system:stream-renamed:${Date.now()}`,
        author: { id: 'system', name: 'zulip connection' },
        timestamp: new Date().toISOString(),
        content: [{ type: 'text', text }],
        metadata: { system: true, kind: 'stream-renamed', from: `zulip:${from}`, to: fresh },
      }, `zulip_stream_renamed_${key}_${to}`, { kind: 'stream-renamed', from: `zulip:${from}`, to: fresh });
      if (!pushed) console.error(`[zulip-mcp] ${fresh} (was zulip:${from}) was open at last sight; the push saying so did not land`);
      pushedFor.push(key);
    }
    return pushedFor;
  }

  private closeRenamed(keys: ChannelKey[]): void {
    if (keys.length === 0) return;
    for (const key of keys) if (!this.channelManager.isKeyOpen(key)) this.delivery.markClosed(key);
    this.delivery.save();
  }

  // ── Rollback ──

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

  // ── Helpers ──

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
   * Execute a tool for `tools/call`. In MCPL mode the tool surface is gated:
   * `tools` must be in the effective grant (§14.1 / §6.2), and a tool owned
   * by a feature set the host disabled is unavailable with it (§6.7). Plain
   * MCP clients are not subject to a grant — there is none to consult.
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
      // hands it back in `state`, which is the only way a host ever learns
      // one. A rollback to this checkpoint undoes every send made after it
      // (this tool's own send is before it, on the record). Error results
      // carry none: nothing was sent, there is nothing new to return to.
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

  /**
   * Re-enumerate what the adapter can see and announce anything the host
   * does not know yet — after a filters change widened the allowlist, or on
   * request (refresh_channels).
   */
  async applyFilterChange(opts: { retryRefused?: boolean } = {}): Promise<{ visible: number; added: string[]; local: string[]; refused: string[]; reason?: string }> {
    if (!this.conn || !this.mcplActive) {
      return { visible: 0, added: [], local: [], refused: [], reason: 'not connected to an MCPL host; channels are registered in MCPL mode only' };
    }
    const descriptors = await this.adapter.discoverChannels();
    const result = await this.channelManager.registerAdditional(descriptors, { retryBacklog: true, retryRefused: opts.retryRefused });
    void this.reconcileHostIds();
    if (result.announced.length > 0) console.error(`[zulip-mcp] registered ${result.announced.length} newly visible channel(s): ${result.announced.join(', ')}`);
    return { visible: descriptors.length, added: result.announced, local: result.local, refused: result.refused, reason: result.reason };
  }

  private async unheldDescriptor(key: ChannelKey): Promise<ChannelDescriptor | null> {
    if (this.ids.heldIdOf(key) !== undefined) return null;
    if (!this.conn || !this.mcplActive || !this.adapter.describeChannels) return null;
    try {
      const [descriptor] = await this.adapter.describeChannels([key]);
      return descriptor ?? null;
    } catch (error) {
      console.error(`[zulip-mcp] could not register ${this.label(key)} from a message change:`, (error as Error).message);
      return null;
    }
  }

  /**
   * Register the streams `listen` just subscribed the bot to, and say how it
   * went so the tool can report it rather than hiding it in stderr.
   */
  private async registerSubscribed(streams: string[]): Promise<RegisterAdditionalResult> {
    const empty: RegisterAdditionalResult = { announced: [], local: [], refused: [] };
    if (!this.conn || !this.mcplActive) {
      return { ...empty, reason: 'not connected to an MCPL host; channels are registered in MCPL mode only' };
    }
    if (!this.adapter.describeChannels) return empty;
    try {
      const unknown = streams.filter((name) => this.directory.idOf(name) === undefined);
      if (unknown.length > 0) await this.adapter.refreshStreams?.(unknown.map((name) => `#${name}`).join(', '));
      const keys = streams.map((name) => this.directory.idOf(name)).filter((id): id is number => id !== undefined).map((id) => streamKey(id));
      const descriptors = await this.adapter.describeChannels(keys);
      const allowed = descriptors.filter((d) => {
        const key = this.ids.keyOfDescriptor(d);
        return key !== null && this.isAllowed(key);
      });
      if (allowed.length === 0) return empty;
      const result = await this.channelManager.registerAdditional(allowed, { retryBacklog: true });
      void this.reconcileHostIds();
      return result;
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

  private refuseFormerNames(names: string[]): void {
    if (!this.mcplActive) return;
    const renamed = names.flatMap((name) => {
      const former = this.directory.formerOf(name);
      return former ? [`Zulip stream #${name} was renamed #${former.current}.`] : [];
    });
    if (renamed.length > 0) throw new Error(`${renamed.join(' ')} Nothing was written.`);
  }

  private streamArg(value: unknown): string {
    const raw = String(value ?? '').trim().replace(/^#/, '');
    if (!raw) throw new Error('channel is required');
    const name = raw.startsWith('zulip:') ? raw.slice('zulip:'.length) : raw;
    if (isDmChannelId(`zulip:${name}`) || name.startsWith('dm:')) throw new Error('DM conversations cannot be muted or filtered by stream; use the dmUsers allowlist.');
    return name;
  }

  /** Tools that read the server's own delivery state. undefined = not ours. */
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
        this.refuseFormerNames([...addStreams, ...removeStreams]);
        let materialized = false;
        const result = plane.update((f) => {
          let streams = f.streams ? [...f.streams] : null;
          if (removeStreams.length > 0 && streams === null) {
            // Removing from "everything" first materializes the list as every
            // stream currently registered, so nothing silently drops.
            streams = this.channelManager.listChannels().channels
              .map((c) => this.ids.keyOfDescriptor(c))
              .map((key) => (key === null ? null : this.streamNameOf(key)))
              .filter((name): name is string => name !== null);
            materialized = true;
          }
          if (streams !== null) {
            for (const name of addStreams) if (!streams.includes(name)) streams.push(name);
            streams = streams.filter((name) => !removeStreams.includes(name));
            // An empty allowlist means UNRESTRICTED. Removing the last
            // allowed stream would therefore re-open every stream — the
            // opposite of what was asked. Refuse; "nothing" is what mutes are for.
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
        this.refuseFormerNames([stream]);
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
            : 'Messages from this stream reach you again by the usual rules (addressed messages always; ambient when the channel is open).',
        };
      }
      case 'set_reaction_visibility': {
        const plane = this.requireFilters();
        const channelId = this.channelIdArg(args.channel);
        if (!isDmChannelId(channelId)) this.refuseFormerNames([channelId.slice('zulip:'.length)]);
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
        // Agent-initiated: a refusal the host gave earlier is worth asking
        // about again, since whatever made the host say no may have changed.
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
        const answer = await this.ids.keyOf(channelId);
        if ('refusal' in answer) throw new Error(answer.refusal);
        const key = 'key' in answer ? answer.key : null;
        const tally = key !== null ? this.delivery.tally(key) : undefined;
        const open = key !== null && this.channelManager.isKeyOpen(key);
        if (!tally) {
          return {
            channelId,
            tracked: false,
            open,
            note: open
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
      (message, newChannel) => this.onIncoming(message, newChannel),
      (event) => {
        void this.onSystemEvent(event).catch((err) => {
          console.error('[zulip-mcp] system event handling failed:', (err as Error).message);
        });
      },
      (reaction) => this.lanes?.lane(reaction.key).enter(() => this.onReaction(reaction)),
      (change) => {
        const lane = this.lanes?.lane(change.key);
        lane?.enter(() => this.onMessageChange(change, lane));
      },
    );
  }

  /**
   * Reaction visibility is a per-channel opt-in (default off). A reaction
   * carries only its reaction tag, so a wake policy keyed on tags leaves
   * the agent asleep (one that wakes on everything in a channel does not);
   * it never advances a watermark and lands in context for the agent's next
   * turn. Suppression is decided before any model-visible text or the event
   * id exists, so a suppressed reaction leaves no glyph or name anywhere.
   */
  private async onReaction(ev: ReactionEvent): Promise<void> {
    if (!this.filters || !this.filters.reactionsVisible(this.ids.freshIdOf(ev.key) ?? '')) return;
    if (this.isMuted(ev.key) || !this.isAllowed(ev.key)) return;
    if (this.filters.reactionSuppressed(ev.emoji, ev.emojiCode, ev.emojiType)) return;
    if (!this.mcplActive || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const verb = ev.action === 'add' ? 'reacted' : 'removed a reaction';
    const target = ev.onOwnMessage ? 'your message' : `message ${ev.messageId}`;
    const quoted = ev.messageSnippet ? ` — "${ev.messageSnippet}"` : '';
    const line = `[reaction] ${ev.reactorName} ${verb} :${ev.emoji}: on ${target}${quoted}`;
    const message: KeyedMessage = {
      key: ev.key,
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
    if (this.channelManager.isKeyOpen(ev.key)) {
      this.channelManager.onIncomingMessage(ev.key, message);
    } else {
      void this.pushEvent(message, `zulip_reaction_${ev.action}_${ev.messageId}_${ev.reactorId}_${ev.timestamp.getTime()}`, {
        reaction: true,
        action: ev.action,
        onOwnMessage: ev.onOwnMessage,
      });
    }
  }

  /**
   * An edit, move or deletion is as visible as its message was. On an open
   * channel every message is delivered, so a change to one the host has
   * accepted (at or below the watermark), one that addresses the bot, or
   * one the bot wrote is delivered too. On a closed channel only addressed
   * traffic is pushed, so only an addressed change is — the mention the
   * agent is about to answer was rewritten, or the DM it is reading changed.
   * A change to a message the host never accepted is noise: it arrives
   * already changed if it arrives at all. The synthetic id never advances a
   * watermark; the line carries the message's own id for fetch_around.
   */
  private async onMessageChange(ev: MessageChangeEvent, lane: Lane<null>, announce = true): Promise<void> {
    if (ev.kind === 'edit' && ev.replyToSelf === false && ev.replyToSelfVerified === true) this.forgetPendingReply(ev.key, Number(ev.messageId));
    if (this.isMuted(ev.key) || !this.isAllowed(ev.key)) return;
    if (!this.mcplActive || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const addressed = ev.mentioned || ev.isDM || ev.replyToSelf === true;
    // An edit or a deletion can be the first thing this server sees from a
    // stream the bot was added to after startup — a message edited into a
    // mention, say. Register the channel from it for the same reason a
    // message registers one (#20): the agent cannot open what it was told
    // about but this server never announced.
    if (announce) {
      const descriptor = await this.unheldDescriptor(ev.key);
      if (descriptor) {
        void this.channelManager.registerAdditional([descriptor]);
        lane.enterFirst(() => this.onMessageChange(ev, lane, false));
        return;
      }
    }
    const open = this.channelManager.isKeyOpen(ev.key);
    if (!open && !addressed) return;
    if (open && !addressed && !ev.onOwnMessage) {
      // Accepted (at or below the watermark) or offered and not yet accepted
      // (held): "post, then fix the typo" lands before the host's acceptance
      // round trip and must not be lost.
      const watermark = this.delivery.watermark(ev.key) ?? 0;
      const held = new Set(this.delivery.heldIds(ev.key));
      const ids = ev.messageIds.map(Number).filter((n) => Number.isFinite(n));
      if (!ids.some((n) => n <= watermark || held.has(n))) return;
    }

    const head = messageLineHead({
      id: ev.messageId,
      time: Number.isNaN(ev.timestamp.getTime()) ? '' : this.formatTime(ev.timestamp),
      stream: ev.isDM ? null : this.streamNameOf(ev.key) ?? '',
      topic: ev.topic,
      author: ev.authorName ?? 'unknown author',
      mentioned: ev.mentioned,
    });
    const others = ev.messageIds.length > 1 ? ` (${ev.messageIds.length} messages)` : '';
    const byOther = ev.actorId !== null && ev.actorId !== ev.authorId ? ` [by user ${ev.actorId}]` : '';
    const wasQuoted = ev.previousContent ? ` — was: "${ev.previousContent}"` : '';
    const movedToChannelId = ev.movedToKey !== null ? this.ids.channelIdOf(ev.movedToKey) : null;
    const movedTo = movedToChannelId ? ` (now in ${movedToChannelId})` : '';
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
    const message: KeyedMessage = {
      key: ev.key,
      messageId: `${ev.kind}:${ev.messageId}:${stamp}`,
      // No threadId: a marker about a message is not the conversation and
      // must not retarget the reply the agent is composing (see
      // ChannelManager.onIncomingMessage).
      author: { id: ev.authorId ?? 'unknown', name: ev.authorName ?? 'unknown author' },
      timestamp: ev.timestamp.toISOString(),
      content: [{ type: 'text', text: line }],
      // The same addressing tag a message carries, so a debounced or
      // tag-keyed policy sees an ambient change as ambient; the host folds
      // chat:mention / chat:reply / chat:dm into chat:addressed.
      tags: [
        tag,
        ...(ev.kind === 'move' ? ['zulip:moved'] : []),
        ...(ev.isDM ? [CHAT_TAGS.dm, CHAT_TAGS.private] : ev.mentioned ? [CHAT_TAGS.mention] : ev.replyToSelf ? [CHAT_TAGS.reply] : [CHAT_TAGS.ambient]),
      ],
      metadata: {
        change: ev.kind,
        targetMessageId: ev.messageId,
        targetMessageIds: ev.messageIds,
        ...(ev.actorId !== null ? { actorId: ev.actorId } : {}),
        ...(ev.authorEmail !== null ? { senderEmail: ev.authorEmail } : {}),
        topic: ev.topic,
        ...(ev.previousTopic !== null ? { previousTopic: ev.previousTopic } : {}),
        ...(movedToChannelId !== null ? { movedToChannelId } : {}),
        ...(ev.previousContent !== null ? { previousContent: ev.previousContent } : {}),
        mentioned: ev.mentioned,
        replyToSelf: ev.replyToSelf === true,
        isMention: ev.mentioned || ev.replyToSelf === true,
        isExplicitMention: ev.mentioned,
        isReplyToBot: ev.replyToSelf === true,
        ...(ev.previouslyReplyToSelf != null ? { previouslyReplyToSelf: ev.previouslyReplyToSelf } : {}),
        ...(ev.previouslyMentioned !== null ? { previouslyMentioned: ev.previouslyMentioned } : {}),
        ...(ev.vanished ? { vanished: true } : {}),
        isDM: ev.isDM,
        onOwnMessage: ev.onOwnMessage,
        // The line already names who, where and when.
        attributed: true,
        attributionHeader: `[${ev.kind === 'delete' ? 'deleted' : ev.kind === 'move' ? 'moved' : 'edited'}] ${head}`,
      },
    };
    if (open) {
      this.channelManager.onIncomingMessage(ev.key, message);
    } else {
      void this.pushEvent(message, `zulip_${ev.kind}_${ev.messageId}_${stamp}`, {
        change: ev.kind,
        targetMessageId: ev.messageId,
      });
    }
  }

  /** Apply reaction suppression to replayed history (channels/open, gap recovery). */
  private projectHistoryReactions(messages: KeyedMessage[]): KeyedMessage[] {
    if (!this.filters) return messages;
    const plane = this.filters;
    return messages.map((m) => {
      const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
      const reactions = Array.isArray(meta.reactions) ? (meta.reactions as ReactionSummary[]) : null;
      if (!reactions) return m;
      return { ...m, metadata: { ...meta, reactions: reactions.filter((r) => !plane.reactionSuppressed(r.name, r.code, r.type)) } };
    });
  }

  // ── Server → host ──

  private async registerChannelsWithHost(channels: ChannelDescriptor[]): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_REGISTER, { channels })) as ChannelsRegisterResult | undefined;
  }

  private async channelsChangedWithHost(params: ChannelsChangedParams, timeoutMs = this.announceTimeoutMs): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    const answer = conn.sendRequest(method.CHANNELS_CHANGED, params, timeoutMs);
    for (const descriptor of params.added ?? []) {
      const key = this.ids.keyOfDescriptor(descriptor);
      const renamed = key !== null && (params.removed ?? []).includes(this.ids.heldIdOf(key) ?? '');
      if (key !== null && !renamed) this.lanes?.lane(key).holdOtherUntil(answer);
    }
    return (await answer) as ChannelsRegisterResult | undefined;
  }

  private async sendIncomingToHost(messages: IncomingChannelMessage[]): Promise<ChannelsIncomingResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    return (await conn.sendRequest(method.CHANNELS_INCOMING, { messages })) as ChannelsIncomingResult | undefined;
  }
}

function lifecycleChannelId(req: JsonRpcRequest): string | null {
  if (req.method !== method.CHANNELS_OPEN && req.method !== method.CHANNELS_CLOSE) return null;
  const channelId = (req.params as { channelId?: unknown } | undefined)?.channelId;
  return typeof channelId === 'string' ? channelId : null;
}

function recovered(m: KeyedMessage): KeyedMessage {
  return {
    ...m,
    tags: [...(m.tags ?? []), 'zulip:missed'],
    metadata: { ...(m.metadata as Record<string, unknown>), backscroll: undefined, recovered: true },
  };
}

/** A message's cursor id; null for reactions and system markers, which have none. */
function numericId(m: { messageId: string }): number | null {
  const n = Number(m.messageId);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/** The human sender of a real message; null for synthetic ones (system markers, reactions, catch-up blocks). */
function senderOf(m: KeyedMessage): { id: number; email: string } | null {
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
