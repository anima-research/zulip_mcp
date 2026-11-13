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
 * live replay from history once the host answers, by the catch-up on
 * the next connection, or by the gap recovery a Zulip event-queue expiry
 * triggers. Nothing is lost to a later acceptance.
 *
 * One connection at a time. `serve()` resolves when the peer disconnects;
 * every connection starts from a fresh grant, registration and catch-up.
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
import { Lanes, type Job, type JobContext, type Lane, type Moments, type ReadContext, type ReadJob } from './lanes.js';
import { DeliveryState, type DeliveryReader, attributeMessage, renderMissedBlock, selectMissed, viewOf, isAddressed, DEFAULT_MISSED_BLOCK_MAX_CHARS } from './delivery.js';
import { McplRpcError, capabilityDenied } from './errors.js';
import { MESSAGING_FEATURE_SET, buildServerCapabilities, featureSetForTool } from './feature-sets.js';
import { isDmChannelId, parseDmChannelId } from './history.js';
import { LONG_TIMEOUT_MS, ZulipNoAnswer, ZulipRefused } from './zulip-api.js';
import type { FiltersPlane } from './filters.js';
import { buildAttachmentBlocks, type AttachmentSource, type InlineOptions } from './attachments.js';
import type { AttachmentRef } from './content.js';
import { agentLineTimeFormatter } from './timezone.js';
import { CapabilityGrant } from './grant.js';
import { StateTracker } from './state.js';
import type { IncomingEntry, MessageChangeEvent, PlatformAdapter, PlatformSystemEvent, ReactionEvent } from './platforms/adapter.js';
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
const MAX_WAITING_MESSAGES = 5000;
/** How long shutdown waits for deliveries still building their content
 *  (attachment fetches) before flushing what has been queued. */
const SHUTDOWN_INFLIGHT_GRACE_MS = 5000;
const ATTACHMENT_DEADLINE_MS = 15_000;
const READ_RETRY_FIRST_MS = 2_000;
const READ_RETRY_MAX_MS = 60_000;
const READ_WAIT_LINE_MS = 90_000;

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
  /** Per-channel ceiling for each channel's catch-up at a connection's start, and for gap recovery. */
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
  /** How long shutdown waits for a delivery still running, such as one fetching its attachments (default 5 s). */
  shutdownGraceMs?: number;
  /** At most this many messages wait in all channels' lanes together; past it, the channel with the most gives them up to a read from history (default 5,000). */
  maxWaitingMessages?: number;
  /** How a lane waits before it tries a read from history again that got no answer (default: a timer of `ms`). */
  readRetryWait?: (ms: number) => Promise<void>;
  /** The clock for how long a lane has waited for a read from history (default: Date.now). */
  now?: () => number;
  directory?: StreamDirectory;
}

interface GapTally {
  recovered: number;
  caughtUp: Set<ChannelKey>;
}

interface GapRead {
  tally: GapTally;
  answers: Promise<boolean>[];
}

interface DeliveryWriter {
  readonly delivery: DeliveryState;
  readonly channels: Pick<ChannelManager, 'noteConversation' | 'onIncomingMessage'>;
}

type Ctx = JobContext<DeliveryWriter>;
type Reading = ReadContext<DeliveryWriter>;
type Hand = Job<DeliveryWriter>;

interface HistoryRead {
  messages: KeyedMessage[];
  truncated: boolean;
  complete: boolean;
  scannedThrough: number | null;
  unverifiedMessageIds: number[];
}

/** One pass of a catch-up read, read from `after`; no `fetched` when the read failed. */
interface CatchUpPass {
  after: number;
  bound: number | undefined;
  retry: boolean;
  fetched?: HistoryRead;
}

interface CatchUpRead {
  keepAll: boolean;
  after?: number;
  through?: () => number;
  answers?: Promise<boolean>[];
  ceiling?: { reached: boolean };
  fill?: boolean;
  onAttempt?: (w: DeliveryWriter, through: number) => void;
}

export class ZulipMcplServer {
  private conn: McplConnection | null = null;
  private mcplActive = false;

  readonly adapters: Map<string, PlatformAdapter>;
  readonly grant: CapabilityGrant;
  readonly manifestTracker: ManifestTracker;
  readonly channelManager: Omit<ChannelManager, 'noteConversation' | 'onIncomingMessage'>;
  readonly contextProvider: ContextProvider;
  readonly delivery: DeliveryReader;
  readonly directory: StreamDirectory;
  readonly ids: ChannelIds;
  readonly stateTracker = new StateTracker();

  private detachManifest: (() => void) | null = null;
  private eventsStarted = false;
  private lanes: Lanes<DeliveryWriter> | null = null;
  private readonly makeLanes: () => Lanes<DeliveryWriter>;
  private starting: Promise<void> | null = null;
  private requestTail: Promise<void> = Promise.resolve();
  private requestsWaiting = 0;
  private readonly hostRequestsOut = new Set<Promise<unknown>>();
  private readonly lifecycleOnChain = new Map<string, number>();
  private grantUpdatesOnChain = 0;
  private readonly heldReadWaiting = new WeakSet<Lane<DeliveryWriter>>();
  private messagingHold: (() => void) | null = null;
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
  private readWaits = new Set<ChannelKey>();
  private toldReadWait = false;

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
    const delivery = new DeliveryState(options.stateDir ?? null, options.sessionId ?? 'default', this.directory);
    this.delivery = delivery;
    this.directory.onChange((change) => this.onDirectoryChange(change));
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
    const channels = new ChannelManager(host, this.adapters, this.grant, this.ids, options.batchWindowMs, {
      // The only place an open channel's watermark moves: on the host's
      // itemized acceptance of a channels/incoming batch.
      onDelivered: (key, accepted, rejected) => this.lanes?.lane(key).answer((ctx) => this.onDelivered(ctx.w, key, accepted, rejected)),
      // Policy is rechecked when the batch goes out, not only when it was
      // queued: a mute, a narrowed allowlist or a disabled feature set that
      // landed during the batch window applies to what is still waiting.
      deliverable: (message) => this.mayDeliver(message),
      onWithheld: (key, messages) => this.lanes?.lane(key).answer((ctx) => this.onWithheld(ctx.w, key, messages)),
      onGivenUp: (key, messages) => this.lanes?.lane(key).answer((ctx) => this.onGivenUp(ctx.w, key, messages)),
    });
    this.channelManager = channels;
    const writer: DeliveryWriter = { delivery, channels };
    const moments: Moments<DeliveryWriter> = {
      entry: (w, key, id, ended) => {
        w.delivery.hold(key, id, ended ? 'failed' : 'queued');
        if (this.messagingHold !== null && w.delivery.watermarkBelowHolds(key)) this.delivery.save();
      },
      overflow: (w, lane) => this.giveUp(w, lane),
      start: (w) => w.delivery.restartReached(),
      end: (w) => w.delivery.failQueued(),
    };
    this.makeLanes = () => new Lanes(writer, moments, { maxWaitingMessages: options.maxWaitingMessages ?? MAX_WAITING_MESSAGES });
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
   * no delivery until this peer's registration settles. Nothing of a
   * previous peer's grant, registration or lanes carries over.
   */
  private beginConnection(): void {
    this.directory.restart(this.delivery.directoryCopy());
    this.grant.reset();
    this.channelManager.reset();
    this.toldAgent.clear();
    this.readWaits.clear();
    this.toldReadWait = false;
    this.messagingHold = null;
    this.lanes = this.makeLanes();
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
      // Every lane waits until the registration settles and the stream
      // directory is filled. Each channel's catch-up is then its lane's first
      // job, so its pushes land on registered channels inside the granted
      // window, and nothing received meanwhile can advance a watermark over
      // the offline gap.
      const lanes = this.lanes!;
      const openLanes = lanes.holdAll();
      this.messagingChanged();
      void this.grant.whenReady().then(() => {
        if (conn.isClosed) return;
        const start = this.start(conn, lanes, openLanes);
        this.starting = start;
        const started = () => {
          if (this.starting === start) this.starting = null;
        };
        start.then(started, started);
      });

      // The event queue is registered at once — Zulip delivers nothing that
      // predates the queue, so every second of delay here is a second of
      // messages that no catch-up can recover for a never-watermarked channel.
      this.startEvents();
    }

    this.requestTail = Promise.resolve();
    try {
      while (!conn.isClosed) {
        const msg = await conn.nextMessage();
        if (msg.type === 'request' && this.enterHostQueue(conn, msg.request)) continue;
        const lifecycleId = msg.type === 'request' ? this.lifecycleTarget(msg.request) : null;
        if (lifecycleId !== null) this.lifecycleOnChain.set(lifecycleId, (this.lifecycleOnChain.get(lifecycleId) ?? 0) + 1);
        const grantUpdate = msg.type === 'request' && msg.request.method === method.FEATURE_SETS_UPDATE;
        if (grantUpdate) this.grantUpdatesOnChain++;
        this.requestsWaiting++;
        this.requestTail = this.requestTail.then(async () => {
          try {
            if (conn.isClosed) return;
            await (msg.type === 'request'
              ? this.handleRequest(conn, msg.request)
              : this.handleNotification(conn, msg.notification));
          } finally {
            this.requestsWaiting--;
            if (grantUpdate) this.grantUpdatesOnChain--;
            if (lifecycleId !== null) {
              const left = (this.lifecycleOnChain.get(lifecycleId) ?? 1) - 1;
              if (left > 0) this.lifecycleOnChain.set(lifecycleId, left);
              else this.lifecycleOnChain.delete(lifecycleId);
            }
          }
        });
      }
    } finally {
      this.lanes?.end();
      await this.requestTail;
    }
  }

  /**
   * Settles once nothing is under way: no delivery job runs or waits, no
   * request waits to be served, no batch waits for its window or is on the
   * wire, no request to the host waits for its answer, and no replay runs.
   * For tests and diagnostics.
   */
  async quiet(): Promise<void> {
    while (!this.isQuiet()) {
      await Promise.all([
        this.starting,
        this.lanes?.quiet(),
        this.requestTail,
        Promise.allSettled(this.hostRequestsOut),
        this.channelManager.idle(),
      ]);
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  private isQuiet(): boolean {
    return this.starting === null && (this.lanes?.isQuiet() ?? true) && this.requestsWaiting === 0 && this.hostRequestsOut.size === 0 && this.channelManager.isIdle();
  }

  private lifecycleTarget(req: JsonRpcRequest): string | null {
    const channelId = lifecycleChannelId(req);
    if (channelId !== null || (req.method !== method.CHANNELS_OPEN && req.method !== method.CHANNELS_CLOSE)) return channelId;
    try {
      return this.channelManager.findChannel(req.params as unknown as ChannelsOpenParams).id;
    } catch {
      return null;
    }
  }

  private enterHostQueue(conn: McplConnection, req: JsonRpcRequest): boolean {
    const channelId = lifecycleChannelId(req);
    if (channelId === null || !this.mcplActive || !this.lanes || this.lifecycleOnChain.has(channelId) || this.grantUpdatesOnChain > 0) return false;
    if (!this.grant.has('channels.lifecycle')) return false;
    const answer = this.ids.resolve(channelId);
    if (!answer || !('key' in answer)) return false;
    const key = answer.key;
    if (req.method === method.CHANNELS_CLOSE) {
      const close = req.params as unknown as ChannelsCloseParams;
      this.serveInLane(conn, req, key, (ctx) => this.closeInLane(ctx.w, close));
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
    this.serveInLane(conn, req, key, (ctx) => this.openChannel(ctx, open, descriptor, key, conn));
    return true;
  }

  private serveInLane(conn: McplConnection, req: JsonRpcRequest, key: ChannelKey, serve: (ctx: Ctx) => unknown): void {
    this.lanes?.lane(key).host(async (ctx) => {
      if (conn.isClosed) return;
      try {
        const result = await serve(ctx);
        if (!conn.isClosed) conn.sendResponse(req.id, result);
      } catch (err) {
        if (!conn.isClosed) this.answerError(conn, req, err);
      }
    });
  }

  /**
   * Stop platform event delivery, wait (bounded) for deliveries still
   * building their content, push out anything still batched, and persist
   * delivery state. A flush over a connection that is already gone fails
   * harmlessly: those watermarks stay put and the next start's catch-up
   * reads those messages again.
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
    const settle = async () => {
      try {
        await this.channelManager.flush();
      } catch (error) {
        console.error('[zulip-mcp] final flush failed:', (error as Error).message ?? error);
      }
      this.channelManager.destroy();
    };
    let stillRunning = 0;
    if (this.lanes) stillRunning = await this.lanes.finish(this.options.shutdownGraceMs ?? SHUTDOWN_INFLIGHT_GRACE_MS, settle);
    else await settle();
    if (stillRunning > 0) {
      console.error(`[zulip-mcp] ${stillRunning} job(s) still running on the lanes at shutdown; left for the next catch-up`);
    }
    this.delivery.save();
  }

  private async start(conn: McplConnection, lanes: Lanes<DeliveryWriter>, openLanes: () => void): Promise<void> {
    try {
      await this.channelManager.registerChannels();
    } catch (error) {
      console.error('Failed to register channels after initial policy:', error);
    }
    await this.directory.whenFilled();
    if (conn.isClosed) return;
    const renamed = new Set<ChannelKey>();
    for (const key of this.renamedUnannounced.keys()) {
      if (this.delivery.wasOpen(key) && this.isAllowed(key) && this.ids.channelIdOf(key) !== null) renamed.add(key);
      else this.renamedUnannounced.delete(key);
    }
    const keys = new Set([...this.delivery.watermarkedChannels(), ...this.delivery.talliedChannels(), ...this.delivery.lastOpenChannels()]);
    for (const key of keys) {
      const lane = lanes.lane(key);
      const answers: Promise<boolean>[] = [];
      lane.enterReadFirst((r) => (r.ended ? undefined : this.fillRead(r, (r2) => this.readStart(r2, key, renamed.has(key), answers))));
    }
    for (const key of renamed) {
      lanes.lane(key).enterFirst((ctx) => {
        if (!ctx.ended && !conn.isClosed) this.pushRenamed(key);
      });
    }
    openLanes();
    void this.reconcileHostIds();
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
          this.messagingChanged();
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
          this.serveInLane(conn, req, key, (ctx) => this.openChannel(ctx, open, descriptor, key, conn));
          break;
        }

        case method.CHANNELS_CLOSE: {
          this.requireMcpl();
          const close = params as unknown as ChannelsCloseParams;
          const answer = typeof close.channelId === 'string' ? this.ids.resolve(close.channelId) : null;
          if (answer && 'key' in answer) this.serveInLane(conn, req, answer.key, (ctx) => this.closeInLane(ctx.w, close));
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
          if (this.mcplActive) {
            this.grant.apply(params as unknown as FeatureSetsUpdateParams, 'notification');
            this.messagingChanged();
          }
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

  private async openChannel(ctx: Ctx, params: ChannelsOpenParams, descriptor: ChannelDescriptor, key: ChannelKey, conn: McplConnection): Promise<ChannelsOpenResult> {
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

    if (conn.isClosed) throw new Error(`the connection ended while ${descriptor.id} was opening`);
    this.channelManager.markOpen(descriptor.id);
    if (page) {
      const newest = Math.max(...page.messages.map((m) => Number(m.messageId)).filter(Number.isFinite));
      if (Number.isFinite(newest) && (page.sinceLastSeen || this.placeOf(key) === undefined)) ctx.decide(newest);
      const place = this.placeOf(key);
      // Handed to the host in this very response: forwarded, up to the lane's place.
      for (const m of page.messages) if (place !== undefined && Number(m.messageId) <= place) this.accepted(ctx.w, key, Number(m.messageId));
      // This history goes to the agent, which answers it through
      // channels/publish, so its newest message gives the topic. A page
      // fetched before a given message is older history, read for context,
      // and moves nothing.
      if (!page.older) {
        for (let i = page.messages.length - 1; i >= 0; i--) {
          if (ctx.w.channels.noteConversation(key, page.messages[i])) break;
        }
      }
    }
    ctx.w.delivery.markOpen(key);
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
    return this.channelManager.closeChannel(params);
  }

  private closeInLane(w: DeliveryWriter, params: ChannelsCloseParams): { closed: boolean } {
    const result = this.handleChannelClose(params);
    const key = this.channelManager.keyOfHeld(params.channelId);
    if (key !== null && this.ids.channelIdOf(key) === params.channelId) {
      w.delivery.markClosed(key);
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

  /** The host accepted (or was handed) a message: its hold is released and the watermark may advance. */
  private accepted(w: DeliveryWriter, key: ChannelKey, id: number): boolean {
    if (!Number.isFinite(id) || id <= 0) return false;
    const released = w.delivery.release(key, id);
    return w.delivery.advance(key, id) || released;
  }

  /**
   * The host answered a channels/incoming batch. Accepted messages release
   * their holds and the watermark advances towards the highest of them —
   * never past a message still held: one the host rejected here, one
   * queued behind, or one given up on earlier. A rejected message stays
   * below the watermark until something delivers it again.
   */
  private onDelivered(w: DeliveryWriter, key: ChannelKey, accepted: KeyedMessage[], rejected: KeyedMessage[]): void {
    let moved = false;
    let high = 0;
    for (const m of accepted) {
      const id = numericId(m);
      if (id === null) continue;
      moved = w.delivery.release(key, id) || moved;
      high = Math.max(high, id);
    }
    for (const m of rejected) {
      const id = numericId(m);
      if (id !== null) this.notAccepted(w, key, id, 'rejected', m);
    }
    if (high > 0) moved = w.delivery.advance(key, high) || moved;
    if (moved) this.delivery.save();
    if (accepted.length > 0) this.enterHeldReads();
  }

  /** A batch outlived its transport retry: the host never answered. Held for replay. */
  private onGivenUp(w: DeliveryWriter, key: ChannelKey, messages: KeyedMessage[]): void {
    for (const m of messages) {
      const id = numericId(m);
      if (id !== null) this.notAccepted(w, key, id, 'failed', m);
    }
  }

  private notAccepted(w: DeliveryWriter, key: ChannelKey, id: number, status: 'failed' | 'rejected', message?: KeyedMessage): void {
    if (this.isMuted(key) || !this.isAllowed(key, message)) {
      this.letGo(w, key, id);
    } else {
      w.delivery.hold(key, id, status);
    }
  }

  /**
   * Policy withheld queued messages. A mute or a narrowed allowlist means
   * they may never reach the agent: let them go. A disabled feature set
   * means "not now": they stay held, replayed once it is enabled again.
   */
  private onWithheld(w: DeliveryWriter, key: ChannelKey, messages: KeyedMessage[]): void {
    let moved = false;
    for (const m of messages) {
      const id = numericId(m);
      if (id === null) continue;
      if (this.isMuted(key) || !this.isAllowed(key, m)) {
        moved = w.delivery.release(key, id) || moved;
      } else w.delivery.hold(key, id, 'failed');
    }
    if (moved) this.delivery.save();
  }

  /** Queue a message for an open channel, holding its id until the host accepts it. */
  private deliverToOpen(w: DeliveryWriter, key: ChannelKey, message: KeyedMessage): boolean {
    if (!this.channelManager.isKeyOpen(key)) return false;
    const id = numericId(message);
    if (id !== null) w.delivery.hold(key, id, 'queued');
    w.channels.onIncomingMessage(key, message);
    return true;
  }

  /** One message from the adapter: it enters its channel's lane, held until it is decided. */
  private onIncoming(entry: IncomingEntry): void {
    if (!this.lanes) return;
    const key = entry.key;
    const lane = this.lanes.lane(key);
    const id = numericId(entry);
    const place = this.placeOf(key);
    if (id !== null && place !== undefined && id <= place) return;
    lane.enter(async (ctx) => {
      const ready = await entry.ready();
      if (ctx.ended) return;
      const placeNow = this.placeOf(key);
      if (id !== null && placeNow !== undefined && id <= placeNow) return;
      if (ready === null) {
        if (id === null) return;
        ctx.decide(id);
        return this.letGo(ctx.w, key, id);
      }
      const { message, newChannel } = ready;
      if (id !== null && (message.metadata as Record<string, unknown> | undefined)?.replyToSelf === true) ctx.w.delivery.verifyReply(key, id);
      return this.route(ctx, message, newChannel);
    }, { kind: 'message', ...(id !== null ? { id } : {}) });
  }

  private letGo(w: DeliveryWriter, key: ChannelKey, id: number): void {
    if (w.delivery.release(key, id)) this.delivery.save();
  }

  private placeOf(key: ChannelKey): number | undefined {
    const own = this.lanes?.lane(key).place;
    const watermark = this.delivery.watermark(key);
    if (own === undefined) return watermark;
    return watermark === undefined ? own : Math.max(own, watermark);
  }

  /**
   * Route one message. See the delivery model in the file header. A forward
   * advances the watermark once the host has accepted it; an ambient message
   * dropped on a closed channel does not, so the next start's catch-up can still find
   * it if the host opens the channel meanwhile.
   */
  private async route(ctx: Ctx, message: KeyedMessage, newChannel?: ChannelDescriptor, gap?: GapRead): Promise<void> {
    const key = message.key;
    const id = Number(message.messageId);
    const meta = (typeof message.metadata === 'object' && message.metadata !== null ? message.metadata : {}) as Record<string, unknown>;
    const addressed = meta.mentioned === true || meta.isDM === true || meta.replyToSelf === true;

    // Muted stream, or outside the allowlist: nothing reaches the agent —
    // ambient AND mentions — and nothing is tallied, before any other
    // routing. Rechecked here (not only at the event) for what was held.
    if (this.isMuted(key) || !this.isAllowed(key, message)) {
      ctx.decide(id);
      return this.letGo(ctx.w, key, id);
    }

    // §6.7: a disabled zulip.messaging stops its traffic at once — incoming
    // and push alike. Held, not let go: the host asked not to hear it now,
    // which is not the same as having heard it.
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      ctx.decide(id);
      return ctx.w.delivery.hold(key, id, 'failed');
    }

    // A channel the host has never seen — a DM from someone new, or a stream
    // the bot was added to after startup (#20): make it a registered channel
    // first, so the host can open it and route a reply back to it. The
    // message that arrived is itself the proof that the channel is reachable.
    if (newChannel && this.ids.heldIdOf(key) === undefined) {
      void this.channelManager.registerAdditional([newChannel]);
      const entryId = numericId(message);
      ctx.lane.enterFirst((c) => this.route(c, message), { kind: 'message', ...(entryId !== null ? { id: entryId } : {}) });
      return;
    }

    // Live delivery shows what was shared: images downsampled to model-max,
    // small text files inline. The reference note stays so anything not
    // inlined can still be fetched.
    const refs = Array.isArray(meta.attachments) ? (meta.attachments as AttachmentRef[]) : [];
    if (refs.length > 0 && this.options.attachments && (addressed || this.channelManager.isKeyOpen(key))) {
      const conn = this.conn;
      const { source, inline, deadline = () => AbortSignal.timeout(ATTACHMENT_DEADLINE_MS) } = this.options.attachments;
      const blocks = await buildAttachmentBlocks(refs, source, inline, deadline());
      if (conn === null || conn.isClosed || this.conn !== conn) return;
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

    ctx.decide(id);
    if (this.isMuted(key) || !this.isAllowed(key, message)) return this.letGo(ctx.w, key, id);

    // The watermark moves in onDelivered, once the host has accepted it.
    if (this.deliverToOpen(ctx.w, key, message)) {
      if (gap) gap.tally.recovered++;
      return;
    }

    if (addressed) {
      const answered = (w: DeliveryWriter, delivered: boolean) => {
        if (delivered) {
          // The agent's answer goes back through channels/publish, so it goes
          // to this message's topic, as an answer on an open channel does.
          w.channels.noteConversation(key, message);
          if (this.accepted(w, key, id)) this.delivery.save();
          this.enterHeldReads();
        } else {
          this.notAccepted(w, key, id, 'failed', message);
        }
      };
      const sent = this.pushEvent(message, `zulip_msg_${message.messageId}`);
      void sent.then((delivered) => ctx.lane.answer((c) => answered(c.w, delivered)));
      return;
    }

    this.letGo(ctx.w, key, id);
    if (ctx.w.delivery.countMissed(key, { id, text: bodyText })) this.delivery.save();
  }

  private messagingChanged(): void {
    if (!this.lanes || !this.mcplActive) return;
    const active = this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET);
    if (!active && this.messagingHold === null) {
      this.messagingHold = this.lanes.holdAll();
    } else if (active && this.messagingHold !== null) {
      this.messagingHold();
      this.messagingHold = null;
      this.enterHeldReads(true);
    }
  }

  private enterHeldReads(first = false): void {
    if (!this.lanes || this.lanes.allHeld || !this.conn || !this.mcplActive) return;
    for (const key of this.delivery.heldChannels()) {
      const letGo = this.isMuted(key) || !this.isAllowed(key);
      if (!letGo && this.delivery.heldIds(key, { replayable: true }).length === 0) continue;
      const lane = this.lanes.lane(key);
      if (lane.isEnded || this.heldReadWaiting.has(lane)) continue;
      this.heldReadWaiting.add(lane);
      const read: ReadJob<DeliveryWriter> = (r) => {
        this.heldReadWaiting.delete(lane);
        if (!r.ended) return this.readHeld(r, key);
      };
      if (first) lane.enterReadFirst(read);
      else lane.enterRead(read);
    }
  }

  private async readHeld(r: Reading, key: ChannelKey, gap?: GapRead): Promise<Hand | void> {
    if (this.isMuted(key) || !this.isAllowed(key)) {
      return (ctx) => {
        const place = this.placeOf(key);
        for (const id of ctx.w.delivery.heldIds(key)) if (place !== undefined && id <= place) ctx.w.delivery.release(key, id);
        this.delivery.save();
      };
    }
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const place = this.placeOf(key);
    if (place === undefined) return;
    const ids = this.delivery.heldIds(key, { replayable: true }).filter((id) => id <= place);
    if (ids.length === 0) return;
    const open = this.channelManager.isKeyOpen(key);
    let hand: Hand | void;
    try {
      if (open) {
        hand = await this.readReplay(r, key, ids, gap);
      } else {
        const watermark = this.delivery.watermark(key);
        const after = (this.delivery.floor(key) ?? ids[0]) < ids[0] ? ids[0] - 1 : watermark ?? ids[0] - 1;
        hand = await this.readCatchUp(r, key, {
          keepAll: this.delivery.wasOpen(key), after, through: () => place, answers: gap?.answers,
          onAttempt: (w, through) => w.delivery.markReplayed(key, ids.filter((id) => id <= through)),
        });
      }
    } catch (err) {
      console.error(`[zulip-mcp] replay of undelivered messages on ${this.label(key)} failed:`, (err as Error).message);
    }
    return async (ctx) => {
      if (ctx.ended) return;
      if (open && !hand) ctx.w.delivery.markReplayed(key, ids);
      try {
        await hand?.(ctx);
      } catch (err) {
        console.error(`[zulip-mcp] replay of undelivered messages on ${this.label(key)} failed:`, (err as Error).message);
      }
      this.delivery.save();
    };
  }

  private async readReplay(r: Reading, key: ChannelKey, ids: number[], gap?: GapRead): Promise<Hand | void> {
    if (!this.adapter.fetchHistory || ids.length === 0) return;
    const fetched = await this.fetchAfter(r, key, ids[0] - 1, ids[ids.length - 1]);
    return async (ctx) => {
      if (ctx.ended) return;
      const wanted = new Set(ids);
      const msgs = fetched.messages.filter((m) => {
        const id = numericId(m);
        return id !== null && wanted.has(id);
      });
      this.letGoUnfound(ctx.w, key, ids, fetched);
      const rows = this.projectHistoryReactions(msgs);
      for (const m of rows) {
        if (ctx.ended) return;
        await this.route(ctx, recovered(m), undefined, gap);
        const id = numericId(m);
        if (id !== null && !ctx.ended) ctx.w.delivery.markReplayed(key, [id]);
      }
      if (rows.length > 0) console.error(`[zulip-mcp] ${this.label(key)}: replaying ${rows.length} undelivered message(s) from history`);
    };
  }

  private letGoUnfound(w: DeliveryWriter, key: ChannelKey, ids: number[], fetched: HistoryRead): void {
    const found = new Set(fetched.messages.map((m) => numericId(m)));
    const scanned = fetched.scannedThrough ?? 0;
    const gone = ids.filter((id) => !found.has(id) && (id <= scanned || fetched.complete));
    for (const id of gone) w.delivery.release(key, id);
    if (gone.length > 0) this.delivery.save();
  }

  private heldBetween(key: ChannelKey, after: number, through: number): number[] {
    return this.delivery.heldIds(key).filter((id) => id > after && id <= through);
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
      const result = (await this.outToHost(conn.sendRequest(method.PUSH_EVENT, params))) as { accepted?: boolean; reason?: string } | undefined;
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
   * On (re)connect, read what arrived on a channel while the server was
   * offline, and hand it on: the channel's full missed backscroll if the
   * host had it open, else each addressed message with its vicinity, from
   * the lane's place up to the first message that waits in the lane.
   * Deferred quote verification can split a channel into multiple
   * `<missed>` pushes. On a closed channel the tally's backfill follows.
   */
  private async readStart(r: Reading, key: ChannelKey, renamed: boolean, answers: Promise<boolean>[]): Promise<Hand> {
    let catchUp: Hand | void;
    let backfill: Hand | void;
    if (this.conn && this.mcplActive && this.adapter.fetchHistory && this.catchupLimit > 0 &&
        this.grant.has('pushEvents') && this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) {
      const place = this.placeOf(key);
      if (place !== undefined) {
        catchUp = await this.readCatchUp(r, key, {
          keepAll: this.delivery.wasOpen(key), after: place, through: () => this.firstWaiting(key) - 1, answers, fill: true,
        });
      }
      if (!r.ended && this.delivery.tally(key)) backfill = await this.readBackfill(r, key);
    }
    return async (ctx) => {
      await catchUp?.(ctx);
      if (!ctx.ended) await backfill?.(ctx);
      if (answers.length > 0) {
        void Promise.all(answers).then((oks) => {
          if (oks.includes(true)) console.error(`[zulip-mcp] Reconnect catch-up: delivered missed messages on ${this.label(key)}`);
        });
      }
      if (renamed) {
        const closeUnlessOpen = (c: Ctx) => {
          if (c.ended || this.channelManager.isKeyOpen(key)) return;
          c.w.delivery.markClosed(key);
          this.delivery.save();
        };
        if (answers.length === 0) closeUnlessOpen(ctx);
        else void Promise.all(answers).then(() => ctx.lane.answer(closeUnlessOpen));
      }
      this.delivery.save();
    };
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
    r: Reading,
    key: ChannelKey,
    afterId: number,
    throughId?: number,
    quoteLookupBudget?: { remainingMs: number },
  ): Promise<HistoryRead> {
    const out: KeyedMessage[] = [];
    const unverified = new Set<number>();
    let cursor = afterId;
    let complete = false;
    while (out.length < this.catchupLimit) {
      if (!r.reading) throw new Error(`a read of ${this.label(key)}'s history after its job gave the turn back`);
      const want = this.catchupLimit - out.length;
      const page = await this.adapter.fetchHistory!(key, { limit: want, afterMessageId: String(cursor), quoteLookupBudget, timeoutMs: LONG_TIMEOUT_MS });
      if (page.scannedThrough === null) {
        complete = true;
        break;
      }
      for (const id of page.unverifiedMessageIds ?? []) {
        const numeric = Number(id);
        if (Number.isFinite(numeric) && !this.delivery.verifiedReply(key, numeric)) unverified.add(numeric);
      }
      out.push(...page.messages.map((message) => {
        if (!this.delivery.verifiedReply(key, Number(message.messageId))) return message;
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

  /**
   * Read what a closed (or not-yet-reopened) channel accumulated past its
   * watermark. What it hands on delivers it as `<missed>` push events: the
   * full backscroll when `keepAll` (the host had it open, or it is a DM),
   * else each addressed message with its vicinity. It advances the
   * watermark past everything scanned once the host has accepted the push.
   */
  private async readCatchUp(r: Reading, key: ChannelKey, read: CatchUpRead): Promise<Hand | void> {
    if (!this.adapter.fetchHistory || this.catchupLimit === 0) return;
    if (this.ids.heldIdOf(key) === undefined) return;
    if (this.isMuted(key) || !this.isAllowed(key)) return;
    if (!this.grant.has('pushEvents') || !this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const after = read.after ?? this.delivery.watermark(key);
    if (after === undefined) return;
    // At most two quote-wait allowances for the entire call, not per row or
    // page. Reserve the second allowance for one pass over ALL deferred rows.
    const budget = { remainingMs: 6_000 };
    const first = await this.readCatchUpPass(r, key, read, { after, bound: read.through?.(), retry: false }, budget, Math.min(budget.remainingMs, 3_000));
    if (!first.fetched || first.fetched.scannedThrough === null) return (ctx) => this.handCatchUp(ctx, key, read, first);
    if (read.ceiling) read.ceiling.reached = first.fetched.truncated;
    // The one retry stays inside the first walk's ceiling. Any quote still
    // unverified on that pass fails closed rather than starting another walk.
    const window = { through: Math.min(first.fetched.scannedThrough, read.through?.() ?? Infinity), truncated: first.fetched.truncated };
    const blocked = first.fetched.unverifiedMessageIds.find((id) => id > after && id <= window.through);
    if (blocked === undefined) return (ctx) => this.handCatchUp(ctx, key, read, first, window);
    const second = await this.readCatchUpPass(r, key, read, { after: blocked - 1, bound: window.through, retry: true }, budget, budget.remainingMs);
    return (ctx) => this.handCatchUp(ctx, key, read, first, window, second);
  }

  private async readCatchUpPass(
    r: Reading,
    key: ChannelKey,
    read: CatchUpRead,
    pass: CatchUpPass,
    budget: { remainingMs: number },
    passMs: number,
  ): Promise<CatchUpPass> {
    const passBudget = { remainingMs: passMs };
    try {
      const bound = pass.bound !== undefined && Number.isFinite(pass.bound) ? pass.bound : undefined;
      return { ...pass, fetched: await this.fetchAfter(r, key, pass.after, bound, passBudget) };
    } catch (err) {
      if (read.fill && triesAgain(err)) throw err;
      console.error(`[zulip-mcp] catch-up: history fetch failed for ${this.label(key)}:`, (err as Error).message);
      return pass;
    } finally {
      budget.remainingMs -= passMs - passBudget.remainingMs;
    }
  }

  /**
   * Hand one pass of a catch-up read on, up to the first message that waits
   * in the lane now. A first pass that a row with an unverified quote ended
   * hands the second pass on after it.
   */
  private handCatchUp(ctx: Ctx, key: ChannelKey, read: CatchUpRead, pass: CatchUpPass, window?: { through: number; truncated: boolean }, second?: CatchUpPass): void {
    const { after, fetched } = pass;
    if (!fetched) {
      read.onAttempt?.(ctx.w, Infinity); // A history failure counts as the held read's attempt, unlike a local policy abort.
      return;
    }
    if (ctx.ended) return;
    if (fetched.scannedThrough === null || !window) {
      this.letGoUnfound(ctx.w, key, this.heldBetween(key, after, Math.min(pass.bound ?? Infinity, read.through?.() ?? Infinity)), fetched);
      read.onAttempt?.(ctx.w, Infinity);
      return;
    }
    const truncated = fetched.truncated || (pass.retry && window.truncated);
    let newestId = Math.min(fetched.scannedThrough, window.through, read.through?.() ?? Infinity);
    if (newestId <= after) return;
    let msgs = fetched.messages.filter((message) => Number(message.messageId) > after && Number(message.messageId) <= newestId);
    const blocked = second && fetched.unverifiedMessageIds.find((id) => id > after && id <= newestId);
    const handSecond = () => {
      if (second) this.handCatchUp(ctx, key, read, second, window);
    };
    if (blocked !== undefined) {
      newestId = Math.min(newestId, blocked - 1);
      msgs = msgs.filter((message) => Number(message.messageId) < blocked);
      if (newestId <= after) return handSecond();
    }
    ctx.decide(newestId);
    const views = msgs.map(viewOf);
    const keepAll = read.keepAll || isDmKey(key);
    const kept = selectMissed(views, { keepAll, vicinity: MISSED_VICINITY });
    if (kept.length === 0) {
      read.onAttempt?.(ctx.w, Infinity);
      ctx.w.delivery.advanceThrough(key, newestId, after);
      if (blocked !== undefined) handSecond();
      return;
    }
    const channelId = this.ids.channelIdOf(key)!;
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
      moreBeyond: (truncated && !read.ceiling) || blocked !== undefined,
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
    ctx.w.delivery.hold(key, firstId, 'queued');
    const sent = this.pushEvent(synthetic, `zulip_missed_${channelId}_${newestId}`, {
      missed: true,
      reason,
      messages: kept.length,
      ...((truncated && !read.ceiling) || blocked !== undefined ? { truncated: true } : {}),
    }, () => {
      offered = true;
      read.onAttempt?.(ctx.w, newestId);
    });
    const answered = (w: DeliveryWriter, ok: boolean) => {
      if (ok) {
        // Advance past everything scanned, not just what was delivered, so a
        // mention-only channel does not re-surface its non-mention tail; the
        // block covered every held id in the range, so their holds go too.
        w.delivery.advanceThrough(key, newestId, after);
        // The agent answers the block through channels/publish too, so the
        // conversation is where its newest message that addresses the agent
        // is (in a backscroll, its newest message), as live pushes or an open
        // channel's deliveries would have left it.
        for (let i = msgs.length - 1; i >= 0; i--) {
          if (!keepAll && !isAddressed(views[i])) continue;
          if (w.channels.noteConversation(key, msgs[i])) break;
        }
      } else if (offered && (this.isMuted(key) || !this.isAllowed(key))) {
        for (const id of w.delivery.heldIds(key)) if (id > after && id <= newestId) w.delivery.release(key, id);
      } else if (offered) {
        w.delivery.hold(key, firstId, 'rejected');
        w.delivery.refuse(key, after, newestId);
      } else {
        w.delivery.hold(key, firstId, 'failed');
      }
      this.delivery.save();
    };
    const answer = sent.then((ok) => new Promise<boolean>((resolve) => ctx.lane.answer((c) => {
      answered(c.w, ok);
      resolve(ok);
    })));
    read.answers?.push(answer);
    if (blocked !== undefined) handSecond();
  }

  /** Read the ambient that arrived on a tallied channel during downtime; what it hands on counts it. */
  private async readBackfill(r: Reading, key: ChannelKey): Promise<Hand | void> {
    if (!this.adapter.fetchHistory || this.isMuted(key) || !this.isAllowed(key)) return;
    const tally = this.delivery.tally(key);
    if (!tally?.talliedThrough) return;
    let fetched: HistoryRead;
    try {
      fetched = await this.fetchAfter(r, key, tally.talliedThrough);
    } catch {
      return;
    }
    const scannedThrough = fetched.scannedThrough;
    if (scannedThrough === null) return;
    // Only ambient counts as missed: addressed messages are delivered by the catch-up.
    const ambient = fetched.messages.map(viewOf).filter((v) => !isAddressed(v));
    return (ctx) => {
      if (!ctx.ended) ctx.w.delivery.backfillMissed(key, ambient, scannedThrough);
    };
  }

  /**
   * A Zulip event queue died and its replacement starts from "now". Each
   * channel's lane offers again what it holds, then reads the hole from
   * history, from its place up to the first message that waits in it, and
   * the gap marker follows on each open channel once every lane's read is
   * done, so the agent gets the messages, not advice to go looking for
   * them: open channels get them as recovered messages; every other
   * watermarked channel gets its addressed messages as a `<missed>` push,
   * as after a restart.
   */
  private onSystemEvent(event: PlatformSystemEvent): void {
    const lanes = this.lanes;
    const tally: GapTally = { recovered: 0, caughtUp: new Set() };
    if (event.kind !== 'gap' || !lanes || !this.adapter.fetchHistory) {
      this.channelManager.broadcastSystemEvent(this.adapter.type, event.kind === 'gap' ? gapNotice(event, tally) : event);
      return;
    }
    const keys = new Set<ChannelKey>(this.openKeys());
    for (const key of this.delivery.watermarkedChannels()) keys.add(key);
    for (const key of lanes.keys()) keys.add(key);
    const reads: Promise<void>[] = [];
    for (const key of keys) {
      if (this.isMuted(key) || !this.isAllowed(key)) continue;
      const lane = lanes.lane(key);
      if (lane.isEnded) continue;
      lane.enter((ctx) => {
        if (!ctx.ended) ctx.w.delivery.clearReplayed(key);
      });
      reads.push(new Promise((done) => lane.enterRead(async (r) => {
        const read: GapRead = { tally, answers: [] };
        const finish = () => void Promise.all(read.answers).then((oks) => {
          if (oks.includes(true)) tally.caughtUp.add(key);
          done();
        });
        let hand: Hand | void;
        try {
          if (!r.ended) hand = await this.readGap(r, key, read);
        } catch (err) {
          finish();
          throw err;
        }
        return async (ctx) => {
          try {
            await hand?.(ctx);
          } finally {
            finish();
          }
        };
      })));
    }
    void Promise.all(reads).then(() => {
      const notice = gapNotice(event, tally);
      const open = this.openKeys();
      if (open.length === 0) return this.channelManager.broadcastSystemEvent(this.adapter.type, notice);
      const timestamp = new Date().toISOString();
      for (const key of open) {
        lanes.lane(key).enter(() => this.channelManager.systemEventTo(key, this.adapter.type, notice, timestamp));
      }
    });
  }

  private openKeys(): ChannelKey[] {
    const keys: ChannelKey[] = [];
    for (const channelId of this.channelManager.getOpenChannels()) {
      const key = this.channelManager.keyOfHeld(channelId);
      if (key !== null && this.ids.channelIdOf(key) === channelId) keys.push(key);
    }
    return keys;
  }

  /** The break's job sends both its reads before it hands a row on: the held ids' rows, then its own. */
  private async readGap(r: Reading, key: ChannelKey, gap: GapRead): Promise<Hand> {
    const held = await this.readHeld(r, key, gap);
    const fill = await this.fillRead(r, (r2) => this.readFill(r2, key, gap));
    return async (ctx) => {
      await held?.(ctx);
      await fill?.(ctx);
    };
  }

  private giveUp(w: DeliveryWriter, lane: Lane<DeliveryWriter>): void {
    const key = lane.key;
    const ids = lane.replaceMessages((r) => (r.ended ? undefined : this.fillRead(r, (r2) => this.readGivenUp(r2, key))));
    const held = ids.filter(Number.isFinite);
    if (held.length > 0) {
      const first = Math.min(...held);
      for (const id of held) if (id !== first) w.delivery.release(key, id);
      w.delivery.hold(key, first, 'failed');
      this.delivery.save();
    }
    console.error(`[zulip-mcp] ${this.label(key)}: ${ids.length} waiting message(s) given up; a read from history takes their place`);
  }

  private async readGivenUp(r: Reading, key: ChannelKey): Promise<Hand> {
    const before = this.placeOf(key);
    const ceiling = { reached: false };
    const fill = await this.readFill(r, key, undefined, ceiling);
    return async (ctx) => {
      await fill?.(ctx);
      const after = this.placeOf(key);
      if (ctx.ended || !ceiling.reached || after === undefined || after === before || after >= this.firstWaiting(key) - 1) return;
      ctx.lane.enterReadFirst((r2) => (r2.ended ? undefined : this.fillRead(r2, (r3) => this.readGivenUp(r3, key))));
    };
  }

  private async fillRead(r: Reading, read: (r: Reading) => Promise<Hand | void>, tries = 0, since = this.now()): Promise<Hand | void> {
    const lane = r.lane;
    let hand: Hand | void;
    try {
      hand = await read(r);
    } catch (err) {
      if (!triesAgain(err)) {
        if (tries > 0) this.readWaitEnded(lane);
        throw err;
      }
      if (r.ended) return;
      const step = Math.min(READ_RETRY_FIRST_MS * 2 ** Math.min(tries, 30), READ_RETRY_MAX_MS);
      const ms = err instanceof ZulipRefused ? Math.max(step, (err.retryAfterSeconds ?? 0) * 1000) : step;
      console.error(`[zulip-mcp] reading ${this.label(lane.key)}'s history to fill a hole failed: ${err.message}; trying again in ${ms / 1000} s`);
      this.readWaits.add(lane.key);
      const ranOut = err instanceof ZulipNoAnswer && err.kind === 'timeout';
      if (!this.toldReadWait && (ranOut || this.now() - since + ms >= READ_WAIT_LINE_MS)) this.tellReadWait();
      lane.enterReadFirst((r2) => (r2.ended ? undefined : this.fillRead(r2, read, tries + 1, since)));
      lane.holdOtherUntil((this.options.readRetryWait ?? waitMs)(ms));
      return;
    }
    if (tries === 0) return hand;
    return async (ctx) => {
      try {
        await hand?.(ctx);
      } finally {
        this.readWaitEnded(lane);
      }
    };
  }

  private now(): number {
    return (this.options.now ?? Date.now)();
  }

  private tellReadWait(): void {
    this.toldReadWait = true;
    const channels = [...this.readWaits].map((key) => this.ids.channelIdOf(key) ?? this.label(key));
    this.tellOpen(
      `Zulip isn't serving reads of the history of ${channels.join(', ')}, and they are tried again. ` +
        'A channel whose read waits is silent until Zulip answers it; its messages then arrive in order.',
      { kind: 'history-waiting', channels },
    );
  }

  private readWaitEnded(lane: Lane<DeliveryWriter>): void {
    if (lane.isEnded || !this.readWaits.delete(lane.key)) return;
    if (!this.toldReadWait || this.readWaits.size > 0) return;
    this.toldReadWait = false;
    this.tellOpen('No channel waits for a read of its history from Zulip any more.', { kind: 'history-answered' });
  }

  private tellOpen(text: string, metadata: Record<string, unknown>): void {
    const open = this.openKeys();
    if (open.length === 0) {
      console.error(`[zulip-mcp] ${text} (no open channel to tell)`);
      return;
    }
    for (const key of open) this.channelManager.notifyChannel(key, text, metadata);
  }

  /** With `ceiling`, the read is one of a give-up's passes, and `ceiling.reached` says whether it stopped at the catch-up ceiling. */
  private async readFill(r: Reading, key: ChannelKey, gap?: GapRead, ceiling?: { reached: boolean }): Promise<Hand | void> {
    if (r.ended || this.isMuted(key) || !this.isAllowed(key)) return;
    if (!this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET)) return;
    const place = this.placeOf(key);
    const below = () => this.firstWaiting(key) - 1;
    if (place === undefined || below() <= place) return;
    if (!this.channelManager.isKeyOpen(key)) {
      return this.readCatchUp(r, key, { keepAll: false, after: place, through: below, answers: gap?.answers, ceiling, fill: true });
    }
    let fetched: HistoryRead;
    const bound = below();
    try {
      fetched = await this.fetchAfter(r, key, place, Number.isFinite(bound) ? bound : undefined);
    } catch (err) {
      if (triesAgain(err)) throw err;
      console.error(`[zulip-mcp] reading ${this.label(key)}'s history to fill a hole failed:`, (err as Error).message);
      return;
    }
    if (ceiling) ceiling.reached = fetched.truncated;
    return async (ctx) => {
      if (ctx.ended) return;
      this.letGoUnfound(ctx.w, key, this.heldBetween(key, place, Math.min(bound, below())), fetched);
      for (const m of this.projectHistoryReactions(fetched.messages)) {
        const id = numericId(m);
        if (id === null) continue;
        if (ctx.ended || id > below()) break;
        await this.route(ctx, recovered(m), undefined, gap);
      }
    };
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
    const lanes = this.lanes;
    let release!: () => void;
    const answered = new Promise<void>((resolve) => { release = resolve; });
    try {
      await Promise.all([
        ...renames.map(({ key, held, fresh }) => this.passJob(lanes, key, answered, () => this.tellRenamed(key, held, fresh))),
        ...removals.map(({ key, held }) => this.passJob(lanes, key, answered, () => this.tellRemoved(key, held))),
      ]);
      await this.channelManager.flush();
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
        if (wasOpen.get(key)) {
          lanes?.lane(key).answer((c) => {
            if (this.channelManager.isKeyOpen(key)) return;
            c.w.delivery.markClosed(key);
            this.delivery.save();
          });
        }
        console.error(`[zulip-mcp] stream ${streamIdOfKey(key)}: the host ${verdict === 'accepted' ? `now holds ${fresh}` : `refused ${fresh}`} in place of ${held}`);
      }
      for (const { key, held } of removals) console.error(`[zulip-mcp] stream ${streamIdOfKey(key)} is gone: the host no longer holds ${held}`);
      this.delivery.save();
    } finally {
      release();
    }
  }

  private passJob(lanes: Lanes<DeliveryWriter> | null, key: ChannelKey, answered: Promise<void>, line: () => void): Promise<void> {
    const lane = lanes?.lane(key);
    if (!lane) {
      line();
      return Promise.resolve();
    }
    return new Promise((ran) => lane.answer(() => {
      if (!lane.isEnded) {
        lane.holdOtherUntil(answered);
        line();
      }
      ran();
    }));
  }

  private tellRenamed(key: ChannelKey, held: string, fresh: string): void {
    if (held === fresh || !this.channelManager.isOpen(held) || this.toldAgent.get(key) === fresh) return;
    const from = held.slice('zulip:'.length);
    this.channelManager.notifyChannel(key,
      `Zulip stream #${from} was renamed #${fresh.slice('zulip:'.length)}. Its channel is now ${fresh}, and ${this.oldIdNow(key, from)}. ` +
        `Open ${fresh} to keep hearing it, unless it is already open. What you had read stays read.`,
      { kind: 'stream-renamed', from: held, to: fresh });
    this.toldAgent.set(key, fresh);
  }

  private tellRemoved(key: ChannelKey, held: string): void {
    if (!this.channelManager.isOpen(held) || this.toldAgent.get(key) === REMOVED_LINE) return;
    this.channelManager.notifyChannel(key,
      `Zulip stream #${held.slice('zulip:'.length)} was archived, or this bot lost access to it. This channel is gone.`,
      { kind: 'stream-removed', channelId: held });
    this.toldAgent.set(key, REMOVED_LINE);
  }

  private oldIdNow(key: ChannelKey, oldName: string): string {
    const holder = this.directory.idOf(oldName);
    return holder !== undefined && holder !== streamIdOfKey(key)
      ? `zulip:${oldName} now belongs to another stream, which took the name #${oldName}`
      : `zulip:${oldName} is gone`;
  }

  private pushRenamed(key: ChannelKey): void {
    const note = this.renamedUnannounced.get(key);
    const fresh = this.ids.channelIdOf(key);
    if (note === undefined || fresh === null) return;
    this.renamedUnannounced.delete(key);
    const { from, to } = note;
    const text = `Zulip stream #${from} was renamed #${to} since its channel was last open. Its channel is now ${fresh}, and ${this.oldIdNow(key, from)}. ` +
      `Open ${fresh} to keep hearing it. What you had read stays read.`;
    void this.pushEvent({
      key,
      messageId: `system:stream-renamed:${Date.now()}`,
      author: { id: 'system', name: 'zulip connection' },
      timestamp: new Date().toISOString(),
      content: [{ type: 'text', text }],
      metadata: { system: true, kind: 'stream-renamed', from: `zulip:${from}`, to: fresh },
    }, `zulip_stream_renamed_${key}_${to}`, { kind: 'stream-renamed', from: `zulip:${from}`, to: fresh }).then((pushed) => {
      if (!pushed) console.error(`[zulip-mcp] ${fresh} (was zulip:${from}) was open at last sight; the push saying so did not land`);
    });
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
      (entry) => this.onIncoming(entry),
      (event) => {
        try {
          this.onSystemEvent(event);
        } catch (err) {
          console.error('[zulip-mcp] system event handling failed:', (err as Error).message);
        }
      },
      (reaction) => {
        if (this.messagingHold !== null) return;
        const unnamed = !isDmKey(reaction.key) && this.streamNameOf(reaction.key) === null;
        if (!unnamed && !this.reactionShown(reaction)) return;
        this.lanes?.lane(reaction.key).enter((ctx) => {
          const placed = reaction.placed();
          if (placed.key === reaction.key) return this.onReaction(ctx.w, placed);
          this.lanes?.lane(placed.key).enter((c) => this.onReaction(c.w, placed));
        });
      },
      (entry) => {
        const lane = this.lanes?.lane(entry.key);
        if (!lane) return;
        if (this.messagingHold !== null) {
          const waiting = new Set(lane.waitingMessages());
          if (!entry.messageIds.some((id) => waiting.has(id))) return;
        }
        lane.enter(async (ctx) => {
          const change = await entry.ready();
          if (change === null || ctx.ended) return;
          if (change.key === entry.key) return this.onMessageChange(ctx, change);
          this.lanes?.lane(change.key).enter((c) => this.onMessageChange(c, change), { kind: 'other', about: entry.messageIds });
        }, { kind: 'other', about: entry.messageIds });
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
  private async onReaction(w: DeliveryWriter, ev: ReactionEvent): Promise<void> {
    if (!this.reactionShown(ev)) return;
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
      w.channels.onIncomingMessage(ev.key, message);
    } else {
      void this.pushEvent(message, `zulip_reaction_${ev.action}_${ev.messageId}_${ev.reactorId}_${ev.timestamp.getTime()}`, {
        reaction: true,
        action: ev.action,
        onOwnMessage: ev.onOwnMessage,
      });
    }
  }

  private reactionShown(ev: ReactionEvent): boolean {
    if (!this.filters || !this.filters.reactionsVisible(this.ids.freshIdOf(ev.key) ?? '')) return false;
    if (this.isMuted(ev.key) || !this.isAllowed(ev.key)) return false;
    if (this.filters.reactionSuppressed(ev.emoji, ev.emojiCode, ev.emojiType)) return false;
    return this.mcplActive && this.grant.isFeatureSetActive(MESSAGING_FEATURE_SET);
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
  private async onMessageChange(ctx: Ctx, ev: MessageChangeEvent, announce = true): Promise<void> {
    if (ev.kind === 'edit' && ev.replyToSelf === false && ev.replyToSelfVerified === true) ctx.w.delivery.forgetReply(ev.key, Number(ev.messageId));
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
        ctx.lane.enterFirst((c) => this.onMessageChange(c, ev, false));
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
      ctx.w.channels.onIncomingMessage(ev.key, message);
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
    return (await this.outToHost(conn.sendRequest(method.CHANNELS_REGISTER, { channels }))) as ChannelsRegisterResult | undefined;
  }

  private async channelsChangedWithHost(params: ChannelsChangedParams, timeoutMs = this.announceTimeoutMs): Promise<ChannelsRegisterResult | undefined> {
    const conn = this.conn;
    if (!conn) throw new Error('not connected');
    const answer = this.outToHost(conn.sendRequest(method.CHANNELS_CHANGED, params, timeoutMs));
    for (const descriptor of params.added ?? []) {
      const key = this.ids.keyOfDescriptor(descriptor);
      if (key !== null) this.lanes?.lane(key).holdOtherUntil(answer);
    }
    return (await answer) as ChannelsRegisterResult | undefined;
  }

  private outToHost<T>(answer: Promise<T>): Promise<T> {
    this.hostRequestsOut.add(answer);
    const settled = () => this.hostRequestsOut.delete(answer);
    answer.then(settled, settled);
    return answer;
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

function gapNotice(event: PlatformSystemEvent, tally: GapTally): PlatformSystemEvent {
  const notes = [
    tally.recovered > 0 ? `${tally.recovered} message(s) on open channels were recovered from history and delivered above.` : null,
    tally.caughtUp.size > 0 ? `Addressed messages on ${tally.caughtUp.size} closed channel(s) were delivered as catch-up events.` : null,
  ].filter(Boolean);
  return {
    ...event,
    text: notes.length > 0 ? `${event.text} ${notes.join(' ')}` : event.text,
    metadata: { ...event.metadata, recoveredMessages: tally.recovered, closedChannelsCaughtUp: tally.caughtUp.size },
  };
}

function triesAgain(err: unknown): err is ZulipNoAnswer | ZulipRefused {
  return err instanceof ZulipNoAnswer || (err instanceof ZulipRefused && err.code === 'RATE_LIMIT_HIT');
}

function waitMs(ms: number): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    (timer as { unref?: () => void }).unref?.();
  });
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
