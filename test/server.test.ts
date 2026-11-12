/**
 * ZulipMcplServer — the wire. A host talks to the server over an in-memory
 * stream pair through the same `McplConnection` both ends use in
 * production, so these cases exercise the actual JSON-RPC framing, the
 * initialize handshake, the §5.3 policy exchange, channel registration,
 * incoming delivery with tags, publish routing, and the tool surface in both
 * plain-MCP and MCPL modes.
 *
 * Run: node --import tsx --test test/server.test.ts
 */

import test, { beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import {
  McplConnection,
  method,
  type ChannelDescriptor,
  type ChannelsIncomingParams,
  type ChannelsRegisterParams,
  type ContentBlock,
  type ContextInjection,
  type IncomingChannelMessage,
  type JsonRpcRequest,
  type PushEventParams,
  type TextContent,
} from '@animalabs/mcpl-core';
import { mkdtempSync, rmSync, readFileSync, utimesSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ChannelHistoryPage, ChannelHistoryQuery, MessageChangeEvent, OnIncomingMessage, OnMessageChange, OnReaction, OnSystemEvent, PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
import { ZulipMcplServer, type ZulipMcplServerOptions } from '../src/server.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import type { ZulipRawMessage } from '../src/history.ts';
import { FiltersPlane } from '../src/filters.ts';
import type { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { toolDefinitions } from '../src/tools.ts';
import { TOOL_CLASSES } from '../src/tool-classes.ts';
import { standInApi } from './zulip-stand-in.ts';
import { ZulipNoAnswer, ZulipRefused } from '../src/zulip-api.ts';
import { dmKey, streamKey, type ChannelKey, type KeyedMessage } from '../src/channel-key.ts';
import { parseDmChannelId } from '../src/history.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import type { DeliveryState } from '../src/delivery.ts';

// Isolate the process-wide limiter between tests, without bypassing it in
// individual cases (including the multi-adapter saturation regression).
beforeEach(() => {
  (ZulipAdapter as unknown as {
    quoteBudget: { tokens: number; updatedAt: number };
  }).quoteBudget = { tokens: 30, updatedAt: performance.now() };
});

const DESCRIPTOR: ChannelDescriptor = {
  id: 'zulip:general',
  type: 'zulip',
  label: '#general',
  direction: 'bidirectional',
  address: { stream_name: 'general', stream_id: 7 },
};

const HISTORY_STREAMS = [{ stream_id: 7, name: 'general' }, { stream_id: 8, name: 'other' }];

function historyStreams(): StreamDirectory {
  const directory = new StreamDirectory();
  directory.registration(HISTORY_STREAMS);
  return directory;
}

function narrowedStream(narrow: unknown[]): string {
  const operand = (narrow[0] as { operand?: unknown }).operand;
  return HISTORY_STREAMS.find((st) => st.stream_id === operand)?.name ?? String(operand);
}

interface FakeAdapter extends Omit<PlatformAdapter, 'startEvents'> {
  published: { channelId: string; content: ContentBlock[]; hints?: RoutingHints }[];
  typing: { channelId: string; op: string; metadata?: Record<string, unknown> }[];
  emit: ((message: IncomingChannelMessage | KeyedMessage, newChannel?: ChannelDescriptor) => void) | null;
  enter: OnIncomingMessage | null;
  enterChange: OnMessageChange | null;
  systemEvent: OnSystemEvent | null;
  react: ((event: Omit<Parameters<OnReaction>[0], 'key'> & { channelId: string }) => void) | null;
  change: ((event: Omit<MessageChangeEvent, 'key' | 'movedToKey'> & { channelId: string; movedToChannelId: string | null }) => void) | null;
  /** The stream's messages, oldest first; fetchHistory pages over them by id. */
  history: IncomingChannelMessage[];
  historyCalls: { channelId: string; query: ChannelHistoryQuery }[];
  subscribed: string[];
  acked: string[];
  deleted: string[];
  /** Streams whose subscription fails, with the reason. */
  subscribeError: Map<string, string>;
  /** The platform's page ceiling (Zulip: 5000); smaller than the ask forces paging. */
  pageCap: number;
  /** What discoverChannels answers now — a stream the bot joins later is pushed here. */
  visible: ChannelDescriptor[];
  startEvents(onMessage: OnIncomingMessage, onSystemEvent?: OnSystemEvent, onReaction?: OnReaction, onMessageChange?: OnMessageChange): void;
  bind(server: ZulipMcplServer): void;
  keyOf(channelId: string): ChannelKey;
  wireOf(key: ChannelKey): string;
}

function fakeAdapter(withTyping = true): FakeAdapter {
  let server: ZulipMcplServer | null = null;
  let nextStreamId = 1000;
  const directory = () => server!.directory;
  const streamList = (descriptors: ChannelDescriptor[]) => descriptors.map((d) => {
    const a = (d.address ?? {}) as { stream_id?: unknown; stream_name?: unknown };
    return { stream_id: a.stream_id, name: a.stream_name };
  });
  const learn = (descriptors: ChannelDescriptor[]): ChannelDescriptor[] => {
    directory().list(streamList(descriptors));
    return descriptors;
  };
  let discover = async () => [...adapter.visible];
  let describe = async (keys: ChannelKey[]) => adapter.visible.filter((c) => keys.includes(adapter.keyOf(c.id)));
  const adapter = {
    type: 'zulip',
    published: [],
    typing: [],
    emit: null,
    enter: null,
    enterChange: null,
    systemEvent: null,
    react: null,
    change: null,
    history: [],
    historyCalls: [],
    subscribed: [],
    acked: [],
    deleted: [],
    subscribeError: new Map(),
    pageCap: Infinity,
    visible: [DESCRIPTOR],
    bind(s: ZulipMcplServer) { server = s; s.directory.registration(streamList(adapter.visible)); },
    keyOf(channelId: string): ChannelKey {
      const dm = parseDmChannelId(channelId);
      if (dm) return dmKey(dm);
      learn(adapter.visible);
      const held = server!.ids.resolve(channelId);
      if (held && 'key' in held) return held.key;
      const name = channelId.slice('zulip:'.length);
      let id = directory().idOf(name);
      if (id === undefined) {
        id = nextStreamId++;
        directory().list([{ stream_id: id, name }]);
      }
      return streamKey(id);
    },
    wireOf(key: ChannelKey): string {
      return server!.ids.channelIdOf(key) ?? String(key);
    },
    async refreshStreams() { learn(adapter.visible); },
    async fetchHistory(key: ChannelKey, query: ChannelHistoryQuery): Promise<ChannelHistoryPage> {
      adapter.historyCalls.push({ channelId: adapter.wireOf(key), query });
      let rows = adapter.history.filter((m) => adapter.keyOf(m.channelId) === key);
      // As history.ts does: the after-cursor wins when both are given.
      if (query.afterMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) > Number(query.afterMessageId));
      else if (query.beforeMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) < Number(query.beforeMessageId));
      const limit = Math.min(query.limit, adapter.pageCap);
      const page = query.afterMessageId !== undefined ? rows.slice(0, limit) : rows.slice(-limit);
      const newest = page[page.length - 1];
      return {
        messages: page.map(({ channelId: _c, ...m }) => ({ ...m, key, metadata: { ...(m.metadata as object), backscroll: true } })),
        scannedThrough: newest ? Number(newest.messageId) : null,
        reachedNewest: !newest || newest === rows[rows.length - 1],
      };
    },
    async ensureSubscribed(key: ChannelKey) {
      const channelId = adapter.wireOf(key);
      const reason = adapter.subscribeError.get(channelId);
      if (reason) throw new Error(reason);
      adapter.subscribed.push(channelId);
    },
    async acknowledge(_key: ChannelKey | null, messageId: string, value?: string) { adapter.acked.push(`${messageId}:${value ?? ''}`); return `:${value ?? 'eyes'}:`; },
    async deleteMessage(_channelId: string, messageId: string) { adapter.deleted.push(messageId); },
    async publish(target: ChannelKey | { streamName: string }, _descriptor: ChannelDescriptor | undefined, content: ContentBlock[], hints?: RoutingHints) {
      const channelId = typeof target === 'string' ? adapter.wireOf(target) : `zulip:${target.streamName}`;
      adapter.published.push({ channelId, content, hints });
      const n = adapter.published.length;
      const text = content.filter((b): b is TextContent => b.type === 'text').map((b) => b.text).join('\n');
      adapter.onSent?.({ messageId: String(40 + n), channelId, content: text });
      return { delivered: true, messageId: String(40 + n), messageIds: [String(40 + n)] };
    },
    async fetchContext() {
      return { position: 'beforeUser' as const, content: 'recent history' };
    },
    startEvents(onMessage: OnIncomingMessage, onSystemEvent?: OnSystemEvent, onReaction?: OnReaction, onMessageChange?: OnMessageChange) {
      directory().registration(streamList(adapter.visible));
      adapter.emit = (message, newChannel) => {
        if (newChannel) learn([newChannel]);
        let keyed = message as unknown as KeyedMessage;
        if (!('key' in message)) {
          const { channelId, ...m } = message;
          keyed = { ...m, key: adapter.keyOf(channelId) } as KeyedMessage;
        }
        onMessage({ key: keyed.key, messageId: keyed.messageId, ready: async () => ({ message: keyed, ...(newChannel ? { newChannel } : {}) }) });
      };
      adapter.enter = onMessage;
      adapter.enterChange = onMessageChange ?? null;
      adapter.systemEvent = onSystemEvent ?? null;
      adapter.react = onReaction ? ({ channelId, ...ev }) => onReaction({ ...ev, key: adapter.keyOf(channelId) }) : null;
      adapter.change = onMessageChange
        ? ({ channelId, movedToChannelId, ...ev }) => {
            const change = { ...ev, key: adapter.keyOf(channelId), movedToKey: movedToChannelId === null ? null : adapter.keyOf(movedToChannelId) };
            onMessageChange({ key: change.key, messageIds: change.messageIds.map(Number), ready: async () => change });
          }
        : null;
    },
    stopEvents() { adapter.emit = null; adapter.enter = null; adapter.enterChange = null; },
  } as unknown as FakeAdapter;
  Object.defineProperty(adapter, 'discoverChannels', {
    get: () => async () => learn(await discover()),
    set: (fn: () => Promise<ChannelDescriptor[]>) => { discover = fn; },
  });
  Object.defineProperty(adapter, 'describeChannels', {
    get: () => async (keys: ChannelKey[]) => learn(await describe(keys)),
    set: (fn: (keys: ChannelKey[]) => Promise<ChannelDescriptor[]>) => { describe = fn; },
  });
  if (withTyping) {
    adapter.sendTyping = async (key, _d, m, op) => { adapter.typing.push({ channelId: adapter.wireOf(key), op, metadata: m as Record<string, unknown> | undefined }); };
  }
  return adapter;
}

const KEY_LISTS = new Set(['watermarkedChannels', 'heldChannels', 'lastOpenChannels', 'talliedChannels']);

function byChannelId(delivery: DeliveryState, adapter: FakeAdapter): DeliveryState {
  return new Proxy(delivery, {
    get(target, prop) {
      const value = Reflect.get(target, prop, target);
      if (typeof value !== 'function') return value;
      return (...args: unknown[]) => {
        if (typeof args[0] === 'string' && args[0].startsWith('zulip:')) args[0] = adapter.keyOf(args[0]);
        const out = value.apply(target, args);
        return KEY_LISTS.has(String(prop)) ? (out as ChannelKey[]).map((k) => adapter.wireOf(k)) : out;
      };
    },
  });
}

const fakeTools = {
  calls: [] as { name: string; args: Record<string, unknown> }[],
  /** Set by the server; the real runtime reports every message a tool sends. */
  onSent: null as null | ((sent: { messageId: string; channelId: string; content: string }) => void),
  /** Set by the server; the real runtime reports streams `listen` subscribed to. */
  onSubscribed: null as null | ((streams: string[]) => unknown),
  hold: null as Promise<unknown> | null,
  async handleToolCall(name: string, args: Record<string, unknown>) {
    fakeTools.calls.push({ name, args });
    if (name === 'explode') throw new Error('boom');
    if (name === 'slow') {
      await fakeTools.hold;
      fakeTools.calls.push({ name: 'slow:end', args });
    }
    if (name === 'send_message') fakeTools.onSent?.({ messageId: '99', channelId: 'zulip:general', content: String(args.content ?? '') });
    if (name === 'listen') {
      // The real runtime subscribes, then awaits onSubscribed and reports it.
      const registration = await fakeTools.onSubscribed?.(args.channels as string[]);
      return { result: 'success', subscribed: args.channels, ...(registration ? { registration } : {}) };
    }
    return { ok: true, name };
  },
  listResources() {
    return [{ uri: 'zulip://monitoring/status', name: 'status', description: '', mimeType: 'application/json' }];
  },
  async readResource(uri: string) {
    if (uri !== 'zulip://monitoring/status') throw new Error(`Unknown resource: ${uri}`);
    return { contents: [{ uri, mimeType: 'application/json', text: '{}' }] };
  },
};

interface Harness {
  server: ZulipMcplServer;
  delivery: DeliveryState;
  adapter: FakeAdapter;
  host: McplConnection;
  /** Server→host requests the host reactor answered, in order. */
  hostSaw: JsonRpcRequest[];
  incoming: IncomingChannelMessage[];
  pushed: PushEventParams[];
  served: Promise<void>;
  serverSide: McplConnection;
  /** How the host answers channels/incoming: per-message rejections (for
   *  good, or once), and whole batches that error out (counted down per batch). */
  policy: { rejectIds: Set<string>; rejectOnce: Set<string>; failIncomingBatches: number; changedAnswer: 'accept' | 'refuse' | 'error' | 'reconcile' | 'held'; reconcileOpens: { channelId: string; ok: boolean; detail: string }[]; rejectPushes: Set<string>; heldChanged: (() => void)[]; holdPushes: boolean; heldPushes: (() => void)[]; holdRegister: boolean; heldRegister: (() => void)[] };
  close(): Promise<void>;
}

interface HarnessOptions {
  mcpl?: boolean;
  typing?: boolean;
  stateDir?: string;
  sessionId?: string;
  history?: IncomingChannelMessage[];
  filters?: FiltersPlane;
  attachments?: ZulipMcplServerOptions['attachments'];
  batchWindowMs?: number;
  attributeDelivery?: boolean;
  announceTimeoutMs?: number;
  shutdownGraceMs?: number;
  maxWaitingMessages?: number;
  readRetryWait?: (ms: number) => Promise<void>;
  now?: () => number;
  /** A second connection to a server that already served one (the TCP case). */
  reuse?: { server: ZulipMcplServer; adapter: FakeAdapter };
  visible?: ChannelDescriptor[];
}

function harness(opts: HarnessOptions = {}): Harness {
  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const serverConn = McplConnection.fromStreams(toServer, toHost);
  const host = McplConnection.fromStreams(toHost, toServer);

  const adapter = opts.reuse?.adapter ?? fakeAdapter(opts.typing ?? true);
  if (opts.history) adapter.history = opts.history;
  if (opts.visible) adapter.visible = opts.visible;
  const server = opts.reuse?.server ?? new ZulipMcplServer(adapter as unknown as PlatformAdapter, fakeTools as unknown as ZulipToolRuntime, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: opts.mcpl ?? true,
    batchWindowMs: opts.batchWindowMs ?? 5,
    contextHistorySize: 3,
    stateDir: opts.stateDir ?? null,
    sessionId: opts.sessionId ?? 'test',
    catchupLimit: 100,
    formatTime: () => 'T',
    announceTimeoutMs: opts.announceTimeoutMs ?? 100,
    shutdownGraceMs: opts.shutdownGraceMs,
    maxWaitingMessages: opts.maxWaitingMessages,
    readRetryWait: opts.readRetryWait,
    now: opts.now,
    filters: opts.filters,
    attachments: opts.attachments,
    attributeDelivery: opts.attributeDelivery,
  });

  if (!opts.reuse) {
    adapter.bind(server);
  }
  const hostSaw: JsonRpcRequest[] = [];
  const incoming: IncomingChannelMessage[] = [];
  const pushed: PushEventParams[] = [];
  const policy = {
    rejectIds: new Set<string>(),
    rejectOnce: new Set<string>(),
    failIncomingBatches: 0,
    changedAnswer: 'accept' as 'accept' | 'refuse' | 'error' | 'reconcile' | 'held',
    reconcileOpens: [] as { channelId: string; ok: boolean; detail: string }[],
    rejectPushes: new Set<string>(),
    heldChanged: [] as (() => void)[],
    holdPushes: false,
    heldPushes: [] as (() => void)[],
    holdRegister: false,
    heldRegister: [] as (() => void)[],
  };
  // The host reactor: answer every server→host Request the way conhost does.
  host.on('request', (req) => {
    hostSaw.push(req);
    if (req.method === method.CHANNELS_REGISTER) {
      const p = req.params as ChannelsRegisterParams;
      const answer = () => host.sendResponse(req.id, { results: p.channels.map((c) => ({ id: c.id, accepted: true })) });
      if (policy.holdRegister) policy.heldRegister.push(answer);
      else answer();
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      if (policy.failIncomingBatches > 0) {
        policy.failIncomingBatches--;
        host.sendError(req.id, -32603, 'host hiccup');
        return;
      }
      const refused = new Set(p.messages.filter((m) => policy.rejectIds.has(m.messageId) || policy.rejectOnce.delete(m.messageId)).map((m) => m.messageId));
      incoming.push(...p.messages.filter((m) => !refused.has(m.messageId)));
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: !refused.has(m.messageId) })) });
    } else if (req.method === method.PUSH_EVENT) {
      const p = req.params as PushEventParams;
      pushed.push(p);
      const answer = () => host.sendResponse(req.id, { accepted: !policy.rejectPushes.has(String((p.origin as { messageId?: unknown }).messageId)) });
      if (policy.holdPushes) policy.heldPushes.push(answer);
      else answer();
    } else if (req.method === method.CHANNELS_CHANGED) {
      const p = req.params as { added?: ChannelDescriptor[] };
      // 'error' stands for every host that does not answer this Request:
      // a transport drop, a timeout, or one that treats it as a Notification.
      if (policy.changedAnswer === 'error') host.sendError(req.id, -32601, 'channels/changed unsupported');
      else if (policy.changedAnswer === 'held') policy.heldChanged.push(() => host.sendResponse(req.id, { results: (p.added ?? []).map((c) => ({ id: c.id, accepted: true })) }));
      else if (policy.changedAnswer === 'reconcile') {
        // agent-framework's handleChanged in 0.18.0 and older: reconcile (a
        // blocking channels/open per descriptor) BEFORE answering.
        void (async () => {
          for (const c of p.added ?? []) {
            // As over a real transport, the open comes in a later turn of the event loop.
            await new Promise((resolve) => setImmediate(resolve));
            try {
              const opened = (await host.sendRequest(method.CHANNELS_OPEN, { channelId: c.id, type: c.type, address: {} })) as { channel?: ChannelDescriptor };
              policy.reconcileOpens.push({ channelId: c.id, ok: Boolean(opened?.channel), detail: 'opened' });
            } catch (err) {
              policy.reconcileOpens.push({ channelId: c.id, ok: false, detail: (err as Error).message });
            }
          }
          // The test may have torn the connection down while this host was
          // still reconciling; that write is the harness's, not the server's.
          try {
            host.sendResponse(req.id, { results: (p.added ?? []).map((c) => ({ id: c.id, accepted: true })) });
          } catch { /* connection closed */ }
        })();
      }
      else host.sendResponse(req.id, { results: (p.added ?? []).map((c) => ({ id: c.id, accepted: policy.changedAnswer === 'accept' })) });
    } else {
      host.sendError(req.id, -32601, `unexpected ${req.method}`);
    }
  });

  const served = server.serve(serverConn);
  return {
    server,
    delivery: byChannelId(server.delivery as DeliveryState, adapter),
    adapter,
    host,
    hostSaw,
    incoming,
    pushed,
    served,
    serverSide: serverConn,
    policy,
    async close() {
      // EOF on the server's stdin analog: readline closes on 'end', not on
      // destroy, so ending the stream is what actually returns serve().
      toServer.end();
      await served;
      await server.shutdown();
      host.close();
    },
  };
}

async function initialize(h: Harness, mcpl: boolean) {
  const result = (await h.host.sendRequest(method.INITIALIZE, {
    protocolVersion: '2025-03-26',
    capabilities: mcpl ? { experimental: { mcpl: { version: '0.5', channels: true } } } : {},
    clientInfo: { name: 'test-host', version: '0' },
  })) as { protocolVersion: string; capabilities: Record<string, unknown>; serverInfo: { name: string } };
  h.host.sendNotification('notifications/initialized');
  return result;
}

const FULL_GRANT = [
  'tools',
  'pushEvents',
  'channels.acknowledge',
  'channels.streaming',
  'channels.register',
  'channels.lifecycle',
  'channels.publish',
  'channels.incoming',
  'channels.typing',
  'contextHooks.beforeInference.inject.beforeUser',
];

/** Wait until `predicate` holds, polling — the server's async work is real. */
async function until(predicate: () => boolean, what: string, ms = 2000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** Registration completes only once the host's answer reaches the server;
 *  channels/list is the observable that it did. Each channel's catch-up
 *  follows it, and most cases go on once the server is quiet after them. */
async function awaitRegistered(h: Harness, andLive = true): Promise<void> {
  await until(() => h.adapter.emit !== null, 'event delivery to start');
  const deadline = Date.now() + 2000;
  while (true) {
    const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    if (listed.channels.length > 0) break;
    if (Date.now() > deadline) throw new Error('timed out waiting for registration');
    await new Promise((r) => setTimeout(r, 5));
  }
  if (andLive) await h.server.quiet();
}

// --- plain MCP ---------------------------------------------------------------

test('a plain-MCP client gets tools and resources, and no MCPL manifest', async () => {
  const h = harness();
  const init = await initialize(h, false);
  assert.equal(init.protocolVersion, '2025-03-26');
  assert.equal(init.serverInfo.name, 'zulip-mcp-test');
  assert.deepEqual(Object.keys(init.capabilities).sort(), ['resources', 'tools']);
  assert.equal(h.server.mcplMode, false);

  assert.deepEqual(await h.host.sendRequest('ping'), {});

  const tools = (await h.host.sendRequest('tools/list')) as { tools: { name: string }[] };
  assert.ok(tools.tools.some((t) => t.name === 'send_message'));
  assert.ok(tools.tools.some((t) => t.name === 'fetch_attachment'));

  const called = (await h.host.sendRequest('tools/call', { name: 'list_streams', arguments: { verbose: false } })) as {
    content: { type: string; text: string }[];
  };
  assert.deepEqual(JSON.parse(called.content[0].text), { ok: true, name: 'list_streams' });

  const failed = (await h.host.sendRequest('tools/call', { name: 'explode', arguments: {} })) as {
    isError?: boolean;
    content: { text: string }[];
  };
  assert.equal(failed.isError, true);
  assert.match(failed.content[0].text, /boom/);

  const resources = (await h.host.sendRequest('resources/list')) as { resources: { uri: string }[] };
  assert.equal(resources.resources[0].uri, 'zulip://monitoring/status');
  const read = (await h.host.sendRequest('resources/read', { uri: 'zulip://monitoring/status' })) as {
    contents: { text: string }[];
  };
  assert.equal(read.contents[0].text, '{}');

  // MCPL methods are not on offer to a client that did not negotiate MCPL.
  await assert.rejects(h.host.sendRequest(method.CHANNELS_LIST), /-32601/);
  await assert.rejects(h.host.sendRequest('no/such/method'), /-32601/);

  // Events are never started for a plain-MCP client.
  assert.equal(h.adapter.emit, null);
  await h.close();
});

test('requests are served one at a time, in the order they arrive', async () => {
  const h = harness({ mcpl: false });
  await initialize(h, false);
  fakeTools.calls.length = 0;
  fakeTools.hold = new Promise((resolve) => setTimeout(resolve, 50));
  const first = h.host.sendRequest('tools/call', { name: 'slow', arguments: {} });
  const second = h.host.sendRequest('tools/call', { name: 'send_message', arguments: { content: 'two' } });
  await Promise.all([first, second]);
  assert.deepEqual(fakeTools.calls.map((c) => c.name), ['slow', 'slow:end', 'send_message']);
  await h.close();
});

test('when the connection ends, the request being served finishes before serve returns, and one waiting behind it is not carried out', async () => {
  const h = harness({ mcpl: false });
  await initialize(h, false);
  fakeTools.calls.length = 0;
  let release!: () => void;
  fakeTools.hold = new Promise<void>((resolve) => { release = resolve; });
  void h.host.sendRequest('tools/call', { name: 'slow', arguments: {} }).catch(() => {});
  await until(() => fakeTools.calls.length === 1, 'the first call to begin');
  void h.host.sendRequest('tools/call', { name: 'send_message', arguments: { content: 'late' } }).catch(() => {});
  h.serverSide.once('close', () => setTimeout(release, 50));
  const atServed = h.served.then(() => fakeTools.calls.map((c) => c.name));
  await h.close();
  assert.deepEqual(await atServed, ['slow', 'slow:end']);
});

test('tools/list carries each tool\'s RFC-008 class and leaves the model-facing definition alone', async () => {
  for (const mcpl of [false, true]) {
    const h = harness({ mcpl });
    await initialize(h, mcpl);
    const { tools } = (await h.host.sendRequest('tools/list')) as {
      tools: { name: string; _meta?: Record<string, unknown> }[];
    };
    assert.deepEqual(tools.map((t) => t.name), toolDefinitions.map((t) => t.name));
    for (const tool of tools) {
      assert.deepEqual(tool._meta?.['mcpl/class'], TOOL_CLASSES[tool.name], tool.name);
      const { _meta, ...rest } = tool;
      assert.deepEqual(rest, toolDefinitions.find((t) => t.name === tool.name), tool.name);
    }
    assert.deepEqual(tools.find((t) => t.name === 'send_message')?._meta, { 'mcpl/class': ['comms', 'files'] });
    await h.close();
  }
});

test('an unknown MCP protocol revision is answered with the fallback', async () => {
  const h = harness();
  const result = (await h.host.sendRequest(method.INITIALIZE, {
    protocolVersion: '1999-01-01',
    capabilities: {},
    clientInfo: { name: 'old', version: '0' },
  })) as { protocolVersion: string };
  assert.equal(result.protocolVersion, '2024-11-05');
  await h.close();
});

// --- MCPL --------------------------------------------------------------------

test('initialize carries the 0.5 manifest, and mcpl/manifest answers with the same snapshot (§5.1, §17.4)', async () => {
  const h = harness();
  const init = await initialize(h, true);
  const manifest = (init.capabilities.experimental as { mcpl: Record<string, unknown> }).mcpl;
  assert.equal(manifest.version, '0.5');
  assert.ok(String(manifest.revision).startsWith('sha256:'));
  const featureSets = manifest.featureSets as Record<string, { uses: string[]; tagOntology?: unknown }>;
  assert.ok(featureSets['zulip.messaging'].uses.includes('channels.typing'));
  assert.ok(featureSets['zulip.messaging'].tagOntology);
  assert.ok(featureSets['zulip.context']);

  const answered = await h.host.sendRequest(method.MCPL_MANIFEST);
  assert.deepEqual(answered, manifest);
  await h.close();
});

test('nothing is available before the policy exchange; the Request form settles it and registration follows (§5.3, §6.7, §14.3)', async () => {
  const h = harness();
  await initialize(h, true);

  // Absence is denial: channel methods are refused with the capability named.
  await assert.rejects(h.host.sendRequest(method.CHANNELS_LIST), (err: Error & { code?: number; data?: unknown }) => {
    assert.equal(err.code, -32002);
    assert.deepEqual(err.data, { capability: 'channels.register' });
    return true;
  });
  // ...and so is the tool surface.
  await assert.rejects(
    h.host.sendRequest('tools/call', { name: 'list_streams', arguments: {} }),
    (err: Error & { code?: number }) => err.code === -32002,
  );
  assert.equal(h.hostSaw.length, 0, 'no channels/register before policy');

  const receipt = (await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT })) as {
    accepted: boolean;
    mode: string;
    unavailableFeatures: unknown[];
  };
  assert.equal(receipt.accepted, true);
  assert.equal(receipt.mode, 'full');
  assert.deepEqual(receipt.unavailableFeatures, []);

  await until(() => h.hostSaw.some((r) => r.method === method.CHANNELS_REGISTER), 'channels/register');
  const registered = h.hostSaw.find((r) => r.method === method.CHANNELS_REGISTER)!.params as ChannelsRegisterParams;
  assert.deepEqual(registered.channels.map((c) => c.id), ['zulip:general']);

  await until(() => h.adapter.emit !== null, 'event delivery to start');

  const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
  assert.deepEqual(listed.channels.map((c) => c.id), ['zulip:general']);
  await h.close();
});

test('a degraded grant is reported as such and disables the tools of the feature set (§6.4, §6.7)', async () => {
  const h = harness();
  await initialize(h, true);
  const receipt = (await h.host.sendRequest(method.FEATURE_SETS_UPDATE, {
    effectiveCapabilities: ['tools', 'channels.register'],
  })) as { mode: string; unavailableFeatures: { featureSet: string; missingCapabilities: string[] }[] };
  assert.equal(receipt.mode, 'degraded');
  assert.ok(receipt.unavailableFeatures.some((f) => f.featureSet === 'zulip.messaging'));

  // A messaging tool is unavailable with its feature set; a plain lookup is not.
  const send = (await h.host.sendRequest('tools/call', { name: 'send_message', arguments: {} })) as { isError?: boolean };
  assert.equal(send.isError, true);
  const upload = (await h.host.sendRequest('tools/call', { name: 'upload_file', arguments: { data: 'aGk=', name: 'a.txt' } })) as { isError?: boolean; content: { text: string }[] };
  assert.equal(upload.isError, true, 'upload_file is a messaging tool and goes with the feature set');
  assert.match(upload.content[0].text, /zulip.messaging/);
  const list = (await h.host.sendRequest('tools/call', { name: 'list_streams', arguments: {} })) as { isError?: boolean };
  assert.notEqual(list.isError, true);
  await h.close();
});

test('a malformed policy is rejected and fails closed (§5.4)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await assert.rejects(
    h.host.sendRequest(method.FEATURE_SETS_UPDATE, {
      effectiveCapabilities: ['tools'],
      deniedCapabilities: ['tools'],
    }),
    (err: Error & { code?: number }) => err.code === -32602,
  );
  await assert.rejects(
    h.host.sendRequest('tools/call', { name: 'list_streams', arguments: {} }),
    (err: Error & { code?: number }) => err.code === -32002,
  );
  await h.close();
});

test('open → incoming with tags → publish routes to the topic of the conversation (§14)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);

  // Not open yet: an incoming message on the channel goes nowhere.
  h.adapter.emit!({
    channelId: 'zulip:general',
    messageId: '1',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: 'before open' }],
    tags: ['chat:ambient'],
  });
  await h.server.quiet();
  assert.equal(h.incoming.length, 0);

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} })) as {
    channel: ChannelDescriptor;
  };
  assert.equal(opened.channel.id, 'zulip:general');

  h.adapter.emit!({
    channelId: 'zulip:general',
    messageId: '2',
    threadId: 'deploys',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: 'ship it?' }],
    tags: ['chat:mention', 'chat:from-human'],
    metadata: { topic: 'deploys', mentioned: true },
  });
  await until(() => h.incoming.length === 1, 'channels/incoming');
  assert.equal(h.incoming[0].messageId, '2');
  assert.deepEqual(h.incoming[0].tags, ['chat:mention', 'chat:from-human']);
  // The body the model reads says who, where and when; the fields stay.
  assert.equal((h.incoming[0].content[0] as { text: string }).text, '[T id=2] [#general > deploys] Ann (mention): ship it?');
  assert.deepEqual(h.incoming[0].author, { id: '9', name: 'Ann' });
  assert.equal(h.incoming[0].threadId, 'deploys');

  const published = (await h.host.sendRequest(method.CHANNELS_PUBLISH, {
    conversationId: 'c1',
    channelId: 'zulip:general',
    content: [{ type: 'text', text: 'shipping' }],
  })) as { delivered: boolean; messageId?: string };
  assert.deepEqual(published, { delivered: true, messageId: '41' });
  assert.equal(h.adapter.published.length, 1);
  assert.equal(h.adapter.published[0].hints?.threadId, 'deploys');
  assert.equal((h.adapter.published[0].hints?.metadata as { topic: string }).topic, 'deploys');

  // Typing, both carriers.
  await h.host.sendRequest(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'start' });
  h.host.sendNotification(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'stop' });
  await until(() => h.adapter.typing.length === 2, 'typing');
  assert.deepEqual(h.adapter.typing.map((t) => t.op), ['start', 'stop']);

  // A platform system event reaches the open channel as a system message.
  h.adapter.systemEvent!({ kind: 'gap', text: 'queue expired', metadata: { platform: 'zulip' } });
  await until(() => h.incoming.length === 2, 'system event');
  assert.equal((h.incoming[1].metadata as { system: boolean; kind: string }).system, true);
  assert.equal(h.incoming[1].author.id, 'system');

  const closed = (await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' })) as { closed: boolean };
  assert.equal(closed.closed, true);
  await h.close();
});

test('context/beforeInference injects history for open channels under zulip.context (§10.1, §6.5)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  const params = {
    inferenceId: 'i1',
    conversationId: 'c1',
    turnIndex: 0,
    userMessage: null,
    model: { id: 'm', vendor: 'v', contextWindow: 1, capabilities: [] },
  };
  const result = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, params)) as {
    featureSet: string;
    contextInjections: ContextInjection[];
  };
  assert.equal(result.featureSet, 'zulip.context');
  assert.equal(result.contextInjections.length, 1);
  assert.equal(result.contextInjections[0].position, 'beforeUser');

  // Disabling the feature set by Notification is a reduction, honoured at once.
  h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.context'] });
  const reduced = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, params)) as { contextInjections: unknown[] };
  assert.deepEqual(reduced.contextInjections, []);
  await h.close();
});

test('MCPL_ENABLED=false keeps an MCPL host on the plain-MCP surface', async () => {
  const h = harness({ mcpl: false });
  const init = await initialize(h, true);
  assert.equal(init.capabilities.experimental, undefined);
  assert.equal(h.server.mcplMode, false);
  await h.close();
});

test('the manifest omits channels.typing when the adapter cannot type', async () => {
  const h = harness({ typing: false });
  const init = await initialize(h, true);
  const manifest = (init.capabilities.experimental as { mcpl: { channels: { typing: boolean } } }).mcpl;
  assert.equal(manifest.channels.typing, false);
  await h.close();
});

// --- delivery model ---------------------------------------------------------

function streamMsg(id: number, over: Partial<IncomingChannelMessage> & { mentioned?: boolean; text?: string } = {}): IncomingChannelMessage {
  const { mentioned = false, text = `msg ${id}`, ...rest } = over;
  return {
    channelId: 'zulip:general',
    messageId: String(id),
    threadId: 'deploys',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date(1_700_000_000_000 + id * 1000).toISOString(),
    content: [{ type: 'text', text }],
    tags: [mentioned ? 'chat:mention' : 'chat:ambient', 'chat:from-human'],
    metadata: { topic: 'deploys', mentioned, isDM: false },
    ...rest,
  };
}

async function settled(h: Harness): Promise<void> {
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);
}

test('a mention on a closed channel is pushed, ambient is tallied, and channel_missed reports it', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);

  // Open then close: closing is what starts the tally.
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.incoming.length === 1, 'open-channel delivery');
  assert.equal(h.delivery.watermark('zulip:general'), 1);
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });

  h.adapter.emit!(streamMsg(2, { text: 'chatter' }));
  h.adapter.emit!(streamMsg(3, { text: 'more chatter' }));
  await h.server.quiet();
  assert.equal(h.incoming.length, 1, 'ambient on a closed channel is not delivered');
  assert.equal(h.pushed.length, 0);
  assert.equal(h.delivery.watermark('zulip:general'), 1, 'a dropped message does not advance the watermark');

  h.adapter.emit!(streamMsg(4, { mentioned: true, text: '@bot ping' }));
  await until(() => h.pushed.length === 1, 'push/event for the mention');
  const push = h.pushed[0];
  assert.equal(push.featureSet, 'zulip.messaging');
  assert.equal(push.eventId, 'zulip_msg_4');
  assert.deepEqual(push.tags, ['chat:mention', 'chat:from-human']);
  const origin = push.origin as Record<string, unknown>;
  assert.equal(origin.mcplChannelId, 'zulip:general');
  assert.equal(origin.stream, 'general');
  assert.equal(origin.isMention, true);
  assert.equal(origin.missedMessages, 2);
  assert.equal(origin.missedCharacters, 'chatter'.length + 'more chatter'.length);
  // The watermark moves once the host has acknowledged the push, not when
  // the request leaves — an unacknowledged forward is not a forward.
  await until(() => h.delivery.watermark('zulip:general') === 4, 'watermark after acknowledged push');

  const missed = (await h.host.sendRequest('tools/call', { name: 'channel_missed', arguments: { channel: '#general' } })) as {
    content: { text: string }[];
  };
  const report = JSON.parse(missed.content[0].text);
  assert.equal(report.tracked, true);
  assert.equal(report.missedMessages, 2);
  assert.equal(report.sinceMessageId, 1);

  // Reopening ends the tally.
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const cleared = JSON.parse(
    ((await h.host.sendRequest('tools/call', { name: 'channel_missed', arguments: { channel: 'general' } })) as { content: { text: string }[] }).content[0].text,
  );
  assert.equal(cleared.tracked, false);
  assert.equal(cleared.open, true);
  await h.close();
});

test('an answer in a closed stream goes to the topic of the mention the host accepted as a push', async () => {
  const answer = async (h: Harness, text: string) => {
    await h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c1', channelId: 'zulip:general', content: [{ type: 'text', text }] });
    const hints = h.adapter.published.at(-1)?.hints;
    return { threadId: hints?.threadId, topic: (hints?.metadata as { topic?: string } | undefined)?.topic };
  };
  const mention = (id: number, topic: string) =>
    streamMsg(id, { mentioned: true, text: `@bot a question in ${topic}`, threadId: topic, metadata: { topic, mentioned: true, isDM: false } });

  // A stream the agent had open, its last message in "deploys", then closed.
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.incoming.length === 1, 'open-channel delivery');
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });

  // Each push is answered once the host has accepted it, as an agent answers
  // what it was given: the watermark moves on that acceptance.
  h.adapter.emit!(mention(2, 'billing'));
  await until(() => h.delivery.watermark('zulip:general') === 2, 'the mention in billing accepted');
  // Typing with no topic from the host follows the same hint.
  await h.host.sendRequest(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'start' });
  await until(() => h.adapter.typing.length === 1, 'typing start');
  assert.equal(h.adapter.typing[0].metadata?.topic, 'billing');
  assert.deepEqual(await answer(h, 'about billing'), { threadId: 'billing', topic: 'billing' });
  h.adapter.emit!(mention(3, 'support'));
  await until(() => h.delivery.watermark('zulip:general') === 3, 'the mention in support accepted');
  assert.deepEqual(await answer(h, 'about support'), { threadId: 'support', topic: 'support' });

  // A message that doesn't address the agent isn't pushed, and moves nothing.
  h.adapter.emit!(streamMsg(4, { text: 'chatter', threadId: 'chatter', metadata: { topic: 'chatter', mentioned: false, isDM: false } }));
  await until(() => h.delivery.tally('zulip:general')?.talliedThrough === 4, 'the ambient message tallied');
  assert.equal(h.pushed.length, 2, 'not pushed');
  assert.deepEqual(await answer(h, 'still about support'), { threadId: 'support', topic: 'support' });

  // A mention the host refuses never reached the agent, so it moves nothing.
  h.policy.rejectPushes.add('5');
  h.adapter.emit!(mention(5, 'random'));
  await until(() => h.delivery.heldIds('zulip:general', { replayable: true }).includes(5), 'the refusal handled: held as failed');
  assert.equal(h.delivery.watermark('zulip:general'), 3, 'refused: the watermark stays');
  assert.deepEqual(await answer(h, 'and still about support'), { threadId: 'support', topic: 'support' });
  await h.close();

  // A stream never open: the first answer goes to the mention's topic too.
  const g = harness();
  await initialize(g, true);
  await settled(g);
  g.adapter.emit!(mention(5, 'billing'));
  await until(() => g.delivery.watermark('zulip:general') === 5, 'the first contact accepted');
  assert.deepEqual(await answer(g, 'first answer'), { threadId: 'billing', topic: 'billing' });
  await g.close();
});

test('an answer after channels/open with history goes to the topic of its newest message, unless the history was fetched before a message', async () => {
  const answerTopic = async (h: Harness) => {
    await h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c1', channelId: 'zulip:general', content: [{ type: 'text', text: 'answer' }] });
    return (h.adapter.published.at(-1)?.hints?.metadata as { topic?: string } | undefined)?.topic;
  };
  const inTopic = (id: number, topic: string) => streamMsg(id, { threadId: topic, metadata: { topic, mentioned: false, isDM: false } });
  const history = [inTopic(1, 'deploys'), inTopic(2, 'printer')];

  // A stream never open in this process: the agent opens it, reads its
  // backscroll and answers, and typing with no topic from the host follows.
  const h = harness({ history });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 20 } });
  await h.host.sendRequest(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'start' });
  await until(() => h.adapter.typing.length === 1, 'typing start');
  assert.equal(h.adapter.typing[0].metadata?.topic, 'printer');
  assert.equal(await answerTopic(h), 'printer');
  await h.close();

  // History fetched before a given message is older history, read for
  // context, so it moves nothing, and the answer has no topic from it. With
  // no watermark yet, sinceLastSeen has nothing to read forward from, so the
  // page is fetched before the message.
  const g = harness({ history });
  await initialize(g, true);
  await settled(g);
  await g.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 20, beforeMessageId: '3', sinceLastSeen: true },
  });
  const anchored = g.adapter.historyCalls.at(-1)!.query;
  assert.equal(anchored.beforeMessageId, '3');
  assert.equal(anchored.afterMessageId, undefined);
  assert.equal(await answerTopic(g), undefined);
  await g.close();

  // A mention pushed while the stream is closed keeps its topic through an
  // open without history, as a host opens a stream to deliver an answer,
  // and through an open whose history is older than the mention.
  const k = harness({ history });
  await initialize(k, true);
  await settled(k);
  k.adapter.emit!(streamMsg(10, { mentioned: true, threadId: 'billing', metadata: { topic: 'billing', mentioned: true, isDM: false } }));
  await until(() => k.delivery.watermark('zulip:general') === 10, 'the mention accepted');
  await k.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  assert.equal(await answerTopic(k), 'billing');
  await k.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  await k.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 20 } });
  assert.equal(await answerTopic(k), 'billing');
  await k.close();
});

test('ZULIP_ATTRIBUTE_DELIVERY=false delivers bare bodies on every attributed surface: incoming, recovered replay, push', async () => {
  const h = harness({ attributeDelivery: false, history: [streamMsg(1), streamMsg(2), streamMsg(3)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  h.adapter.emit!(streamMsg(1, { mentioned: true, text: '@bot ping' }));
  await until(() => h.incoming.length === 1, 'incoming');
  assert.equal((h.incoming[0].content[0] as { text: string }).text, '@bot ping');
  assert.equal((h.incoming[0].metadata as { attributed?: boolean }).attributed, undefined);

  // A gap recovers 2 and 3 onto the open channel through the replay path.
  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.incoming.length === 4, 'recovered + marker');
  assert.equal((h.incoming[1].metadata as { recovered: boolean }).recovered, true);
  assert.equal((h.incoming[1].content[0] as { text: string }).text, 'msg 2');

  // A mention on a closed channel goes out as push/event, bare, with no stamp in origin.
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  h.adapter.emit!(streamMsg(4, { mentioned: true, text: '@bot closed' }));
  await until(() => h.pushed.length === 1, 'push');
  assert.equal((h.pushed[0].payload.content[0] as { text: string }).text, '@bot closed');
  assert.equal((h.pushed[0].origin as { attributed?: boolean }).attributed, undefined);
  await h.close();
});

test('channels/open returns capped history before the lifecycle commits, and subscribes the bot (§14.4)', async () => {
  const h = harness({ history: Array.from({ length: 12 }, (_, i) => streamMsg(i + 1)) });
  await initialize(h, true);
  await settled(h);

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 5 },
  })) as { channel: ChannelDescriptor; history?: IncomingChannelMessage[]; historyTruncated?: boolean };
  assert.deepEqual(opened.history!.map((m) => m.messageId), ['8', '9', '10', '11', '12']);
  assert.equal(opened.historyTruncated, false);
  // Backscroll comes back to the agent as the channel_open tool result, JSON
  // that already shows author and topic: left bare.
  assert.equal((opened.history![0].content[0] as { text: string }).text, 'msg 8');
  assert.equal((opened.history![0].metadata as { attributed?: boolean }).attributed, undefined);
  assert.equal((opened.history![0].metadata as { backscroll: boolean }).backscroll, true);
  assert.deepEqual(h.adapter.subscribed, ['zulip:general']);
  assert.equal(h.delivery.watermark('zulip:general'), 12, 'returned history counts as forwarded');

  // A request past the descriptor's cap is truncated and says so.
  const capped = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 9999 },
  })) as { historyTruncated?: boolean; history?: unknown[] };
  assert.equal(capped.historyTruncated, true);

  // sinceLastSeen pages from the watermark.
  h.adapter.history.push(streamMsg(13), streamMsg(14));
  const since = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 50, sinceLastSeen: true },
  })) as { history?: IncomingChannelMessage[] };
  assert.deepEqual(since.history!.map((m) => m.messageId), ['13', '14']);
  const lastCall = h.adapter.historyCalls[h.adapter.historyCalls.length - 1];
  assert.equal(lastCall.query.afterMessageId, '12');

  // sinceLastSeen wins over a message to page back from, so the page is new
  // history, and its newest message gives the answer's topic.
  h.adapter.history.push(streamMsg(15, { threadId: 'printer', metadata: { topic: 'printer', mentioned: false, isDM: false } }));
  const both = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 50, sinceLastSeen: true, beforeMessageId: '13' },
  })) as { history?: IncomingChannelMessage[] };
  assert.deepEqual(both.history!.map((m) => m.messageId), ['15']);
  const bothCall = h.adapter.historyCalls.at(-1)!;
  assert.equal(bothCall.query.afterMessageId, '14');
  assert.equal(bothCall.query.beforeMessageId, undefined);
  await h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c1', channelId: 'zulip:general', content: [{ type: 'text', text: 'answer' }] });
  assert.equal((h.adapter.published.at(-1)?.hints?.metadata as { topic?: string } | undefined)?.topic, 'printer');
  await h.close();
});

test('the reconnect sweep delivers what arrived while offline, by what the host had open', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-sweep-'));
  try {
    // Session 1: general open, watermark at 3; dev only watermarked (closed).
    const DEV = { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } };
    const first = harness({ stateDir: dir, sessionId: 'sw', visible: [DESCRIPTOR, DEV] });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    first.delivery.advance('zulip:dev', 50);
    first.delivery.save();
    await first.close();

    // Session 2: new messages exist beyond both watermarks.
    // dev's one mention is in "triage", and the talk after it in "chatter".
    const devTopic = (id: number) => (id === 60 ? 'triage' : id > 60 ? 'chatter' : 'deploys');
    const later = [
      streamMsg(4, { text: 'while you were away' }),
      streamMsg(5, { mentioned: true, text: '@bot are you back?' }),
      streamMsg(6, { text: 'shipping now', threadId: 'release', metadata: { topic: 'release', mentioned: false, isDM: false } }),
      ...Array.from({ length: 20 }, (_, i) => {
        const id = 51 + i;
        return streamMsg(id, {
          channelId: 'zulip:dev', text: `dev ${id}`, mentioned: id === 60, threadId: devTopic(id),
          metadata: { topic: devTopic(id), mentioned: id === 60, isDM: false },
        });
      }),
    ];
    // dev must be a registered channel for the sweep to consider it.
    const second = harness({ stateDir: dir, sessionId: 'sw', history: later, visible: [DESCRIPTOR, DEV] });
    await initialize(second, true);
    await settled(second);
    await until(() => second.pushed.length === 2, 'two catch-up pushes');

    const general = second.pushed.find((p) => (p.origin as { mcplChannelId: string }).mcplChannelId === 'zulip:general')!;
    const generalText = (general.payload.content[0] as { text: string }).text;
    assert.match(generalText, /^<missed stream="#general" channelId="zulip:general" count="3" reason="backscroll">/);
    assert.match(generalText, /\[T id=4\] \[#general > deploys\] Ann: while you were away/);
    assert.match(generalText, /\[T id=5\] \[#general > deploys\] Ann \(mention\): @bot are you back\?/);
    assert.match(generalText, /\[T id=6\] \[#general > release\] Ann: shipping now/);
    assert.ok(general.tags!.includes('zulip:missed'));
    assert.ok(general.tags!.includes('chat:mention'));

    const dev = second.pushed.find((p) => (p.origin as { mcplChannelId: string }).mcplChannelId === 'zulip:dev')!;
    const devText = (dev.payload.content[0] as { text: string }).text;
    assert.match(devText, /count="1" lines="15" reason="mention"/, 'closed channel: the mention plus ±7 vicinity');
    assert.doesNotMatch(devText, /dev 51\b/, 'far ambient is not replayed');

    assert.equal(second.delivery.watermark('zulip:general'), 6);
    assert.equal(second.delivery.watermark('zulip:dev'), 70, 'advanced past everything scanned');
    // The agent answers the blocks through channels/publish: a backscroll's
    // answer goes to its newest message's topic, a mention block's to its
    // newest mention's, not the talk after it.
    const topicOfAnswer = async (channelId: string) => {
      await second.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c1', channelId, content: [{ type: 'text', text: 'back now' }] });
      return (second.adapter.published.at(-1)?.hints?.metadata as { topic?: string } | undefined)?.topic;
    };
    assert.equal(await topicOfAnswer('zulip:general'), 'release');
    assert.equal(await topicOfAnswer('zulip:dev'), 'triage');
    await second.close();

    // Session 3: nothing new → nothing pushed, and the sweep runs once.
    const third = harness({ stateDir: dir, sessionId: 'sw', history: later, visible: [DESCRIPTOR, DEV] });
    await initialize(third, true);
    await settled(third);
    const registered = third.hostSaw.find((r) => r.method === method.CHANNELS_REGISTER)!.params as ChannelsRegisterParams;
    assert.ok(registered.channels.some((c) => c.id === 'zulip:dev'), 'dev is registered at the third start');
    assert.ok(third.adapter.historyCalls.some((c) => c.channelId === 'zulip:dev' && c.query.afterMessageId === '70'), 'the sweep read dev from its watermark');
    assert.equal(third.pushed.length, 0);
    await third.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a queue-expiry gap is healed from history for open channels before the marker is delivered', async () => {
  const h = harness({ history: [streamMsg(1), streamMsg(2), streamMsg(3)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.incoming.length === 1, 'delivery');

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.incoming.length === 4, 'recovered messages + marker');
  assert.deepEqual(h.incoming.slice(1, 3).map((m) => m.messageId), ['2', '3']);
  assert.ok(h.incoming[1].tags!.includes('zulip:missed'));
  assert.equal((h.incoming[1].metadata as { recovered: boolean }).recovered, true);
  assert.equal((h.incoming[1].content[0] as { text: string }).text, '[T id=2] [#general > deploys] Ann: msg 2', 'a recovered message is attributed like a live one');
  const marker = h.incoming[3];
  assert.equal((marker.metadata as { kind: string; recoveredMessages: number }).kind, 'gap');
  assert.equal((marker.metadata as { recoveredMessages: number }).recoveredMessages, 2);
  assert.match((marker.content[0] as { text: string }).text, /2 message\(s\) on open channels were recovered/);
  assert.equal(h.delivery.watermark('zulip:general'), 3);
  await h.close();
});

test('a DM from a new conversation registers its channel, is pushed with a reply affordance, and can be answered', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);

  const dmDesc: ChannelDescriptor = {
    id: 'zulip:dm:42',
    type: 'zulip',
    label: 'DM: Bo',
    direction: 'bidirectional',
    address: { dm: true, user_ids: [42], emails: ['bo@example.com'] },
    metadata: { channelType: 'dm', recipientName: 'Bo', recipientId: '42' },
  };
  const dm: IncomingChannelMessage = {
    channelId: 'zulip:dm:42',
    messageId: '500',
    author: { id: '42', name: 'Bo' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: 'hey, got a minute?' }],
    tags: ['chat:dm', 'chat:private', 'chat:from-human'],
    metadata: { isDM: true, mentioned: false },
  };
  h.adapter.emit!(dm, dmDesc);

  await until(() => h.pushed.length === 1, 'push for the DM');
  assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED), 'the new conversation was announced first');
  const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
  assert.ok(listed.channels.some((c) => c.id === 'zulip:dm:42'));

  const push = h.pushed[0];
  assert.equal((push.origin as { isDM: boolean }).isDM, true);
  assert.equal((push.origin as { stream?: string }).stream, undefined);
  // The stamp rides in origin: agent-framework stores origin as the message metadata on this path.
  assert.equal((push.origin as { attributed?: boolean }).attributed, true);
  assert.equal((push.origin as { attributionHeader?: string }).attributionHeader, '[T id=500] [DM] Bo: ');
  const first = (push.payload.content[0] as { text: string }).text;
  assert.match(first, /^<system>Direct message from Bo \(user id 42\)\. To reply, use send_dm\(\["42"\]\) or publish to channel zulip:dm:42/);
  assert.equal((push.payload.content[1] as { text: string }).text, '[T id=500] [DM] Bo: hey, got a minute?');

  // The second message from the same conversation carries no affordance.
  await until(() => h.delivery.watermark('zulip:dm:42') === 500, 'watermark');
  h.adapter.emit!({ ...dm, messageId: '501', content: [{ type: 'text', text: 'still there?' }] });
  await until(() => h.pushed.length === 2, 'second push');
  assert.equal((h.pushed[1].payload.content[0] as { text: string }).text, '[T id=501] [DM] Bo: still there?');

  // Opening the DM channel routes the conversation through channels/incoming,
  // and a publish reaches the adapter with the DM channel id.
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:dm:42', type: 'zulip', address: {} });
  h.adapter.emit!({ ...dm, messageId: '502', content: [{ type: 'text', text: 'ok' }] });
  await until(() => h.incoming.length === 1, 'incoming on the open DM');
  await h.host.sendRequest(method.CHANNELS_PUBLISH, {
    conversationId: 'c1', channelId: 'zulip:dm:42', content: [{ type: 'text', text: 'here now' }],
  });
  assert.equal(h.adapter.published[0].channelId, 'zulip:dm:42');
  await h.close();
});

test('a muted stream delivers nothing, and the filters tools read and write the plane', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-filters-wire-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    const call = async (name: string, args: Record<string, unknown>) =>
      (await h.host.sendRequest('tools/call', { name, arguments: args })) as { isError?: boolean; content: { text: string }[] };
    const json = (r: { content: { text: string }[] }) => JSON.parse(r.content[0].text);

    const mute = json(await call('mute_channel', { channel: '#general' }));
    assert.equal(mute.muted, true);
    assert.deepEqual(plane.current().mutedStreams, ['general']);

    h.adapter.emit!(streamMsg(1, { mentioned: true, text: '@bot?' }));
    h.adapter.emit!(streamMsg(2));
    await h.server.quiet();
    assert.equal(h.incoming.length, 0, 'muted: not even a mention on an open channel gets through');
    assert.equal(h.pushed.length, 0);

    json(await call('unmute_channel', { channel: 'zulip:general' }));
    h.adapter.emit!(streamMsg(3));
    await until(() => h.incoming.length === 1, 'delivery after unmute');

    const got = json(await call('filters_get', {}));
    assert.equal(got.streams, null);
    assert.equal(got.dmUsers, null);
    assert.deepEqual(got.mutedStreams, []);
    assert.equal(got.plane.status, 'live');
    assert.equal(got.reactionSuppression.status, 'not-configured');

    // Removing the only allowed stream would empty the list — and an empty
    // allowlist is unrestricted, so the edit is refused rather than
    // silently re-opening everything.
    const emptied = await call('filters_update', { removeStreams: ['general'] });
    assert.equal(emptied.isError, true);
    assert.match(emptied.content[0].text, /last allowed stream/);
    assert.equal(plane.current().streams, undefined, 'nothing was written');

    // Removing from an unrestricted allowlist materializes it first; adding
    // a stream re-discovers and announces channels the host lacks.
    h.adapter.discoverChannels = async () => [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
    const upd = json(await call('filters_update', { addStreams: ['dev'], removeStreams: ['general'], setDmUsers: ['42'] }));
    assert.deepEqual(upd.streams, ['dev']);
    assert.deepEqual(upd.dmUsers, ['42']);
    assert.match(upd.note, /materialized/);
    assert.deepEqual(upd.registered, ['zulip:dev']);
    assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED));
    assert.equal(plane.streamAllowed('general'), false);
    assert.equal(plane.streamAllowed('dev'), true);
    assert.equal(plane.dmAllowed({ id: 42, email: 'x' }), true);
    assert.equal(plane.dmAllowed({ id: 7, email: 'x' }), false);

    // Clearing the DM allowlist is allowed, and says what it means.
    const anyone = json(await call('filters_update', { setDmUsers: [] }));
    assert.equal(anyone.dmUsers, null);
    assert.match(anyone.note, /UNRESTRICTED/);

    // A DM cannot be muted; the error names the right lever.
    const bad = await call('mute_channel', { channel: 'zulip:dm:42' });
    assert.equal(bad.isError, true);
    assert.match(bad.content[0].text, /dmUsers/);
    plane.stop();
    await h.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('attachments on live delivery are inlined from the configured source', async () => {
  const fetched: string[] = [];
  const h = harness({
    attachments: {
      source: {
        async fetch(path) {
          fetched.push(path);
          if (path.endsWith('.png')) {
            // A 1x1 PNG.
            const buf = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==', 'base64');
            return { buf, mimeType: 'image/png', overflow: false };
          }
          return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false };
        },
      },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
  });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  h.adapter.emit!({
    ...streamMsg(1, { text: 'see attached' }),
    content: [{ type: 'text', text: 'see attached' }, { type: 'text', text: '[attachments: 2]\n- shot.png\n- run.log' }],
    metadata: {
      topic: 'deploys', mentioned: false, isDM: false,
      attachments: [
        { path: '/user_uploads/1/a/shot.png', name: 'shot.png', mimeType: 'image/png', isImage: true },
        { path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false },
      ],
    },
  });
  await until(() => h.incoming.length === 1, 'delivery');
  const content = h.incoming[0].content;
  assert.deepEqual(fetched, ['/user_uploads/1/a/shot.png', '/user_uploads/1/a/run.log']);
  assert.equal(content[0].type, 'text');
  assert.match((content[1] as { text: string }).text, /^\[attachments: 2\]/, 'the reference note stays');
  assert.equal(content[2].type, 'image');
  assert.equal((content[2] as { mimeType: string }).mimeType, 'image/png');
  assert.equal((content[3] as { text: string }).text, '[image attachment: shot.png]');
  assert.equal((content[4] as { text: string }).text, '[attachment: run.log (8B)]\nlog line');
  await h.close();
});

test("a message whose attachment fetches never answer goes at its one deadline, with a note for each", { timeout: 5000 }, async () => {
  const fetched: string[] = [];
  const deadline = new AbortController();
  const h = harness({
    attachments: {
      source: { fetch(path) { fetched.push(path); return new Promise(() => {}); } },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
      deadline: () => deadline.signal,
    },
  });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  h.adapter.emit!({
    ...streamMsg(1, { text: 'see attached' }),
    metadata: {
      topic: 'deploys', mentioned: false, isDM: false,
      attachments: [
        { path: '/user_uploads/1/a/shot.png', name: 'shot.png', mimeType: 'image/png', isImage: true },
        { path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false },
      ],
    },
  });
  await until(() => fetched.length === 1, 'the first fetch to start');
  deadline.abort();
  await until(() => h.incoming.length === 1, 'delivery at the deadline');
  const notes = h.incoming[0].content.slice(1).map((b) => (b as { text: string }).text);
  assert.deepEqual(notes, [
    "[attachment: shot.png — could not fetch (out of time for this message's attachments); fetch_attachment /user_uploads/1/a/shot.png]",
    "[attachment: run.log — could not fetch (out of time for this message's attachments); fetch_attachment /user_uploads/1/a/run.log]",
  ]);
  await h.close();
});

/** An attachment source whose fetches wait until the test lets them go, or for `ms` when given. */
function heldFetches(ms?: number) {
  const started: string[] = [];
  let release!: () => void;
  const gate = ms === undefined ? new Promise<void>((resolve) => { release = resolve; }) : null;
  return {
    started,
    release: () => release(),
    options: {
      source: {
        async fetch(path: string) {
          started.push(path);
          await (gate ?? new Promise((resolve) => setTimeout(resolve, ms)));
          return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false };
        },
      },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
  };
}

function withLog(m: IncomingChannelMessage): IncomingChannelMessage {
  return { ...m, metadata: { ...(m.metadata as object), attachments: [{ path: `/user_uploads/1/a/${m.messageId}.log`, name: 'run.log', mimeType: 'text/plain', isImage: false }] } };
}

test('a close waits only for the job running on its channel, and the messages behind that job are handled as on a closed channel', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, 'the fetch to start');
  h.adapter.emit!(streamMsg(2));
  h.adapter.emit!(streamMsg(3));
  h.adapter.emit!(streamMsg(4));
  let closed = false;
  const close = h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' }).then(() => { closed = true; });
  await h.host.sendRequest('ping');
  assert.equal(closed, false, 'the close waits for the job that is running');
  fetches.release();
  await close;
  await h.server.quiet();
  assert.deepEqual(h.incoming.filter((m) => ['2', '3', '4'].includes(m.messageId)), []);
  assert.equal(h.pushed.length, 0);
  const missed = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'channel_missed', arguments: { channel: '#general' } })) as { content: { text: string }[] }).content[0].text);
  assert.equal(missed.missedMessages, 3, 'the three that waited are counted, and the one whose job was running is not');
  await h.close();
});

test('an open\'s page holds no message that waits in its lane, and each waiting message follows the open once, as a message', async () => {
  const fetches = heldFetches();
  const gates: (() => void)[] = [];
  const attachments = {
    ...fetches.options,
    source: {
      async fetch(path: string) {
        fetches.started.push(path);
        await new Promise<void>((resolve) => gates.push(resolve));
        return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false };
      },
    },
  };
  const h = harness({ attachments, history: [1, 2, 3, 4, 5].map((n) => streamMsg(n)) });
  await initialize(h, true);
  await settled(h);
  const open = async (history: { limit: number; sinceLastSeen?: boolean }) =>
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history }) as Promise<{ history?: { messageId: string }[] }>;

  // The newest rows: the page is the two below the two that wait.
  h.adapter.emit!(withLog(streamMsg(3)));
  await until(() => fetches.started.length === 1, 'the fetch to start');
  h.adapter.emit!(streamMsg(4));
  h.adapter.emit!(streamMsg(5));
  const newest = open({ limit: 2 });
  await h.host.sendRequest('ping');
  gates.shift()!();
  assert.deepEqual((await newest).history?.map((m) => m.messageId), ['2', '3']);
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['4', '5']);

  // Since last seen: the page ends below the two that wait.
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  h.adapter.history.push(...[6, 7, 8].map((n) => streamMsg(n)));
  h.adapter.emit!(withLog(streamMsg(6)));
  await until(() => fetches.started.length === 2, 'the second fetch to start');
  h.adapter.emit!(streamMsg(7));
  h.adapter.emit!(streamMsg(8));
  const since = open({ limit: 10, sinceLastSeen: true });
  await h.host.sendRequest('ping');
  gates.shift()!();
  assert.deepEqual((await since).history?.map((m) => m.messageId), ['6']);
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['4', '5', '7', '8']);

  // A message that comes to wait while the page is read is left out of it.
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  let letRead!: () => void;
  const gate = new Promise<void>((resolve) => { letRead = resolve; });
  let reading = false;
  const read = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    reading = true;
    await gate;
    return read(key, query);
  };
  const during = open({ limit: 2 });
  await until(() => reading, 'the page read to start');
  h.adapter.history.push(streamMsg(9));
  h.adapter.emit!(streamMsg(9));
  letRead();
  assert.deepEqual((await during).history?.map((m) => m.messageId), ['8']);
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['4', '5', '7', '8', '9']);
  await h.close();
});

test('a message whose event arrives after an open\'s since-last-seen page handed it is dropped, and reaches the host once', async () => {
  const h = harness({ history: [streamMsg(1), streamMsg(2), streamMsg(3)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, 'the first message accepted');
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 10, sinceLastSeen: true },
  })) as { history?: { messageId: string }[] };
  assert.deepEqual(opened.history?.map((m) => m.messageId), ['2', '3']);
  h.adapter.emit!(streamMsg(3));
  h.adapter.emit!(streamMsg(4));
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '4'], 'the page\'s 3 isn\'t delivered again');
  assert.equal(h.delivery.watermark('zulip:general'), 4);
  await h.close();
});

test('an open with a page on a channel with no watermark gives it one, and the next connection\'s catch-up starts above the page', async () => {
  const first = harness({ history: [streamMsg(1), streamMsg(2)] });
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2 } });
  assert.equal(first.delivery.watermark('zulip:general'), 2);
  await first.close();
  first.adapter.history.push(streamMsg(3));

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  await second.server.quiet();
  assert.equal(second.pushed.length, 1, 'the catch-up block');
  const block = (second.pushed[0].payload.content[0] as { text: string }).text;
  assert.match(block, /id=3\]/);
  assert.doesNotMatch(block, /id=2\]/);
  await second.close();
});

test('a message waiting for its attachments reaches the host before a later message of its channel', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, 'the fetch to start');
  h.adapter.emit!(streamMsg(2));
  fetches.release();
  await until(() => h.incoming.length === 2, 'both messages');
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '2']);
  await h.close();
});

test('the host refuses a message that waited for its attachments and accepts the next: the refused one is offered again', async () => {
  const h = harness({ attachments: heldFetches(50).options, history: [withLog(streamMsg(1)), streamMsg(2)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.policy.rejectOnce.add('1');
  h.adapter.emit!(withLog(streamMsg(1)));
  h.adapter.emit!(streamMsg(2));
  await until(() => h.incoming.some((m) => m.messageId === '1'), 'the refused message, offered again');
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['2', '1']);
  assert.equal((h.incoming[1].metadata as { recovered?: boolean }).recovered, true);
  await until(() => h.delivery.watermark('zulip:general') === 2, 'the watermark past both');
  await h.close();
});

test('a reaction and an edit to a message waiting for its attachments reach the host after it', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-lane-order-'));
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const fetches = heldFetches();
    const h = harness({ filters: plane, attachments: fetches.options });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } });

    h.adapter.emit!(withLog(streamMsg(1)));
    await until(() => fetches.started.length === 1, 'the fetch to start');
    h.adapter.react!({
      action: 'add', channelId: 'zulip:general', messageId: '1', emoji: 'eyes',
      reactorId: '9', reactorName: 'Ann', onOwnMessage: false, messageSnippet: 'msg 1', timestamp: new Date(1_700_000_002_000),
    });
    h.adapter.change!({
      kind: 'edit', channelId: 'zulip:general', messageId: '1', messageIds: ['1'], authorId: '9', authorName: 'Ann', authorEmail: 'ann@example.com', actorId: '9',
      topic: 'deploys', previousTopic: null, movedToChannelId: null, content: 'msg 1, fixed', previousContent: 'msg 1',
      mentioned: false, previouslyMentioned: false, vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_003_000),
    });
    fetches.release();
    await until(() => h.incoming.length === 3, 'the message, the reaction and the edit');
    assert.deepEqual(h.incoming.map((m) => m.messageId.split(':')[0]), ['1', 'reaction', 'edit']);
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a message still waiting in its channel when the connection ends is offered by the next connection', async () => {
  const fetches = heldFetches();
  const first = harness({ attachments: fetches.options, history: [withLog(streamMsg(1)), streamMsg(2, { mentioned: true })] });
  await initialize(first, true);
  await settled(first);
  first.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, 'the fetch to start');
  first.adapter.emit!(streamMsg(2, { mentioned: true }));
  first.serverSide.once('close', () => setTimeout(fetches.release, 50));
  await first.close();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  const offered = () => [...first.pushed, ...second.pushed].map((p) => (p.payload.content[0] as { text: string }).text).join('\n');
  await until(() => /id=2\]/.test(offered()), 'the waiting mention, offered');
  await second.close();
});

const BO_DM: ChannelDescriptor = {
  id: 'zulip:dm:42',
  type: 'zulip',
  label: 'DM: Bo',
  direction: 'bidirectional',
  address: { dm: true, user_ids: [42], emails: ['bo@example.com'] },
  metadata: { channelType: 'dm', recipientName: 'Bo', recipientId: '42' },
};

function boDm(id: number, text = `dm ${id}`): IncomingChannelMessage {
  return {
    channelId: 'zulip:dm:42',
    messageId: String(id),
    author: { id: '42', name: 'Bo' },
    timestamp: new Date(1_700_000_000_000 + id * 1000).toISOString(),
    content: [{ type: 'text', text }],
    tags: ['chat:dm', 'chat:private', 'chat:from-human'],
    metadata: { isDM: true, mentioned: false },
  };
}

const firstText = (p: PushEventParams) => (p.payload.content[0] as { text: string }).text;

test('a first message the host refused, on a channel where it had accepted nothing, is offered again after a restart', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-first-refused-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'fr' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.policy.rejectIds.add('5');
    first.adapter.emit!(streamMsg(5));
    await first.server.quiet();
    assert.equal(first.incoming.length, 0, 'the host refused it');
    const onDisk = JSON.parse(readFileSync(join(dir, 'fr.delivery.json'), 'utf8')) as { watermarks: Record<string, number> };
    assert.equal(onDisk.watermarks['zulip:general'], 4, 'on disk before the connection ends');
    await first.close();

    const second = harness({ stateDir: dir, sessionId: 'fr', history: [streamMsg(5)] });
    await initialize(second, true);
    await settled(second);
    await second.server.quiet();
    assert.equal(second.pushed.length, 1, 'the next start offers it');
    assert.match(firstText(second.pushed[0]), /id=5\]/);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a message that enters its channel after the host left, while a request is still served, is held and nothing runs it, and the next start offers it once; an event for a message its channel has dealt with makes no hold", async () => {
  const DEV = { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } };
  const late = streamMsg(20, { channelId: 'zulip:dev', mentioned: true, text: '@bot after the host left' });
  const first = harness({ visible: [DESCRIPTOR, DEV], history: [streamMsg(11), streamMsg(12), late] });
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.policy.rejectIds.add('11');
  first.adapter.emit!(streamMsg(11));
  first.adapter.emit!(streamMsg(12));
  await until(() => first.incoming.some((m) => m.messageId === '12'), 'the host accepting 12');
  await first.server.quiet();
  assert.deepEqual(first.delivery.heldIds('zulip:general'), [11], 'the refused 11 keeps the watermark below 12');

  fakeTools.calls.length = 0;
  let releaseTool!: () => void;
  fakeTools.hold = new Promise<void>((resolve) => { releaseTool = resolve; });
  void first.host.sendRequest('tools/call', { name: 'slow', arguments: {} }).catch(() => {});
  await until(() => fakeTools.calls.length === 1, 'the slow tool call on the chain');
  const sawClose = new Promise((resolve) => first.serverSide.once('close', resolve));
  const closing = first.close();
  await sawClose;
  await new Promise((resolve) => setImmediate(resolve));

  let ran = false;
  const enter = (m: IncomingChannelMessage) => {
    const { channelId, ...rest } = m;
    const key = first.adapter.keyOf(channelId);
    first.adapter.enter!({ key, messageId: m.messageId, ready: async () => { ran = true; return { message: { ...rest, key } as KeyedMessage }; } });
  };
  enter(late);
  enter(streamMsg(12));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(ran, false, 'nothing runs a message that enters after the host left');
  releaseTool();
  await closing;
  assert.deepEqual(first.delivery.heldIds('zulip:general'), [11], 'the late event for 12 made no hold');

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  await second.server.quiet();
  const offered = second.pushed.map(firstText).filter((t) => /id=20\]/.test(t));
  assert.equal(offered.length, 1, 'the next start offers it');
  assert.equal(offered[0].match(/id=20\]/g)?.length, 1, 'once');
  await second.close();
});

test('a message still waiting when the connection ends, on a channel where the host had accepted nothing, is offered after a restart', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-first-waiting-'));
  try {
    const fetches = heldFetches();
    const first = harness({ stateDir: dir, sessionId: 'fw', attachments: fetches.options });
    await initialize(first, true);
    await settled(first);
    first.adapter.emit!(withLog(streamMsg(5)));
    await until(() => fetches.started.length === 1, 'the fetch to start');
    first.adapter.emit!(streamMsg(6, { mentioned: true }));
    first.serverSide.once('close', () => setTimeout(fetches.release, 50));
    await first.close();

    const second = harness({ stateDir: dir, sessionId: 'fw', history: [withLog(streamMsg(5)), streamMsg(6, { mentioned: true })] });
    await initialize(second, true);
    await settled(second);
    await second.server.quiet();
    assert.equal(second.pushed.length, 1, 'the next start offers it');
    assert.match(firstText(second.pushed[0]), /id=6\]/);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a first message the host refused, on a channel where it had accepted nothing, is offered once by the next connection', async () => {
  const first = harness({ history: [streamMsg(5)] });
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.policy.rejectIds.add('5');
  first.adapter.emit!(streamMsg(5));
  await first.server.quiet();
  assert.equal(first.incoming.length, 0, 'the host refused it');
  await first.close();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  await second.server.quiet();
  assert.deepEqual(second.pushed.map((p) => /id=5\]/.test(firstText(p))), [true], 'offered once');
  await second.close();
});

test('two DMs that arrive close together in a new conversation both carry the first-message note', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  h.adapter.emit!(boDm(500), BO_DM);
  h.adapter.emit!(boDm(501), BO_DM);
  await until(() => h.pushed.length === 2, 'both pushes');
  for (const p of h.pushed) assert.match(firstText(p), /^<system>Direct message from Bo/);
  await h.close();
});

test('after the host refuses a DM conversation\'s first message, the next DM carries no first-message note', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  h.policy.rejectPushes.add('500');
  h.adapter.emit!(boDm(500), BO_DM);
  await until(() => h.pushed.length === 1, 'the first push');
  await h.server.quiet();
  h.adapter.emit!(boDm(501));
  await until(() => h.pushed.length === 2, 'the second push');
  assert.equal(firstText(h.pushed[1]), '[T id=501] [DM] Bo: dm 501');
  await h.close();
});

test('a message that arrives while zulip.messaging is disabled is offered once it is enabled again', async () => {
  const h = harness({ history: [streamMsg(1, { mentioned: true })] });
  await initialize(h, true);
  await settled(h);
  h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
  await h.host.sendRequest('ping');
  h.adapter.emit!(streamMsg(1, { mentioned: true }));
  await h.server.quiet();
  assert.equal(h.pushed.length, 0, 'nothing under a disabled feature set');
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await h.server.quiet();
  assert.equal(h.pushed.length, 1, 'offered once it is enabled again');
  assert.match(firstText(h.pushed[0]), /id=1\]/);
  await h.close();
});

test('a stream muted while an addressed message of its closed channel waits for its attachments pushes nothing', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-mute-mid-fetch-'));
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const fetches = heldFetches();
    const h = harness({ filters: plane, attachments: fetches.options });
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(withLog(streamMsg(1, { mentioned: true })));
    await until(() => fetches.started.length === 1, 'the fetch to start');
    assert.equal(plane.update((f) => ({ ...f, mutedStreams: ['general'] })).ok, true);
    fetches.release();
    await h.server.quiet();
    assert.equal(h.pushed.length, 0, 'muted while it waited: nothing reaches the agent');
    assert.equal(h.delivery.floor('zulip:general'), undefined, 'and nothing stays held');
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a catch-up block the host refuses keeps the watermark below it, and is offered again once the host accepts a later message', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-refused-block-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'rb' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.delivery.watermark('zulip:general') === 3, 'delivery');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    await first.close();

    const second = harness({ stateDir: dir, sessionId: 'rb', history: [streamMsg(4, { mentioned: true }), streamMsg(5)] });
    second.policy.rejectPushes.add('5');
    await initialize(second, true);
    await settled(second);
    await second.server.quiet();
    assert.equal(second.pushed.length, 1, 'the catch-up block, refused');
    assert.match(firstText(second.pushed[0]), /id=4\]/);

    second.policy.holdPushes = true;
    second.adapter.history.push(streamMsg(6, { mentioned: true }));
    second.adapter.emit!(streamMsg(6, { mentioned: true }));
    await until(() => second.pushed.length === 2, 'the live mention');
    second.policy.heldPushes.shift()!();
    await until(() => second.pushed.length === 3, 'the refused block, offered again');
    assert.equal(second.delivery.watermark('zulip:general'), 3, 'below the refused block while it is offered again');
    assert.match(firstText(second.pushed[2]), /id=4\]/);
    second.policy.holdPushes = false;
    second.policy.heldPushes.shift()!();
    await second.server.quiet();
    assert.equal(second.delivery.watermark('zulip:general'), 6);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

const PNG_1X1 = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==', 'base64');

function withImage(m: IncomingChannelMessage): IncomingChannelMessage {
  return {
    ...m,
    content: [...m.content, { type: 'text', text: '[attachments: 1]\n- shot.png' }],
    metadata: { ...(m.metadata as object), attachments: [{ path: `/user_uploads/1/a/${m.messageId}.png`, name: 'shot.png', mimeType: 'image/png', isImage: true }] },
  };
}

test('a message offered again from history on an open channel has its image inline', async () => {
  const h = harness({
    history: [withImage(streamMsg(1)), streamMsg(2)],
    attachments: {
      source: { async fetch() { return { buf: PNG_1X1, mimeType: 'image/png', overflow: false }; } },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
  });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.policy.rejectOnce.add('1');
  h.adapter.emit!(withImage(streamMsg(1)));
  h.adapter.emit!(streamMsg(2));
  await until(() => h.incoming.some((m) => m.messageId === '1'), 'the refused message, offered again');
  const again = h.incoming.find((m) => m.messageId === '1')!;
  assert.equal((again.metadata as { recovered?: boolean }).recovered, true);
  assert.ok(again.content.some((b) => b.type === 'image'), 'the image is inline');
  await h.close();
});

test('the filters file changing on disk to mute a stream lets go of what is held for it', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-mute-on-disk-'));
  try {
    const path = join(dir, 'filters.json');
    const plane = new FiltersPlane(path, {}, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    h.policy.rejectIds.add('1');
    h.adapter.emit!(streamMsg(1));
    await h.server.quiet();
    assert.equal(h.delivery.floor('zulip:general'), 1, 'the refused message is held');
    writeFileSync(path, JSON.stringify({ ...plane.current(), mutedStreams: ['general'] }));
    const later = new Date(Date.now() + 60_000);
    utimesSync(path, later, later);
    plane.tick();
    assert.deepEqual(plane.current().mutedStreams, ['general']);
    await h.server.quiet();
    assert.equal(h.delivery.floor('zulip:general'), undefined, 'the hold goes');
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a catch-up block refused after its stream was muted leaves nothing held', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-muted-block-'));
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane, history: [streamMsg(5, { mentioned: true })] });
    await initialize(h, true);
    await settled(h);
    h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
    await h.host.sendRequest('ping');
    h.adapter.emit!(streamMsg(5, { mentioned: true }));
    await h.server.quiet();
    h.policy.holdPushes = true;
    h.policy.rejectPushes.add('5');
    await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await until(() => h.pushed.length === 1, 'the catch-up block for the held mention');
    assert.equal(plane.update((f) => ({ ...f, mutedStreams: ['general'] })).ok, true);
    h.policy.holdPushes = false;
    h.policy.heldPushes.shift()!();
    await h.server.quiet();
    assert.equal(h.delivery.floor('zulip:general'), undefined, 'muted: nothing stays held');
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a message refused at its offer and again at its replay is offered a third time only after a break of the event queue', async () => {
  const h = harness({ history: [1, 2, 3, 4].map((id) => streamMsg(id)) });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const offers = () => h.hostSaw
    .filter((r) => r.method === method.CHANNELS_INCOMING)
    .flatMap((r) => (r.params as unknown as ChannelsIncomingParams).messages)
    .filter((m) => m.messageId === '2').length;
  h.policy.rejectIds.add('2');
  for (const id of [1, 2, 3]) h.adapter.emit!(streamMsg(id));
  await until(() => offers() === 2, 'refused again at its replay');
  await h.server.quiet();
  h.policy.rejectIds.delete('2');
  h.adapter.emit!(streamMsg(4));
  await until(() => h.incoming.some((m) => m.messageId === '4'), '4 accepted');
  await h.server.quiet();
  assert.equal(offers(), 2, 'no third offer from an accepted message');
  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.incoming.some((m) => m.messageId === '2'), 'offered after the break');
  assert.equal(offers(), 3);
  await h.close();
});

const gapNoticeIn = (h: Harness) => h.incoming.some((m) => (m.metadata as { kind?: string }).kind === 'gap');
const seenIds = (h: Harness) => h.incoming.map((m) => (m.messageId.startsWith('system:') ? 'notice' : m.messageId));
const breakQueue = (h: Harness) => h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });

test('a break of the event queue offers again what the host hasn\'t accepted, before what the break lost, and nothing it accepted', async () => {
  const h = harness({ history: [1, 2, 3, 4].map((id) => streamMsg(id)) });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const offered = () => h.hostSaw
    .filter((r) => r.method === method.CHANNELS_INCOMING)
    .flatMap((r) => (r.params as unknown as ChannelsIncomingParams).messages)
    .map((m) => (m.messageId.startsWith('system:') ? 'notice' : m.messageId));
  h.policy.rejectIds.add('2');
  for (const id of [1, 2, 3]) h.adapter.emit!(streamMsg(id));
  await until(() => offered().filter((id) => id === '2').length === 2, 'refused again at its replay');
  await h.server.quiet();
  const before = offered().length;
  breakQueue(h);
  await until(() => gapNoticeIn(h), 'the gap notice');
  await h.server.quiet();
  assert.deepEqual(offered().slice(before), ['2', '4', 'notice'], 'the refused 2, then the lost 4, then the notice; the accepted 3 isn\'t offered again');
  await h.close();
});

test('after a break of the event queue, the lost message is recovered, a message already waiting follows it once, and the gap notice follows both', async () => {
  const h = harness({ history: [1, 2, 3].map((id) => streamMsg(id)) });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  breakQueue(h);
  h.adapter.emit!(streamMsg(3));
  await until(() => gapNoticeIn(h), 'the gap notice');
  await h.server.quiet();
  assert.deepEqual(seenIds(h), ['1', '2', '3', 'notice']);
  assert.equal((h.incoming[1].metadata as { recovered?: boolean }).recovered, true, '2 is recovered');
  assert.equal((h.incoming[2].metadata as { recovered?: boolean }).recovered, undefined, '3 comes as a message');
  assert.equal((h.incoming[3].metadata as { recoveredMessages: number }).recoveredMessages, 1);
  await h.close();
});

for (const open of [true, false]) {
  test(`a message whose event arrives after a read after a break handed it is dropped at entry on ${open ? 'an open' : 'a closed'} channel, and the watermark passes it once the host accepts the read`, async () => {
    const h = harness({ batchWindowMs: 50, history: [1, 2, 3].map((id) => streamMsg(id, { mentioned: !open })) });
    await initialize(h, true);
    await settled(h);
    if (open) {
      await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
      h.adapter.emit!(streamMsg(1));
      await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
    } else {
      h.delivery.advance('zulip:general', 1);
      h.policy.holdPushes = true;
    }
    breakQueue(h);
    if (open) await until(() => h.delivery.heldIds('zulip:general').includes(3), 'the read handed 3');
    else await until(() => h.pushed.length === 1, 'the read\'s block');
    h.adapter.emit!(streamMsg(3, { mentioned: !open }));
    h.policy.holdPushes = false;
    for (const answer of h.policy.heldPushes.splice(0)) answer();
    await h.server.quiet();
    if (open) assert.deepEqual(seenIds(h).filter((id) => id === '3'), ['3']);
    else assert.equal(h.pushed.length, 1, 'the block alone');
    assert.equal(h.delivery.watermark('zulip:general'), 3);
    await h.close();
  });
}

test('a message whose event arrives while a read after a break waits on an earlier row\'s attachment comes once, as a message, after the read', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options, history: [streamMsg(1), withLog(streamMsg(2)), streamMsg(3)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  breakQueue(h);
  await until(() => fetches.started.length === 1 || h.incoming.some((m) => m.messageId === '2'), 'the read at 2');
  h.adapter.emit!(streamMsg(3));
  fetches.release();
  await until(() => gapNoticeIn(h), 'the gap notice');
  await h.server.quiet();
  assert.deepEqual(seenIds(h), ['1', '2', '3', 'notice']);
  assert.ok(h.incoming[1].content.some((c) => (c as { text?: string }).text === '[attachment: run.log (8B)]\nlog line'), 'the recovered 2 has its attachment inline');
  assert.equal((h.incoming[2].metadata as { recovered?: boolean }).recovered, undefined, '3 comes as a message');
  await h.close();
});

test('a message whose event arrives while a closed channel\'s read after a break is at Zulip is left out of the block, and reaches the host once', async () => {
  const h = harness({ history: [1, 2, 3].map((id) => streamMsg(id, { mentioned: true })) });
  await initialize(h, true);
  await settled(h);
  h.delivery.advance('zulip:general', 1);
  let letRead!: () => void;
  const gate = new Promise<void>((resolve) => { letRead = resolve; });
  let reading = false;
  const read = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    reading = true;
    await gate;
    return read(key, query);
  };
  breakQueue(h);
  await until(() => reading, 'the read at Zulip');
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  letRead();
  await until(() => h.pushed.length === 2, 'the block and the message');
  await h.server.quiet();
  const texts = h.pushed.map((p) => (p.payload.content[0] as { text: string }).text);
  assert.match(texts[0], /id=2\]/);
  assert.doesNotMatch(texts[0], /id=3\]/);
  assert.equal(texts.filter((t) => /id=3\]/.test(t)).length, 1);
  assert.equal(h.pushed.length, 2);
  await h.close();
});

test('a read after a break whose scan passes a waiting message and ends on the bot\'s own sends a block whose newest id is below the waiting message', async () => {
  const h = harness({ history: [streamMsg(1), streamMsg(2, { mentioned: true }), streamMsg(3, { mentioned: true }), streamMsg(4)] });
  const read = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    const page = await read(key, query);
    return { ...page, messages: page.messages.filter((m) => m.messageId !== '4') };
  };
  await initialize(h, true);
  await settled(h);
  h.delivery.advance('zulip:general', 1);
  breakQueue(h);
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  await until(() => h.pushed.length === 2, 'the block and the message');
  await h.server.quiet();
  const block = h.pushed.find((p) => (p.origin as { missed?: boolean }).missed === true)!;
  assert.equal((block.origin as { messageId: string }).messageId, '2');
  const texts = h.pushed.map((p) => (p.payload.content[0] as { text: string }).text);
  assert.equal(texts.filter((t) => /id=3\]/.test(t)).length, 1);
  assert.equal(h.pushed.length, 2);
  await h.close();
});

test('after an open whose since-last-seen page its limit cut, a break of the event queue brings nothing from below the channel\'s place', async () => {
  const h = harness({ history: [1, 2, 3, 4, 5].map((id) => streamMsg(id)) });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  for (const id of [2, 3, 4, 5]) h.adapter.emit!(streamMsg(id));
  await h.server.quiet();
  assert.equal(h.delivery.tally('zulip:general')?.messages, 4, 'the four counted as missed');
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2, sinceLastSeen: true },
  })) as { history?: { messageId: string }[] };
  assert.deepEqual(opened.history?.map((m) => m.messageId), ['2', '3']);
  breakQueue(h);
  await until(() => gapNoticeIn(h), 'the gap notice');
  await h.server.quiet();
  assert.deepEqual(seenIds(h), ['1', 'notice'], 'the counted 4 and 5 don\'t arrive as recovered');
  await h.close();
});

test('a mention that enters a closed channel while its read after a break is at Zulip keeps its hold: refused after a later mention was accepted, it is offered again', async () => {
  const h = harness({ history: [streamMsg(1)] });
  await initialize(h, true);
  await settled(h);
  h.delivery.advance('zulip:general', 1);
  // The read's answer is history as Zulip had it when the read was sent: nothing after 1.
  let letRead!: () => void;
  const gate = new Promise<void>((resolve) => { letRead = resolve; });
  let reading = false;
  const read = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    const page = await read(key, query);
    reading = true;
    await gate;
    return page;
  };
  const offersOf = (id: string) => h.pushed.filter((p) =>
    (p.origin as { messageId?: unknown }).messageId === id ||
    p.payload.content.some((c) => new RegExp(`id=${id}\\]`).test((c as { text?: string }).text ?? ''))).length;
  h.policy.holdPushes = true;
  h.policy.rejectPushes.add('3');
  breakQueue(h);
  await until(() => reading, 'the read at Zulip');
  h.adapter.history.push(streamMsg(3, { mentioned: true }), streamMsg(4, { mentioned: true }));
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  h.adapter.emit!(streamMsg(4, { mentioned: true }));
  letRead();
  await until(() => h.policy.heldPushes.length === 2, 'the pushes of 3 and 4');
  assert.deepEqual(h.delivery.heldIds('zulip:general'), [3, 4], 'both held while their pushes are out');
  h.policy.heldPushes[1]();
  await until(() => !h.delivery.heldIds('zulip:general').includes(4), "4's acceptance");
  h.policy.heldPushes[0]();
  h.policy.holdPushes = false;
  h.policy.heldPushes.length = 0;
  await h.server.quiet();
  assert.equal(h.delivery.watermark('zulip:general'), 2, 'the refused 3 keeps the watermark below it');
  h.adapter.history.push(streamMsg(5, { mentioned: true }));
  h.adapter.emit!(streamMsg(5, { mentioned: true }));
  await h.server.quiet();
  assert.equal(offersOf('3'), 2, '3 is offered again once the host accepts 5');
  await h.close();
});

test('the gap notice counts a closed channel whose block the host accepted', async () => {
  const both = [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
  const h = harness({ history: [streamMsg(60, { channelId: 'zulip:dev', mentioned: true })] });
  h.adapter.discoverChannels = async () => both;
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.delivery.advance('zulip:dev', 50);
  h.policy.holdPushes = true;
  breakQueue(h);
  await until(() => h.pushed.length === 1, 'the block');
  h.policy.holdPushes = false;
  for (const answer of h.policy.heldPushes.splice(0)) answer();
  await until(() => gapNoticeIn(h), 'the gap notice');
  const notice = h.incoming.find((m) => (m.metadata as { kind?: string }).kind === 'gap')!;
  assert.match((notice.content[0] as { text: string }).text, /Addressed messages on 1 closed channel\(s\) were delivered as catch-up events\./);
  assert.equal((notice.metadata as { closedChannelsCaughtUp: number }).closedChannelsCaughtUp, 1);
  await h.close();
});

test('quiet() settles once what the server was handed has reached the host: delivery jobs, the batch window, a replay, and a request being served', async () => {
  const h = harness({ attachments: heldFetches(30).options, batchWindowMs: 30, history: [streamMsg(1), streamMsg(2)] });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const read = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    await new Promise((resolve) => setTimeout(resolve, 30));
    return read(key, query);
  };
  h.policy.rejectOnce.add('1');
  h.adapter.emit!(withLog(streamMsg(1)));
  h.adapter.emit!(streamMsg(2));
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['2', '1']);

  fakeTools.calls.length = 0;
  fakeTools.hold = new Promise((resolve) => setTimeout(resolve, 30));
  void h.host.sendRequest('tools/call', { name: 'slow', arguments: {} });
  await until(() => fakeTools.calls.length === 1, 'the call to begin');
  await h.server.quiet();
  assert.deepEqual(fakeTools.calls.map((c) => c.name), ['slow', 'slow:end']);
  await h.close();
});

test('a push the host hasn\'t answered holds back no later message, reaction or edit of its channel, and quiet() waits for the answers', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-push-answer-'));
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } });
    h.policy.holdPushes = true;

    h.adapter.emit!(streamMsg(1, { mentioned: true, text: '@bot one' }));
    await until(() => h.pushed.length === 1, "the first message's push");
    h.adapter.react!({
      action: 'add', channelId: 'zulip:general', messageId: '1', emoji: 'eyes',
      reactorId: '9', reactorName: 'Ann', onOwnMessage: true, messageSnippet: 'one', timestamp: new Date(1_700_000_002_000),
    });
    await until(() => h.pushed.length === 2, "the reaction's push, the first unanswered");
    h.adapter.change!({
      kind: 'edit', channelId: 'zulip:general', messageId: '3', messageIds: ['3'], authorId: '9', authorName: 'Ann', authorEmail: 'ann@example.com', actorId: '9',
      topic: 'deploys', previousTopic: null, movedToChannelId: null, content: '@bot three', previousContent: 'three',
      mentioned: true, previouslyMentioned: false, vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_003_000),
    });
    await until(() => h.pushed.length === 3, "the edit's push, two unanswered");
    h.adapter.emit!(streamMsg(4, { mentioned: true, text: '@bot four' }));
    await until(() => h.pushed.length === 4, "the last message's push, three unanswered");
    assert.deepEqual(h.pushed.map((p) => p.eventId.split('_')[1]), ['msg', 'reaction', 'edit', 'msg']);

    h.policy.holdPushes = false;
    // The answers come a little later, so that a quiet() which didn't wait for them could show it.
    setTimeout(() => { for (const answer of h.policy.heldPushes.splice(0)) answer(); }, 30);
    await h.server.quiet();
    assert.equal(h.delivery.watermark('zulip:general'), 4, 'both messages accepted');
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('reactions surface only on channels opted in, never wake, and honour suppression', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-reactions-wire-'));
  const original = console.error;
  console.error = () => {};
  try {
    // One name-shaped entry and one glyph, as the host's baseline carries them.
    const plane = new FiltersPlane(join(dir, 'filters.json'), { ZULIP_SUPPRESSED_REACTIONS_BASELINE: 'biohazard,🛑' }, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    const reaction = (over: Partial<Parameters<OnReaction>[0]> = {}) => h.adapter.react!({
      action: 'add', channelId: 'zulip:general', messageId: '77', emoji: 'thumbs_up',
      reactorId: '9', reactorName: 'Ann', onOwnMessage: true, messageSnippet: 'ship it', timestamp: new Date(1_700_000_000_000),
      ...over,
    });

    // Default off: nothing surfaces.
    reaction();
    await h.server.quiet();
    assert.equal(h.incoming.length, 0);

    const on = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } })) as { content: { text: string }[] }).content[0].text);
    assert.equal(on.visible, true);
    assert.deepEqual(plane.current().reactionChannels, ['zulip:general']);

    reaction();
    await until(() => h.incoming.length === 1, 'reaction on the open channel');
    const r = h.incoming[0];
    assert.deepEqual(r.tags, ['chat:reaction']);
    assert.equal((r.content[0] as { text: string }).text, '[reaction] Ann reacted :thumbs_up: on your message — "ship it"');
    assert.equal((r.metadata as { reaction: boolean; targetMessageId: string }).targetMessageId, '77');
    assert.equal(h.delivery.watermark('zulip:general'), undefined, 'a reaction never advances the watermark');

    reaction({ action: 'remove', onOwnMessage: false, messageSnippet: null });
    await until(() => h.incoming.length === 2, 'reaction removal');
    assert.deepEqual(h.incoming[1].tags, ['chat:reaction-remove']);
    assert.equal((h.incoming[1].content[0] as { text: string }).text, '[reaction] Ann removed a reaction :thumbs_up: on message 77');

    // Suppressed emoji: no glyph, no event, nowhere — by name, or by the
    // codepoints a glyph-shaped baseline entry is matched on.
    reaction({ emoji: 'biohazard' });
    reaction({ emoji: 'octagonal_sign', emojiCode: '1f6d1', emojiType: 'unicode_emoji' });
    await h.server.quiet();
    assert.equal(h.incoming.length, 2);

    // Closed channel with visibility on → push/event.
    await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    reaction({ emoji: 'eyes' });
    await until(() => h.pushed.length === 1, 'reaction push on a closed channel');
    assert.deepEqual(h.pushed[0].tags, ['chat:reaction']);
    assert.equal((h.pushed[0].origin as { reaction: boolean }).reaction, true);

    plane.stop();
    await h.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a publish refused partway leaves its posted chunks on record, so a rollback deletes them', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const sent = (await h.host.sendRequest('tools/call', { name: 'send_message', arguments: { content: 'via tool' } })) as { state?: { checkpoint: string } };
  const checkpoint = sent.state!.checkpoint;

  h.adapter.publish = async (channelId) => {
    h.adapter.onSent?.({ messageId: '61', channelId, content: 'first chunk' });
    throw new Error('Zulip refused message to #general: nope');
  };
  await assert.rejects(h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text: 'long' }] }), /nope/);

  const rolled = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint })) as { success: boolean };
  assert.equal(rolled.success, true);
  assert.deepEqual(h.adapter.deleted, ['61']);
  await h.close();
});

test('messaging tools mint checkpoints the host can roll back to; publishes and tool sends after it are deleted; acknowledge reacts; streaming terminators are inert', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  const publish = (text: string) => h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text }] });
  const call = async (name: string, args: Record<string, unknown>) =>
    (await h.host.sendRequest('tools/call', { name, arguments: args })) as { isError?: boolean; state?: { featureSet: string; checkpoint: string; parent: string | null } };

  await publish('one');
  // The only way a host learns a checkpoint is a tool result's `state` (§8):
  // a messaging tool mints one after its own send is on the record.
  const sent = await call('send_message', { content: 'via tool' });
  assert.equal(sent.state?.featureSet, 'zulip.messaging');
  assert.match(sent.state!.checkpoint, /^chk_/);
  assert.equal(sent.state!.parent, null);
  const afterOne = sent.state!.checkpoint;
  // A read-only tool carries no checkpoint; a later messaging tool chains.
  assert.equal((await call('list_streams', {})).state, undefined);
  const next = await call('add_reaction', { message_id: 1, emoji_name: 'eyes' });
  assert.equal(next.state!.parent, afterOne);
  await publish('two');
  await publish('three');

  const unknown = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint: 'chk_nope' })) as { success: boolean; reason?: string };
  assert.equal(unknown.success, false);
  assert.match(unknown.reason!, /not found/);
  const wrongSet = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.context', checkpoint: afterOne })) as { success: boolean };
  assert.equal(wrongSet.success, false);

  const rolled = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint: afterOne })) as { success: boolean; reason?: string };
  assert.equal(rolled.success, true);
  assert.equal(rolled.reason, undefined);
  assert.deepEqual(h.adapter.deleted, ['42', '43'], 'everything sent after the checkpoint (publish 41 and tool send 99 came before)');

  // Rolling back to the later checkpoint now undoes the tool's own send.
  const rolledFurther = (await h.host.sendRequest(method.STATE_ROLLBACK, { featureSet: 'zulip.messaging', checkpoint: afterOne })) as { success: boolean };
  assert.equal(rolledFurther.success, true);
  assert.deepEqual(h.adapter.deleted, ['42', '43'], 'nothing left to undo after the checkpoint');

  const ack = (await h.host.sendRequest(method.CHANNELS_ACKNOWLEDGE, { channelId: 'zulip:general', messageId: '7', intent: 'seen-not-opening', value: 'eyes' })) as { acknowledged: boolean; representation?: string };
  assert.deepEqual(ack, { acknowledged: true, representation: ':eyes:' });
  assert.deepEqual(h.adapter.acked, ['7:eyes']);

  // Streaming notifications are accepted and deliver nothing.
  h.host.sendNotification(method.CHANNELS_OUTGOING_CHUNK, { inferenceId: 'i', conversationId: 'c', channelId: 'zulip:general', index: 0, delta: 'never sent' });
  h.host.sendNotification(method.CHANNELS_OUTGOING_COMPLETE, { inferenceId: 'i', conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text: 'never sent' }] });
  await h.host.sendRequest('ping');
  assert.equal(h.adapter.published.length, 3);
  await h.close();
});

// --- review fix round ---------------------------------------------------------

test('the watermark never passes an undelivered message: a refusal or a lost batch holds it, the next answer replays them, shutdown flushes', async () => {
  const original = console.error;
  console.error = () => {};
  try {
    const h = harness({ history: Array.from({ length: 9 }, (_, i) => streamMsg(i + 1)) });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    // The host itemizes: 2 is refused (once). The watermark stops short of
    // it although 3, in the same batch, was accepted.
    h.policy.rejectOnce.add('2');
    h.adapter.emit!(streamMsg(1));
    h.adapter.emit!(streamMsg(2));
    h.adapter.emit!(streamMsg(3));
    await until(() => h.incoming.length === 2, 'the accepted pair');
    assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '3']);
    assert.equal(h.delivery.watermark('zulip:general'), 1, 'held below the refused id');
    // The host is answering, so the refused message is fetched back from
    // history and offered again; accepted now, the watermark catches up.
    await until(() => h.incoming.length === 3, 'the replay');
    assert.equal(h.incoming[2].messageId, '2');
    assert.ok(h.incoming[2].tags!.includes('zulip:missed'));
    assert.equal((h.incoming[2].metadata as { recovered: boolean }).recovered, true);
    await until(() => h.delivery.watermark('zulip:general') === 3, 'watermark through everything accepted');
    h.adapter.emit!(streamMsg(4));
    await until(() => h.delivery.watermark('zulip:general') === 4, 'watermark 4');

    // Two transport failures in a row — the attempt and its retry — then
    // the host recovers and accepts the next message: the lost one is not
    // buried under that acceptance.
    h.policy.failIncomingBatches = 2;
    h.adapter.emit!(streamMsg(5));
    await until(() => h.policy.failIncomingBatches === 0 && h.server.channelManager.pendingCount() === 0, 'given up after the retry');
    assert.equal(h.delivery.watermark('zulip:general'), 4, 'a message the host never accepted is not forwarded');
    assert.equal(h.delivery.floor('zulip:general'), 5, 'and holds the watermark below it');
    h.adapter.emit!(streamMsg(6));
    await until(() => h.incoming.some((m) => m.messageId === '6'), '6 accepted');
    assert.equal(h.delivery.watermark('zulip:general'), 4, 'still not past the lost message');
    await until(() => h.incoming.some((m) => m.messageId === '5'), '5 fetched back from history and accepted');
    await until(() => h.delivery.watermark('zulip:general') === 6, 'watermark 6 once 5 is in');
    assert.equal(h.delivery.floor('zulip:general'), undefined);

    // Shutdown on a live connection pushes the last window out instead of
    // cancelling it.
    h.adapter.emit!(streamMsg(7));
    await until(() => h.server.channelManager.pendingCount() === 1, '7 in the batch window');
    await h.server.shutdown();
    assert.equal(h.incoming[h.incoming.length - 1].messageId, '7');
    assert.equal(h.delivery.watermark('zulip:general'), 7);
    await h.close();
  } finally {
    console.error = original;
  }
});

test('a muted stream is silent on every surface: open-history, context injection, reactions, gap recovery, and the reconnect sweep', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-mute-everywhere-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), { ZULIP_MUTED_STREAMS: 'general' }, { pollMs: 60_000 });
    plane.start();
    const h = harness({ filters: plane, history: [streamMsg(1), streamMsg(2, { mentioned: true }), streamMsg(3)] });
    await initialize(h, true);
    await settled(h);

    const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 10 },
    })) as { history?: unknown[] };
    assert.deepEqual(opened.history, [], 'no backscroll from a muted stream');
    assert.equal(h.adapter.historyCalls.length, 0);

    const ctx = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, {
      inferenceId: 'i', conversationId: 'c', turnIndex: 0, userMessage: null,
      model: { id: 'm', vendor: 'v', contextWindow: 1, capabilities: [] },
    })) as { contextInjections: unknown[] };
    assert.deepEqual(ctx.contextInjections, [], 'no context injection for a muted open channel');

    await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } });
    h.adapter.react!({ action: 'add', channelId: 'zulip:general', messageId: '1', emoji: 'eyes', reactorId: '9', reactorName: 'Ann', onOwnMessage: true, messageSnippet: null, timestamp: new Date() });
    h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
    await until(() => h.incoming.length === 1, 'the gap marker itself');
    const marker = h.incoming[0];
    assert.equal((marker.metadata as { kind: string }).kind, 'gap');
    assert.equal((marker.metadata as { recoveredMessages: number }).recoveredMessages, 0, 'nothing recovered from the muted stream');
    assert.equal(h.adapter.historyCalls.length, 0);
    await h.close();

    // The reconnect sweep: a watermarked muted stream with new mentions pushes nothing.
    const first = harness({ stateDir: dir, sessionId: 'mute' });
    await initialize(first, true);
    await settled(first);
    first.delivery.advance('zulip:general', 1);
    first.delivery.save();
    await first.close();
    const second = harness({ stateDir: dir, sessionId: 'mute', filters: plane, history: [streamMsg(2, { mentioned: true }), streamMsg(3)] });
    await initialize(second, true);
    await settled(second);
    assert.equal(second.pushed.length, 0);
    assert.equal(second.adapter.historyCalls.length, 0, 'not even fetched');
    plane.stop();
    await second.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('channels/open fails — and commits nothing — when the bot cannot subscribe to the stream', async () => {
  const h = harness({ history: [streamMsg(1)] });
  await initialize(h, true);
  await settled(h);
  h.adapter.subscribeError.set('zulip:general', 'not authorized to subscribe to #general (private stream; the bot must be invited)');

  await assert.rejects(
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 5 } }),
    (err: Error & { code?: number; data?: unknown }) => {
      assert.equal(err.code, -32024, 'ERR_CHANNEL_OPEN_FAILED');
      assert.match(err.message, /not authorized to subscribe to #general/);
      assert.deepEqual(err.data, { channelId: 'zulip:general' });
      return true;
    },
  );
  assert.equal(h.adapter.historyCalls.length, 0, 'history is not fetched for an open that failed');
  assert.equal(h.server.channelManager.isOpen('zulip:general'), false);
  assert.equal(h.delivery.wasOpen('zulip:general'), false);
  // Still closed: ambient goes nowhere, and a mention is pushed as usual.
  h.adapter.emit!(streamMsg(2));
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  await until(() => h.pushed.length === 1, 'mention on the still-closed channel');
  assert.equal(h.incoming.length, 0);

  // Once the subscription can succeed, the open goes through.
  h.adapter.subscribeError.clear();
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:general');
  assert.deepEqual(h.adapter.subscribed, ['zulip:general']);
  await h.close();
});

test('channels/open re-discovers once when the channel was created after startup', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  const later: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:late-stream', label: '#late-stream', address: { stream_name: 'late-stream', stream_id: 9 } };
  h.adapter.discoverChannels = async () => [DESCRIPTOR, later];

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:late-stream', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:late-stream');
  assert.ok(
    h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED && JSON.stringify(r.params).includes('zulip:late-stream')),
    'announced to the host before the open answered',
  );
  assert.deepEqual(h.adapter.subscribed, ['zulip:late-stream']);
  assert.equal(h.server.channelManager.isOpen('zulip:late-stream'), true);

  // A channel that still is not there fails as before, and says what to do.
  await assert.rejects(
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:nope', type: 'zulip', address: {} }),
    (err: Error & { code?: number }) => {
      assert.equal(err.code, -32023, 'ERR_UNKNOWN_CHANNEL');
      assert.match(err.message, /Unknown channel: zulip:nope/);
      assert.match(err.message, /refresh_channels/);
      return true;
    },
  );
  await h.close();
});

test('a mention in a stream created after startup registers it, so the invitation can be accepted', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  const fresh: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:late-stream', label: '#late-stream', address: { stream_name: 'late-stream', stream_id: 9 } };
  // Discovery would not find it (the stub still lists only #general): the
  // descriptor the adapter attaches to the message is the only source.
  h.adapter.emit!(streamMsg(20, { channelId: 'zulip:late-stream', mentioned: true, text: '@bot join us?' }), fresh);
  await until(() => h.pushed.length === 1, 'the mention is pushed');
  assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED), 'the new stream was announced first');

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:late-stream', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:late-stream');
  await h.close();
});

test("a message that arrives while the registration is unanswered reaches the host after its channel's catch-up block, as a message", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-prelive-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'pl' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    await first.close();

    const second = harness({ stateDir: dir, sessionId: 'pl', history: [streamMsg(4), streamMsg(5, { mentioned: true })] });
    second.policy.holdRegister = true;
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await until(() => second.policy.heldRegister.length === 1 && second.adapter.emit !== null, 'the registration to be with the host');
    second.adapter.history.push(streamMsg(6, { mentioned: true, text: 'live @bot' }));
    second.adapter.emit!(streamMsg(6, { mentioned: true, text: 'live @bot' }));
    await second.host.sendRequest('ping');
    assert.equal(second.pushed.length, 0, 'nothing goes while the registration is with the host');
    second.policy.heldRegister.shift()!();
    await second.server.quiet();
    assert.deepEqual(second.pushed.map((p) => p.eventId), ['zulip_missed_zulip:general_5', 'zulip_msg_6'], 'the block, then the message');
    const block = (second.pushed[0].payload.content[0] as { text: string }).text;
    assert.match(block, /id=4\]/);
    assert.match(block, /id=5\]/);
    assert.doesNotMatch(block, /id=6\]/);
    assert.equal(second.delivery.watermark('zulip:general'), 6);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("at a connection's start, a channel with nothing to catch up goes on while another channel's catch-up read waits", async () => {
  const DEV = { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } };
  const dir = mkdtempSync(join(tmpdir(), 'zulip-start-own-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'own', visible: [DESCRIPTOR, DEV] });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    await first.close();

    const mention = streamMsg(20, { channelId: 'zulip:dev', mentioned: true, text: '@bot meanwhile' });
    const second = harness({ stateDir: dir, sessionId: 'own', visible: [DESCRIPTOR, DEV], history: [streamMsg(4, { mentioned: true }), mention] });
    let letRead!: () => void;
    const gate = new Promise<void>((resolve) => { letRead = resolve; });
    let readingGeneral = false;
    const read = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (key, query) => {
      if (second.adapter.wireOf(key) === 'zulip:general') {
        readingGeneral = true;
        await gate;
      }
      return read(key, query);
    };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    await until(() => readingGeneral, "general's catch-up read to start");
    second.adapter.emit!(mention);
    await until(() => second.pushed.some((p) => p.eventId === 'zulip_msg_20'), "dev's mention while general's catch-up read waits");
    assert.equal(second.pushed.some((p) => p.eventId.startsWith('zulip_missed_zulip:general')), false, "general's block waits for its read");
    letRead();
    await until(() => second.pushed.some((p) => p.eventId === 'zulip_missed_zulip:general_4'), "general's block once its read answers");
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('gap recovery also catches up closed channels, paging through history up to the ceiling', async () => {
  // Two mentions, an older one in "billing" and the newest in "triage",
  // among talk in "deploys".
  const mention = (id: number, topic: string) =>
    streamMsg(id, { mentioned: true, threadId: topic, metadata: { topic, mentioned: true, isDM: false } });
  const h = harness({ history: Array.from({ length: 250 }, (_, i) =>
    i + 2 === 45 ? mention(45, 'billing') : i + 2 === 50 ? mention(50, 'triage') : streamMsg(i + 2)) });
  await initialize(h, true);
  await settled(h);
  h.adapter.pageCap = 30;
  h.delivery.advance('zulip:general', 1);

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 1, 'catch-up push for the closed channel');
  const calls = h.adapter.historyCalls.filter((c) => c.channelId === 'zulip:general');
  assert.deepEqual(calls.map((c) => c.query.afterMessageId), ['1', '31', '61', '91'], 'paged from the watermark, page by page');
  assert.deepEqual(calls.map((c) => c.query.limit), [100, 70, 40, 10], 'never over the ceiling in total');
  const block = (h.pushed[0].payload.content[0] as { text: string }).text;
  assert.match(block, /count="2" lines="20" reason="mention" truncated="true"/);
  assert.match(block, /catch-up ceiling was reached.*after=101/);
  await until(() => h.delivery.watermark('zulip:general') === 101, 'the block accepted');
  assert.equal(h.delivery.watermark('zulip:general'), 101, 'advanced through everything scanned');
  assert.equal((h.pushed[0].origin as { truncated?: boolean }).truncated, true);
  // The agent answers the block in the topic of its newest mention.
  await h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c1', channelId: 'zulip:general', content: [{ type: 'text', text: 'on it' }] });
  assert.equal((h.adapter.published.at(-1)?.hints?.metadata as { topic?: string } | undefined)?.topic, 'triage');
  await h.close();
});

test('a catch-up block the host refuses moves no topic', async () => {
  const h = harness({ history: Array.from({ length: 20 }, (_, i) => i + 2 === 10
    ? streamMsg(10, { mentioned: true, threadId: 'triage', metadata: { topic: 'triage', mentioned: true, isDM: false } })
    : streamMsg(i + 2)) });
  await initialize(h, true);
  await settled(h);
  // The last mention the agent was given is in "support", and older than the
  // block's, so a refused block that took or cleared the hint would show.
  h.adapter.emit!(streamMsg(1, { mentioned: true, threadId: 'support', metadata: { topic: 'support', mentioned: true, isDM: false } }));
  await until(() => h.delivery.watermark('zulip:general') === 1, 'the mention in support accepted');
  // The block goes out under the newest id it scanned, 21.
  h.policy.rejectPushes.add('21');

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 2, 'catch-up push for the closed channel');
  assert.equal((h.pushed[1].origin as { messageId?: string }).messageId, '21');
  // A round trip after the refusal: the server has handled it by the answer.
  await h.host.sendRequest(method.CHANNELS_LIST);
  assert.equal(h.delivery.watermark('zulip:general'), 1, 'refused: the watermark stays');
  await h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c1', channelId: 'zulip:general', content: [{ type: 'text', text: 'on it' }] });
  assert.equal((h.adapter.published.at(-1)?.hints?.metadata as { topic?: string } | undefined)?.topic, 'support');
  await h.close();
});

test('disabling zulip.messaging stops incoming delivery and its tools until re-enabled', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
  await h.host.sendRequest('ping');
  h.adapter.emit!(streamMsg(1));
  h.adapter.emit!(streamMsg(2, { mentioned: true }));
  await h.server.quiet();
  assert.equal(h.incoming.length, 0, 'no channels/incoming under a disabled feature set');
  assert.equal(h.pushed.length, 0);
  assert.equal(h.delivery.watermark('zulip:general'), 0, 'just below the held messages: nothing is pretended forwarded');
  const refresh = (await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { isError?: boolean; content: { text: string }[] };
  assert.equal(refresh.isError, true);
  assert.match(refresh.content[0].text, /not enabled/);
  await h.close();
});

test("messages that arrive before their channel's catch-up keep their order behind it, and a later one waits behind one fetching its attachments", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-drain-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'dr' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.close();

    let releaseRead!: () => void;
    const readGate = new Promise<void>((r) => { releaseRead = r; });
    let reading = false;
    let releaseFetch!: () => void;
    const fetchGate = new Promise<void>((r) => { releaseFetch = r; });
    let fetchStarted = false;
    const second = harness({
      stateDir: dir, sessionId: 'dr',
      attachments: {
        source: { async fetch() { fetchStarted = true; await fetchGate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
        inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
      },
    });
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { reading = true; await readGate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    await until(() => reading, "the channel's catch-up read");
    const opened = second.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    second.adapter.emit!({
      ...streamMsg(20, { text: 'see the log' }),
      metadata: { topic: 'deploys', mentioned: false, isDM: false, attachments: [{ path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false }] },
    });
    second.adapter.emit!(streamMsg(21));
    await second.host.sendRequest('ping');
    releaseRead();
    await opened;
    await until(() => fetchStarted, "20's attachment fetch");
    second.adapter.emit!(streamMsg(22));
    await second.host.sendRequest('ping');
    assert.equal(second.incoming.length, 0, 'nothing overtakes the message fetching its attachments');
    releaseFetch();
    await second.server.quiet();
    assert.deepEqual(second.incoming.map((m) => m.messageId), ['20', '21', '22']);
    assert.ok(second.incoming[0].content.some((c) => c.type === 'text' && /run\.log/.test(c.text)), 'the stalled attachment was inlined');
    assert.equal(second.delivery.watermark('zulip:general'), 22);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a narrowed allowlist is enforced on every surface: live, push, backscroll, context, gap markers, the sweep, and DMs by sender', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-allowlist-'));
  const original = console.error;
  console.error = () => {};
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const both = [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
    const h = harness({ filters: plane, stateDir: dir, sessionId: 'al' });
    h.adapter.discoverChannels = async () => both;
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    h.adapter.emit!(streamMsg(1));
    await until(() => h.incoming.length === 1, 'delivery while allowed');
    const call = async (name: string, args: Record<string, unknown>) =>
      JSON.parse(((await h.host.sendRequest('tools/call', { name, arguments: args })) as { content: { text: string }[] }).content[0].text);

    // Narrow to #dev. The host still has #general registered and open.
    await call('filters_update', { addStreams: ['dev'], removeStreams: ['general'] });
    assert.equal(plane.streamAllowed('general'), false);
    h.adapter.history = [streamMsg(1), streamMsg(2, { mentioned: true }), streamMsg(3)];

    // Live: dropped; a mention: not pushed; nothing tallied or watermarked.
    h.adapter.emit!(streamMsg(2, { mentioned: true }));
    h.adapter.emit!(streamMsg(3));
    await h.server.quiet();
    assert.equal(h.incoming.length, 1);
    assert.equal(h.pushed.length, 0);
    assert.equal(h.delivery.watermark('zulip:general'), 1);

    // Backscroll on open and context injection: nothing, and no fetch either.
    const reopened = (await h.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 10 },
    })) as { history?: unknown[] };
    assert.deepEqual(reopened.history, []);
    const ctx = (await h.host.sendRequest(method.CONTEXT_BEFORE_INFERENCE, {
      inferenceId: 'i', conversationId: 'c', turnIndex: 0, userMessage: null,
      model: { id: 'm', vendor: 'v', contextWindow: 1, capabilities: [] },
    })) as { contextInjections: unknown[] };
    assert.deepEqual(ctx.contextInjections, []);
    // Gap recovery: no replay from the stream. The marker itself is about the
    // connection, not the stream, and still rides the open channel.
    h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
    await until(() => h.incoming.length === 2, 'the gap marker');
    assert.equal((h.incoming[1].metadata as { kind: string; recoveredMessages: number }).kind, 'gap');
    assert.equal((h.incoming[1].metadata as { recoveredMessages: number }).recoveredMessages, 0);
    assert.equal(h.adapter.historyCalls.length, 0, 'never fetched');
    h.delivery.advance('zulip:dev', 50);
    h.delivery.save();
    await h.close();

    // The reconnect sweep: #general is watermarked with new mentions beyond
    // the watermark, but outside the allowlist — not fetched, not pushed.
    // #dev, inside it, is caught up.
    const second = harness({
      filters: plane, stateDir: dir, sessionId: 'al',
      history: [streamMsg(2, { mentioned: true }), streamMsg(3), streamMsg(60, { channelId: 'zulip:dev', mentioned: true, text: 'dev mention' })],
    });
    second.adapter.discoverChannels = async () => both;
    await initialize(second, true);
    await settled(second);
    await until(() => second.pushed.length === 1, 'the dev catch-up');
    assert.equal((second.pushed[0].origin as { mcplChannelId: string }).mcplChannelId, 'zulip:dev');
    assert.ok(second.adapter.historyCalls.every((c) => c.channelId !== 'zulip:general'), 'the excluded stream is never fetched');

    // DMs are judged by sender: only user 42 may DM the bot now.
    const call2 = async (name: string, args: Record<string, unknown>) =>
      JSON.parse(((await second.host.sendRequest('tools/call', { name, arguments: args })) as { content: { text: string }[] }).content[0].text);
    await call2('filters_update', { setDmUsers: ['42'] });
    const dmFrom = (id: number): [IncomingChannelMessage, ChannelDescriptor] => [
      {
        channelId: `zulip:dm:${id}`, messageId: String(500 + id), author: { id: String(id), name: `User ${id}` },
        timestamp: new Date().toISOString(), content: [{ type: 'text', text: 'hi' }], tags: ['chat:dm', 'chat:private', 'chat:from-human'],
        metadata: { isDM: true, mentioned: false, senderEmail: `u${id}@example.com` },
      },
      {
        id: `zulip:dm:${id}`, type: 'zulip', label: `DM: User ${id}`, direction: 'bidirectional',
        address: { dm: true, user_ids: [id], emails: [`u${id}@example.com`] },
        metadata: { channelType: 'dm', participants: [{ id, name: `User ${id}`, email: `u${id}@example.com` }] },
      },
    ];
    second.adapter.emit!(...dmFrom(7));
    await second.server.quiet();
    assert.equal(second.pushed.length, 1, 'a DM from an excluded sender goes nowhere');
    assert.ok(!second.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED && JSON.stringify(r.params).includes('zulip:dm:7')), 'and is not even announced');
    second.adapter.emit!(...dmFrom(42));
    await until(() => second.pushed.length === 2, 'a DM from the allowed sender');
    plane.stop();
    await second.close();
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a feature set disabled while a message waits in the batch window withholds it; enabling it again replays it', async () => {
  const original = console.error;
  console.error = () => {};
  try {
    const h = harness({ batchWindowMs: 120, history: [streamMsg(1)] });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

    h.adapter.emit!(streamMsg(1));
    h.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
    await h.host.sendRequest('ping');
    await h.server.quiet();
    assert.equal(h.incoming.length, 0, 'withheld at send time, not only at enqueue');
    assert.equal(h.delivery.watermark('zulip:general'), 0, 'just below the withheld message');
    assert.equal(h.delivery.floor('zulip:general'), 1, 'held, not forgotten: not heard is not the same as heard');

    await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await until(() => h.incoming.length === 1, 'replayed once the feature set is back');
    assert.equal(h.incoming[0].messageId, '1');
    assert.equal((h.incoming[0].metadata as { recovered: boolean }).recovered, true);
    await until(() => h.delivery.watermark('zulip:general') === 1, 'watermark 1');
    await h.close();
  } finally {
    console.error = original;
  }
});

const AAA: ChannelDescriptor = { id: 'zulip:aaa', type: 'zulip', label: '#aaa', direction: 'bidirectional', address: { stream_name: 'aaa', stream_id: 6 } };

function aaaHoldsTheTurn(h: Harness): { reading: () => boolean; release: () => void } {
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  let reading = false;
  const real = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    if (h.adapter.wireOf(key) === 'zulip:aaa') {
      reading = true;
      await gate;
    }
    return real(key, query);
  };
  return { reading: () => reading, release: () => release() };
}

async function openedWhileAaaReads(dir: string, sessionId: string, history: IncomingChannelMessage[], open: Record<string, unknown>): Promise<{ second: Harness; turn: ReturnType<typeof aaaHoldsTheTurn>; opened: { history?: IncomingChannelMessage[] } }> {
  const first = harness({ stateDir: dir, sessionId, visible: [AAA, DESCRIPTOR] });
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.adapter.emit!(streamMsg(3));
  await until(() => first.delivery.watermark('zulip:general') === 3, 'delivery');
  first.delivery.advance('zulip:aaa', 1);
  await first.close();

  const second = harness({ stateDir: dir, sessionId, visible: [AAA, DESCRIPTOR], history });
  const turn = aaaHoldsTheTurn(second);
  await initialize(second, true);
  await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await until(turn.reading, "#aaa's catch-up read to hold Zulip's turn");
  const opened = (await second.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, ...open,
  })) as { history?: IncomingChannelMessage[] };
  return { second, turn, opened };
}

const generalBlock = (h: Harness) => {
  const blocks = h.pushed.filter((p) => (p.origin as { mcplChannelId?: string }).mcplChannelId === 'zulip:general' && p.tags?.includes('zulip:missed'));
  return blocks.map((p) => (p.payload.content[0] as { text: string }).text);
};

test("a page of the newest rows, opened before its channel's catch-up read, counts nothing below it as seen, and the catch-up block brings every unread row", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-page-before-read-'));
  try {
    const unread = [streamMsg(4, { mentioned: true }), ...[5, 6, 7, 8].map((id) => streamMsg(id))];
    const { second, turn, opened } = await openedWhileAaaReads(dir, 'pr', unread, { history: { limit: 2 } });
    assert.deepEqual(opened.history!.map((m) => m.messageId), ['7', '8']);
    assert.equal(second.delivery.watermark('zulip:general'), 3, 'a page of the newest rows counts nothing below it as seen');
    turn.release();
    await second.server.quiet();
    const blocks = generalBlock(second);
    assert.equal(blocks.length, 1);
    for (const id of [4, 5, 6, 7, 8]) assert.match(blocks[0], new RegExp(`id=${id}\\]`));
    assert.equal(second.delivery.watermark('zulip:general'), 8, 'the watermark moves once the host accepts the block');
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a page of the newest rows, answered before the catch-up block, loses nothing below it when the connection ends before the block', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-page-before-block-'));
  try {
    const unread = () => [streamMsg(4, { mentioned: true }), ...[5, 6, 7, 8].map((id) => streamMsg(id))];
    const { second, turn } = await openedWhileAaaReads(dir, 'pb', unread(), { history: { limit: 2 } });
    const closing = second.close();
    await second.server.quiet();
    turn.release();
    await closing;

    const third = harness({ stateDir: dir, sessionId: 'pb', visible: [AAA, DESCRIPTOR], history: unread() });
    await initialize(third, true);
    await settled(third);
    const blocks = generalBlock(third);
    assert.equal(blocks.length, 1, 'the catch-up block');
    for (const id of [4, 5, 6, 7, 8]) assert.match(blocks[0], new RegExp(`id=${id}\\]`));
    await third.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a sinceLastSeen page opened before its channel's catch-up read isn't repeated in the catch-up block", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-since-before-read-'));
  try {
    const unread = [4, 5, 6, 7, 8].map((id) => streamMsg(id));
    const { second, turn, opened } = await openedWhileAaaReads(dir, 'sr', unread, { history: { limit: 2, sinceLastSeen: true } });
    assert.deepEqual(opened.history!.map((m) => m.messageId), ['4', '5']);
    assert.equal(second.delivery.watermark('zulip:general'), 5);
    turn.release();
    await second.server.quiet();
    const blocks = generalBlock(second);
    assert.equal(blocks.length, 1);
    for (const id of [6, 7, 8]) assert.match(blocks[0], new RegExp(`id=${id}\\]`));
    for (const id of [4, 5]) assert.doesNotMatch(blocks[0], new RegExp(`id=${id}\\]`));
    assert.equal(second.delivery.watermark('zulip:general'), 8);
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("an open with a page gives a channel that has no watermark one, and the next connection's catch-up brings what was written after it", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-open-gives-watermark-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'ow', history: [1, 2].map((id) => streamMsg(id)) });
    await initialize(first, true);
    await settled(first);
    assert.equal(first.delivery.watermark('zulip:general'), undefined);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2 } });
    assert.equal(first.delivery.watermark('zulip:general'), 2);
    await first.close();

    const second = harness({ stateDir: dir, sessionId: 'ow', history: [1, 2, 3, 4].map((id) => streamMsg(id)) });
    await initialize(second, true);
    await settled(second);
    const blocks = generalBlock(second);
    assert.equal(blocks.length, 1);
    for (const id of [3, 4]) assert.match(blocks[0], new RegExp(`id=${id}\\]`));
    for (const id of [1, 2]) assert.doesNotMatch(blocks[0], new RegExp(`id=${id}\\]`));
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("after a reconnect within one process, the watermark follows what the new connection's host accepts", async () => {
  const first = harness({ visible: [AAA, DESCRIPTOR], history: [1, 2, 3, 4, 5].map((id) => streamMsg(id)) });
  await initialize(first, true);
  await settled(first);
  first.delivery.advance('zulip:aaa', 1);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.adapter.emit!(streamMsg(1));
  await until(() => first.delivery.watermark('zulip:general') === 1, 'delivery');
  first.policy.rejectIds.add('2');
  for (const id of [2, 3, 4, 5]) first.adapter.emit!(streamMsg(id));
  await first.server.quiet();
  assert.equal(first.delivery.watermark('zulip:general'), 1, 'the refused message holds the watermark below it');
  await first.close();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  const turn = aaaHoldsTheTurn(second);
  await initialize(second, true);
  await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await until(turn.reading, "#aaa's catch-up read to hold Zulip's turn");
  const opened = (await second.host.sendRequest(method.CHANNELS_OPEN, {
    channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 1, sinceLastSeen: true },
  })) as { history?: IncomingChannelMessage[] };
  assert.deepEqual(opened.history!.map((m) => m.messageId), ['2']);
  assert.equal(second.delivery.watermark('zulip:general'), 2, "the page's newest row, not what the earlier connection's host accepted");
  turn.release();
  await second.server.quiet();
  await second.close();
});

test('a second connection to the same server starts from a fresh grant, registration and sweep', async () => {
  const one = harness();
  await initialize(one, true);
  await settled(one);
  await one.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  one.adapter.emit!(streamMsg(1));
  await until(() => one.incoming.length === 1, 'delivery to the first peer');
  const toServer = one as unknown as { served: Promise<void> };
  await one.close();
  await toServer.served;

  const two = harness({ reuse: { server: one.server, adapter: one.adapter } });
  await initialize(two, true);
  // Nothing of the first peer's grant carries over: absence is denial again.
  await assert.rejects(two.host.sendRequest(method.CHANNELS_LIST), (err: Error & { code?: number }) => err.code === -32002);
  assert.equal(two.server.channelManager.isOpen('zulip:general'), false, 'nor what it had open');
  await settled(two);
  assert.ok(two.hostSaw.some((r) => r.method === method.CHANNELS_REGISTER), 'registered afresh');
  await two.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  two.adapter.emit!(streamMsg(2));
  await until(() => two.incoming.length === 1, 'delivery to the second peer');
  assert.equal(one.incoming.length, 1, 'and none of it went to the first');
  await two.close();
});

test('a host that leaves while a delivery waits on an attachment leaves the next host\'s delivery to its own start', async () => {
  let releaseFetch!: () => void;
  let fetching = false;
  const fetchGate = new Promise<void>((r) => { releaseFetch = r; });
  const one = harness({
    attachments: {
      source: { async fetch() { fetching = true; await fetchGate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
    shutdownGraceMs: 20,
  });
  await initialize(one, true);
  await until(() => one.adapter.emit !== null, 'event delivery started before the policy');
  one.adapter.emit!({
    ...streamMsg(1, { text: 'see the log', mentioned: true }),
    metadata: { topic: 'deploys', mentioned: true, isDM: false, attachments: [{ path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false }] },
  });
  await one.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await until(() => fetching, 'the delivery waiting on the attachment');
  await one.close();
  const two = harness({ reuse: { server: one.server, adapter: one.adapter } });
  try {
    await initialize(two, true);
    releaseFetch();
    await new Promise((r) => setTimeout(r, 100));
    assert.deepEqual([...two.pushed.map((p) => p.origin.messageId), ...two.incoming.map((m) => m.messageId)], [], 'nothing reaches the second host before its policy');
  } finally {
    releaseFetch();
    await two.close();
  }
});

test('shutdown waits for a delivery still fetching its attachment, then flushes it', async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  let fetchStarted = false;
  const h = harness({
    attachments: {
      source: { async fetch() { fetchStarted = true; await gate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
      inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
    },
  });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!({
    ...streamMsg(1, { text: 'see the log' }),
    metadata: { topic: 'deploys', mentioned: false, isDM: false, attachments: [{ path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false }] },
  });
  await until(() => fetchStarted, 'the fetch to start');
  assert.equal(h.server.channelManager.pendingCount(), 0, 'still building its content');
  const done = h.server.shutdown();
  assert.equal(h.incoming.length, 0);
  release();
  await done;
  assert.equal(h.incoming.length, 1);
  assert.equal(h.delivery.watermark('zulip:general'), 1);
  await h.close();
});

test('a batch answer that comes while its channel waits on an attachment still counts when the server shuts down first', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  h.adapter.emit!(withLog(streamMsg(2)));
  await until(() => fetches.started.length === 1 && h.incoming.some((m) => m.messageId === '1'), '1 delivered while 2 waits on its attachment');
  await h.host.sendRequest('ping');
  const done = h.server.shutdown();
  fetches.release();
  await done;
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '2']);
  assert.equal(h.delivery.watermark('zulip:general'), 2, 'the host accepted both, so the next start offers neither');
  await h.close();
});

test('catch-up pages past a page holding nothing but the bot\'s own messages', async () => {
  const h = harness({ history: [streamMsg(101), streamMsg(102), streamMsg(103, { mentioned: true })] });
  // The adapter withholds the bot's own messages after fetching; 101 and
  // 102 are its own, and the page size is two.
  const realFetch = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (channelId, query) => {
    const page = await realFetch(channelId, query);
    return { ...page, messages: page.messages.filter((m) => Number(m.messageId) > 102) };
  };
  h.adapter.pageCap = 2;
  await initialize(h, true);
  await settled(h);
  h.delivery.advance('zulip:general', 100);
  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 1, 'the mention beyond the self-only page');
  assert.match((h.pushed[0].payload.content[0] as { text: string }).text, /id=103\]/);
  assert.deepEqual(h.adapter.historyCalls.map((c) => c.query.afterMessageId), ['100', '102'], 'the cursor advanced on what was scanned');
  assert.equal((h.pushed[0].origin as { truncated?: boolean }).truncated, undefined);
  assert.equal(h.delivery.watermark('zulip:general'), 103);
  await h.close();
});

// ── Streams joined after startup (#20) ──

const LATE_STREAM: ChannelDescriptor = {
  id: 'zulip:ops',
  type: 'zulip',
  label: '#ops',
  direction: 'bidirectional',
  address: { stream_name: 'ops', stream_id: 12 },
};

test('refresh_channels announces a newly visible stream and the host can then open it', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  const registered = h.hostSaw.find((r) => r.method === method.CHANNELS_REGISTER)!.params as ChannelsRegisterParams;
  assert.deepEqual(registered.channels.map((c) => c.id), ['zulip:general'], 'only what was visible at startup');

  // Unknown until it is announced: the host cannot open what was never registered.
  await assert.rejects(
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} }),
    (err: { code?: number; message?: string }) => err.code === -32023 && /Unknown channel: zulip:ops/.test(err.message ?? ''),
  );

  // The bot joins #ops; the adapter can now see it.
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const refreshed = (await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] };
  const report = JSON.parse(refreshed.content[0].text) as { visible: number; added: string[]; note: string };
  assert.equal(report.visible, 2);
  assert.deepEqual(report.added, ['zulip:ops'], 'the newly visible stream is announced');
  assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED), 'announced via channels/changed');

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:ops');
  assert.deepEqual(h.adapter.subscribed, ['zulip:ops']);
  await h.close();
});

test('a message that carries a descriptor for an unregistered channel registers it before the push goes out', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);

  // Not registered at startup: the bot was not in #ops then.
  await assert.rejects(
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} }),
    (err: { code?: number }) => err.code === -32023,
  );

  // A mention arrives from #ops. The adapter describes the stream with it,
  // exactly as it does for a DM from a new conversation.
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];
  h.adapter.emit!({
    channelId: 'zulip:ops',
    messageId: '900',
    threadId: 'incidents',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date().toISOString(),
    content: [{ type: 'text', text: '@bot are you there?' }],
    tags: ['chat:mention', 'chat:from-human'],
    metadata: { topic: 'incidents', mentioned: true, isDM: false },
  }, LATE_STREAM);

  await until(() => h.pushed.length === 1, 'the mention is pushed');
  assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED && JSON.stringify(r.params).includes('zulip:ops')), 'announced before the push');

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:ops', 'the channel the agent was mentioned in is openable, with no restart');

  // A second message on the same stream does not re-announce it.
  const announcements = h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED).length;
  h.adapter.emit!(streamMsg(901, { channelId: 'zulip:ops', metadata: { topic: 'incidents', mentioned: false, isDM: false } }));
  await until(() => h.incoming.length === 1, 'delivery on the now-open channel');
  assert.equal(h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED).length, announcements);
  await h.close();
});

test('a channel the host never answered for stays usable here and is re-announced when the host answers again (#20)', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);

  // The host does not answer channels/changed — a Notification-only host, a
  // timeout, a dropped transport. The channel must not be lost.
  h.policy.changedAnswer = 'error';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];
  const first = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; pendingAnnouncement?: string[]; note: string };
  assert.deepEqual(first.added, []);
  assert.deepEqual(first.pendingAnnouncement, ['zulip:ops']);
  assert.match(first.note, /could not be announced/);

  // The host answers again: the next refresh re-announces what was pending.
  h.policy.changedAnswer = 'accept';
  const second = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; note: string };
  assert.deepEqual(second.added, ['zulip:ops']);
  assert.match(second.note, /Registered 1 newly visible channel/);

  // And a third refresh has nothing left to say.
  const third = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; pendingAnnouncement?: string[] };
  assert.deepEqual(third.added, []);
  assert.equal(third.pendingAnnouncement, undefined);
  await h.close();
});

test('a host that opens or closes a pending channel has confirmed it; the backlog does not grow forever (#20)', async () => {
  // A host that takes channels/changed as a notification never answers it.
  // The host acting on the channel is the only confirmation available, so
  // it has to count as one.
  const h = harness({ announceTimeoutMs: 80 });
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'error';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const first = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { pendingAnnouncement?: string[] };
  assert.deepEqual(first.pendingAnnouncement, ['zulip:ops'], 'no answer came');
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} });
  h.policy.changedAnswer = 'accept';

  // Second call: nothing pending, no second announcement, no stall.
  const announcements = h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED).length;
  const second = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; pendingAnnouncement?: string[]; note: string };
  assert.equal(second.pendingAnnouncement, undefined, 'the host proved it knows the channel by opening it');
  assert.deepEqual(second.added, []);
  assert.equal(h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED).length, announcements, 'and it is not announced again');
  await h.close();
});

test('a channel the host itemizes as refused is not registered, and refresh_channels says so (#20)', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'refuse';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const report = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; refused?: string[]; note: string };
  assert.deepEqual(report.added, []);
  assert.deepEqual(report.refused, ['zulip:ops']);
  assert.match(report.note, /refused 1 channel/);
  await assert.rejects(
    h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} }),
    (err: { code?: number }) => err.code === -32023,
    'a refusal is the host\'s answer and is respected',
  );
  await h.close();
});

test('listen registers what it subscribed the bot to and reports how the host answered (#20)', async () => {
  const h = harness({ announceTimeoutMs: 80 });
  await initialize(h, true);
  await settled(h);
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const ok = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'listen', arguments: { channels: ['ops'] } })) as { content: { text: string }[] }).content[0].text) as
    { registration: { announced: string[]; local: string[]; refused: string[] } };
  assert.deepEqual(ok.registration.announced, ['zulip:ops']);
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:ops');

  // A host that does not answer: the tool says so rather than logging it.
  h.policy.changedAnswer = 'error';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM, { ...LATE_STREAM, id: 'zulip:infra', label: '#infra', address: { stream_name: 'infra', stream_id: 13 } }];
  const stalled = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'listen', arguments: { channels: ['infra'] } })) as { content: { text: string }[] }).content[0].text) as
    { registration: { announced: string[]; local: string[] } };
  assert.deepEqual(stalled.registration.local, ['zulip:infra'], 'the tool result carries the outcome; the note is the runtime\'s (see toolRuntime tests)');
  await h.close();
});

test('an open whose grant loses channels.lifecycle while it reads its page is refused, and no row of the page counts as seen', async () => {
  const h = harness({ history: [streamMsg(1), streamMsg(2)] });
  await initialize(h, true);
  await settled(h);
  let release!: () => void;
  const gate = new Promise<void>((resolve) => { release = resolve; });
  let reading = false;
  const read = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    reading = true;
    await gate;
    return read(key, query);
  };
  const open = h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2 } });
  await until(() => reading, 'the page read to start');
  let updated = false;
  void h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT.filter((c) => c !== 'channels.lifecycle') }).then(() => { updated = true; });
  try {
    await until(() => updated, 'the grant to change while the open reads its page');
  } finally {
    release();
  }
  await assert.rejects(open, (err: Error) => /Capability denied: channels\.lifecycle/.test(err.message));
  assert.equal(h.server.channelManager.isOpen('zulip:general'), false);
  assert.equal(h.delivery.watermark('zulip:general'), undefined, 'no row of the page counts as seen');
  await h.close();
});

test('a host that opens a channel before it answers its announcement gets the open served while a tool call waits for that answer', async () => {
  const h = harness({ announceTimeoutMs: 5_000 });
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'reconcile';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const report = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; pendingAnnouncement?: string[]; note: string };
  assert.deepEqual(h.policy.reconcileOpens, [{ channelId: 'zulip:ops', ok: true, detail: 'opened' }]);
  assert.deepEqual(report.added, ['zulip:ops'], 'the host answered the announcement');
  assert.equal(report.pendingAnnouncement, undefined);
  assert.equal(h.server.channelManager.isOpen('zulip:ops'), true);
  await h.close();
});

test('channels/open re-discovery settles against a host that reconciles before it answers (#20)', async () => {
  // The re-discovery runs inside the open, so such a host cannot answer its
  // announcement until the open returns. With the descriptor recorded before
  // the announcement goes out, the open succeeds after the timeout, and the
  // host's queued reconcile-time open then finds the channel instead of
  // re-discovering it and starting the same cycle over.
  const h = harness({ announceTimeoutMs: 80 });
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'reconcile';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:ops');
  await until(() => h.policy.reconcileOpens.length > 0, "the host's reconcile-time open is served");
  assert.deepEqual(h.policy.reconcileOpens, [{ channelId: 'zulip:ops', ok: true, detail: 'opened' }]);

  // Several timeouts' worth of quiet: announced once, not once per cycle.
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED).length, 1);
  assert.equal(h.server.channelManager.isOpen('zulip:ops'), true);
  await h.close();
});

test('a new stream\'s first message arrives on its channel when the host opens it before it answers the announcement, and as on a closed channel when it answers first', async () => {
  const h = harness({ announceTimeoutMs: 5_000 });
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'reconcile';
  h.adapter.emit!(streamMsg(20, { channelId: 'zulip:ops', text: 'first in ops' }), LATE_STREAM);
  await until(() => h.incoming.length === 1, 'the message on the opened channel');
  assert.deepEqual(h.incoming.map((m) => [m.channelId, m.messageId]), [['zulip:ops', '20']]);

  h.policy.changedAnswer = 'accept';
  const infra: ChannelDescriptor = { ...LATE_STREAM, id: 'zulip:infra', label: '#infra', address: { stream_name: 'infra', stream_id: 13 } };
  h.adapter.emit!(streamMsg(21, { channelId: 'zulip:infra', text: 'first in infra' }), infra);
  await h.server.quiet();
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:infra', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:infra');
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['20'], 'the infra message was handled as on a closed channel');
  await h.close();
});

test('an edit from a new stream arrives on its channel when the host opens it before it answers the announcement', async () => {
  const h = harness({ announceTimeoutMs: 5_000 });
  await initialize(h, true);
  await settled(h);
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];
  h.policy.changedAnswer = 'reconcile';
  h.adapter.change!({
    kind: 'edit', channelId: 'zulip:ops', messageId: '900', messageIds: ['900'], authorId: '9', authorName: 'Ann',
    authorEmail: 'ann@example.com', actorId: '9', topic: 'incidents', previousTopic: null, movedToChannelId: null,
    content: '@bot look at this', previousContent: 'look at this', mentioned: true, previouslyMentioned: false,
    vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_060_000),
  });
  await until(() => h.incoming.length === 1, 'the edit on the opened channel');
  assert.equal(h.incoming[0].channelId, 'zulip:ops');
  assert.match((h.incoming[0].content[0] as TextContent).text, /^\[edited\]/);
  assert.equal(h.pushed.length, 0);
  await h.close();
});

test('an open and then a close for a channel id the directory does not hold yet are served in that order', async () => {
  const h = harness({ announceTimeoutMs: 5_000 });
  await initialize(h, true);
  await settled(h);
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];
  h.policy.changedAnswer = 'held';
  const answered: string[] = [];
  const open = h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} }).then(() => answered.push('open'));
  await until(() => h.policy.heldChanged.length === 1, "the open's re-discovery to announce the stream");
  const close = h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:ops' }).then(() => answered.push('close'));
  // Time for the close to be read while the announcement is out. It changes
  // only whether a server that served the close first could show it.
  await new Promise((r) => setTimeout(r, 50));
  h.policy.heldChanged.shift()!();
  await Promise.all([open, close]);
  assert.deepEqual(answered, ['open', 'close']);
  assert.equal(h.server.channelManager.isOpen('zulip:ops'), false);
  await h.close();
});

test('an edit, move or deletion is as visible as its message: open channels see accepted ones, closed channels only addressed ones (#22)', async () => {
  const h = harness();
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  await settled(h);

  // Ann's message is delivered and accepted: the watermark now covers id 10.
  h.adapter.emit!({
    channelId: 'zulip:general', messageId: '10', author: { id: '9', name: 'Ann' }, timestamp: new Date(1_700_000_000_000).toISOString(),
    content: [{ type: 'text', text: 'ship it' }], tags: ['chat:ambient'], metadata: { topic: 'deploys', mentioned: false, isDM: false },
  });
  await until(() => h.incoming.length === 1, 'the message itself');
  assert.equal(h.delivery.watermark('zulip:general'), 10);

  const change = (over: Partial<MessageChangeEvent> = {}): MessageChangeEvent => ({
    kind: 'edit', channelId: 'zulip:general', messageId: '10', messageIds: ['10'], authorId: '9', authorName: 'Ann', authorEmail: 'ann@example.com', actorId: '9',
    topic: 'deploys', previousTopic: null, movedToChannelId: null, content: 'ship it tomorrow', previousContent: 'ship it',
    mentioned: false, previouslyMentioned: false, vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_060_000),
    ...over,
  });

  // An edit of the accepted message surfaces on the open channel, with the
  // message's own id in the line and a synthetic id that moves no watermark.
  h.adapter.change!(change());
  await until(() => h.incoming.length === 2, 'edit on the open channel');
  const edited = h.incoming[1];
  assert.equal((edited.content[0] as { text: string }).text, '[edited] [T id=10] [#general > deploys] Ann: ship it tomorrow');
  assert.deepEqual(edited.tags, ['chat:edited', 'chat:ambient'], 'an ambient change is tagged ambient, so a debounced policy treats it as such');
  assert.match(edited.messageId, /^edit:10:1700000060000\.\d+$/);
  assert.equal(edited.threadId, undefined, 'a marker about a message is not the conversation');
  const meta = edited.metadata as Record<string, unknown>;
  assert.equal(meta.targetMessageId, '10');
  assert.equal(meta.previousContent, 'ship it');
  assert.equal(meta.senderEmail, 'ann@example.com');
  assert.equal(meta.attributed, true);
  assert.equal(h.delivery.watermark('zulip:general'), 10, 'a change never advances the watermark');

  // A message the host never accepted: its edit is noise.
  h.adapter.change!(change({ messageId: '11', messageIds: ['11'] }));
  await h.server.quiet();
  assert.equal(h.incoming.length, 2);

  // ...unless the edit now mentions the bot, or the message is the bot's own.
  h.adapter.change!(change({ messageId: '11', messageIds: ['11'], mentioned: true, content: 'ship it @Bot?' }));
  await until(() => h.incoming.length === 3, 'a newly addressing edit');
  assert.equal((h.incoming[2].content[0] as { text: string }).text, '[edited] [T id=11] [#general > deploys] Ann (mention): ship it @Bot?');
  assert.deepEqual(h.incoming[2].tags, ['chat:edited', 'chat:mention']);
  h.adapter.change!(change({ kind: 'delete', messageId: '12', messageIds: ['12'], content: null, previousContent: 'my own line', onOwnMessage: true, actorId: null }));
  await until(() => h.incoming.length === 4, 'a deletion of the bot\'s own message');
  assert.equal((h.incoming[3].content[0] as { text: string }).text, '[deleted] [T id=12] [#general > deploys] Ann: message deleted — was: "my own line"');
  assert.deepEqual(h.incoming[3].tags, ['chat:deleted', 'chat:ambient']);

  // A message offered but not yet accepted (held) counts as seen: "post,
  // then fix the typo" lands before the host's acceptance round trip.
  h.policy.rejectIds.add('13');
  h.adapter.emit!({
    channelId: 'zulip:general', messageId: '13', author: { id: '9', name: 'Ann' }, timestamp: new Date(1_700_000_070_000).toISOString(),
    content: [{ type: 'text', text: 'shp it' }], tags: ['chat:ambient'], metadata: { topic: 'deploys', mentioned: false, isDM: false },
  });
  await until(() => h.delivery.heldIds('zulip:general').includes(13), 'the refused message is held');
  h.adapter.change!(change({ messageId: '13', messageIds: ['13'], content: 'ship it', previousContent: 'shp it' }));
  await until(() => h.incoming.length === 5, 'an edit of a held message');
  assert.equal((h.incoming[4].content[0] as { text: string }).text, '[edited] [T id=13] [#general > deploys] Ann: ship it');

  // A moderator's topic move of three messages reads as one line and does
  // not retarget the agent's reply: the conversation is still in `deploys`.
  h.adapter.change!(change({ kind: 'move', messageIds: ['10', '8', '9'], content: null, previousContent: null, previousTopic: 'deploys', topic: 'deploys-2', actorId: '12' }));
  await until(() => h.incoming.length === 6, 'a topic move');
  assert.equal((h.incoming[5].content[0] as { text: string }).text, '[moved] [T id=10] [#general > deploys-2] Ann: topic changed from "deploys" (3 messages) [by user 12]');
  assert.deepEqual(h.incoming[5].tags, ['chat:edited', 'zulip:moved', 'chat:ambient']);
  await h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text: 'on it' }] });
  assert.equal(h.adapter.published.length, 1);
  assert.equal((h.adapter.published[0].hints?.metadata as { topic: string }).topic, 'deploys', 'a move marker must not hijack reply routing');

  // Closed channel: an ambient edit is dropped, an addressed one is pushed.
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  h.adapter.change!(change());
  await h.server.quiet();
  assert.equal(h.pushed.length, 0);
  h.adapter.change!(change({ mentioned: true, content: 'ship it @Bot' }));
  await until(() => h.pushed.length === 1, 'an addressed edit on a closed channel');
  assert.deepEqual(h.pushed[0].tags, ['chat:edited', 'chat:mention']);
  assert.match(h.pushed[0].eventId, /^zulip_edit_10_1700000060000\.\d+$/);
  assert.equal((h.pushed[0].origin as { change: string; isMention: boolean }).change, 'edit');
  assert.equal((h.pushed[0].origin as { change: string; isMention: boolean }).isMention, true);
  // Two edits within the same second are two pushes, not a host-side duplicate.
  h.adapter.change!(change({ mentioned: true, content: 'ship it @Bot now' }));
  await until(() => h.pushed.length === 2, 'a second edit within the same second');
  assert.notEqual(h.pushed[1].eventId, h.pushed[0].eventId);
  // A deleted mention on a closed channel is pushed: the message the agent
  // was about to answer is gone.
  h.adapter.change!(change({ kind: 'delete', content: null, mentioned: true, previouslyMentioned: true, actorId: null }));
  await until(() => h.pushed.length === 3, 'a deleted mention on a closed channel');
  assert.deepEqual(h.pushed[2].tags, ['chat:deleted', 'chat:mention']);
  // A move into a stream the bot cannot see reads as a vanishing, not a deletion.
  h.adapter.change!(change({ kind: 'delete', content: null, mentioned: true, previouslyMentioned: true, actorId: null, vanished: true }));
  await until(() => h.pushed.length === 4, 'a vanished mention');
  assert.equal((h.pushed[3].payload as { content: { text: string }[] }).content[0].text, '[deleted] [T id=10] [#general > deploys] Ann (mention): no longer visible to the bot (moved to a stream it cannot see) — was: "ship it"');
  assert.equal((h.pushed[3].origin as { change: string }).change, 'delete');
  await h.close();
});

test('an edit from a stream the bot joined after startup registers the channel too (#20)', async () => {
  // The first thing a newly joined stream sends can be an edit — a message
  // edited into a mention. That marker must not name a channel the agent
  // cannot then open.
  const h = harness();
  await initialize(h, true);
  await settled(h);
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  h.adapter.change!({
    kind: 'edit', channelId: 'zulip:ops', messageId: '900', messageIds: ['900'], authorId: '9', authorName: 'Ann',
    authorEmail: 'ann@example.com', actorId: '9', topic: 'incidents', previousTopic: null, movedToChannelId: null,
    content: '@bot look at this', previousContent: 'look at this', mentioned: true, previouslyMentioned: false,
    vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_060_000),
  });

  await until(() => h.pushed.length === 1, 'the edit is pushed');
  assert.ok(h.hostSaw.some((r) => r.method === method.CHANNELS_CHANGED && JSON.stringify(r.params).includes('zulip:ops')), 'and the channel was announced');
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:ops');
  await h.close();
});

test('a quote reply pushes on a closed channel without claiming an explicit mention', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(5, { metadata: { topic: 'answers', mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
    await until(() => h.pushed.length === 1, 'reply push');
    assert.deepEqual(h.pushed[0].tags, ['chat:reply']);
    assert.equal((h.pushed[0].origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.equal((h.pushed[0].origin as { isMention: boolean }).isMention, true);
    assert.doesNotMatch((h.pushed[0].payload.content[0] as { text: string }).text, /\(mention\)/);
    assert.equal(h.delivery.tally('zulip:general')?.messages ?? 0, 0);
  } finally { await h.close(); }
});

/** Real history classification, with Zulip's unmentioned raw quote row. */
function quoteHistory(readableTarget = true): ZulipAdapter {
  const original: ZulipRawMessage = {
    id: 2, sender_id: 100, sender_full_name: 'Bot', sender_email: 'reader-bot@example.invalid',
    display_recipient: 'general', subject: 'answers', timestamp: 1_700_000_000,
    type: 'stream', flags: [], content: 'original',
  };
  const reply: ZulipRawMessage = {
    ...original, id: 5, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
    content: '@_**Bot|100** [said](/#narrow/channel/7-general/near/2):\n\n```quote\noriginal\n```\nreply',
  };
  return new ZulipAdapter(standInApi({
    'GET /messages': async (q: { anchor: string | number; num_after: number; num_before: number }) => ({
      messages: q.num_after > 0 ? [original, reply].filter((m) => m.id > Number(q.anchor)) : [original, reply],
      found_newest: true,
    }),
    'GET /messages/:id': async () => {
      if (!readableTarget) throw new Error('target unavailable');
      return { message: original };
    },
  }).api, 100, 'session', { directory: historyStreams() });
}

test('a refused quote-reply push is redelivered from classified history when another stream is accepted', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.pushed.length = 0;
    const history = quoteHistory();
    h.adapter.fetchHistory = history.fetchHistory.bind(history);
    const reply = (await history.fetchHistory(streamKey(7), { limit: 100 })).messages[0];
    h.policy.rejectPushes.add('5');
    h.adapter.emit!(reply);
    await until(() => h.delivery.heldIds('zulip:general', { replayable: true }).includes(5), 'refused reply held');
    assert.equal(h.delivery.watermark('zulip:general'), 1);
    h.policy.rejectPushes.delete('5');
    const other = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
    h.adapter.emit!(streamMsg(6, { channelId: other.id, mentioned: true }), other);
    await until(() => h.delivery.watermark('zulip:general') === 5, 'reply healed');
    const recovered = h.pushed.find((p) => p.tags?.includes('zulip:missed'))!;
    assert.ok(recovered);
    assert.equal((recovered.origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.ok(recovered.tags?.includes('chat:reply'));
    assert.ok(!recovered.tags?.includes('chat:mention'));
    assert.equal((recovered.origin as { isMention: boolean }).isMention, true);
    const text = (recovered.payload.content[0] as { text: string }).text;
    assert.match(text, /id=5\]/);
    assert.match(text, /count="1" lines="1" reason="addressed"/);
    assert.equal((recovered.origin as { reason: string }).reason, 'addressed');
    assert.doesNotMatch(text, /\(mention\)/);
    assert.deepEqual(h.delivery.heldIds('zulip:general'), []);
  } finally { await h.close(); }
});

test("a quote reply received during its channel's catch-up reaches the host once, as itself, with reply tags", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-quote-sweep-'));
  let second: Harness | undefined;
  let release!: () => void;
  try {
    const first = harness({ stateDir: dir, sessionId: 'quote-sweep' });
    try {
      await initialize(first, true);
      await settled(first);
      first.adapter.emit!(streamMsg(1, { mentioned: true }));
      await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    } finally { await first.close(); }
    second = harness({ stateDir: dir, sessionId: 'quote-sweep' });
    const history = quoteHistory();
    const gate = new Promise<void>((resolve) => { release = resolve; });
    second.adapter.fetchHistory = async (channelId, query) => {
      await gate;
      return history.fetchHistory(channelId, query);
    };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    const reply = (await history.fetchHistory(streamKey(7), { limit: 100 })).messages[0];
    second.adapter.emit!(reply);
    release();
    await second.server.quiet();
    assert.equal(second.delivery.watermark('zulip:general'), 5);
    assert.equal(second.pushed.length, 1);
    assert.equal(second.pushed[0].eventId, 'zulip_msg_5', 'the reply waited for the catch-up and went as itself');
    assert.ok(second.pushed[0].tags?.includes('chat:reply'));
    assert.equal((second.pushed[0].origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.ok(!second.pushed[0].tags?.includes('chat:mention'));
    const text = (second.pushed[0].payload.content[0] as { text: string }).text;
    assert.doesNotMatch(text, /\(mention\)/);
  } finally {
    release?.();
    await second?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('an edited quote reply pushes on a closed channel with reply tags and no mention label', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.change!({
      kind: 'edit', channelId: 'zulip:general', messageId: '10', messageIds: ['10'],
      authorId: '101', authorName: 'User', authorEmail: 'user@example.invalid', actorId: '101',
      topic: 'answers', previousTopic: null, movedToChannelId: null,
      content: 'corrected reply', previousContent: 'reply', replyToSelf: true,
      previouslyReplyToSelf: true, mentioned: false, previouslyMentioned: false,
      vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(),
    });
    await until(() => h.pushed.length === 1, 'quote-reply edit push');
    assert.deepEqual(h.pushed[0].tags, ['chat:edited', 'chat:reply']);
    assert.equal((h.pushed[0].origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.equal((h.pushed[0].origin as { isMention: boolean }).isMention, true);
    assert.doesNotMatch((h.pushed[0].payload.content[0] as { text: string }).text, /\(mention\)/);
  } finally { await h.close(); }
});

test('DM catch-up retains DM tags without synthesizing an explicit mention', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    const descriptor: ChannelDescriptor = {
      id: 'zulip:dm:42', type: 'zulip', label: 'DM: User', direction: 'bidirectional',
      address: { dm: true, user_ids: [42] },
    };
    const dm = (id: number) => streamMsg(id, {
      channelId: descriptor.id, tags: ['chat:dm', 'chat:private'],
      metadata: { isDM: true, mentioned: false },
    });
    h.adapter.emit!(dm(1), descriptor);
    await until(() => h.delivery.watermark(descriptor.id) === 1, 'initial DM acceptance');
    h.adapter.history = [dm(5)];
    h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
    await until(() => h.delivery.watermark(descriptor.id) === 5, 'DM catch-up acceptance');
    const recovered = h.pushed.find((p) => p.tags?.includes('zulip:missed'))!;
    assert.deepEqual(recovered.tags, ['zulip:missed', 'chat:dm', 'chat:private']);
    assert.equal((recovered.origin as { isReplyToBot: boolean }).isReplyToBot, false);
    assert.equal((recovered.origin as { isMention: boolean }).isMention, false);
  } finally { await h.close(); }
});

test('catch-up counts mixed mentions and replies as addressed messages rather than mentions', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.pushed.length = 0;
    h.adapter.history = [
      streamMsg(4), streamMsg(5, { mentioned: true }),
      streamMsg(6, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }),
    ];
    h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
    await until(() => h.delivery.watermark('zulip:general') === 6, 'mixed catch-up acceptance');
    const recovered = h.pushed[0];
    assert.match((recovered.payload.content[0] as { text: string }).text, /count="2" lines="3" reason="addressed"/);
    assert.equal((recovered.origin as { reason: string }).reason, 'addressed');
    assert.equal((recovered.origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.equal((recovered.origin as { isMention: boolean }).isMention, true);
  } finally { await h.close(); }
});

test('a reconnect sweep gives each history page and channel its own quote-lookup allowance', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-sweep-budget-'));
  let second: Harness | undefined;
  try {
    const other = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
    const first = harness({ stateDir: dir, sessionId: 'budget' });
    try {
      await initialize(first, true);
      await settled(first);
      first.adapter.emit!(streamMsg(1, { mentioned: true }));
      first.adapter.emit!(streamMsg(2, { channelId: other.id, mentioned: true }), other);
      await until(() => first.delivery.watermark(other.id) === 2, 'both initial channels accepted');
      await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
      await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
      first.adapter.emit!(streamMsg(3));
      await until(() => first.delivery.tally('zulip:general')?.talliedThrough === 3, 'ambient tally seeded');
    } finally { await first.close(); }
    let gets = 0;
    const queries: ChannelHistoryQuery[] = [];
    const history = new ZulipAdapter(standInApi({
      'GET /messages': async (q: { narrow: unknown[]; anchor: number; num_after: number }) => {
        const stream = narrowedStream(q.narrow);
        const offset = stream === 'general' ? 0 : 20;
        const rows = Array.from({ length: 12 }, (_, i): ZulipRawMessage => ({
          id: offset + 5 + i, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
          display_recipient: stream, subject: 'topic', timestamp: 1_700_000_000, type: 'stream', flags: [],
          content: `[said](/#narrow/channel/7/near/${500 + offset + i}):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`,
        })).filter((m) => m.id > q.anchor);
        return { messages: rows.slice(0, 3), found_newest: rows.length <= 3 };
      },
      'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
        gets++;
        return { message: {
          id: message_id, sender_id: 102, sender_full_name: 'Other', sender_email: 'other@example.invalid',
          display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000, type: 'stream', content: 'original',
        } };
      },
    }).api, 100, 'session', { directory: historyStreams() });
    second = harness({ stateDir: dir, sessionId: 'budget' });
    second.adapter.visible.push(other);
    second.adapter.fetchHistory = (channelId, query) => {
      queries.push(query);
      return history.fetchHistory(channelId, query);
    };
    await initialize(second, true);
    await settled(second);
    assert.equal(gets, 24, 'all off-page targets in both channels are verified');
    assert.equal(queries.length, 12, 'four pages per channel, then four missed-tally backfill pages');
    assert.ok(queries.every((q) => !('lookupBudget' in q)));
    assert.equal(second.delivery.watermark('zulip:general'), 16);
    assert.equal(second.delivery.watermark(other.id), 36);
  } finally {
    await second?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the reconnect catch-up and the missed-tally backfill wait 90 s for each history page, and channels/open\'s page keeps the default', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-recovery-bound-'));
  let second: Harness | undefined;
  try {
    const first = harness({ stateDir: dir, sessionId: 'bound' });
    try {
      await initialize(first, true);
      await settled(first);
      first.adapter.emit!(streamMsg(1, { mentioned: true }));
      await until(() => first.delivery.watermark('zulip:general') === 1, 'the addressed message accepted');
      await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
      await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
      first.adapter.emit!(streamMsg(2));
      await until(() => first.delivery.tally('zulip:general')?.talliedThrough === 2, 'ambient tally seeded');
    } finally { await first.close(); }
    const bounds: (number | undefined)[] = [];
    const { api } = standInApi({ 'GET /messages': { result: 'success', messages: [], found_newest: true, found_oldest: true } });
    const history = new ZulipAdapter({
      ...api,
      request: ((verb, path, what, params, opts) => {
        if (path === '/messages') bounds.push(opts?.timeoutMs);
        return api.request(verb, path, what, params, opts);
      }) as typeof api.request,
    }, 100, 'session', { directory: historyStreams() });
    second = harness({ stateDir: dir, sessionId: 'bound' });
    second.adapter.fetchHistory = (channelId, query) => history.fetchHistory(channelId, query);
    await initialize(second, true);
    await settled(second);
    assert.deepEqual(bounds, [90_000, 90_000], 'one catch-up page, then one backfill page');
    await second.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 5 } });
    assert.deepEqual(bounds, [90_000, 90_000, undefined], 'the page for channels/open takes the default bound');
  } finally {
    await second?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a reply the event path verified, refused by the host, comes again as a reply when the lookup in its history fails', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(3, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 3, 'acceptance above the quoted message, so a read from here leaves it off the page');
    const reply = (await quoteHistory().fetchHistory(streamKey(7), { limit: 100 })).messages[0];
    const unreadable = quoteHistory(false);
    h.adapter.fetchHistory = unreadable.fetchHistory.bind(unreadable);
    h.policy.rejectPushes.add('5');
    h.adapter.emit!(reply);
    await until(() => h.delivery.heldIds('zulip:general', { replayable: true }).includes(5), 'refused reply held');
    h.policy.rejectPushes.delete('5');
    h.pushed.length = 0;
    const other = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
    h.adapter.emit!(streamMsg(6, { channelId: other.id, mentioned: true }), other);
    await h.server.quiet();
    assert.equal(h.delivery.watermark('zulip:general'), 5);
    const again = h.pushed.find((p) => (p.origin as { mcplChannelId?: string }).mcplChannelId === 'zulip:general')!;
    assert.ok(again, 'offered again');
    assert.ok(again.tags?.includes('chat:reply'));
    assert.equal((again.origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.match((again.payload.content[0] as { text: string }).text, /id=5\]/);
  } finally { await h.close(); }
});

test('a reply the event path verified, refused by the host, comes again as a reply after a reconnect when the lookup in its history fails', async () => {
  const first = harness();
  let second: Harness | undefined;
  try {
    await initialize(first, true);
    await settled(first);
    first.adapter.emit!(streamMsg(3, { mentioned: true }));
    await until(() => first.delivery.watermark('zulip:general') === 3, 'acceptance above the quoted message, so a read from here leaves it off the page');
    const reply = (await quoteHistory().fetchHistory(streamKey(7), { limit: 100 })).messages[0];
    first.policy.rejectPushes.add('5');
    first.adapter.emit!(reply);
    await until(() => first.delivery.heldIds('zulip:general').includes(5), 'refused reply held');
    await first.server.quiet();
    await first.close();

    const unreadable = quoteHistory(false);
    first.adapter.fetchHistory = unreadable.fetchHistory.bind(unreadable);
    second = harness({ reuse: { server: first.server, adapter: first.adapter } });
    await initialize(second, true);
    await settled(second);
    assert.equal(second.pushed.length, 1, 'offered again');
    assert.ok(second.pushed[0].tags?.includes('chat:reply'));
    assert.equal((second.pushed[0].origin as { isReplyToBot: boolean }).isReplyToBot, true);
    assert.match((second.pushed[0].payload.content[0] as { text: string }).text, /id=5\]/);
    assert.equal(second.delivery.watermark('zulip:general'), 5);
  } finally { await (second ?? first).close(); }
});

for (const reconnect of [false, true]) {
  test(`an accepted catch-up block survives ${reconnect ? 'reconnecting' : 'shutdown'} while its channel's catch-up is still running`, async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-probe-staged-'));
  let second: Harness | undefined;
  let third: Harness | undefined;
  let release!: () => void;
  try {
    const first = harness({ stateDir: dir, sessionId: 'probe' });
    try {
      await initialize(first, true);
      await settled(first);
      first.adapter.emit!(streamMsg(1, { mentioned: true }));
      await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
      await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
      await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
      first.adapter.emit!(streamMsg(2));
      await until(() => first.delivery.tally('zulip:general')?.talliedThrough === 2, 'tally seeded');
    } finally { await first.close(); }

    second = harness({ stateDir: dir, sessionId: 'probe', history: [streamMsg(5, { mentioned: true })], shutdownGraceMs: 20 });
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    let calls = 0;
    const gate = new Promise<void>((resolve) => { release = resolve; });
    second.adapter.fetchHistory = async (channelId, query) => {
      if (++calls === 2) await gate;
      return realFetch(channelId, query);
    };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    await until(() => second!.pushed.some((p) => p.tags?.includes('zulip:missed')) && calls === 2, 'catch-up block pushed, the tally backfill still reading');
    await second.host.sendRequest('ping');

    if (reconnect) {
      await second.close();
      second.adapter.fetchHistory = realFetch;
      third = harness({ reuse: { server: second.server, adapter: second.adapter } });
      await initialize(third, true);
      await settled(third);
      assert.equal(third.pushed.length, 0, 'accepted catch-up is not offered again');
      assert.equal(third.delivery.watermark('zulip:general'), 5);
    } else {
      await second.server.shutdown();
    }
    const persisted = JSON.parse(readFileSync(join(dir, 'probe.delivery.json'), 'utf8')) as { watermarks: Record<string, number> };
    assert.equal(persisted.watermarks['zulip:general'], 5,
      `host accepted the catch-up through id 5 but the persisted watermark is ${persisted.watermarks['zulip:general']}: the next start re-delivers it`);
  } finally {
    release?.();
    await third?.close();
    await second?.close().catch(() => {});
    rmSync(dir, { recursive: true, force: true });
  }
});
}

test('a budget-starved quote row is retried before catch-up advances past it', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.pushed.length = 0;
    let gets = 0;
    const rows: ZulipRawMessage[] = Array.from({ length: 9 }, (_, i) => ({
      id: i + 2, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
      display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000, type: 'stream',
      content: `[said](/#narrow/near/${500 + i}):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`, flags: [],
    }));
    const history = new ZulipAdapter(standInApi({
      'GET /messages': async ({ anchor }: { anchor: number }) => ({ messages: rows.filter((m) => m.id > anchor), found_newest: true }),
      'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
        gets++;
        return { message: { ...rows[0], id: message_id, sender_id: message_id === 508 ? 100 : 102 } };
      },
    }).api, 100, 'session', { directory: historyStreams() });
    h.adapter.fetchHistory = history.fetchHistory.bind(history);
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.delivery.watermark('zulip:general') === 10, 'immediate retry verifies and delivers row 10 without another gap');
    assert.equal(gets, 9);
    assert.ok(h.pushed[0].tags?.includes('chat:reply'));
  } finally { await h.close(); }
});

test('the same unverified row defers a channel only once per catch-up call', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    let pages = 0;
    h.adapter.fetchHistory = async () => {
      pages++;
      return { messages: [streamMsg(5)], scannedThrough: 5, reachedNewest: true, unverifiedMessageIds: ['5'] };
    };
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.delivery.watermark('zulip:general') === 5, 'immediate repeat verdict fails closed without pinning');
    assert.equal(pages, 2, 'one extra history read for the deferred row');
  } finally { await h.close(); }
});

for (const verified of [false, true]) {
  test(`a pending reply is ${verified ? 'cleared by a verified quote-removing edit' : 'preserved by an unverified intact-quote edit'}`, async () => {
    const h = harness();
    try {
      await initialize(h, true);
      await settled(h);
      h.policy.rejectPushes.add('5');
      h.adapter.emit!(streamMsg(5, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
      await until(() => h.delivery.heldIds('zulip:general', { replayable: true }).includes(5), 'reply refused and held');
      h.adapter.change!({
        kind: 'edit', channelId: 'zulip:general', messageId: '5', messageIds: ['5'],
        authorId: '101', authorName: 'User', authorEmail: 'user@example.invalid', actorId: '102',
        topic: 'topic', previousTopic: null, movedToChannelId: null, content: verified ? 'quote removed' : '[said](/#narrow/near/200):\n```quote\noriginal\n```\ncorrected',
        previousContent: 'reply', mentioned: false, previouslyMentioned: false,
        replyToSelf: false, replyToSelfVerified: verified, previouslyReplyToSelf: true,
        vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(),
      });
      h.adapter.history = [streamMsg(5)]; // History also cannot verify a quote.
      h.policy.rejectPushes.delete('5');
      const other = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
      h.adapter.emit!(streamMsg(6, { channelId: other.id, mentioned: true }), other);
      await until(() => h.delivery.watermark('zulip:general') === 5, 'history retry completed');
      const recovered = h.pushed.filter((p) => p.tags?.includes('zulip:missed'));
      assert.equal(recovered.length, verified ? 0 : 1);
      if (!verified) assert.ok(recovered[0].tags?.includes('chat:reply'));
    } finally { await h.close(); }
  });
}

test("a reply verdict goes with its hold when a mute lets the hold go: after the unmute, a reconnect's catch-up reads the message as history has it", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-pending-release-'));
  const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
  plane.start();
  const h = harness({ filters: plane });
  let second: Harness | undefined;
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.policy.rejectPushes.add('5');
    h.adapter.emit!(streamMsg(5, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
    await until(() => h.delivery.heldIds('zulip:general').includes(5), 'reply held');
    assert.equal(plane.update((f) => ({ ...f, mutedStreams: ['general'] })).ok, true);
    await until(() => h.delivery.heldIds('zulip:general').length === 0, 'muted channel released');
    assert.equal(h.delivery.verifiedReply('zulip:general', 5), false);
    assert.equal(plane.update((f) => ({ ...f, mutedStreams: [] })).ok, true);
    await h.server.quiet();
    await h.close();

    h.adapter.history = [streamMsg(5)];
    second = harness({ reuse: { server: h.server, adapter: h.adapter } });
    await initialize(second, true);
    await settled(second);
    assert.equal(second.pushed.length, 0, 'history reads 5 as ambient, and nothing makes it a reply');
    assert.equal(second.delivery.watermark('zulip:general'), 5);
  } finally {
    await (second ?? h).close();
    plane.stop();
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a reply missing from replay history releases both its hold and pending verdict', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    h.adapter.emit!(streamMsg(1));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.policy.rejectIds.add('5');
    h.adapter.emit!(streamMsg(5, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
    await until(() => h.delivery.heldIds('zulip:general').includes(5), 'reply held');
    h.adapter.history = [];
    h.adapter.emit!(streamMsg(6));
    await until(() => h.delivery.watermark('zulip:general') === 6, 'missing reply released');
    assert.deepEqual(h.delivery.heldIds('zulip:general'), []);
    assert.equal(h.delivery.verifiedReply('zulip:general', 5), false);
  } finally { await h.close(); }
});

test('a starved first row is retried within the sweep so its addressed tail is delivered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-probe-defer-'));
  let second: Harness | undefined;
  try {
    const first = harness({ stateDir: dir, sessionId: 'probe' });
    try {
      await initialize(first, true);
      await settled(first);
      first.adapter.emit!(streamMsg(1, { mentioned: true }));
      await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    } finally { await first.close(); }
    // Offline backlog: id 2 is a quote row whose target lookup was starved,
    // id 3 is an ordinary mention the host must see.
    second = harness({ stateDir: dir, sessionId: 'probe' });
    let pages = 0;
    second.adapter.fetchHistory = async () => {
      pages++;
      return {
        messages: [streamMsg(2), streamMsg(3, { mentioned: true }), ...(pages > 1 ? [streamMsg(4, { mentioned: true })] : [])],
        scannedThrough: pages > 1 ? 4 : 3, reachedNewest: true, unverifiedMessageIds: ['2'],
      };
    };
    await initialize(second, true);
    await settled(second);
    await until(() => second!.delivery.watermark('zulip:general') === 3, 'the addressed backlog is delivered without another gap or reconnect');
    assert.equal(pages, 2, 'exactly one immediate retry');
    assert.doesNotMatch(JSON.stringify(second.pushed), /id=4\]/, 'retry stays within the original catch-up window');
    assert.ok(second.pushed.some((p) => p.tags?.includes('zulip:missed')),
      `the mention at id 3 was never offered on this connection (watermark ${second.delivery.watermark('zulip:general')}, ${pages} history page(s) fetched)`);
  } finally {
    await second?.close().catch(() => {});
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a deferred partial block marks its withheld tail in text and push origin', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.pushed.length = 0;
    h.adapter.fetchHistory = async () => ({
      messages: [streamMsg(2, { mentioned: true }), streamMsg(3), streamMsg(4, { mentioned: true })],
      scannedThrough: 4, reachedNewest: true, unverifiedMessageIds: ['3'],
    });
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.pushed.some((p) => p.tags?.includes('zulip:missed')), 'missed block pushed');
    const block = h.pushed.find((p) => p.tags?.includes('zulip:missed'))!;
    const text = (block.payload.content[0] as { text: string }).text;
    assert.match(text, /truncated="true"/);
    assert.match(text, /newer messages exist/);
    assert.match(text, /after=2/);
    assert.equal((block.origin as { truncated: boolean }).truncated, true);
    await until(() => h.delivery.watermark('zulip:general') === 4, 'immediate retry delivers the withheld tail');
    const blocks = h.pushed.filter((p) => p.tags?.includes('zulip:missed'));
    assert.equal(blocks.length, 2);
    const tail = (blocks[1].payload.content[0] as { text: string }).text;
    assert.match(tail, /id=4\]/);
    assert.doesNotMatch(tail, /id=2\]/, 'accepted prefix is not delivered twice');
  } finally { await h.close(); }
});


test('R2-2: a timed-out quote target can answer on the single deferred pass', { timeout: 8000 }, async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.pushed.length = 0;
    let gets = 0;
    const row: ZulipRawMessage = {
      id: 5, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
      display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000, type: 'stream', flags: [],
      content: '[said](/#narrow/near/500):\n\n```quote\noriginal\n```\nreply',
    };
    const history = new ZulipAdapter(standInApi({
      'GET /messages': async () => ({ messages: [row], found_newest: true }),
      'GET /messages/:id': async () => {
        if (++gets === 1) return new Promise(() => {});
        return { message: { ...row, id: 500, sender_id: 100 } };
      },
    }).api, 100, 'session', { directory: historyStreams() });
    h.adapter.fetchHistory = history.fetchHistory.bind(history);
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.delivery.watermark('zulip:general') === 5, 'timeout retried', 7000);
    assert.equal(gets, 2, 'timeouts must not enter the definite-failure negative cache');
    assert.equal(h.pushed.length, 1);
    assert.ok(h.pushed[0].tags?.includes('chat:reply'));
  } finally { await h.close(); }
});

test('R2-2: many hung quote targets use at most two walks and six seconds of waiting', { timeout: 10000 }, async () => {
  const h = harness();
  let cleaningUp = false;
  const release: (() => void)[] = [];
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.pushed.length = 0;
    let walks = 0;
    let gets = 0;
    const rows: ZulipRawMessage[] = Array.from({ length: 100 }, (_, i) => ({
      id: i + 2, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
      display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000, type: 'stream', flags: [],
      content: `[said](/#narrow/near/${500 + i}):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`,
    }));
    const history = new ZulipAdapter(standInApi({
      'GET /messages': async ({ anchor }: { anchor: number }) => {
        walks++;
        return { messages: rows.filter((m) => m.id > anchor), found_newest: true };
      },
      'GET /messages/:id': async () => {
        gets++;
        if (cleaningUp) return { result: 'error' };
        return new Promise((resolve) => release.push(() => resolve({ result: 'error' })));
      },
    }).api, 100, 'session', { directory: historyStreams() });
    h.adapter.fetchHistory = history.fetchHistory.bind(history);
    const started = performance.now();
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.delivery.watermark('zulip:general') === 101, 'bounded fail-closed catch-up', 6500);
    assert.ok(performance.now() - started < 6500);
    assert.equal(walks, 2, 'one initial walk and one retry for ALL deferred rows');
    assert.ok(gets >= 2 && gets <= 16, 'at most eight GETs per page on each of two passes');
    assert.equal(h.pushed.length, 0);
  } finally {
    cleaningUp = true;
    for (const done of release) done();
    await until(() => h.delivery.watermark('zulip:general') === 101, 'cleanup finishes the sweep');
    await h.close();
  }
});


test('R2-2: catch-up shares a total quote wait budget across history pages', async (t) => {
  let now = 0;
  t.mock.method(performance, 'now', () => now);
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    let gets = 0;
    const rows: ZulipRawMessage[] = Array.from({ length: 10 }, (_, i) => ({
      id: i + 2, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
      display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000, type: 'stream', flags: [],
      content: `[said](/#narrow/near/${500 + i}):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`,
    }));
    const history = new ZulipAdapter(standInApi({
      'GET /messages': async ({ anchor }: { anchor: number }) => {
        const tail = rows.filter((m) => m.id > anchor);
        return { messages: tail.slice(0, 1), found_newest: tail.length <= 1 };
      },
      'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
        gets++;
        now += 1500;
        return { message: { ...rows[0], id: message_id, sender_id: 102 } };
      },
    }).api, 100, 'session', { directory: historyStreams() });
    h.adapter.fetchHistory = history.fetchHistory.bind(history);
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.delivery.watermark('zulip:general') === 11, 'all pages scanned with bounded quote waiting');
    assert.equal(gets, 4, 'two three-second pass budgets, not a fresh total budget per page');
    assert.equal(now, 6000);
  } finally { await h.close(); }
});


test('R2-3: push origins expose umbrella mention and specific addressing flags', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    for (const [i, [mentioned, replyToSelf]] of [[true, false], [false, true]].entries()) {
      h.adapter.emit!(streamMsg(5 + i, {
        metadata: { mentioned, replyToSelf, isDM: false },
        tags: [...(mentioned ? ['chat:mention'] : []), ...(replyToSelf ? ['chat:reply'] : [])],
      }));
      await until(() => h.pushed.length === i + 1, 'addressed push');
      const origin = h.pushed[i].origin as Record<string, unknown>;
      assert.equal(origin.isExplicitMention, mentioned);
      assert.equal(origin.isReplyToBot, replyToSelf);
      assert.equal(origin.isMention, mentioned || replyToSelf);
      assert.equal('isReply' in origin, false, 'do not publish the ambiguous reply-to-anything name');
    }
  } finally { await h.close(); }
});


test('R2-7: closed-channel catch-up forgets a held reply missing from history', async () => {
  const h = harness();
  try {
    await initialize(h, true);
    await settled(h);
    h.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    h.policy.rejectPushes.add('5');
    h.adapter.emit!(streamMsg(5, {
      metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'],
    }));
    await until(() => h.delivery.heldIds('zulip:general').includes(5), 'reply held');
    await h.server.quiet();
    assert.equal(h.delivery.verifiedReply('zulip:general', 5), true);
    h.adapter.history = [streamMsg(6)];
    h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
    await until(() => h.delivery.watermark('zulip:general') === 6, 'catch-up releases missing reply hold');
    assert.deepEqual(h.delivery.heldIds('zulip:general'), []);
    assert.equal(h.delivery.verifiedReply('zulip:general', 5), false);
  } finally { await h.close(); }
});

test('R3-1: a reply emitted before featureSets/update is delivered', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-r3-buffer-'));
  let second: Harness | undefined;
  try {
    const first = harness({ stateDir: dir, sessionId: 'r3' });
    await initialize(first, true);
    await settled(first);
    first.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    first.adapter.emit!(streamMsg(2));
    await until(() => first.delivery.tally('zulip:general')?.talliedThrough === 2, 'seed tally backfill');
    await first.close();
    second = harness({ stateDir: dir, sessionId: 'r3', history: [streamMsg(5)] });
    await initialize(second, true);
    await until(() => second!.adapter.emit !== null, 'events started before grant');
    second.adapter.emit!(streamMsg(5, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second);
    assert.equal(second.pushed.length, 1, 'the pre-grant reply must not be deduplicated as ambient');
    assert.ok(second.pushed[0].tags?.includes('chat:reply'));
  } finally {
    await second?.close();
    rmSync(dir, { recursive: true, force: true });
  }
});

for (const slowPart of ['history HTTP', 'prefix acceptance'] as const) {
  test(`R3-3: slow ${slowPart} does not spend the quote-GET wait allowance`, async (t) => {
    let now = 0;
    t.mock.method(performance, 'now', () => now);
    const h = harness();
    try {
      await initialize(h, true);
      await settled(h);
      h.adapter.emit!(streamMsg(1, { mentioned: true }));
      await until(() => h.delivery.watermark('zulip:general') === 1, 'seed watermark');
      h.pushed.length = 0;
      let gets = 0;
      const row: ZulipRawMessage = {
        id: 3, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
        display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000, type: 'stream', flags: [],
        content: '[said](/#narrow/near/500):\n\n```quote\noriginal\n```\nreply',
      };
      const history = new ZulipAdapter(standInApi({
        'GET /messages': async ({ anchor }: { anchor: number }) => {
          if (slowPart === 'history HTTP') now += 3100;
          const rows = slowPart === 'prefix acceptance'
            ? [{ ...row, id: 2, content: 'explicit mention', flags: ['mentioned'] }, row] : [row];
          return { messages: rows.filter((m) => m.id > anchor), found_newest: true };
        },
        'GET /messages/:id': async () => {
          if (++gets === 1 && slowPart === 'prefix acceptance') return { result: 'error', code: 'RATE_LIMIT_HIT' };
          return { message: { ...row, id: 500, sender_id: 100 } };
        },
      }).api, 100, 'session', { directory: historyStreams() });
      h.adapter.fetchHistory = history.fetchHistory.bind(history);
      const respond = h.host.sendResponse.bind(h.host);
      t.mock.method(h.host, 'sendResponse', (...args: Parameters<typeof h.host.sendResponse>) => {
        if (slowPart === 'prefix acceptance' && (args[1] as { accepted?: boolean })?.accepted) now += 7000;
        return respond(...args);
      });
      h.adapter.systemEvent!({ kind: 'gap', text: 'Gap.', metadata: {} });
      await until(() => h.delivery.watermark('zulip:general') === 3, 'catch-up complete');
      assert.equal(gets, slowPart === 'history HTTP' ? 1 : 2, 'unrelated waits must not starve quote verification');
      assert.ok(h.pushed.some((p) => p.tags?.includes('chat:reply')), 'verified quote reaches the host');
    } finally { await h.close(); }
  });
}

test('R3-1 follow-up: a message whose delivery job throws stays held, and the next start offers it', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-r3-throw-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'r3t' });
    await initialize(first, true);
    await settled(first);
    first.adapter.emit!(streamMsg(1, { mentioned: true }));
    await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
    const internal = first.server as unknown as { attributed(message: IncomingChannelMessage): IncomingChannelMessage };
    const original = internal.attributed.bind(internal);
    internal.attributed = (message) => {
      if (message.messageId === '5') throw new Error('injected attribution failure');
      return original(message);
    };
    first.adapter.emit!(streamMsg(5, { mentioned: true }));
    await first.server.quiet();
    assert.deepEqual(first.delivery.heldIds('zulip:general'), [5], 'its hold stays');
    assert.equal(first.pushed.length, 1);
    await first.close();
    const persisted = JSON.parse(readFileSync(join(dir, 'r3t.delivery.json'), 'utf8'));
    assert.ok(persisted.watermarks['zulip:general'] < 5, 'the watermark on disk stays below it');
    const restarted = harness({ stateDir: dir, sessionId: 'r3t', history: [streamMsg(5, { mentioned: true })] });
    try {
      await initialize(restarted, true);
      await settled(restarted);
      assert.equal(restarted.pushed.length, 1, 'the next start offers it');
      assert.match((restarted.pushed[0].payload.content[0] as { text: string }).text, /id=5\]/);
      assert.equal(restarted.delivery.watermark('zulip:general'), 5);
    } finally { await restarted.close(); }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

for (const mode of ['disable-during-heal', 'no-duplicate', 'host-refusal', 'request-error'] as const) {
  test(`R3-1 healing: ${mode}`, async (t) => {
    const second = harness({ history: [streamMsg(5)] });
    let releaseHeal = () => {};
    try {
      await initialize(second, true);
      await settled(second);
      second.adapter.emit!(streamMsg(1, { mentioned: true }));
      await until(() => second.delivery.watermark('zulip:general') === 1, 'initial acceptance');
      await second.host.sendRequest(method.FEATURE_SETS_UPDATE, {
        effectiveCapabilities: FULL_GRANT, disabled: ['zulip.messaging'],
      });
      second.adapter.emit!(streamMsg(5, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
      await second.server.quiet();

      assert.equal(second.delivery.watermark('zulip:general'), 1);
      assert.ok(second.delivery.heldIds('zulip:general', { replayable: true }).includes(5));
      let healReads = 0;
      const healGate = new Promise<void>((r) => { releaseHeal = r; });
      const originalHistory = second.adapter.fetchHistory!.bind(second.adapter);
      second.adapter.fetchHistory = async (channel, query) => {
        if (second!.adapter.wireOf(channel) === 'zulip:general') {
          healReads++;
          await healGate;
        }
        return originalHistory(channel, query);
      };
      if (mode === 'host-refusal') second.policy.rejectPushes.add('5');
      if (mode === 'request-error') {
        const respond = second.host.sendResponse.bind(second.host);
        t.mock.method(second.host, 'sendResponse', (...args: Parameters<typeof second.host.sendResponse>) => {
          if ((args[1] as { accepted?: boolean })?.accepted === true) {
            second!.host.sendError(args[0], -32603, 'replay outcome unknown');
            return;
          }
          return respond(...args);
        });
      }
      await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
      await until(() => healReads === 1, 'healing awaits history');
      // Repeated policy requests while history is pending must not start
      // overlapping heals, even though the hold has not been offered yet.
      for (let i = 0; i < 3; i++) {
        await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
      }
      assert.equal(healReads, 1, 'one in-flight healing reservation');
      if (mode === 'disable-during-heal') {
        await second.host.sendRequest(method.FEATURE_SETS_UPDATE, {
          effectiveCapabilities: FULL_GRANT, disabled: ['zulip.messaging'],
        });
      }
      releaseHeal();
      await second.server.quiet();
      if (mode === 'disable-during-heal') {
        assert.equal(second.pushed.filter((p) => p.tags?.includes('chat:reply')).length, 0, 'local policy refusal sends nothing');
        assert.ok(second.delivery.heldIds('zulip:general', { replayable: true }).includes(5),
          'a never-offered reply must remain replayable');
        await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
        await second.server.quiet();
      }
      const accepted = mode === 'disable-during-heal' || mode === 'no-duplicate';
      assert.equal(second.delivery.watermark('zulip:general'), accepted ? 5 : 1);
      assert.equal(second.pushed.filter((p) => p.tags?.includes('chat:reply')).length, 1, 'exactly one actual reply offer');
      assert.deepEqual(second.delivery.heldIds('zulip:general'), accepted ? [] : [5]);
      assert.deepEqual(second.delivery.heldIds('zulip:general', { replayable: true }), []);
      for (let i = 0; i < 3; i++) {
        await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
      }
      await second.server.quiet();
      assert.equal(second.pushed.filter((p) => p.tags?.includes('chat:reply')).length, 1,
        'accepted, refused and ambiguous offers must not loop');
    } finally {
      releaseHeal();
      await second.close();
    }
  });
}

for (const outcome of ['refusal', 'RPC error'] as const) {
  for (const acceptTail of [false, true]) {
    test(`R3-1 ceiling: ${outcome}, tail ${acceptTail ? 'accepted' : 'refused'}`, async (t) => {
      const h = harness({ history: Array.from({ length: 101 }, (_, i) => streamMsg(i + 5)) });
      try {
        await initialize(h, true);
        await settled(h);
        h.adapter.emit!(streamMsg(1, { mentioned: true }));
        await until(() => h.delivery.watermark('zulip:general') === 1, 'seed acceptance');
        for (const id of ['5', '104', '105']) h.policy.rejectPushes.add(id);
        for (const id of [5, 105]) {
          h.adapter.emit!(streamMsg(id, {
            metadata: { replyToSelf: true, mentioned: false, isDM: false }, tags: ['chat:reply'],
          }));
        }
        await until(() => h.delivery.heldIds('zulip:general', { replayable: true }).length === 2, 'two refused live replies');
        h.pushed.length = 0;
        if (acceptTail) h.policy.rejectPushes.delete('105');
        if (outcome === 'RPC error') {
          const respond = h.host.sendResponse.bind(h.host);
          t.mock.method(h.host, 'sendResponse', (...args: Parameters<typeof h.host.sendResponse>) => {
            if ((args[1] as { accepted?: boolean })?.accepted === false) {
              h.host.sendError(args[0], -32603, 'unknown healing outcome');
              return;
            }
            return respond(...args);
          });
        }
        await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
        await h.server.quiet();
        assert.deepEqual(h.delivery.heldIds('zulip:general'), [5, 105], 'unaccepted prefix and uncovered tail retain ownership');
        assert.deepEqual(h.delivery.heldIds('zulip:general', { replayable: true }), [105], 'only uncovered tail stays eligible');
        for (let i = 0; i < 3; i++) {
          await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
          await h.server.quiet();
        }
        const offers = h.pushed.map((push) => (push.origin as { messageId: string }).messageId);
        assert.equal(offers.filter((id) => id === '104').length, 1, 'consumed healing prefix is never re-offered on later grants');
        assert.equal(offers.filter((id) => id === '105').length, 1, 'eligible tail gets one offer');
        assert.deepEqual(h.delivery.heldIds('zulip:general'), acceptTail ? [5] : [5, 105]);
        assert.deepEqual(h.delivery.heldIds('zulip:general', { replayable: true }), []);
        assert.ok(h.delivery.watermark('zulip:general')! < 5, 'accepting the tail must not erase the refused prefix floor');
        assert.equal(h.delivery.verifiedReply('zulip:general', 5), true, 'unaccepted prefix verdict remains owned');
        assert.equal(h.delivery.verifiedReply('zulip:general', 105), !acceptTail);
      } finally { await h.close(); }
    });
  }
}

test("a delivery still fetching its attachments when shutdown's grace runs out sends nothing when the fetch ends, and the next connection offers its message once", async () => {
  const fetches = heldFetches();
  const first = harness({ attachments: fetches.options, shutdownGraceMs: 20 });
  await initialize(first, true);
  await settled(first);
  first.adapter.emit!(streamMsg(1, { mentioned: true }));
  await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
  first.adapter.history = [streamMsg(1, { mentioned: true }), withLog(streamMsg(2, { mentioned: true }))];
  first.adapter.emit!(withLog(streamMsg(2, { mentioned: true })));
  await until(() => fetches.started.length === 1, "2's attachment fetch");
  await first.close();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  fetches.release();
  await second.host.sendRequest('ping');
  await second.server.quiet();
  const offersOf2 = second.pushed.filter((p) => p.eventId === 'zulip_msg_2' || (p.tags?.includes('zulip:missed') && /id=2\]/.test((p.payload.content[0] as { text: string }).text)));
  assert.equal(offersOf2.length, 1);
  assert.equal(second.delivery.watermark('zulip:general'), 2);
  await second.close();
});

test('a message that the adapter finds is not for the host holds nothing: the next message the host accepts moves the watermark past it', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  h.adapter.emit!(streamMsg(1, { mentioned: true }));
  await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
  h.adapter.enter!({ key: h.adapter.keyOf('zulip:general'), messageId: '2', ready: async () => null });
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  await until(() => h.delivery.watermark('zulip:general') === 3, 'the watermark past 2');
  assert.deepEqual(h.delivery.heldIds('zulip:general'), []);
  await h.close();
});

test("a message whose quote lookup outlasts its connection's shutdown sends nothing when the lookup answers, and the next connection offers it once", async () => {
  const first = harness({ shutdownGraceMs: 20 });
  await initialize(first, true);
  await settled(first);
  first.adapter.emit!(streamMsg(1, { mentioned: true }));
  await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
  first.adapter.history = [streamMsg(1, { mentioned: true }), streamMsg(2, { mentioned: true })];
  const { channelId, ...two } = streamMsg(2, { mentioned: true });
  const key = first.adapter.keyOf(channelId);
  let answer!: () => void;
  const lookup = new Promise<void>((resolve) => { answer = resolve; });
  let looking = false;
  first.adapter.enter!({ key, messageId: '2', ready: async () => { looking = true; await lookup; return { message: { ...two, key } as KeyedMessage }; } });
  await until(() => looking, "2's quote lookup");
  await first.close();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  answer();
  await second.host.sendRequest('ping');
  await second.server.quiet();
  const offersOf2 = second.pushed.filter((p) => p.eventId === 'zulip_msg_2' || (p.tags?.includes('zulip:missed') && /id=2\]/.test((p.payload.content[0] as { text: string }).text)));
  assert.equal(offersOf2.length, 1);
  assert.equal(second.delivery.watermark('zulip:general'), 2);
  await second.close();
});

test("a change still being placed when its connection's shutdown grace runs out goes to no connection, as on main", async () => {
  const first = harness({ shutdownGraceMs: 20 });
  await initialize(first, true);
  await settled(first);
  first.adapter.emit!(streamMsg(1, { mentioned: true }));
  await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
  const key = first.adapter.keyOf('zulip:general');
  let answer!: () => void;
  const lookup = new Promise<void>((resolve) => { answer = resolve; });
  let placing = false;
  first.adapter.enterChange!({ key, messageIds: [1], ready: async () => {
    placing = true;
    await lookup;
    return {
      kind: 'edit', key, messageId: '1', messageIds: ['1'], authorId: '9', authorName: 'Ann', authorEmail: 'ann@example.com',
      actorId: '9', topic: 'deploys', previousTopic: null, movedToKey: null, content: '@bot fixed', previousContent: '@bot',
      mentioned: true, previouslyMentioned: true, vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_060_000),
    };
  } });
  await until(() => placing, 'the edit being placed');
  await first.close();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  answer();
  await second.host.sendRequest('ping');
  await second.server.quiet();
  assert.deepEqual([...second.pushed.map((p) => p.eventId), ...second.incoming.map((m) => m.messageId)].filter((id) => /edit|changed/.test(String(id))), []);
  await second.close();
});

test("an open whose connection ends while it reads its page counts nothing of the page as seen, and the next connection's catch-up offers the page's rows", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-open-ends-'));
  try {
    const unread = () => [streamMsg(4, { mentioned: true }), streamMsg(5, { mentioned: true })];
    const first = harness({ stateDir: dir, sessionId: 'oe', history: unread() });
    await initialize(first, true);
    await settled(first);
    first.delivery.advance('zulip:general', 3);
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let reading = false;
    const real = first.adapter.fetchHistory!.bind(first.adapter);
    first.adapter.fetchHistory = async (key, query) => { reading = true; await gate; return real(key, query); };
    void first.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2, sinceLastSeen: true },
    }).catch(() => {});
    await until(() => reading, 'the open to read its page');
    const closing = first.close();
    await until(() => first.serverSide.isClosed, 'the server to see the connection end');
    release();
    await closing;
    assert.equal(first.delivery.watermark('zulip:general'), 3);

    const second = harness({ stateDir: dir, sessionId: 'oe', history: unread() });
    await initialize(second, true);
    await settled(second);
    const blocks = generalBlock(second);
    assert.equal(blocks.length, 1);
    for (const id of [4, 5]) assert.match(blocks[0], new RegExp(`id=${id}\\]`));
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a catch-up block the host refuses at a connection's start isn't offered again before the host accepts something", async () => {
  const first = harness();
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.adapter.emit!(streamMsg(1));
  await until(() => first.delivery.watermark('zulip:general') === 1, 'initial acceptance');
  await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
  first.policy.rejectPushes.add('2');
  first.adapter.emit!(streamMsg(2, { mentioned: true }));
  await first.server.quiet();
  assert.ok(first.delivery.heldIds('zulip:general', { replayable: true }).includes(2), 'refused, and replayable');
  await first.close();

  first.adapter.history = [streamMsg(1), streamMsg(2, { mentioned: true })];
  const real = first.adapter.fetchHistory!.bind(first.adapter);
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  let calls = 0;
  first.adapter.fetchHistory = async (key, query) => {
    if (++calls === 2) await gate;
    return real(key, query);
  };
  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  second.policy.rejectPushes.add('2');
  await initialize(second, true);
  await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await until(() => calls === 2 && second.pushed.length === 1, "the catch-up block refused, the tally's backfill reading");
  await second.host.sendRequest('ping');
  release();
  await second.server.quiet();
  const offersOf2 = second.pushed.filter((p) => /id=2\]/.test((p.payload.content[0] as { text: string }).text));
  assert.equal(offersOf2.length, 1, 'the catch-up block, once');
  assert.equal(second.delivery.watermark('zulip:general'), 1);
  await second.close();
});

test("on a closed channel, an ambient message that waits while the tally's backfill reads past it is counted as missed once", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-tally-once-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'tb' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(1));
    await until(() => first.delivery.watermark('zulip:general') === 1, 'delivery');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    await first.close();

    const second = harness({ stateDir: dir, sessionId: 'tb', history: [streamMsg(2)] });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let calls = 0;
    const real = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (key, query) => {
      if (++calls === 2) await gate;
      return real(key, query);
    };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await until(() => calls === 2, "the tally's backfill to be reading");
    second.adapter.history.push(streamMsg(3));
    second.adapter.emit!(streamMsg(3));
    release();
    await second.server.quiet();
    assert.equal(second.delivery.tally('zulip:general')?.messages, 2, '2 and 3, each once');
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

for (const outcome of ['empty', 'error', 'ambient'] as const) {
  test(`R4-3: closed heal consumes one history attempt on ${outcome}`, async () => {
    const h = harness();
    try {
      await initialize(h, true);
      await settled(h);
      h.policy.rejectPushes.add('5');
      h.adapter.emit!(streamMsg(5, { metadata: { mentioned: false, replyToSelf: true, isDM: false }, tags: ['chat:reply'] }));
      await until(() => h.delivery.heldIds('zulip:general').includes(5), 'refused reply');
      let reads = 0;
      h.adapter.fetchHistory = async () => {
        reads++;
        if (outcome === 'error') throw new Error('history unavailable');
        return { messages: outcome === 'ambient' ? [streamMsg(4)] : [], scannedThrough: outcome === 'ambient' ? 4 : null, reachedNewest: true };
      };
      for (let i = 0; i < 5; i++) {
        await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
        await h.server.quiet();
      }
      assert.equal(reads, 1);
    } finally { await h.close(); }
  });
}

for (const kind of ['edit', 'move', 'delete'] as const) {
  test(`R4-5: open-channel ${kind} exposes address flags`, async () => {
    const h = harness();
    try {
      await initialize(h, true);
      await settled(h);
      await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
      h.adapter.change!({
        kind, channelId: 'zulip:general', messageId: '10', messageIds: ['10'],
        authorId: '101', authorName: 'User', authorEmail: 'user@example.invalid', actorId: '101',
        topic: 'answers', previousTopic: null, movedToChannelId: null,
        content: 'corrected reply', previousContent: 'reply', replyToSelf: true,
        previouslyReplyToSelf: true, mentioned: false, previouslyMentioned: false,
        vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(),
      });
      await until(() => h.incoming.some((m) => m.metadata?.change === kind), 'change line');
      const metadata = h.incoming.find((m) => m.metadata?.change === kind)!.metadata;
      assert.equal(metadata?.isMention, true);
      assert.equal(metadata?.isExplicitMention, false);
      assert.equal(metadata?.isReplyToBot, true);
    } finally { await h.close(); }
  });
}


test("two streams renamed in one pass: the job running on the first stream's lane hands its message under the old id before the line, and one channels/changed names both", async () => {
  const fetches = heldFetches();
  const OTHER: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
  const h = harness({ attachments: fetches.options, visible: [DESCRIPTOR, OTHER] });
  await initialize(h, true);
  await settled(h);
  for (const channelId of ['zulip:general', 'zulip:other']) await h.host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {} });
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, "1's attachment fetch");
  h.adapter.visible = [
    { ...DESCRIPTOR, id: 'zulip:lobby', label: '#lobby', address: { stream_name: 'lobby', stream_id: 7 } },
    { ...OTHER, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } },
  ];
  h.server.directory.registration([{ stream_id: 7, name: 'lobby' }, { stream_id: 8, name: 'dev' }]);
  const isLine = (m: IncomingChannelMessage) => (m.metadata as { kind?: string } | undefined)?.kind === 'stream-renamed';
  await until(() => h.incoming.some((m) => m.channelId === 'zulip:other' && isLine(m)), "the idle stream's line");
  assert.equal(h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED).length, 0, 'no announcement while the first stream\'s job runs');
  fetches.release();
  await h.server.quiet();
  assert.deepEqual(h.incoming.filter((m) => m.channelId === 'zulip:general').map((m) => (isLine(m) ? 'line' : m.messageId)), ['1', 'line']);
  const changed = h.hostSaw.filter((r) => r.method === method.CHANNELS_CHANGED);
  assert.equal(changed.length, 1);
  assert.deepEqual([...((changed[0].params as { removed?: string[] }).removed ?? [])].sort(), ['zulip:general', 'zulip:other']);
  const lineBatch = h.hostSaw.findIndex((r) => r.method === method.CHANNELS_INCOMING &&
    (r.params as ChannelsIncomingParams).messages.some((m) => m.channelId === 'zulip:general' && isLine(m)));
  assert.ok(lineBatch >= 0 && lineBatch < h.hostSaw.indexOf(changed[0]), 'the line reaches the host before the announcement');
  await h.close();
});

const offeredIds = (h: Harness) => h.pushed.flatMap((p) => p.tags?.includes('zulip:missed')
  ? [...(p.payload.content[0] as { text: string }).text.matchAll(/id=(\d+)\]/g)].map((m) => m[1])
  : [(p.origin as { messageId: string }).messageId]);

test('messages past the cap that wait while the registration is unanswered each reach the host once, in order', async () => {
  const h = harness({ maxWaitingMessages: 3 });
  h.policy.holdRegister = true;
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await until(() => h.policy.heldRegister.length === 1 && h.adapter.emit !== null, 'the registration to be with the host');
  const all = [1, 2, 3, 4, 5].map((id) => streamMsg(id, { mentioned: true }));
  h.adapter.history = all;
  for (const m of all) h.adapter.emit!(m);
  await h.host.sendRequest('ping');
  assert.equal(h.pushed.length, 0);
  h.policy.heldRegister.shift()!();
  await h.server.quiet();
  assert.deepEqual(offeredIds(h), ['1', '2', '3', '4', '5']);
  await h.close();
});

test('past the cap, a lane gives its waiting messages up, and they reach the host by a read, in order and marked recovered, before a message that arrived after them', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options, maxWaitingMessages: 3 });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.history = [1, 2, 3, 4, 5, 6].map((id) => streamMsg(id));
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, "1's attachment fetch");
  for (const id of [2, 3, 4, 5, 6]) h.adapter.emit!(streamMsg(id));
  fetches.release();
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '2', '3', '4', '5', '6']);
  for (const m of h.incoming) {
    assert.equal((m.metadata as { recovered?: boolean }).recovered === true, ['2', '3', '4', '5'].includes(m.messageId), m.messageId);
  }
  assert.equal(h.delivery.watermark('zulip:general'), 6);
  await h.close();
});

test('a lane that gives up more rows than the catch-up ceiling, twice before its read runs, reads them in passes of at most the ceiling, with another lane read between two passes', async () => {
  const fetches = heldFetches();
  const OTHER: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
  const h = harness({ attachments: fetches.options, maxWaitingMessages: 60, visible: [DESCRIPTOR, OTHER] });
  await initialize(h, true);
  await settled(h);
  for (const channelId of ['zulip:general', 'zulip:other']) await h.host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(500, { channelId: 'zulip:other' }));
  await until(() => h.delivery.watermark('zulip:other') === 500, "#other's watermark, so a break reads it");
  const ids = Array.from({ length: 124 }, (_, i) => i + 1);
  h.adapter.history = ids.map((id) => streamMsg(id));
  const real = h.adapter.fetchHistory!.bind(h.adapter);
  let broke = false;
  h.adapter.fetchHistory = async (key, query) => {
    if (!broke && h.adapter.wireOf(key) === 'zulip:general' && query.afterMessageId === '1') {
      broke = true;
      breakQueue(h);
    }
    return real(key, query);
  };
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, "1's attachment fetch");
  for (const id of ids.slice(1)) h.adapter.emit!(streamMsg(id));
  fetches.release();
  await h.server.quiet();
  assert.deepEqual(h.incoming.filter((m) => m.channelId === 'zulip:general' && !m.messageId.startsWith('system:')).map((m) => Number(m.messageId)), ids, 'each once, in order');
  const reads = h.adapter.historyCalls.map((c, i) => ({ ...c, i }));
  const general = reads.filter((c) => c.channelId === 'zulip:general');
  assert.ok(general.every((c) => c.query.limit <= 100), 'no pass asks for more than the ceiling');
  const first = general.find((c) => c.query.afterMessageId === '1')!;
  const second = general.find((c) => c.query.afterMessageId === '101')!;
  assert.ok(first && second, 'a second pass from where the first stopped');
  const other = reads.find((c) => c.channelId === 'zulip:other')!;
  assert.ok(other && other.i > first.i && other.i < second.i, "another lane's read runs between two passes");
  await h.close();
});

test('on a closed channel, a lane that gives up more rows than the ceiling at once pushes an addressed row past it in a later pass\'s block, and no block of that read says the ceiling was reached', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options, maxWaitingMessages: 110 });
  await initialize(h, true);
  await settled(h);
  h.adapter.emit!(streamMsg(1, { mentioned: true }));
  await until(() => h.delivery.watermark('zulip:general') === 1, 'initial acceptance');
  const ids = Array.from({ length: 112 }, (_, i) => i + 2);
  const rows = ids.map((id) => streamMsg(id, { mentioned: id === 50 || id === 105 }));
  h.adapter.history = [streamMsg(1, { mentioned: true }), ...rows];
  h.adapter.emit!(withLog(streamMsg(2, { mentioned: true })));
  await until(() => fetches.started.length === 1, "2's attachment fetch");
  for (const m of rows.slice(1)) h.adapter.emit!(m);
  fetches.release();
  await h.server.quiet();
  const blocks = h.pushed.filter((p) => p.tags?.includes('zulip:missed'));
  assert.ok(blocks.some((p) => /id=50\]/.test((p.payload.content[0] as { text: string }).text)), 'the first pass, cut at the ceiling, sends its block');
  assert.ok(blocks.some((p) => /id=105\]/.test((p.payload.content[0] as { text: string }).text)), 'the mention past the ceiling, in a later block');
  assert.ok(blocks.every((p) => !/catch-up ceiling was reached/.test((p.payload.content[0] as { text: string }).text)));
  assert.ok(blocks.every((p) => (p.origin as { truncated?: boolean }).truncated !== true));
  assert.equal(offeredIds(h).filter((id) => id === '105').length, 1);
  assert.equal(offeredIds(h).filter((id) => id === '50').length, 1);
  assert.equal(h.delivery.watermark('zulip:general'), 113);
  await h.close();
});

async function reactionsHarness(dir: string, fetches: ReturnType<typeof heldFetches>) {
  const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
  plane.start();
  const h = harness({ filters: plane, attachments: fetches.options });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  const react = (emoji: string) => h.adapter.react!({
    action: 'add', channelId: 'zulip:general', messageId: '1', emoji,
    reactorId: '9', reactorName: 'Ann', onOwnMessage: false, messageSnippet: null, timestamp: new Date(1_700_000_000_000),
  });
  const show = (visible: boolean) => h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible } });
  const handed = () => h.incoming.map((m) => (m.metadata as { emoji?: string }).emoji ?? m.messageId);
  return { plane, h, react, show, handed };
}

test("while its lane is busy, a reaction that its channel doesn't show doesn't wait in the lane, and one that it shows waits there and goes after the message ahead of it", async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-reactions-lane-'));
  try {
    const fetches = heldFetches();
    const { plane, h, react, show, handed } = await reactionsHarness(dir, fetches);
    h.adapter.emit!(withLog(streamMsg(1)));
    await until(() => fetches.started.length === 1, "1's attachment fetch");
    react('eyes');
    await show(true);
    react('thumbs_up');
    await h.host.sendRequest('ping');
    assert.deepEqual(handed(), [], 'nothing passes the message ahead');
    fetches.release();
    await h.server.quiet();
    assert.deepEqual(handed(), ['1', 'thumbs_up']);
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a reaction that waits in its lane is checked again at its turn: its channel stopped showing reactions meanwhile, and nothing goes', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-reactions-turn-'));
  try {
    const fetches = heldFetches();
    const { plane, h, react, show, handed } = await reactionsHarness(dir, fetches);
    await show(true);
    h.adapter.emit!(withLog(streamMsg(1)));
    await until(() => fetches.started.length === 1, "1's attachment fetch");
    react('thumbs_up');
    await show(false);
    fetches.release();
    await h.server.quiet();
    assert.deepEqual(handed(), ['1']);
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('at a give-up, a waiting edit of the first given-up message and a deletion of the second leave with them; an edit of an accepted message and a shown reaction to the first stay, and the reaction follows its row', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-give-up-changes-'));
  try {
    const plane = new FiltersPlane(join(dir, 'filters.json'), {}, { pollMs: 60_000 });
    plane.start();
    const fetches = heldFetches();
    const OTHER: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
    const h = harness({ filters: plane, attachments: fetches.options, maxWaitingMessages: 2, visible: [DESCRIPTOR, OTHER] });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    await h.host.sendRequest('tools/call', { name: 'set_reaction_visibility', arguments: { channel: 'general', visible: true } });
    h.adapter.emit!(streamMsg(1));
    await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
    h.adapter.emit!(withLog(streamMsg(2)));
    await until(() => fetches.started.length === 1, "2's attachment fetch");
    h.adapter.emit!(streamMsg(3));
    h.adapter.emit!(streamMsg(4, { mentioned: true, text: '@bot four' }));
    const change = (over: Partial<MessageChangeEvent>): MessageChangeEvent => ({
      kind: 'edit', channelId: 'zulip:general', messageId: '3', messageIds: ['3'], authorId: '9', authorName: 'Ann', authorEmail: 'ann@example.com', actorId: '9',
      topic: 'deploys', previousTopic: null, movedToChannelId: null, content: 'msg 3, edited', previousContent: 'msg 3',
      mentioned: false, previouslyMentioned: false, vanished: false, isDM: false, onOwnMessage: false, timestamp: new Date(1_700_000_060_000),
      ...over,
    });
    h.adapter.change!(change({}));
    h.adapter.change!(change({ kind: 'delete', messageId: '4', messageIds: ['4'], content: null, previousContent: '@bot four', mentioned: true, previouslyMentioned: true, actorId: null }));
    h.adapter.change!(change({ messageId: '1', messageIds: ['1'], content: 'msg 1, edited', previousContent: 'msg 1' }));
    h.adapter.react!({ action: 'add', channelId: 'zulip:general', messageId: '3', emoji: 'thumbs_up', reactorId: '9', reactorName: 'Ann', onOwnMessage: false, messageSnippet: null, timestamp: new Date(1_700_000_070_000) });
    h.adapter.history = [streamMsg(1), streamMsg(2), streamMsg(3, { text: 'msg 3, edited' })];
    h.adapter.emit!(streamMsg(600, { channelId: 'zulip:other' }));
    fetches.release();
    await h.server.quiet();
    const handed = h.incoming.map((m) => m.messageId.startsWith('reaction:') ? m.messageId.split(':').slice(0, 3).join(':') : m.messageId.split(':').slice(0, 2).join(':'));
    assert.deepEqual(handed, ['1', '2', '3', 'edit:1', 'reaction:add:3']);
    const three = h.incoming.find((m) => m.messageId === '3')!;
    assert.match((three.content[0] as TextContent).text, /msg 3, edited$/);
    assert.equal((three.metadata as { recovered?: boolean }).recovered, true);
    assert.equal(h.pushed.length, 0, 'no line for the deleted mention goes as a push either');
    plane.stop();
    await h.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a lane gives its messages up, and the first of them is deleted before the read: the read lets its hold go, so the watermark passes it with no read for held ids', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options, maxWaitingMessages: 3 });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.history = [1, 3, 4, 5, 6].map((id) => streamMsg(id));
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, "1's attachment fetch");
  for (const id of [2, 3, 4, 5, 6]) h.adapter.emit!(streamMsg(id));
  fetches.release();
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '3', '4', '5', '6']);
  assert.deepEqual(h.incoming.filter((m) => (m.metadata as { recovered?: boolean }).recovered === true).map((m) => m.messageId), ['3', '4', '5'], 'the read brought what history still has');
  assert.deepEqual(h.delivery.heldIds('zulip:general'), []);
  assert.equal(h.delivery.watermark('zulip:general'), 6);
  assert.deepEqual(h.adapter.historyCalls.filter((c) => c.channelId === 'zulip:general').map((c) => c.query.afterMessageId), ['1'], 'one read, the one that fills the hole');
  await h.close();
});

test('on a closed channel, a lane gives its messages up and history has nothing after its place: the read lets their hold go, and the next message the host accepts reaches it once and moves the watermark', async () => {
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options, maxWaitingMessages: 2 });
  await initialize(h, true);
  await settled(h);
  h.adapter.emit!(streamMsg(1, { mentioned: true }));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  h.adapter.history = [streamMsg(1, { mentioned: true }), streamMsg(2, { mentioned: true })];
  h.adapter.emit!(withLog(streamMsg(2, { mentioned: true })));
  await until(() => fetches.started.length === 1, "2's attachment fetch");
  for (const id of [3, 4, 5]) h.adapter.emit!(streamMsg(id));
  fetches.release();
  await h.server.quiet();
  assert.ok(h.adapter.historyCalls.some((c) => c.channelId === 'zulip:general' && c.query.afterMessageId === '2'), 'the read after the give-up ran');
  h.adapter.history = [...h.adapter.history, streamMsg(6, { mentioned: true })];
  h.adapter.emit!(streamMsg(6, { mentioned: true }));
  await until(() => h.delivery.watermark('zulip:general') === 6 || offeredIds(h).length > 3, '6 offered and accepted');
  await h.server.quiet();
  assert.deepEqual(offeredIds(h), ['1', '2', '6']);
  assert.equal(h.delivery.watermark('zulip:general'), 6);
  await h.close();
});

/** The channel's reads from history after a message id fail with `errors`, one each, in order; then they pass. */
function failReads(h: Harness, channelId: string, errors: unknown[]): void {
  const real = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    if (h.adapter.wireOf(key) === channelId && query.afterMessageId !== undefined && errors.length > 0) throw errors.shift();
    return real(key, query);
  };
}
const noAnswer = () => new ZulipNoAnswer('network', 'GET', '/messages', false, 'The connection to Zulip failed during GET /messages.');
const refusal = (code: string, retryAfter?: number) => new ZulipRefused('history of stream 7', {
  result: 'error', msg: code === 'RATE_LIMIT_HIT' ? 'API usage exceeded rate limit' : 'Invalid narrow', code, ...(retryAfter !== undefined ? { 'retry-after': retryAfter } : {}),
});
function heldWaits() {
  const asked: number[] = [];
  const pending: (() => void)[] = [];
  return {
    asked,
    release: () => { for (const resolve of pending.splice(0)) resolve(); },
    wait: (ms: number) => { asked.push(ms); return new Promise<void>((resolve) => pending.push(resolve)); },
  };
}

test("after a break of the event queue, a read that gets no answer is tried again after a wait, and a message behind it isn't handed over: the hole's rows reach the host first, then the message", async () => {
  const waits = heldWaits();
  const h = harness({ history: [1, 2, 3].map((id) => streamMsg(id)), readRetryWait: waits.wait });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  failReads(h, 'zulip:general', [noAnswer()]);
  breakQueue(h);
  h.adapter.emit!(streamMsg(3));
  await until(() => waits.asked.length === 1, 'the wait before the second try');
  await h.host.sendRequest('ping');
  assert.deepEqual(seenIds(h), ['1'], 'nothing is handed over while the read waits');
  waits.release();
  await h.server.quiet();
  assert.deepEqual(seenIds(h), ['1', '2', '3', 'notice']);
  assert.equal((h.incoming[1].metadata as { recovered?: boolean }).recovered, true, '2 is recovered');
  await h.close();
});

for (const rateLimited of [false, true]) {
  test(`after a break of the event queue, a read that Zulip refuses ${rateLimited
    ? 'by its rate limit is tried again, as one that gets no answer'
    : 'for another reason ends the hole, as on main: the message behind it is handed over'}`, async () => {
    const waits = heldWaits();
    const h = harness({ history: [1, 2, 3].map((id) => streamMsg(id)), readRetryWait: waits.wait });
    await initialize(h, true);
    await settled(h);
    await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    h.adapter.emit!(streamMsg(1));
    await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
    failReads(h, 'zulip:general', [refusal(rateLimited ? 'RATE_LIMIT_HIT' : 'BAD_REQUEST')]);
    breakQueue(h);
    h.adapter.emit!(streamMsg(3));
    if (rateLimited) {
      await until(() => waits.asked.length === 1, 'the wait before the second try');
      waits.release();
    }
    await until(() => gapNoticeIn(h), 'the gap notice');
    await h.server.quiet();
    assert.deepEqual(seenIds(h), rateLimited ? ['1', '2', '3', 'notice'] : ['1', '3', 'notice']);
    assert.deepEqual(waits.asked, rateLimited ? [2000] : []);
    await h.close();
  });
}

test('after a break of the event queue, an open lane whose read gets no answer holds back only itself: the other open lanes get the gap notice, and it gets its recovered rows and then the notice once its read answers', async () => {
  const waits = heldWaits();
  const OTHER: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
  const h = harness({
    visible: [DESCRIPTOR, OTHER],
    readRetryWait: waits.wait,
    history: [streamMsg(1), streamMsg(2), streamMsg(500, { channelId: 'zulip:other' }), streamMsg(501, { channelId: 'zulip:other' })],
  });
  await initialize(h, true);
  await settled(h);
  for (const channelId of ['zulip:general', 'zulip:other']) await h.host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  h.adapter.emit!(streamMsg(500, { channelId: 'zulip:other' }));
  await until(() => h.delivery.watermark('zulip:general') === 1 && h.delivery.watermark('zulip:other') === 500, 'both accepted');
  failReads(h, 'zulip:general', [noAnswer()]);
  const on = (channelId: string) => h.incoming.filter((m) => m.channelId === channelId).map((m) => (m.messageId.startsWith('system:') ? 'notice' : m.messageId));
  breakQueue(h);
  await until(() => waits.asked.length === 1 && on('zulip:other').includes('notice'), "general's wait and other's notice");
  assert.deepEqual(on('zulip:other'), ['500', '501', 'notice']);
  assert.deepEqual(on('zulip:general'), ['1']);
  waits.release();
  await h.server.quiet();
  assert.deepEqual(on('zulip:general'), ['1', '2', 'notice']);
  await h.close();
});

test("a connection that ends while a lane waits to try its read again makes no try, and the next connection's catch-up covers the hole", async () => {
  const waits = heldWaits();
  const first = harness({ history: [1, 2].map((id) => streamMsg(id)), readRetryWait: waits.wait });
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.adapter.emit!(streamMsg(1));
  await until(() => first.delivery.watermark('zulip:general') === 1, '1 accepted');
  failReads(first, 'zulip:general', [noAnswer()]);
  breakQueue(first);
  await until(() => waits.asked.length === 1, 'the wait before the second try');
  await first.close();
  const reads = first.adapter.historyCalls.length;
  waits.release();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  await second.server.quiet();
  assert.equal(first.adapter.historyCalls.length - reads, 1, "the next connection's catch-up is the one read");
  assert.equal(second.pushed.length, 1, 'the catch-up block');
  assert.match((second.pushed[0].payload.content[0] as { text: string }).text, /id=2\]/);
  await second.close();
});

test("a read that gets no answer is tried again after 2 s, doubling to at most 60 s, and after a refusal by the rate limit after the longer of that step and Zulip's retry-after", async () => {
  const asked: number[] = [];
  const h = harness({ history: [1, 2].map((id) => streamMsg(id)), readRetryWait: async (ms) => { asked.push(ms); } });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  failReads(h, 'zulip:general', [...Array.from({ length: 7 }, noAnswer), refusal('RATE_LIMIT_HIT', 90), refusal('RATE_LIMIT_HIT', 0.5)]);
  breakQueue(h);
  await until(() => gapNoticeIn(h), 'the gap notice');
  await h.server.quiet();
  assert.deepEqual(asked, [2000, 4000, 8000, 16000, 32000, 60000, 60000, 90000, 60000]);
  assert.deepEqual(seenIds(h), ['1', '2', 'notice']);
  await h.close();
});

test("a connection's catch-up read that gets no answer is tried again, and a mention that arrives meanwhile follows the catch-up's block", async () => {
  const waits = heldWaits();
  const h = harness({ history: [1, 2].map((id) => streamMsg(id, { mentioned: true })), readRetryWait: waits.wait });
  h.delivery.advance('zulip:general', 1);
  failReads(h, 'zulip:general', [noAnswer()]);
  await initialize(h, true);
  await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
  await awaitRegistered(h, false);
  await until(() => waits.asked.length === 1, 'the wait before the second try');
  h.adapter.emit!(streamMsg(3, { mentioned: true }));
  await h.host.sendRequest('ping');
  assert.equal(h.pushed.length, 0, 'the mention waits behind the read');
  waits.release();
  await h.server.quiet();
  assert.deepEqual(offeredIds(h), ['2', '3']);
  await h.close();
});

test('a read after a give-up that gets no answer is tried again, and the given-up messages still come by it, in order, before a later message', async () => {
  const waits = heldWaits();
  const fetches = heldFetches();
  const h = harness({ attachments: fetches.options, maxWaitingMessages: 3, readRetryWait: waits.wait });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.history = [1, 2, 3, 4, 5, 6].map((id) => streamMsg(id));
  h.adapter.emit!(withLog(streamMsg(1)));
  await until(() => fetches.started.length === 1, "1's attachment fetch");
  for (const id of [2, 3, 4, 5, 6]) h.adapter.emit!(streamMsg(id));
  failReads(h, 'zulip:general', [noAnswer()]);
  fetches.release();
  await until(() => waits.asked.length === 1, 'the wait before the second try');
  waits.release();
  await h.server.quiet();
  assert.deepEqual(h.incoming.map((m) => m.messageId), ['1', '2', '3', '4', '5', '6']);
  for (const m of h.incoming) {
    assert.equal((m.metadata as { recovered?: boolean }).recovered === true, ['2', '3', '4', '5'].includes(m.messageId), m.messageId);
  }
  await h.close();
});

const readWaitLines = (h: Harness, kind: 'history-waiting' | 'history-answered') =>
  h.incoming.filter((m) => (m.metadata as { kind?: string }).kind === kind);

/** A system line that goes to every open channel at once: once it has arrived, so has every line queued before it. */
async function sentinelThrough(h: Harness, channels: number): Promise<void> {
  const text = `sentinel ${Math.random()}`;
  h.adapter.systemEvent!({ kind: 'degraded', text, metadata: { platform: 'zulip' } });
  await until(() => h.incoming.filter((m) => (m.content[0] as TextContent).text === text).length === channels, 'the sentinel line');
}

/** Each read of a channel after a message id fails, once for each entry, with the clock moved on by that entry's ms first. */
function failReadsAt(h: Harness, clock: { t: number }, failures: Record<string, ({ ms: number; error?: unknown })[]>): void {
  const real = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (key, query) => {
    const left = failures[h.adapter.wireOf(key)];
    if (query.afterMessageId !== undefined && left && left.length > 0) {
      const { ms, error } = left.shift()!;
      clock.t += ms;
      throw error ?? noAnswer();
    }
    return real(key, query);
  };
}

test('a try that fails after its read has waited 90 s tells every open channel once, its own included, naming the waiting channel; a second lane that waits as long adds no line; once both reads have answered, one line says so, after the recovered rows', async () => {
  const waits = heldWaits();
  const clock = { t: 0 };
  const OTHER: ChannelDescriptor = { ...DESCRIPTOR, id: 'zulip:other', label: '#other', address: { stream_name: 'other', stream_id: 8 } };
  const h = harness({
    visible: [DESCRIPTOR, OTHER],
    readRetryWait: waits.wait,
    now: () => clock.t,
    history: [streamMsg(1), streamMsg(2), streamMsg(500, { channelId: 'zulip:other' }), streamMsg(501, { channelId: 'zulip:other' })],
  });
  await initialize(h, true);
  await settled(h);
  for (const channelId of ['zulip:general', 'zulip:other']) await h.host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  h.adapter.emit!(streamMsg(500, { channelId: 'zulip:other' }));
  await until(() => h.delivery.watermark('zulip:general') === 1 && h.delivery.watermark('zulip:other') === 500, 'both accepted');
  failReadsAt(h, clock, { 'zulip:general': [{ ms: 90_000 }], 'zulip:other': [{ ms: 90_000 }] });
  breakQueue(h);
  await until(() => waits.asked.length === 2, 'both lanes wait');
  await sentinelThrough(h, 2);
  const waiting = readWaitLines(h, 'history-waiting');
  assert.deepEqual(waiting.map((m) => m.channelId).sort(), ['zulip:general', 'zulip:other'], 'one line on each open channel');
  for (const m of waiting) {
    const text = (m.content[0] as TextContent).text;
    assert.match(text, /zulip:general/);
    assert.doesNotMatch(text, /zulip:other/);
  }
  waits.release();
  await h.server.quiet();
  assert.equal(readWaitLines(h, 'history-waiting').length, 2, 'no line was added');
  const answered = readWaitLines(h, 'history-answered');
  assert.deepEqual(answered.map((m) => m.channelId).sort(), ['zulip:general', 'zulip:other']);
  for (const [channelId, row] of [['zulip:general', '2'], ['zulip:other', '501']]) {
    const on = h.incoming.filter((m) => m.channelId === channelId);
    assert.ok(on.findIndex((m) => m.messageId === row) < on.findIndex((m) => (m.metadata as { kind?: string }).kind === 'history-answered'), `${channelId}: the line follows ${row}`);
  }
  await h.close();
});

test('with no channel open, the line that a read waits and the line that it no longer does go to the log', async () => {
  const waits = heldWaits();
  const clock = { t: 0 };
  const h = harness({ history: [1, 2].map((id) => streamMsg(id, { mentioned: true })), readRetryWait: waits.wait, now: () => clock.t });
  await initialize(h, true);
  await settled(h);
  h.delivery.advance('zulip:general', 1);
  failReadsAt(h, clock, { 'zulip:general': [{ ms: 90_000 }] });
  const logs: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => { logs.push(args.map(String).join(' ')); };
  try {
    breakQueue(h);
    await until(() => waits.asked.length === 1, 'the wait before the second try');
    assert.equal(logs.filter((l) => l.includes("Zulip isn't serving reads of the history of zulip:general") && l.includes('(no open channel to tell)')).length, 1);
    waits.release();
    await until(() => h.pushed.length === 1, "the read's block");
    await h.server.quiet();
  } finally {
    console.error = original;
  }
  assert.equal(logs.filter((l) => l.includes('No channel waits for a read of its history from Zulip any more.') && l.includes('(no open channel to tell)')).length, 1);
  assert.equal(h.incoming.length, 0);
  await h.close();
});

test('a try that fails before its read has waited 90 s adds no line, and one that fails at 90 s does', async () => {
  const waits = heldWaits();
  const clock = { t: 0 };
  const h = harness({ history: [1, 2].map((id) => streamMsg(id)), readRetryWait: waits.wait, now: () => clock.t });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  failReadsAt(h, clock, { 'zulip:general': [{ ms: 89_999 }, { ms: 1 }] });
  breakQueue(h);
  await until(() => waits.asked.length === 1, 'the wait before the second try');
  await sentinelThrough(h, 1);
  assert.equal(readWaitLines(h, 'history-waiting').length, 0, 'no line at 89.999 s');
  waits.release();
  await until(() => waits.asked.length === 2, 'the wait before the third try');
  await sentinelThrough(h, 1);
  assert.equal(readWaitLines(h, 'history-waiting').length, 1, 'a line at 90 s');
  waits.release();
  await h.server.quiet();
  await h.close();
});

test('a try that runs out its bound tells at its first failure, whatever the clock says', async () => {
  const waits = heldWaits();
  const clock = { t: 0 };
  const h = harness({ history: [1, 2].map((id) => streamMsg(id)), readRetryWait: waits.wait, now: () => clock.t });
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  h.adapter.emit!(streamMsg(1));
  await until(() => h.delivery.watermark('zulip:general') === 1, '1 accepted');
  const ranOut = new ZulipNoAnswer('timeout', 'GET', '/messages', false, 'Zulip gave no answer to GET /messages within 90 s.');
  failReadsAt(h, clock, { 'zulip:general': [{ ms: 0, error: ranOut }] });
  breakQueue(h);
  await until(() => waits.asked.length === 1, 'the wait before the second try');
  await until(() => readWaitLines(h, 'history-waiting').length === 1, 'the line at the first failure');
  waits.release();
  await h.server.quiet();
  assert.equal(readWaitLines(h, 'history-answered').length, 1);
  await h.close();
});

test("the lines belong to their connection: after a connection ended while the agent was told a read waits, the next connection's wait is told again", async () => {
  const waits = heldWaits();
  const clock = { t: 0 };
  const first = harness({ history: [1, 2].map((id) => streamMsg(id)), readRetryWait: waits.wait, now: () => clock.t });
  await initialize(first, true);
  await settled(first);
  await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  first.adapter.emit!(streamMsg(1));
  await until(() => first.delivery.watermark('zulip:general') === 1, '1 accepted');
  failReadsAt(first, clock, { 'zulip:general': [{ ms: 90_000 }] });
  breakQueue(first);
  await until(() => waits.asked.length === 1 && readWaitLines(first, 'history-waiting').length === 1, 'the first connection told');
  await first.close();
  waits.release();

  const second = harness({ reuse: { server: first.server, adapter: first.adapter } });
  await initialize(second, true);
  await settled(second);
  await second.server.quiet();
  await second.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
  failReadsAt(second, clock, { 'zulip:general': [{ ms: 90_000 }] });
  breakQueue(second);
  await until(() => waits.asked.length === 2, "the second connection's wait");
  await until(() => readWaitLines(second, 'history-waiting').length === 1, 'told again');
  waits.release();
  await second.server.quiet();
  await second.close();
});
