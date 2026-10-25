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

import test from 'node:test';
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
} from '@animalabs/mcpl-core';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { ChannelHistoryPage, ChannelHistoryQuery, MessageChangeEvent, OnIncomingMessage, OnMessageChange, OnReaction, OnSystemEvent, PlatformAdapter, RoutingHints } from '../src/platforms/adapter.ts';
import { ZulipMcplServer, type ZulipMcplServerOptions } from '../src/server.ts';
import { FiltersPlane } from '../src/filters.ts';
import type { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { toolDefinitions } from '../src/tools.ts';
import { TOOL_CLASSES } from '../src/tool-classes.ts';

const DESCRIPTOR: ChannelDescriptor = {
  id: 'zulip:general',
  type: 'zulip',
  label: '#general',
  direction: 'bidirectional',
  address: { stream_name: 'general', stream_id: 7 },
};

interface FakeAdapter extends PlatformAdapter {
  published: { channelId: string; content: ContentBlock[]; hints?: RoutingHints }[];
  typing: { channelId: string; op: string; metadata?: Record<string, unknown> }[];
  emit: OnIncomingMessage | null;
  systemEvent: OnSystemEvent | null;
  react: OnReaction | null;
  change: OnMessageChange | null;
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
}

function fakeAdapter(withTyping = true): FakeAdapter {
  const adapter: FakeAdapter = {
    type: 'zulip',
    published: [],
    typing: [],
    emit: null,
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
    async discoverChannels() { return [...adapter.visible]; },
    async describeChannels(channelIds: string[]) { return adapter.visible.filter((c) => channelIds.includes(c.id)); },
    async fetchHistory(channelId, query): Promise<ChannelHistoryPage> {
      adapter.historyCalls.push({ channelId, query });
      let rows = adapter.history.filter((m) => m.channelId === channelId);
      // As history.ts does: the after-cursor wins when both are given.
      if (query.afterMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) > Number(query.afterMessageId));
      else if (query.beforeMessageId !== undefined) rows = rows.filter((m) => Number(m.messageId) < Number(query.beforeMessageId));
      const limit = Math.min(query.limit, adapter.pageCap);
      const page = query.afterMessageId !== undefined ? rows.slice(0, limit) : rows.slice(-limit);
      const newest = page[page.length - 1];
      return {
        messages: page.map((m) => ({ ...m, metadata: { ...(m.metadata as object), backscroll: true } })),
        scannedThrough: newest ? Number(newest.messageId) : null,
        reachedNewest: !newest || newest === rows[rows.length - 1],
      };
    },
    async ensureSubscribed(channelId) {
      const reason = adapter.subscribeError.get(channelId);
      if (reason) throw new Error(reason);
      adapter.subscribed.push(channelId);
    },
    async acknowledge(_channelId, messageId, value) { adapter.acked.push(`${messageId}:${value ?? ''}`); return `:${value ?? 'eyes'}:`; },
    async deleteMessage(_channelId, messageId) { adapter.deleted.push(messageId); },
    async publish(channelId, _descriptor, content, hints) {
      adapter.published.push({ channelId, content, hints });
      const n = adapter.published.length;
      return { delivered: true, messageId: String(40 + n), messageIds: [String(40 + n)] };
    },
    async fetchContext(channelId): Promise<ContextInjection> {
      return { namespace: channelId, position: 'beforeUser', content: 'recent history' };
    },
    startEvents(onMessage, onSystemEvent, onReaction, onMessageChange) {
      adapter.emit = onMessage;
      adapter.systemEvent = onSystemEvent ?? null;
      adapter.react = onReaction ?? null;
      adapter.change = onMessageChange ?? null;
    },
    stopEvents() { adapter.emit = null; },
  };
  if (withTyping) {
    adapter.sendTyping = async (channelId, _d, m, op) => { adapter.typing.push({ channelId, op, metadata: m as Record<string, unknown> | undefined }); };
  }
  return adapter;
}

const fakeTools = {
  calls: [] as { name: string; args: Record<string, unknown> }[],
  /** Set by the server; the real runtime reports every message a tool sends. */
  onSent: null as null | ((sent: { messageId: string; channelId: string; content: string }) => void),
  /** Set by the server; the real runtime reports streams `listen` subscribed to. */
  onSubscribed: null as null | ((streams: string[]) => unknown),
  async handleToolCall(name: string, args: Record<string, unknown>) {
    fakeTools.calls.push({ name, args });
    if (name === 'explode') throw new Error('boom');
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
  adapter: FakeAdapter;
  host: McplConnection;
  /** Server→host requests the host reactor answered, in order. */
  hostSaw: JsonRpcRequest[];
  incoming: IncomingChannelMessage[];
  pushed: PushEventParams[];
  served: Promise<void>;
  /** How the host answers channels/incoming: per-message rejections (for
   *  good, or once), and whole batches that error out (counted down per batch). */
  policy: { rejectIds: Set<string>; rejectOnce: Set<string>; failIncomingBatches: number; changedAnswer: 'accept' | 'refuse' | 'error' | 'reconcile'; reconcileOpens: { channelId: string; ok: boolean; detail: string }[]; rejectPushes: Set<string> };
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
  /** A second connection to a server that already served one (the TCP case). */
  reuse?: { server: ZulipMcplServer; adapter: FakeAdapter };
}

function harness(opts: HarnessOptions = {}): Harness {
  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const serverConn = McplConnection.fromStreams(toServer, toHost);
  const host = McplConnection.fromStreams(toHost, toServer);

  const adapter = opts.reuse?.adapter ?? fakeAdapter(opts.typing ?? true);
  if (opts.history) adapter.history = opts.history;
  const server = opts.reuse?.server ?? new ZulipMcplServer(adapter, fakeTools as unknown as ZulipToolRuntime, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: opts.mcpl ?? true,
    batchWindowMs: opts.batchWindowMs ?? 5,
    contextHistorySize: 3,
    stateDir: opts.stateDir ?? null,
    sessionId: opts.sessionId ?? 'test',
    catchupLimit: 100,
    formatTime: () => 'T',
    announceTimeoutMs: opts.announceTimeoutMs ?? 100,
    filters: opts.filters,
    attachments: opts.attachments,
    attributeDelivery: opts.attributeDelivery,
  });

  const hostSaw: JsonRpcRequest[] = [];
  const incoming: IncomingChannelMessage[] = [];
  const pushed: PushEventParams[] = [];
  const policy = {
    rejectIds: new Set<string>(),
    rejectOnce: new Set<string>(),
    failIncomingBatches: 0,
    changedAnswer: 'accept' as 'accept' | 'refuse' | 'error' | 'reconcile',
    reconcileOpens: [] as { channelId: string; ok: boolean; detail: string }[],
    rejectPushes: new Set<string>(),
  };
  // The host reactor: answer every server→host Request the way conhost does.
  host.on('request', (req) => {
    hostSaw.push(req);
    if (req.method === method.CHANNELS_REGISTER) {
      const p = req.params as ChannelsRegisterParams;
      host.sendResponse(req.id, { results: p.channels.map((c) => ({ id: c.id, accepted: true })) });
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
      host.sendResponse(req.id, { accepted: !policy.rejectPushes.has(String((p.origin as { messageId?: unknown }).messageId)) });
    } else if (req.method === method.CHANNELS_CHANGED) {
      const p = req.params as { added?: ChannelDescriptor[] };
      // 'error' stands for every host that does not answer this Request:
      // a transport drop, a timeout, or one that treats it as a Notification.
      if (policy.changedAnswer === 'error') host.sendError(req.id, -32601, 'channels/changed unsupported');
      else if (policy.changedAnswer === 'reconcile') {
        // This stands for a host that reconciles (a blocking channels/open per
        // descriptor) before it answers. This server answers one request at a
        // time, so while it waits for this answer it cannot serve that open.
        void (async () => {
          for (const c of p.added ?? []) {
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
    adapter,
    host,
    hostSaw,
    incoming,
    pushed,
    served,
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
 *  channels/list is the observable that it did. Live delivery follows the
 *  catch-up sweep, which is what most cases go on to exercise. */
async function awaitRegistered(h: Harness, andLive = true): Promise<void> {
  await until(() => h.adapter.emit !== null, 'event delivery to start');
  const deadline = Date.now() + 2000;
  while (true) {
    const listed = (await h.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    if (listed.channels.length > 0) break;
    if (Date.now() > deadline) throw new Error('timed out waiting for registration');
    await new Promise((r) => setTimeout(r, 5));
  }
  if (andLive) await until(() => h.server.isLive, 'live delivery');
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
  await new Promise((r) => setTimeout(r, 20));
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
  await new Promise((r) => setTimeout(r, 10));
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
  assert.equal(h.server.delivery.watermark('zulip:general'), 1);
  await h.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });

  h.adapter.emit!(streamMsg(2, { text: 'chatter' }));
  h.adapter.emit!(streamMsg(3, { text: 'more chatter' }));
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(h.incoming.length, 1, 'ambient on a closed channel is not delivered');
  assert.equal(h.pushed.length, 0);
  assert.equal(h.server.delivery.watermark('zulip:general'), 1, 'a dropped message does not advance the watermark');

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
  await until(() => h.server.delivery.watermark('zulip:general') === 4, 'watermark after acknowledged push');

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
  await until(() => h.server.delivery.watermark('zulip:general') === 2, 'the mention in billing accepted');
  // Typing with no topic from the host follows the same hint.
  await h.host.sendRequest(method.CHANNELS_TYPING, { channelId: 'zulip:general', op: 'start' });
  await until(() => h.adapter.typing.length === 1, 'typing start');
  assert.equal(h.adapter.typing[0].metadata?.topic, 'billing');
  assert.deepEqual(await answer(h, 'about billing'), { threadId: 'billing', topic: 'billing' });
  h.adapter.emit!(mention(3, 'support'));
  await until(() => h.server.delivery.watermark('zulip:general') === 3, 'the mention in support accepted');
  assert.deepEqual(await answer(h, 'about support'), { threadId: 'support', topic: 'support' });

  // A message that doesn't address the agent isn't pushed, and moves nothing.
  h.adapter.emit!(streamMsg(4, { text: 'chatter', threadId: 'chatter', metadata: { topic: 'chatter', mentioned: false, isDM: false } }));
  await until(() => h.server.delivery.tally('zulip:general')?.talliedThrough === 4, 'the ambient message tallied');
  assert.equal(h.pushed.length, 2, 'not pushed');
  assert.deepEqual(await answer(h, 'still about support'), { threadId: 'support', topic: 'support' });

  // A mention the host refuses never reached the agent, so it moves nothing.
  h.policy.rejectPushes.add('5');
  h.adapter.emit!(mention(5, 'random'));
  await until(() => h.server.delivery.heldIds('zulip:general', { replayable: true }).includes(5), 'the refusal handled: held as failed');
  assert.equal(h.server.delivery.watermark('zulip:general'), 3, 'refused: the watermark stays');
  assert.deepEqual(await answer(h, 'and still about support'), { threadId: 'support', topic: 'support' });
  await h.close();

  // A stream never open: the first answer goes to the mention's topic too.
  const g = harness();
  await initialize(g, true);
  await settled(g);
  g.adapter.emit!(mention(5, 'billing'));
  await until(() => g.server.delivery.watermark('zulip:general') === 5, 'the first contact accepted');
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
  await until(() => k.server.delivery.watermark('zulip:general') === 10, 'the mention accepted');
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
  assert.equal(h.server.delivery.watermark('zulip:general'), 12, 'returned history counts as forwarded');

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
    const first = harness({ stateDir: dir, sessionId: 'sw' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    first.server.delivery.advance('zulip:dev', 50);
    first.server.delivery.save();
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
    const second = harness({ stateDir: dir, sessionId: 'sw', history: later });
    // dev must be a registered channel for the sweep to consider it.
    second.adapter.discoverChannels = async () => [DESCRIPTOR, { ...DESCRIPTOR, id: 'zulip:dev', label: '#dev', address: { stream_name: 'dev', stream_id: 8 } }];
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

    assert.equal(second.server.delivery.watermark('zulip:general'), 6);
    assert.equal(second.server.delivery.watermark('zulip:dev'), 70, 'advanced past everything scanned');
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
    const third = harness({ stateDir: dir, sessionId: 'sw', history: later });
    await initialize(third, true);
    await settled(third);
    await new Promise((r) => setTimeout(r, 30));
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
  assert.equal(h.server.delivery.watermark('zulip:general'), 3);
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
  await until(() => h.server.delivery.watermark('zulip:dm:42') === 500, 'watermark');
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
    await new Promise((r) => setTimeout(r, 25));
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
    await new Promise((r) => setTimeout(r, 20));
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
    assert.equal(h.server.delivery.watermark('zulip:general'), undefined, 'a reaction never advances the watermark');

    reaction({ action: 'remove', onOwnMessage: false, messageSnippet: null });
    await until(() => h.incoming.length === 2, 'reaction removal');
    assert.deepEqual(h.incoming[1].tags, ['chat:reaction-remove']);
    assert.equal((h.incoming[1].content[0] as { text: string }).text, '[reaction] Ann removed a reaction :thumbs_up: on message 77');

    // Suppressed emoji: no glyph, no event, nowhere — by name, or by the
    // codepoints a glyph-shaped baseline entry is matched on.
    reaction({ emoji: 'biohazard' });
    reaction({ emoji: 'octagonal_sign', emojiCode: '1f6d1', emojiType: 'unicode_emoji' });
    await new Promise((r) => setTimeout(r, 20));
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

test('messaging tools mint checkpoints the host can roll back to; publishes and tool sends after it are deleted; acknowledge reacts; streaming terminators are inert', async () => {
  const h = harness();
  await initialize(h, true);
  await settled(h);
  await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });

  const publish = (text: string) => h.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: 'c', channelId: 'zulip:general', content: [{ type: 'text', text }] });
  const call = async (name: string, args: Record<string, unknown>) =>
    (await h.host.sendRequest('tools/call', { name, arguments: args })) as { isError?: boolean; state?: { featureSet: string; checkpoint: string; parent: string | null } };

  await publish('one');
  // The only way a host learns a checkpoint is a tool result's `state` (§8).
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
  await new Promise((r) => setTimeout(r, 20));
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
    assert.equal(h.server.delivery.watermark('zulip:general'), 1, 'held below the refused id');
    // The host is answering, so the refused message is fetched back from
    // history and offered again; accepted now, the watermark catches up.
    await until(() => h.incoming.length === 3, 'the replay');
    assert.equal(h.incoming[2].messageId, '2');
    assert.ok(h.incoming[2].tags!.includes('zulip:missed'));
    assert.equal((h.incoming[2].metadata as { recovered: boolean }).recovered, true);
    await until(() => h.server.delivery.watermark('zulip:general') === 3, 'watermark through everything accepted');
    h.adapter.emit!(streamMsg(4));
    await until(() => h.server.delivery.watermark('zulip:general') === 4, 'watermark 4');

    // Two transport failures in a row — the attempt and its retry — then
    // the host recovers and accepts the next message: the lost one is not
    // buried under that acceptance.
    h.policy.failIncomingBatches = 2;
    h.adapter.emit!(streamMsg(5));
    await until(() => h.policy.failIncomingBatches === 0 && h.server.channelManager.pendingCount() === 0, 'given up after the retry');
    assert.equal(h.server.delivery.watermark('zulip:general'), 4, 'a message the host never accepted is not forwarded');
    assert.equal(h.server.delivery.floor('zulip:general'), 5, 'and holds the watermark below it');
    h.adapter.emit!(streamMsg(6));
    await until(() => h.incoming.some((m) => m.messageId === '6'), '6 accepted');
    assert.equal(h.server.delivery.watermark('zulip:general'), 4, 'still not past the lost message');
    await until(() => h.incoming.some((m) => m.messageId === '5'), '5 fetched back from history and accepted');
    await until(() => h.server.delivery.watermark('zulip:general') === 6, 'watermark 6 once 5 is in');
    assert.equal(h.server.delivery.floor('zulip:general'), undefined);

    // Shutdown on a live connection pushes the last window out instead of
    // cancelling it.
    h.adapter.emit!(streamMsg(7));
    assert.equal(h.server.channelManager.pendingCount(), 1);
    await h.server.shutdown();
    assert.equal(h.incoming[h.incoming.length - 1].messageId, '7');
    assert.equal(h.server.delivery.watermark('zulip:general'), 7);
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
    first.server.delivery.advance('zulip:general', 1);
    first.server.delivery.save();
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
  assert.equal(h.server.delivery.wasOpen('zulip:general'), false);
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

test('live events received before the catch-up sweep are held, so a live delivery cannot jump the watermark over the offline gap', async () => {
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

    // Session 2: the sweep's history fetch is slow; a live mention (6) and a
    // duplicate of something the sweep will deliver (5) arrive meanwhile.
    const second = harness({ stateDir: dir, sessionId: 'pl', history: [streamMsg(4), streamMsg(5, { mentioned: true })] });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { await gate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    assert.equal(second.server.isLive, false);
    second.adapter.emit!(streamMsg(6, { mentioned: true, text: 'live @bot' }));
    second.adapter.emit!(streamMsg(5, { mentioned: true }));
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(second.pushed.length, 0, 'held until the sweep has run');
    release();
    await until(() => second.pushed.length === 2, 'the sweep block, then the live mention');
    const block = (second.pushed[0].payload.content[0] as { text: string }).text;
    assert.match(block, /^<missed /);
    assert.match(block, /id=4\]/);
    assert.match(block, /id=5\]/);
    assert.equal(second.pushed[1].eventId, 'zulip_msg_6');
    assert.equal(second.server.delivery.watermark('zulip:general'), 6);
    await new Promise((r) => setTimeout(r, 20));
    assert.equal(second.pushed.length, 2, 'the duplicate of 5 was dropped, not pushed twice');
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
  h.server.delivery.advance('zulip:general', 1);

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 1, 'catch-up push for the closed channel');
  const calls = h.adapter.historyCalls.filter((c) => c.channelId === 'zulip:general');
  assert.deepEqual(calls.map((c) => c.query.afterMessageId), ['1', '31', '61', '91'], 'paged from the watermark, page by page');
  assert.deepEqual(calls.map((c) => c.query.limit), [100, 70, 40, 10], 'never over the ceiling in total');
  const block = (h.pushed[0].payload.content[0] as { text: string }).text;
  assert.match(block, /count="2" lines="20" reason="mention" truncated="true"/);
  assert.match(block, /catch-up ceiling was reached.*after=101/);
  await until(() => h.server.delivery.watermark('zulip:general') === 101, 'the block accepted');
  assert.equal(h.server.delivery.watermark('zulip:general'), 101, 'advanced through everything scanned');
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
  await until(() => h.server.delivery.watermark('zulip:general') === 1, 'the mention in support accepted');
  // The block goes out under the newest id it scanned, 21.
  h.policy.rejectPushes.add('21');

  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 2, 'catch-up push for the closed channel');
  assert.equal((h.pushed[1].origin as { messageId?: string }).messageId, '21');
  // A round trip after the refusal: the server has handled it by the answer.
  await h.host.sendRequest(method.CHANNELS_LIST);
  assert.equal(h.server.delivery.watermark('zulip:general'), 1, 'refused: the watermark stays');
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
  await new Promise((r) => setTimeout(r, 10));
  h.adapter.emit!(streamMsg(1));
  h.adapter.emit!(streamMsg(2, { mentioned: true }));
  await new Promise((r) => setTimeout(r, 25));
  assert.equal(h.incoming.length, 0, 'no channels/incoming under a disabled feature set');
  assert.equal(h.pushed.length, 0);
  assert.equal(h.server.delivery.watermark('zulip:general'), undefined, 'and nothing is pretended forwarded');
  const refresh = (await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { isError?: boolean; content: { text: string }[] };
  assert.equal(refresh.isError, true);
  assert.match(refresh.content[0].text, /not enabled/);
  await h.close();
});

test('a live message landing mid-drain queues behind the held ones instead of burying them', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-drain-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'dr' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.close();

    // Session 2: the sweep is slow, and the first held message carries an
    // attachment whose fetch stalls once the drain reaches it.
    let releaseSweep!: () => void;
    const sweepGate = new Promise<void>((r) => { releaseSweep = r; });
    let releaseFetch!: () => void;
    const fetchGate = new Promise<void>((r) => { releaseFetch = r; });
    const second = harness({
      stateDir: dir, sessionId: 'dr',
      attachments: {
        source: { async fetch() { await fetchGate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
        inline: { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 },
      },
    });
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { await sweepGate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    // The host reopens the channel at boot (without history, as AF does).
    await second.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    second.adapter.emit!({
      ...streamMsg(20, { text: 'see the log' }),
      metadata: { topic: 'deploys', mentioned: false, isDM: false, attachments: [{ path: '/user_uploads/1/a/run.log', name: 'run.log', mimeType: 'text/plain', isImage: false }] },
    });
    second.adapter.emit!(streamMsg(21));
    assert.equal(second.server.isLive, false);
    releaseSweep();
    // The drain is now stalled on 20's attachment fetch when a live message lands.
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(second.server.isLive, false, 'delivery stays gated until the buffer is empty');
    second.adapter.emit!(streamMsg(22));
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(second.incoming.length, 0, 'nothing overtakes the held messages');
    releaseFetch();
    await until(() => second.incoming.length === 3, 'all three, in order');
    assert.deepEqual(second.incoming.map((m) => m.messageId), ['20', '21', '22']);
    assert.ok(second.incoming[0].content.some((c) => c.type === 'text' && /run\.log/.test(c.text)), 'the stalled attachment was inlined');
    await until(() => second.server.delivery.watermark('zulip:general') === 22, 'watermark 22');
    assert.equal(second.server.isLive, true);
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
    await new Promise((r) => setTimeout(r, 25));
    assert.equal(h.incoming.length, 1);
    assert.equal(h.pushed.length, 0);
    assert.equal(h.server.delivery.watermark('zulip:general'), 1);

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
    h.server.delivery.advance('zulip:dev', 50);
    h.server.delivery.save();
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
    await new Promise((r) => setTimeout(r, 25));
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
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(h.incoming.length, 0, 'withheld at send time, not only at enqueue');
    assert.equal(h.server.delivery.watermark('zulip:general'), undefined);
    assert.equal(h.server.delivery.floor('zulip:general'), 1, 'held, not forgotten: not heard is not the same as heard');

    await h.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await until(() => h.incoming.length === 1, 'replayed once the feature set is back');
    assert.equal(h.incoming[0].messageId, '1');
    assert.equal((h.incoming[0].metadata as { recovered: boolean }).recovered, true);
    await until(() => h.server.delivery.watermark('zulip:general') === 1, 'watermark 1');
    await h.close();
  } finally {
    console.error = original;
  }
});

test('a channel_open with backscroll during the sweep window does not hide the offline gap from the sweep', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-sweep-anchor-'));
  try {
    const first = harness({ stateDir: dir, sessionId: 'sa' });
    await initialize(first, true);
    await settled(first);
    await first.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:general', type: 'zulip', address: {} });
    first.adapter.emit!(streamMsg(3));
    await until(() => first.incoming.length === 1, 'delivery');
    await first.close();

    // Session 2: the sweep's fetch (the first one) is slow; meanwhile the
    // agent opens the channel asking for the two newest messages.
    const second = harness({ stateDir: dir, sessionId: 'sa', history: [4, 5, 6, 7, 8].map((id) => streamMsg(id)) });
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    let calls = 0;
    const realFetch = second.adapter.fetchHistory!.bind(second.adapter);
    second.adapter.fetchHistory = async (channelId, query) => { if (calls++ === 0) await gate; return realFetch(channelId, query); };
    await initialize(second, true);
    await second.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: FULL_GRANT });
    await awaitRegistered(second, false);
    await until(() => calls === 1, 'the sweep to start fetching');
    const opened = (await second.host.sendRequest(method.CHANNELS_OPEN, {
      channelId: 'zulip:general', type: 'zulip', address: {}, history: { limit: 2 },
    })) as { history?: IncomingChannelMessage[] };
    assert.deepEqual(opened.history!.map((m) => m.messageId), ['7', '8']);
    assert.equal(second.server.delivery.watermark('zulip:general'), 8, 'the backscroll moved the live watermark');
    release();
    await until(() => second.pushed.length === 1, 'the sweep still delivers from where the connection began');
    const block = (second.pushed[0].payload.content[0] as { text: string }).text;
    for (const id of [4, 5, 6]) assert.match(block, new RegExp(`id=${id}\\]`));
    await second.close();
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
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

test('shutdown waits for a delivery still fetching its attachment, then flushes it', async () => {
  let release!: () => void;
  const gate = new Promise<void>((r) => { release = r; });
  const h = harness({
    attachments: {
      source: { async fetch() { await gate; return { buf: Buffer.from('log line'), mimeType: 'text/plain', overflow: false }; } },
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
  await new Promise((r) => setTimeout(r, 10));
  assert.equal(h.server.channelManager.pendingCount(), 0, 'still building its content');
  const done = h.server.shutdown();
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(h.incoming.length, 0);
  release();
  await done;
  assert.equal(h.incoming.length, 1);
  assert.equal(h.server.delivery.watermark('zulip:general'), 1);
  await h.close();
});

test('catch-up pages past a page holding nothing but the bot\'s own messages', async () => {
  const h = harness({ history: [streamMsg(101), streamMsg(102), streamMsg(103, { mentioned: true })] });
  // The adapter withholds the bot's own messages after fetching; 101 and
  // 102 are its own, and the page size is two.
  const lastWithheldId = 102;
  const realFetch = h.adapter.fetchHistory!.bind(h.adapter);
  h.adapter.fetchHistory = async (channelId, query) => {
    const page = await realFetch(channelId, query);
    return { ...page, messages: page.messages.filter((m) => Number(m.messageId) > lastWithheldId) };
  };
  h.adapter.pageCap = 2;
  await initialize(h, true);
  await settled(h);
  h.server.delivery.advance('zulip:general', 100);
  h.adapter.systemEvent!({ kind: 'gap', text: 'Queue expired.', metadata: { platform: 'zulip' } });
  await until(() => h.pushed.length === 1, 'the mention beyond the self-only page');
  assert.match((h.pushed[0].payload.content[0] as { text: string }).text, /id=103\]/);
  assert.deepEqual(h.adapter.historyCalls.map((c) => c.query.afterMessageId), ['100', '102'], 'the cursor advanced on what was scanned');
  assert.equal((h.pushed[0].origin as { truncated?: boolean }).truncated, undefined);
  assert.equal(h.server.delivery.watermark('zulip:general'), 103);
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
  // Against a host that reconciles before it answers, the answer to an
  // announcement made inside a tool call cannot arrive until the call returns,
  // because this server serves one request at a time. So the host acting on
  // the channel is the only confirmation in time.
  const h = harness({ announceTimeoutMs: 80 });
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'reconcile';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const first = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { pendingAnnouncement?: string[] };
  assert.deepEqual(first.pendingAnnouncement, ['zulip:ops'], 'the answer could not arrive in time');
  await until(() => h.policy.reconcileOpens.length > 0, "the host's reconcile-time open is served");

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

  // A host that cannot answer in time: the tool says so rather than logging it.
  h.policy.changedAnswer = 'reconcile';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM, { ...LATE_STREAM, id: 'zulip:infra', label: '#infra', address: { stream_name: 'infra', stream_id: 13 } }];
  const stalled = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'listen', arguments: { channels: ['infra'] } })) as { content: { text: string }[] }).content[0].text) as
    { registration: { announced: string[]; local: string[] } };
  assert.deepEqual(stalled.registration.local, ['zulip:infra'], 'the tool result carries the outcome; the note is the runtime\'s (see toolRuntime tests)');
  await until(() => h.policy.reconcileOpens.some((o) => o.channelId === 'zulip:infra'), "the host's reconcile-time open is served");
  await h.close();
});

test('a host that reconciles before it answers does not cost the channel — the reported failure (#20)', async () => {
  // A host that answers channels/changed only after reconciling can't answer
  // an announcement made inside a tool call until the tool returns, since this
  // server serves one request at a time.
  const h = harness({ announceTimeoutMs: 80 });
  await initialize(h, true);
  await settled(h);
  h.policy.changedAnswer = 'reconcile';
  h.adapter.visible = [DESCRIPTOR, LATE_STREAM];

  const report = JSON.parse(((await h.host.sendRequest('tools/call', { name: 'refresh_channels', arguments: {} })) as { content: { text: string }[] }).content[0].text) as
    { added: string[]; pendingAnnouncement?: string[]; note: string };
  assert.deepEqual(report.added, [], 'the answer never arrived in time');
  assert.deepEqual(report.pendingAnnouncement, ['zulip:ops'], 'and the agent is told exactly that');

  // The channel is usable anyway: the agent can open what it was told about.
  const opened = (await h.host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:ops', type: 'zulip', address: {} })) as { channel: ChannelDescriptor };
  assert.equal(opened.channel.id, 'zulip:ops');

  // And the host's own reconcile-time open is answered once the tool call
  // returns, because the descriptor was recorded before the announcement went
  // out rather than after the answer came back.
  await until(() => h.policy.reconcileOpens.length > 0, "the host's own channels/open is served");
  assert.deepEqual(h.policy.reconcileOpens, [{ channelId: 'zulip:ops', ok: true, detail: 'opened' }]);
  await h.close();
});

test('channels/open re-discovery settles against a host that reconciles before it answers (#20)', async () => {
  // The re-discovery runs inside the open, so such a host cannot answer its
  // announcement until the open returns.
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
  assert.equal(h.server.delivery.watermark('zulip:general'), 10);

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
  assert.equal(h.server.delivery.watermark('zulip:general'), 10, 'a change never advances the watermark');

  // A message the host never accepted: its edit is noise.
  h.adapter.change!(change({ messageId: '11', messageIds: ['11'] }));
  await new Promise((r) => setTimeout(r, 30));
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
  await until(() => h.server.delivery.heldIds('zulip:general').includes(13), 'the refused message is held');
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
  await new Promise((r) => setTimeout(r, 30));
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
