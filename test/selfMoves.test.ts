/** Real event loop, adapter, server and MCPL wire; only Zulip is a stand-in. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { createServer, get } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { McplConnection, method, type IncomingChannelMessage } from '@animalabs/mcpl-core';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import { ZulipMcplServer } from '../src/server.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { FiltersPlane, type ZulipFilters } from '../src/filters.ts';
import type { MessageChangeEvent } from '../src/platforms/adapter.ts';

const SELF = 99;
const GENERAL = 'zulip:general';
const ARCHIVE = 'zulip:archive';

async function until(predicate: () => boolean, what: string) {
  const deadline = Date.now() + 2000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise(r => setTimeout(r, 5));
  }
}

function raw(id: number, topic = 'deploys') {
  return {
    id, type: 'stream', stream_id: 7, display_recipient: 'general', subject: topic,
    sender_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
    timestamp: 1_700_000_000 + id, content: '@**Bot** rename this topic', flags: ['mentioned'],
  };
}

async function fixture(policy: ZulipFilters = {}, invisibleDestination = false, moveReadTimeoutMs?: number) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-self-move-'));
  const filtersPath = join(dir, 'filters.json');
  writeFileSync(filtersPath, JSON.stringify({ reactionChannels: [GENERAL, ARCHIVE], ...policy }));
  const filters = new FiltersPlane(filtersPath, {});
  filters.start();
  const pending: any[] = [];
  let eventId = 0;
  let registrations = 0;
  let expireQueue = false;
  let failReads = false;
  let placementReader: ((id: number) => Promise<any>) | undefined;
  const placementReads: number[] = [];
  let historyMessages: any[] = [];
  const sent: any[] = [];
  const typing: any[] = [];
  const placements = new Map<number, any>();
  const deletedIds = new Set<number>();
  let patchHook: ((event: any) => Promise<void>) | undefined;
  const api = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk);
    const fields = new URLSearchParams(Buffer.concat(chunks).toString());
    const id = Number(req.url!.split('/').at(-1));
    const original = placements.get(id) ?? raw(id);
    const mode = fields.get('propagate_mode') ?? 'change_one';
    const messageIds: number[] = [];
    for (const n of [20, 21, 30]) {
      const m = placements.get(n) ?? raw(n);
      if (n !== id && (mode === 'change_one' || m.subject.toLowerCase() !== original.subject.toLowerCase() ||
          m.stream_id !== original.stream_id || (mode === 'change_later' && n < id))) continue;
      messageIds.push(n);
      placements.set(n, { ...m,
        ...(fields.has('topic') ? { subject: fields.get('topic')!.trim() } : {}),
        ...(fields.has('stream_id') ? { stream_id: Number(fields.get('stream_id')),
          display_recipient: Number(fields.get('stream_id')) === 7 ? 'general'
            : Number(fields.get('stream_id')) === 8 ? 'archive' : 'new-stream' } : {}),
      });
    }
    await patchHook?.(move(messageIds, {
      message_id: id, stream_id: original.stream_id, orig_subject: original.subject, propagate_mode: mode,
      subject: fields.has('topic') ? fields.get('topic')!.trim() : undefined,
      ...(fields.has('stream_id') && Number(fields.get('stream_id')) !== original.stream_id
        ? { new_stream_id: Number(fields.get('stream_id')) } : {}),
    }));
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ result: 'success', msg: '' }));
  });
  api.listen(0, '127.0.0.1');
  await once(api, 'listening');
  const realm = `http://127.0.0.1:${(api.address() as { port: number }).port}`;
  let streamReads = 0;
  const client = {
    streams: { retrieve: async () => {
      streamReads++;
      return { result: 'success', streams: [
        { name: 'general', stream_id: 7 }, { name: 'archive', stream_id: 8 },
      ] };
    } },
    messages: {
      retrieve: async () => ({ result: 'success', messages: historyMessages, found_newest: true, found_oldest: true }),
      getById: async ({ message_id }: any) => {
        placementReads.push(message_id);
        if (placementReader) return placementReader(message_id);
        if (deletedIds.has(message_id)) return { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' };
        if (failReads) return { result: 'error', msg: 'Placement unavailable' };
        const message = placements.get(message_id) ?? raw(message_id);
        if (invisibleDestination && message.stream_id !== 7) return { result: 'error', msg: 'Not visible' };
        return { result: 'success', message };
      },
      deleteById: async ({ message_id }: any) => {
        deletedIds.add(message_id);
        placements.delete(message_id);
        return { result: 'success' };
      },
      send: async (args: any) => { sent.push(args); return { result: 'success', id: 1000 + sent.length }; },
    },
    typing: { send: async (args: any) => { typing.push(args); return { result: 'success' }; } },
    users: { me: { subscriptions: { add: async () => ({ result: 'success', already_subscribed: {} }) } } },
    queues: { register: async () => ({ queue_id: `q-${++registrations}`, last_event_id: -1 }) },
    events: { retrieve: async () => {
      await new Promise(r => setTimeout(r, 5));
      if (expireQueue) {
        expireQueue = false;
        return { result: 'error', code: 'BAD_EVENT_QUEUE_ID' };
      }
      return { events: pending.splice(0) };
    } },
  };
  const adapter = new ZulipAdapter(client, SELF, 'test', { filters, dmDiscoveryLimit: 0 });
  const tools = new ZulipToolRuntime({ client, selfUserId: SELF, sessionId: 'test', realm, authHeader: 'Basic dGVzdDp0ZXN0' }, dir, { moveReadTimeoutMs });
  const server = new ZulipMcplServer(adapter, tools, {
    mcplEnabled: true, filters, stateDir: null, batchWindowMs: 5, formatTime: () => 'T',
  });
  const reconciled: MessageChangeEvent[] = [];
  const forward = adapter.onSelfChanged;
  adapter.onSelfChanged = change => { forward?.(change); reconciled.push(change); };
  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const host = McplConnection.fromStreams(toHost, toServer);
  const incoming: IncomingChannelMessage[] = [];
  const pushes: any[] = [];
  host.on('request', req => {
    const p = req.params as any;
    if (req.method === method.CHANNELS_REGISTER) {
      host.sendResponse(req.id, { results: p.channels.map((c: any) => ({ id: c.id, accepted: true })) });
    } else if (req.method === method.CHANNELS_INCOMING) {
      incoming.push(...p.messages);
      host.sendResponse(req.id, { results: p.messages.map((m: any) => ({ messageId: m.messageId, accepted: true })) });
    } else if (req.method === method.PUSH_EVENT) {
      pushes.push(p);
      host.sendResponse(req.id, { accepted: true });
    } else {
      host.sendResponse(req.id, {});
    }
  });
  const served = server.serve(McplConnection.fromStreams(toServer, toHost));
  await host.sendRequest(method.INITIALIZE, {
    protocolVersion: '2025-03-26',
    capabilities: { experimental: { mcpl: { version: '0.5', channels: true } } },
    clientInfo: { name: 'test-host', version: '0' },
  });
  host.sendNotification('notifications/initialized');
  await host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: [
    'tools', 'pushEvents', 'channels.register', 'channels.lifecycle', 'channels.publish',
    'channels.incoming', 'channels.typing', 'channels.acknowledge', 'channels.streaming',
    'contextHooks.beforeInference.inject.beforeUser',
  ] });
  await until(() => server.isLive, 'live delivery');
  return {
    host, server, tools, sent, typing, incoming, pushes, reconciled, placements,
    outstandingMoves() { assert.equal('selfMoves' in adapter, false); return 0; },
    hasReference(channelId: string) { return (server as any).channelManager.lastIncoming.has(channelId); },
    routingReference(channelId = GENERAL) { return (server as any).channelManager.lastIncoming.get(channelId); },
    markDeleted(id: number) { deletedIds.add(id); placements.delete(id); },
    cachedPlacement(id: number) { return (adapter as any).seen.get(id); },
    forgetPlacement(id: number) { (adapter as any).seen.delete(id); },
    registrations() { return registrations; },
    expireQueue() { expireQueue = true; },
    failReads(value: boolean) { failReads = value; },
    placementReads,
    streamReads() { return streamReads; },
    historyWith(messages: any[]) { historyMessages = messages; },
    readHistory() { return adapter.fetchHistory(GENERAL, { limit: 10 }); },
    readContext() { return adapter.fetchContext(GENERAL, undefined, 10); },
    readPlacementWith(reader: (id: number) => Promise<any>) { placementReader = reader; },
    stopEvents() { adapter.stopEvents(); },
    onPatch(hook?: (event: any) => Promise<void>) { patchHook = hook; },
    async callMove(args: Record<string, unknown>) {
      const result = await host.sendRequest('tools/call', { name: 'move_messages', arguments: args });
      assert.notEqual((result as any).isError, true);
      return result;
    },
    feed(...events: any[]) { pending.push(...events.map(event => ({ ...event, id: ++eventId }))); },
    async open(channelId = GENERAL) {
      await host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {}, backscroll: 0 });
    },
    async publish(channelId = GENERAL) {
      await host.sendRequest(method.CHANNELS_PUBLISH, { channelId, conversationId: 'c', content: [{ type: 'text', text: 'Done' }] });
    },
    async type(op: 'start' | 'stop', channelId = GENERAL) {
      await host.sendRequest(method.CHANNELS_TYPING, { channelId, op });
    },
    async close() {
      toServer.end();
      await served;
      await server.shutdown();
      host.close();
      filters.stop();
      api.close();
      api.closeAllConnections();
      await once(api, 'close');
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

function move(messageIds = [20, 21], extra = {}) {
  return {
    type: 'update_message', message_id: 20, message_ids: messageIds, user_id: SELF,
    edit_timestamp: 1_700_002_000, propagate_mode: 'change_one', stream_id: 7, orig_subject: 'deploys', subject: 'deploys-2026',
    ...extra,
  };
}

for (const opened of [false, true]) {
  test(`self topic rename routes publish and running typing to the new topic on ${opened ? 'an open' : 'a closed'} channel`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    if (opened) await f.open();
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
    await f.type('start');
    f.onPatch(async event => { f.feed(event); await until(() => f.reconciled.length === 1, 'early echo'); });
    await f.callMove({ message_id: 21, topic: 'deploys-2026' });
    await f.publish();
    await f.type('start');
    await f.type('stop');
    assert.deepEqual(f.sent.map(s => [s.to, s.topic]), [['general', 'deploys-2026']]);
    assert.deepEqual(f.typing.map(s => [s.op, s.topic]), [
      ['start', 'deploys'], ['stop', 'deploys'], ['start', 'deploys-2026'], ['stop', 'deploys-2026'],
    ]);
    assert.equal(f.incoming.length + f.pushes.length, 1, 'no change marker or breadcrumb is needed');
  });
}

test('self moves leave a newer unrelated conversation and its typing alone', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) },
    { type: 'message', flags: ['mentioned'], message: raw(30, 'support') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'newest conversation accepted');
  await f.type('start');
  f.feed(move());
  await until(() => f.reconciled.length === 1, 'unrelated self move reconciled');
  await f.publish();
  await f.type('stop');
  assert.equal(f.sent[0].topic, 'support');
  assert.deepEqual(f.typing.map(s => [s.op, s.topic]), [['start', 'support'], ['stop', 'support']]);
});

test('self cross-stream moves transfer reply hints to the destination channel', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.callMove({ message_id: 21, stream_id: 8, topic: 'deploys-2026' });
  await f.publish(ARCHIVE);
  await f.type('start', ARCHIVE);
  assert.deepEqual(f.sent.map(s => [s.to, s.topic]), [['archive', 'deploys-2026']]);
  assert.deepEqual(f.typing.map(s => [s.stream_id, s.topic]), [[8, 'deploys-2026']]);
});

test('a source typing run can still be stopped after a self cross-stream move', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.type('start');
  f.feed(move([20, 21], { new_stream_id: 8 }));
  await until(() => f.reconciled.length === 1, 'cross-stream move reconciled');
  await f.type('stop');
  assert.deepEqual(f.typing.map(s => [s.stream_id, s.op, s.topic]), [
    [7, 'start', 'deploys'], [7, 'stop', 'deploys'],
  ], 'the source stop cancels the original run rather than typing in the fallback topic');
});

for (const restriction of ['none', 'allowlist', 'mute']) {
  test(`later reactions and edits after a self move use destination filtering (${restriction})`, async t => {
    const f = await fixture(restriction === 'allowlist' ? { streams: ['general'] }
      : restriction === 'mute' ? { mutedStreams: ['archive'] } : {});
    t.after(() => f.close());
    await f.open();
    if (restriction !== 'allowlist') await f.open(ARCHIVE);
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(20) },
      { type: 'message', flags: ['mentioned'], message: raw(21) },
      { type: 'message', flags: [], message: { ...raw(22), sender_id: SELF,
        sender_full_name: 'Bot', sender_email: 'bot@example.com', content: 'original bot message' } });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'original messages accepted');
    for (const id of [20, 21]) f.placements.set(id, { ...raw(id, 'deploys-2026'), stream_id: 8, display_recipient: 'archive' });
    f.feed(
      move([20, 21], { new_stream_id: 8 }),
      { type: 'reaction', op: 'add', message_id: 21, user_id: 7,
        emoji_name: 'thumbs_up', emoji_code: '1f44d', reaction_type: 'unicode_emoji' },
      { type: 'update_message', message_id: 21, message_ids: [21], user_id: 7, stream_id: 8,
        edit_timestamp: 1_700_002_001, content: '@**Bot** edited later', orig_content: 'original', flags: ['mentioned'] },
      // A bot-owned edit observes completion of queued changes without delivery;
      // reactions independently await the earlier change-chain tail.
      { type: 'update_message', message_id: 22, message_ids: [22], user_id: SELF, stream_id: 7,
        edit_timestamp: 1_700_002_002, content: 'self edited', orig_content: 'original bot message', flags: [] },
    );
    await until(() => f.reconciled.length === 2, 'all queued changes reconciled');
    if (restriction === 'none') await until(() => f.incoming.length === 4, 'destination markers delivered');
    await new Promise(r => setTimeout(r, 20));
    const markers = f.incoming.filter(m => m.metadata?.reaction || m.metadata?.change);
    assert.deepEqual(markers.map(m => [m.channelId, m.metadata?.reaction ? 'reaction' : m.metadata?.change]),
      restriction === 'none' ? [[ARCHIVE, 'reaction'], [ARCHIVE, 'edit']] : []);
    assert.equal(f.pushes.length, 0, 'self changes do not wake the agent');
  });
}

// R2-3: no refresh is needed to stop the topic where typing began.
test('a stop immediately after a self rename cancels typing in the original topic', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.type('start');
  f.feed(move());
  await until(() => f.reconciled.length === 1, 'rename reconciled');
  await f.type('stop');
  assert.deepEqual(f.typing.map(s => [s.op, s.topic]), [['start', 'deploys'], ['stop', 'deploys']]);
});

for (const busyDestination of [false, true]) {
  test(`cross-stream moves refuse source publishes and confirm by explicit topic (${busyDestination ? 'busy' : 'empty'} destination)`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open(ARCHIVE);
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    if (busyDestination) f.feed({ type: 'message', flags: ['mentioned'], message: {
      ...raw(30, 'ops'), stream_id: 8, display_recipient: 'archive',
    } });
    await until(() => f.server.delivery.watermark(GENERAL) === 21 &&
      (!busyDestination || f.server.delivery.watermark(ARCHIVE) === 30), 'conversations accepted');
    await f.type('start');
    const result = await f.tools.handleToolCall('move_messages', { message_id: 21, stream_id: 8, propagate_mode: 'change_all' });
    await until(() => f.reconciled.length === 1, 'cross-stream move reconciled');
    await f.type('start');
    await assert.rejects(f.publish(), /(?:moved|hint:).*zulip:archive.*deploys.*send_message/);
    await f.type('stop');
    assert.deepEqual(f.typing.map(s => [s.stream_id, s.op, s.topic]), [
      [7, 'start', 'deploys'], [7, 'stop', 'deploys'],
    ]);
    assert.deepEqual(f.sent, [], 'no fallback write to mcpl');
    if (busyDestination) {
      await f.publish(ARCHIVE);
      assert.equal(f.sent[0].topic, 'ops', 'an unrelated newer destination conversation is not overwritten');
    }
    await f.tools.handleToolCall('send_message', { type: 'stream', to: result.moved_to.stream, topic: result.moved_to.topic, content: 'Done' });
    assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['archive', 'deploys']);
  });
}

for (const mode of ['change_one', 'change_later', 'change_all'] as const) {
  for (const hintId of [20, 21, 30]) {
    test(`tool success reconciles ${mode} without an echo (hint ${hintId})`, async t => {
      const f = await fixture();
      t.after(() => f.close());
      const message = raw(hintId, hintId === 30 ? 'support' : 'deploys');
      f.placements.set(hintId, message);
      f.feed({ type: 'message', flags: ['mentioned'], message });
      await until(() => f.server.delivery.watermark(GENERAL) === hintId, 'mention accepted');
      await f.tools.handleToolCall('move_messages', { message_id: 21, topic: 'deploys', propagate_mode: mode });
      if (mode === 'change_all' && hintId === 20) await assert.rejects(f.publish(), /unconfirmed topic/);
      else { await f.publish(); assert.equal(f.sent.pop().topic, message.subject, 'unaffected/anchor route is direct'); }
      await f.tools.handleToolCall('move_messages', { message_id: 21, topic: '  renamed ', propagate_mode: mode });
      if (mode === 'change_all' && hintId === 20) {
        // N1: the first result already made this non-exact placement unknown.
        await assert.rejects(f.publish(), /this conversation may have moved/);
        return;
      }
      await f.publish();
      const affected = hintId === 21 || (mode === 'change_all' && hintId === 20);
      assert.equal(f.sent[0].topic, affected ? 'renamed' : message.subject);
    });
  }
}

test('cross-stream success refuses a co-moved hint until a new message while reactions locate placement', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.type('start');
  const result = await f.tools.handleToolCall('move_messages', { message_id: 20, stream_id: 8, propagate_mode: 'change_later' });
  assert.deepEqual(result.moved_to, { channelId: ARCHIVE, stream: 'archive', topic: 'deploys' });
  await assert.rejects(f.publish(), /(?:moved|hint:).*zulip:archive.*unconfirmed topic/);
  await f.type('start');
  f.feed({ type: 'reaction', op: 'add', message_id: 21, user_id: 7,
    emoji_name: 'thumbs_up', emoji_code: '1f44d', reaction_type: 'unicode_emoji' });
  await until(() => f.incoming.some(m => m.metadata?.reaction), 'reaction delivered');
  assert.equal(f.incoming.find(m => m.metadata?.reaction)!.channelId, ARCHIVE);
  f.feed(move([20, 21], { new_stream_id: 8, subject: undefined, propagate_mode: 'change_later' }));
  await until(() => f.reconciled.some(c => c.routingUncertain), 'echo confirms co-moved ids');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.hasReference(ARCHIVE), false, 'co-hint is not copied to destination');
  // A fresh real message re-establishes the destination conversation.
  f.feed({ type: 'message', flags: ['mentioned'], message: { ...f.placements.get(21), id: 31 } });
  await until(() => f.server.delivery.watermark(ARCHIVE) === 31, 'new destination mention');
  await f.publish(ARCHIVE);
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['archive', 'deploys']);
  assert.deepEqual(f.typing.map(s => [s.op, s.topic]), [['start', 'deploys'], ['stop', 'deploys']]);
});

test('an unresolved destination echo blocks source routing and invalidates old placement', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  f.placements.set(21, { ...raw(21), stream_id: 9, display_recipient: 'new-stream' });
  f.feed(move([20, 21], { new_stream_id: 9, subject: undefined }));
  await until(() => f.reconciled.length === 1, 'unresolved move reconciled');
  await assert.rejects(f.publish(), /(?:moved|hint:).*stream unknown/);
  f.feed({ type: 'reaction', op: 'add', message_id: 21, user_id: 7, emoji_name: 'eyes' });
  await new Promise(r => setTimeout(r, 30));
  assert.ok(!f.incoming.some(m => m.metadata?.reaction && m.channelId === GENERAL), 'no stale source reaction');
});

test('tool success with an invisible unknown destination still blocks the source', async t => {
  const f = await fixture({}, true);
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  const result = await f.tools.handleToolCall('move_messages', { message_id: 20, stream_id: 9, propagate_mode: 'change_all' });
  assert.equal(result.result, 'success');
  assert.equal(result.moved_to.channelId, null);
  await assert.rejects(f.publish(), /(?:moved|hint:).*stream unknown/);
});

test('a result stream name remains only a co-hint even after its echo', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.tools.handleToolCall('move_messages', { message_id: 20, stream_id: 9, propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), /(?:moved|hint:).*zulip:new-stream.*unconfirmed topic/);
  f.feed(move([20, 21], { new_stream_id: 9, subject: undefined, propagate_mode: 'change_all' }));
  await until(() => f.reconciled.some(c => c.routingUncertain), 'echo confirmation');
  await assert.rejects(f.publish(), /(?:moved|hint:).*zulip:new-stream.*unconfirmed topic/);
  assert.equal(f.hasReference('zulip:new-stream'), false, 'co-hint is not copied to destination');
  assert.equal(f.sent.length, 0);
});

test('delayed first self echo refuses instead of undoing a second tool-confirmed rename', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.callMove({ message_id: 21, topic: 'first' });
  await f.callMove({ message_id: 21, topic: 'second' });
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'second');
  f.feed(move([21], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.some(c => c.routingUncertain), 'delayed first echo processed');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.placements.get(21).subject, 'second', 'stored placement did not change');
  assert.equal(f.sent.length, 1, 'old echo must refuse, never authorize a historical topic');
});

const modes = ['change_one', 'change_later', 'change_all'] as const;
for (const firstMode of modes) for (const secondMode of modes) {
  for (const order of ['early', 'late', 'reverse', 'first-early', 'second-early'] as const) {
    test(`move echoes preserve split ranges: ${firstMode}/${secondMode}, ${order}`, async t => {
      const f = await fixture();
      t.after(() => f.close());
      await f.open();
      for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
      await until(() => f.server.delivery.watermark(GENERAL) === 30, 'messages accepted');
      const echoes: any[] = [];
      const delayed: any[] = [];
      f.onPatch(async event => {
        echoes.push(event);
        if (order === 'early' || (order === 'first-early' && echoes.length === 1) ||
            (order === 'second-early' && echoes.length === 2)) {
          const before = f.reconciled.length;
          f.feed(event);
          await until(() => f.reconciled.length > before, 'echo before PATCH response');
        } else delayed.push(event);
      });
      await f.callMove({ message_id: 21, topic: ' first ', propagate_mode: firstMode });
      await f.callMove({ message_id: 21, topic: 'second', propagate_mode: secondMode });
      const expected = [20, 21, 30].map(id => (f.placements.get(id) ?? raw(id)).subject);
      const hintMove = firstMode === 'change_one' ? 0 : secondMode === 'change_one' ? 1 : 2;
      const assertRouting = async () => {
        if (hintMove) {
          // N1: an early range echo loses placement; a second range cannot
          // infer a destination from an already-unknown non-exact reference.
          if (secondMode !== 'change_one' || order === 'early' || order === 'first-early') {
            await assert.rejects(f.publish(), (e: any) => {
              assert.match(e.message, /this conversation may have moved; reply with send_message/);
              assert.equal(e.data.stream, null);
              assert.deepEqual(e.data.movedTo, { channelId: null, topic: null });
              return true;
            });
          } else {
            await assert.rejects(f.publish(), /zulip:general.*unconfirmed topic.*get_stream_topics/);
          }
        } else {
          await f.publish();
          assert.equal(f.sent.at(-1).topic, expected[2], 'never follow an older operation');
        }
      };
      await assertRouting();
      assert.equal(echoes[0].edit_timestamp, echoes[1].edit_timestamp, 'same-second operations are deliberate');
      if (order !== 'early') {
        for (const echo of order === 'reverse' ? [...delayed].reverse() : delayed) {
          const before = f.reconciled.length;
          f.feed(echo);
          await until(() => f.reconciled.length > before, 'delayed echo');
          assert.equal(f.cachedPlacement(21).topic, expected[1], 'echo cannot overwrite the tool-result anchor');
          await assertRouting();
        }
      }
      // Content-only edits read cached placement rather than the tool fixture's
      // stored placements. Check every side of each partial propagation range.
      for (const id of [20, 21, 30]) f.feed({ type: 'update_message', message_id: id, message_ids: [id],
        user_id: 7, stream_id: 7, edit_timestamp: 1_700_002_000,
        content: `edited ${id}`, orig_content: `old ${id}`, flags: ['mentioned'] });
      await until(() => f.incoming.filter(m => m.metadata?.change === 'edit').length === 3, 'placed edits');
      assert.deepEqual(f.incoming.filter(m => m.metadata?.change === 'edit').map(m => m.metadata?.topic), expected);
      if (hintMove) {
        f.feed({ type: 'message', flags: ['mentioned'], message: { ...f.placements.get(30), id: 31 } });
        await until(() => f.incoming.some(m => m.messageId === '31' && !m.metadata?.change), 'new conversation message');
        await f.publish();
        assert.equal(f.sent.at(-1).topic, expected[2], 'only a real message restores co-moved routing');
      }
    });
  }
}

test('old cross-stream range echo cannot move newer reactions or edits back', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'messages accepted');
  const echoes: any[] = [];
  f.onPatch(async event => { echoes.push(event); });
  await f.callMove({ message_id: 20, topic: 'first', stream_id: 8, propagate_mode: 'change_all' });
  await f.callMove({ message_id: 21, topic: 'second', stream_id: 7, propagate_mode: 'change_one' });
  f.feed(echoes[0], echoes[1]);
  await until(() => f.reconciled.filter(c => c.routingUncertain).length >= 2, 'both echoes confirmed');
  for (const id of [20, 21, 30]) f.feed(
    { type: 'reaction', op: 'add', message_id: id, user_id: 7, emoji_name: 'eyes' },
    { type: 'update_message', message_id: id, message_ids: [id], user_id: 7,
      edit_timestamp: 1_700_002_000, content: `edited ${id}`, orig_content: `old ${id}`, flags: ['mentioned'] },
  );
  await until(() => f.incoming.filter(m => m.metadata?.reaction).length === 3 &&
    f.incoming.filter(m => m.metadata?.change === 'edit').length === 3, 'placed reactions and edits');
  for (const kind of ['reaction', 'edit']) {
    const markers = f.incoming.filter(m => kind === 'reaction' ? m.metadata?.reaction : m.metadata?.change === kind)
      .sort((a, b) => Number(a.metadata?.targetMessageId) - Number(b.metadata?.targetMessageId));
    assert.deepEqual(markers.map(m => [m.metadata?.targetMessageId, m.channelId]), [
      ['20', ARCHIVE], ['21', GENERAL], ['30', ARCHIVE],
    ]);
  }
  assert.deepEqual(f.incoming.filter(m => m.metadata?.change === 'edit')
    .sort((a, b) => Number(a.metadata?.targetMessageId) - Number(b.metadata?.targetMessageId))
    .map(m => m.metadata?.topic), ['first', 'second', 'first']);
});

for (const actor of [SELF, 7]) {
  test(`genuine later move still applies after tool confirmations (actor ${actor})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    await f.open(ARCHIVE);
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
    await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
    await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'second' });
    // Same-second genuine change, including a content change not made by the
    // move tool: it must not be mistaken for one of its delayed echoes.
    f.placements.set(21, { ...raw(21, 'external'), stream_id: 8, display_recipient: 'archive' });
    f.feed(move([21, 30], { message_id: 21, user_id: actor, orig_subject: 'second', subject: 'external', new_stream_id: 8 }),
      { type: 'reaction', op: 'add', message_id: 21, user_id: 7, emoji_name: 'eyes' },
      { type: 'update_message', message_id: 21, message_ids: [21], user_id: 7, stream_id: 8,
        edit_timestamp: 1_700_002_000, content: 'edited externally', orig_content: 'old', flags: ['mentioned'] });
    await until(() => f.incoming.some(m => m.metadata?.reaction) &&
      f.incoming.some(m => m.metadata?.change === 'edit'), 'external placement observed');
    assert.equal(f.incoming.find(m => m.metadata?.reaction)!.channelId, ARCHIVE);
    const edit = f.incoming.find(m => m.metadata?.change === 'edit')!;
    assert.deepEqual([edit.channelId, edit.metadata?.topic], [ARCHIVE, 'external']);
    if (actor === SELF) await assert.rejects(f.publish(), /this conversation may have moved/);
    // Delayed tool echoes must not roll back even this later external move.
    const beforeEchoes = f.reconciled.length;
    f.feed(move([21, 30], { message_id: 21, subject: 'first' }),
      move([21, 30], { message_id: 21, orig_subject: 'first', subject: 'second' }));
    await until(() => f.reconciled.length >= beforeEchoes + 2, 'old operations fail closed');
    assert.equal(f.reconciled.at(-1).routingUncertain, true);
    await assert.rejects(f.publish(), actor === SELF ? /this conversation may have moved/ : /Conversation moved to/);
    f.feed({ type: 'update_message', message_id: 21, message_ids: [21], user_id: 7,
      edit_timestamp: 1_700_002_000, content: 'edited again', orig_content: 'old', flags: ['mentioned'] });
    await until(() => f.incoming.filter(m => m.metadata?.change === 'edit').length === 2, 'later edit placed');
    const last = f.incoming.filter(m => m.metadata?.change === 'edit').at(-1)!;
    assert.deepEqual([last.channelId, last.metadata?.topic], [ARCHIVE, 'external']);
  });
}

for (const crossStream of [false, true]) {
  test(`lost echo does not consume a genuine later identical ${crossStream ? 'cross-stream' : 'same-stream'} move`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    await f.open(ARCHIVE);
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
    await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first', ...(crossStream ? { stream_id: 8 } : {}) });
    // The tool echo is lost. Another client using the same identity cycles
    // back, then repeats the original transition in the same second.
    f.placements.set(21, raw(21));
    f.feed(move([21, 30], { message_id: 21, stream_id: crossStream ? 8 : 7,
      orig_subject: 'first', subject: 'deploys', ...(crossStream ? { new_stream_id: 7 } : {}) }));
    await until(() => f.reconciled.length >= 2, 'external move back');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    assert.equal(f.reconciled.at(-1).routingUncertain, true);
    assert.equal(f.outstandingMoves(), 0, 'intervening move invalidates the lost token');
    f.placements.set(21, { ...raw(21, 'first'), ...(crossStream ? { stream_id: 8, display_recipient: 'archive' } : {}) });
    const before = f.reconciled.length;
    f.feed(move([21, 30], { message_id: 21, subject: 'first', ...(crossStream ? { new_stream_id: 8 } : {}) }));
    await until(() => f.reconciled.length > before, 'genuine repeated transition');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    assert.equal(f.reconciled.at(-1).routingUncertain, true, 'genuine transition is not swallowed as confirmation');
    assert.equal(f.reconciled.at(-1).routingUncertain, true);
    f.feed({ type: 'reaction', op: 'add', message_id: 21, user_id: 7, emoji_name: 'eyes' },
      { type: 'update_message', message_id: 21, message_ids: [21], user_id: 7,
        edit_timestamp: 1_700_002_000, content: 'later edit', orig_content: 'old', flags: ['mentioned'] });
    await until(() => f.incoming.some(m => m.metadata?.reaction) &&
      f.incoming.some(m => m.metadata?.change === 'edit'), 'reaction/edit placement');
    assert.equal(f.incoming.find(m => m.metadata?.reaction)!.channelId, crossStream ? ARCHIVE : GENERAL);
    const edit = f.incoming.find(m => m.metadata?.change === 'edit')!;
    assert.deepEqual([edit.channelId, edit.metadata?.topic], [crossStream ? ARCHIVE : GENERAL, 'first']);
  });
}

test('24 sequential moves retain no echo tracking and old echoes cannot confirm co-hints', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  for (let i = 0; i < 24; i++) await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: `move-${i}` });
  assert.ok(f.outstandingMoves() <= 16, `retained ${f.outstandingMoves()} records`);
  // Eviction must not bring back blind replay of the oldest lost echo.
  const before = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'move-0' }));
  await until(() => f.reconciled.length > before, 'evicted-token echo processed');
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.equal(f.placements.get(21).subject, 'move-23', 'old echo never replays');
});

test('aged echoes cannot confirm co-hints without any retained tokens', async t => {
  const f = await fixture();
  t.after(() => f.close());
  t.mock.timers.enable({ apis: ['Date'], now: Date.now() });
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'second' });
  assert.equal(f.outstandingMoves(), 0);
  t.mock.timers.tick(60_001);
  const before = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length > before, 'expired-token echo processed');
  assert.equal(f.outstandingMoves(), 0);
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.equal(f.placements.get(21).subject, 'second');
});

test('queue re-registration leaves no tokens and late echoes cannot confirm co-hints', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'second' });
  assert.equal(f.outstandingMoves(), 0);
  f.expireQueue();
  await until(() => f.registrations() === 2, 'replacement queue');
  assert.equal(f.outstandingMoves(), 0);
  const before = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length > before, 'old echo after replacement');
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.equal(f.placements.get(21).subject, 'second');
});

test('stopping event delivery has no outstanding echo state', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  assert.equal(f.outstandingMoves(), 0);
  f.stopEvents();
  assert.equal(f.outstandingMoves(), 0);
});

test('content-only edits retain echo matching but supersede placement proof', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  assert.equal(f.outstandingMoves(), 0);
  f.feed({ type: 'update_message', message_id: 21, message_ids: [21], user_id: 7,
    edit_timestamp: 1_700_002_000, content: 'intervening edit', orig_content: 'old', flags: ['mentioned'] });
  await until(() => f.incoming.some(m => m.metadata?.change === 'edit'), 'content edit applied');
  assert.equal(f.outstandingMoves(), 0);
  f.failReads(true);
  const before = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length > before, 'unverifiable self move');
  await assert.rejects(f.publish(), /unconfirmed topic/, 'ordinary observations supersede pending placement proof');
  f.failReads(false);
  const next = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length > next, 'second ambiguous echo');
  await assert.rejects(f.publish(), /unconfirmed topic/, 'restoring GET availability is not a validation request');
  await f.callMove({ message_id: 30, topic: 'confirmed' });
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'confirmed');
});

test('idle time retains no echo state without another move or event', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  t.mock.timers.enable({ apis: ['Date', 'setTimeout'], now: Date.now() });
  try {
    await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
    assert.equal(f.outstandingMoves(), 0);
    t.mock.timers.tick(60_001);
    assert.equal(f.outstandingMoves(), 0, 'idle expiry must not depend on another API operation');
  } finally { t.mock.timers.reset(); }
});

test('queue replacement during a pending result never creates echo state', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  f.onPatch(async () => {
    assert.equal(f.outstandingMoves(), 0);
    f.expireQueue();
    await until(() => f.registrations() === 2, 'replacement while PATCH response pending');
    assert.equal(f.outstandingMoves(), 0);
  });
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  assert.equal(f.outstandingMoves(), 0, 'completion must not resurrect the old-generation token');
  const before = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length > before, 'echo with no retained token');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.placements.get(21).subject, 'first');
});


test('ambiguous bulk self echo has a zero placement-request budget for 1,000 IDs', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  const reads = f.placementReads.length;
  const before = f.reconciled.length;
  const ids = [21, ...Array.from({ length: 999 }, (_, i) => 1000 + i)];
  f.feed(move(ids, { message_id: 21, orig_subject: 'first', subject: 'ambiguous' }));
  await until(() => f.reconciled.length > before, 'bulk echo processed');
  assert.equal(f.placementReads.length - reads, 0, 'ambiguous echo budget is zero, including uncached IDs');
  assert.equal(f.cachedPlacement(1000), undefined, 'bulk echoes do not populate unseen messages');
  await assert.rejects(f.publish(), /unconfirmed topic/);
});

test('ambiguous self echoes leave zero live HTTP requests across events and after stop', async t => {
  const f = await fixture();
  const sockets = new Set<import('node:net').Socket>();
  let live = 0, peak = 0, requests = 0;
  const blackhole = createServer((_req, res) => {
    requests++; live++; peak = Math.max(peak, live);
    res.on('close', () => live--);
    // Deliberately never send headers or a body.
  });
  blackhole.on('connection', socket => {
    sockets.add(socket);
    socket.on('close', () => sockets.delete(socket));
  });
  blackhole.listen(0, '127.0.0.1');
  await once(blackhole, 'listening');
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    blackhole.close();
    blackhole.closeAllConnections();
    await once(blackhole, 'close');
    await f.close();
  });
  const url = `http://127.0.0.1:${(blackhole.address() as { port: number }).port}`;
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.callMove({ message_id: 21, topic: 'first' });
  f.readPlacementWith(id => new Promise((resolve, reject) => {
    const req = get(`${url}/${id}`, res => {
      res.resume();
      res.on('end', () => resolve({ result: 'error' }));
    });
    req.on('error', reject);
  }));
  for (let event = 0; event < 3; event++) {
    // A real message event re-establishes cached placement before each echo,
    // so baseline failures exercise the bulk read path, not locate().
    const beforeMessage = f.reconciled.length;
    const previousReads = f.placementReads.length;
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    // Message events and subsequent changes share one poll batch in order.
    f.feed(move([21, ...Array.from({ length: 7 }, (_, i) => 100 + event * 7 + i)],
      { message_id: 21, subject: `ambiguous-${event}` }));
    await until(() => f.reconciled.length > beforeMessage || f.placementReads.length > previousReads,
      'echo reconciled or baseline GETs started');
    if (f.placementReads.length > previousReads) {
      // On the rejected implementation, allow the 15s Promise timeout to
      // expire: it does not cancel these sockets, and the next event adds 8.
      await new Promise(r => setTimeout(r, 15_050));
      await until(() => f.reconciled.length > beforeMessage, 'baseline timeout completed');
    }
  }
  f.stopEvents();
  await new Promise(r => setTimeout(r, 25));
  assert.deepEqual({ requests, peak, liveAfterStop: live }, { requests: 0, peak: 0, liveAfterStop: 0 },
    'ambiguous-echo validation must create no requests, including after stop');
});

test('a delayed pending echo cannot roll back a newer completed tool move', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let firstEcho: any;
  f.onPatch(async event => {
    if (event.subject === 'first') { firstEcho = event; await held; }
  });
  const first = f.callMove({ message_id: 21, topic: 'first' });
  try {
    await until(() => firstEcho !== undefined, 'first PATCH held');
    // The host serializes tools/call requests. Invoke the second runtime call
    // directly to exercise genuinely overlapping local tool operations.
    await f.tools.handleToolCall('move_messages', { message_id: 21, topic: 'second' });
    assert.equal(f.cachedPlacement(21).topic, 'second');
    const before = f.reconciled.length;
    f.feed(firstEcho);
    await until(() => f.reconciled.length > before, 'old pending echo');
    assert.equal(f.cachedPlacement(21).topic, 'second', 'old pending echo never replays first');
  } finally { release(); await first; }
  await assert.rejects(f.publish(), /unconfirmed topic/, 'intervening invalidation prevents older completion confirmation');
  assert.equal(f.sent.length, 0);
});

test('an uncached ambiguous self anchor and unknown stream do not trigger fallback GETs', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'mention accepted');
  await f.callMove({ message_id: 21, propagate_mode: 'change_all', topic: 'first' });
  const reads = f.placementReads.length;
  const before = f.reconciled.length;
  f.feed(move([999, 21, 30], { message_id: 999, stream_id: 404, new_stream_id: 405, subject: 'ambiguous' }));
  await until(() => f.reconciled.length > before, 'uncached-anchor echo');
  assert.equal(f.placementReads.length - reads, 0);
  await assert.rejects(f.publish(), /unconfirmed topic/);
});


test('uncertain reply hint cannot be revalidated by a different anchor topic-only scope', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source conversation accepted');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  // Another client using the bot identity moves only the conversation source.
  f.placements.set(30, raw(30, 'external'));
  const before = f.reconciled.length;
  f.feed(move([30], { message_id: 30, orig_subject: 'first', subject: 'external' }));
  await until(() => f.reconciled.length > before, 'unmatched self move invalidates route');
  assert.equal(f.cachedPlacement(30).placementUnknown, true);
  await assert.rejects(f.publish(), /unconfirmed topic/);
  // This tool moves 20/21, not 30. The historical hint topic is not proof.
  await f.callMove({ message_id: 21, topic: 'confirmed', propagate_mode: 'change_all' });
  assert.equal(f.reconciled.at(-1).messageIds.includes('30'), false, 'no exact affected-message proof');
  assert.equal(f.cachedPlacement(30).placementUnknown, true);
  assert.equal(f.placements.get(30).subject, 'external');
  await assert.rejects(f.publish(), /this conversation may have moved/, 'another anchor must not clear uncertainty');
  assert.equal(f.sent.length, 0, 'no reply to the incorrectly inferred topic');
  // Exact proof for the source message is still allowed to restore routing.
  await f.callMove({ message_id: 30, topic: 'resolved', propagate_mode: 'change_one' });
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'resolved');
});

test('uncached co-moved reply hint stays unconfirmed through an exact echo', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source conversation accepted');
  // Cache eviction alone is not uncertainty: the conversation hint is trusted.
  f.forgetPlacement(30);
  await f.callMove({ message_id: 21, topic: 'confirmed', propagate_mode: 'change_all' });
  assert.equal(f.reconciled.at(-1).messageIds.includes('30'), false, 'control must use scope inference');
  assert.equal(f.cachedPlacement(30), undefined);
  assert.equal(f.placements.get(30).subject, 'confirmed');
  await assert.rejects(f.publish(), /unconfirmed topic/);
});

for (const mode of ['change_all', 'change_later'] as const) for (const actor of [SELF, 7]) {
  test(`pending range result does not certify co-moved placement: ${mode}, actor ${actor}`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source conversation accepted');
    await f.type('start');
    f.onPatch(async () => {
      // The original range PATCH has applied "first", but its response is held.
      // Another client moves only its anchor; the post-result GET sees "second".
      f.placements.set(21, raw(21, 'second'));
      const before = f.reconciled.length;
      f.feed(move([21], { message_id: 21, user_id: actor, orig_subject: 'first', subject: 'second' }));
      await until(() => actor === SELF ? f.reconciled.length > before : f.cachedPlacement(21)?.topic === 'second',
        'intervening anchor event applied');
    });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: mode });
    assert.equal(f.placements.get(30).subject, 'first');
    await assert.rejects(f.publish(), /unconfirmed topic/, 'anchor read must not certify the original range');
    assert.equal(f.sent.length, 0);
    assert.equal(f.cachedPlacement(21).topic, 'second');
    assert.equal(f.cachedPlacement(21).placementUnknown, false, 'tool result alone confirms the anchor');
    assert.equal(f.cachedPlacement(30).placementUnknown, true);
    assert.equal(Boolean(f.cachedPlacement(20).placementUnknown), mode === 'change_all');
    await f.type('start');
    assert.deepEqual(f.typing.map(s => [s.op, s.topic]), [['start', 'deploys'], ['stop', 'deploys']],
      'uncertain co-moved source must not start typing in the anchor-only destination');
  });
}

test('uncontested range result confirms only the anchor, never co-hints', async t => {
  for (const mode of ['change_all', 'change_later'] as const) {
    const f = await fixture();
    try {
      for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
      await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source conversation accepted');
      await f.type('start');
      await f.callMove({ message_id: 21, topic: ' first ', propagate_mode: mode });
      await assert.rejects(f.publish(), /zulip:general.*unconfirmed topic/);
      assert.equal(f.cachedPlacement(21).topic, 'first');
      assert.equal(f.cachedPlacement(30).placementUnknown, true);
      await f.type('start');
      assert.deepEqual(f.typing.map(s => [s.op, s.topic]),
        [['start', 'deploys'], ['stop', 'deploys']]);
    } finally { await f.close(); }
  }
});

test('a mismatched anchor read without an event does not certify a range', async t => {
  for (const mode of ['change_all', 'change_later'] as const) {
    const f = await fixture();
    try {
      for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
      await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source conversation accepted');
      f.onPatch(async () => { f.placements.set(21, raw(21, 'second')); });
      await f.callMove({ message_id: 21, topic: 'first', propagate_mode: mode });
      assert.equal(f.placements.get(30).subject, 'first');
      await assert.rejects(f.publish(), /unconfirmed topic/);
      assert.equal(f.cachedPlacement(21).topic, 'second');
    } finally { await f.close(); }
  }
});

// R3: real tool/event/MCPL regressions; PATCH hooks control the echo window.
for (const activity of ['message', 'reaction'] as const) for (const echoFirst of [false, true]) {
  test(`R3-1 unrelated ${activity}, echo ${echoFirst ? 'before' : 'after'} activity preserves conservative range refusal`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    f.feed({ type: 'message', flags: [], message: { ...raw(40), stream_id: 9, display_recipient: 'random' } });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    f.onPatch(async event => {
      const before = f.reconciled.length;
      const unrelated = activity === 'message'
        ? { type: 'message', flags: [], message: { ...raw(41), stream_id: 9, display_recipient: 'random' } }
        : { type: 'reaction', op: 'add', message_id: 40, user_id: 7, emoji_name: 'eyes' };
      f.feed(...(echoFirst ? [event, unrelated] : [unrelated, event]));
      await until(() => f.reconciled.length > before, 'echo');
      await new Promise(r => setTimeout(r, 20));
    });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
    await assert.rejects(f.publish(), /this conversation may have moved/);
    assert.equal(f.sent.length, 0);
    assert.equal(f.cachedPlacement(30).placementUnknown, true);
  });
}

test('R3-1 a new source-topic hint outside the echo ids stays unconfirmed', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'hint');
  f.onPatch(async event => {
    f.feed(event, { type: 'message', flags: ['mentioned'], message: raw(40) });
    await until(() => f.server.delivery.watermark(GENERAL) === 40, 'new source hint');
  });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), /zulip:general.*unconfirmed topic.*get_stream_topics/);
});

test('R3-2 case-insensitive range without echo refuses instead of reviving the old topic', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [20, 21, 30]) {
    const message = raw(id, id === 30 ? 'Deploys' : 'deploys');
    f.placements.set(id, message);
    f.feed({ type: 'message', flags: ['mentioned'], message });
  }
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  assert.equal(f.placements.get(30).subject, 'first');
  await assert.rejects(f.publish(), /zulip:general.*unconfirmed topic/);
  assert.equal(f.sent.length, 0);
  assert.equal(f.cachedPlacement(30).placementUnknown, true);
  const before = f.reconciled.length;
  f.feed(move([20, 21, 30], { message_id: 21, subject: 'first', propagate_mode: 'change_all' }));
  await until(() => f.reconciled.length > before, 'case-insensitive echo');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.sent.length, 0);
});

test('R3-2 unknown uncached source invalidates the conversation hint', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  let reads = 0;
  f.readPlacementWith(async id => ++reads === 1 ? { result: 'error' }
    : { result: 'success', message: f.placements.get(id) ?? raw(id) });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), /this conversation may have moved/);
});

for (const destination of ['rename', 'third'] as const) {
  test(`R3-3 A to B to ${destination} updates every earlier source marker`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'hint');
    f.onPatch(async event => {
      const before = f.reconciled.length;
      f.feed(event);
      await until(() => f.reconciled.length > before, 'echo');
    });
    await f.callMove({ message_id: 21, stream_id: 8, propagate_mode: 'change_all' });
    await f.callMove({ message_id: 21, topic: 'final', ...(destination === 'third' ? { stream_id: 9 } : {}), propagate_mode: 'change_all' });
    await assert.rejects(f.publish(), destination === 'third' ? /zulip:new-stream.*final/ : /zulip:archive.*final/);
    if (destination === 'third') await assert.rejects(f.publish(ARCHIVE), /zulip:new-stream.*final/);
    else {
      await f.publish(ARCHIVE);
      assert.equal(f.sent.at(-1).topic, 'final');
    }
  });
}

test('R3-4 moved_to supplies send_message arguments without channel-id conversion', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'hint');
  const result = await f.tools.handleToolCall('move_messages', { message_id: 21, stream_id: 8 });
  assert.equal(result.moved_to.stream, 'archive');
  await f.tools.handleToolCall('send_message', { type: 'stream', to: result.moved_to.stream,
    topic: result.moved_to.topic, content: 'Done' });
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['archive', 'deploys']);
});

for (const mode of ['change_all', 'change_later'] as const) {
  test(`R3-1 lost ${mode} echo gives the known same-stream destination`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    for (const id of [20, 21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    f.onPatch(async () => {
      f.feed({ type: 'message', flags: [], message: { ...raw(41), stream_id: 9, display_recipient: 'random' } });
      await new Promise(r => setTimeout(r, 20));
    });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: mode });
    await assert.rejects(f.publish(), /zulip:general.*unconfirmed topic.*get_stream_topics/);
    assert.equal(f.sent.length, 0);
  });
}

for (const mismatch of ['topic', 'stream'] as const) {
test(`R3-2 a stale-source ${mismatch} echo invalidates success placement`, async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  const before = f.reconciled.length;
  f.feed(move([21], { message_id: 21, orig_subject: mismatch === 'topic' ? 'different-source' : 'deploys', stream_id: mismatch === 'stream' ? 8 : 7, subject: 'first', propagate_mode: 'change_all' }));
  await until(() => f.reconciled.length > before, 'stale-source echo');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.cachedPlacement(21).placementUnknown, true);
  assert.equal(f.cachedPlacement(30).placementUnknown, true);
});

}

test('R3-6 a different propagation mode is not this tools echo', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  const before = f.reconciled.length;
  f.feed(move([21, 30], { message_id: 21, subject: 'first', propagate_mode: 'change_one' }));
  await until(() => f.reconciled.length > before, 'different mode echo');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.cachedPlacement(21).placementUnknown, true);
});


test('R3-2 an early echo into an undiscovered stream needs an explicit send', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  f.onPatch(async event => {
    const before = f.reconciled.length;
    f.feed(event);
    await until(() => f.reconciled.length > before, 'early unknown-stream echo');
  });
  const result = await f.tools.handleToolCall('move_messages', { message_id: 21, topic: 'first', stream_id: 9, propagate_mode: 'change_all' });
  assert.equal(f.cachedPlacement(30).placementUnknown, true);
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.equal(f.sent.length, 0);
  await f.tools.handleToolCall('send_message', { type: 'stream', to: result.moved_to.stream, topic: result.moved_to.topic, content: 'Done' });
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['new-stream', 'first']);
});


for (const hintId of [21, 30]) {
  test(`R3-1 an early echo cannot replace an unavailable result read (hint ${hintId})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    for (const id of hintId === 21 ? [21] : [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === hintId, 'hint');
    f.onPatch(async event => {
      const before = f.reconciled.length;
      f.feed(event);
      await until(() => f.reconciled.length > before, 'early echo');
      f.failReads(true);
    });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
    await assert.rejects(f.publish(), hintId === 21 ? /unconfirmed topic/ : /this conversation may have moved/);
    assert.equal(f.sent.length, 0);
    assert.equal(f.cachedPlacement(hintId).placementUnknown, true);
  });
}


for (const destination of ['rename', 'third'] as const) {
  test(`R3-3 an unconfirmed co-moved marker follows A to B to ${destination}`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'co-moved hint');
    await f.callMove({ message_id: 21, stream_id: 8, propagate_mode: 'change_all' });
    await f.callMove({ message_id: 21, topic: 'final', ...(destination === 'third' ? { stream_id: 9 } : {}), propagate_mode: 'change_all' });
    // N1: the first move left this non-exact reference's placement unknown.
    await assert.rejects(f.publish(), /this conversation may have moved/);
    assert.equal(f.hasReference(ARCHIVE), false);
    assert.equal(f.hasReference('zulip:new-stream'), false);
    assert.equal(f.sent.length, 0);
  });
}

// R3 round 2: ordering must survive placement-cache eviction.
test('R3-fix2 natural public traffic eviction cannot roll back range placement', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  const echoes: any[] = [];
  f.onPatch(async event => { echoes.push(event); });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await f.callMove({ message_id: 21, topic: 'second', propagate_mode: 'change_all' });
  for (let id = 100; id <= 2100; id++) f.feed({ type: 'message', flags: [], message: {
    ...raw(id, 'noise'), stream_id: 9, display_recipient: 'random', content: 'public traffic',
  } });
  await until(() => f.cachedPlacement(2100) !== undefined, 'natural eviction');
  assert.equal(f.cachedPlacement(30), undefined);
  for (const echo of [...echoes].reverse()) {
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'echo');
    await assert.rejects(f.publish(), /this conversation may have moved/);
    assert.equal(f.sent.length, 0);
  }
  assert.equal(f.placements.get(30).subject, 'second');
});

for (const crossStream of [false, true]) {
  test(`R3-fix2 initially uncached overlapping moves preserve ${crossStream ? 'cross-stream markers' : 'same-stream hints'}`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    f.forgetPlacement(30);
    const echoes: any[] = [];
    f.onPatch(async event => { echoes.push(event); });
    await f.callMove({ message_id: 21, topic: 'first', ...(crossStream ? { stream_id: 8 } : {}), propagate_mode: 'change_all' });
    await f.callMove({ message_id: 21, topic: 'second', ...(crossStream ? { stream_id: 9 } : {}), propagate_mode: 'change_all' });
    for (const echo of [...echoes].reverse()) {
      const before = f.reconciled.length;
      f.feed(echo);
      await until(() => f.reconciled.length > before, 'reverse echo');
      await assert.rejects(f.publish(), /this conversation may have moved/);
      if (crossStream) {
        assert.equal(f.hasReference(ARCHIVE), false);
        assert.equal(f.hasReference('zulip:new-stream'), false);
      }
      assert.equal(f.sent.length, 0);
    }
  });
}

for (const loss of ['cap', 'ttl', 'mode'] as const) {
  test(`R3-fix2 ${loss}-unmatched genuine old echo never advertises an emptied origin`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    if (loss === 'ttl') t.mock.timers.enable({ apis: ['Date'], now: Date.now() });
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    const echoes: any[] = [];
    f.onPatch(async event => { echoes.push(event); });
    await f.callMove({ message_id: 21, stream_id: 8, propagate_mode: 'change_all' });
    await f.callMove({ message_id: 21, stream_id: 9, topic: 'final', propagate_mode: 'change_all' });
    if (loss === 'cap') for (let i = 0; i < 15; i++) await f.callMove({ message_id: 21, topic: `final-${i}` });
    if (loss === 'ttl') t.mock.timers.tick(60_001);
    const before = f.reconciled.length;
    f.feed({ ...echoes[0], ...(loss === 'mode' ? { propagate_mode: 'change_one' } : {}) });
    await until(() => f.reconciled.length > before, 'unmatched old echo');
    await assert.rejects(f.publish(), /this conversation may have moved/);
    assert.equal(f.hasReference(ARCHIVE), false);
    assert.equal(f.hasReference('zulip:new-stream'), false);
    assert.equal(f.sent.length, 0);
    assert.equal(f.placements.get(21).display_recipient, 'new-stream');
  });
}

for (const crossStream of [false, true]) {
test(`R3-fix2 absent hint and cache order refuses historical exact ids and recovers (${crossStream ? 'cross-stream' : 'same-stream'})`, async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  let echo: any;
  f.onPatch(async event => { echo = event; });
  await f.callMove({ message_id: 21, topic: 'first', ...(crossStream ? { stream_id: 8 } : {}), propagate_mode: 'change_all' });
  f.placements.set(30, raw(30, 'external'));
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'external') });
  await until(() => f.cachedPlacement(30)?.topic === 'external', 'fresh hint');
  // Like natural eviction, but after the real message reset hint provenance.
  f.forgetPlacement(30);
  const before = f.reconciled.length;
  f.feed(echo);
  await until(() => f.reconciled.length > before, 'historical echo');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  if (crossStream) assert.equal(f.hasReference(ARCHIVE), false);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(31, 'external') });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'recovery');
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'external');
});

}

test('R3-fix2 an ambiguous uncached co-move cannot be revalidated by an older anchor echo', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  f.forgetPlacement(30);
  let echo: any;
  f.onPatch(async event => { echo = event; });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  f.placements.set(30, raw(30, 'external'));
  let before = f.reconciled.length;
  f.feed(move([30], { message_id: 30, orig_subject: 'first', subject: 'external' }));
  await until(() => f.reconciled.length > before, 'ambiguous co-move');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  before = f.reconciled.length;
  f.feed(echo);
  await until(() => f.reconciled.length > before, 'older anchor echo');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.sent.length, 0);
});

for (const early of [true, false]) {
test(`R3-fix3 ${early ? 'early completion' : 'late echo'} cannot repair an unproven reaction-refilled co-message`, async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let echo: any;
  f.onPatch(async event => { echo = event; await held; });
  // Direct runtime invocation leaves the MCPL connection free to publish while
  // PATCH is held; publishing inside its tools/call handler would deadlock.
  const moving = f.tools.handleToolCall('move_messages', {
    message_id: 21, stream_id: 8, topic: 'first', propagate_mode: 'change_all',
  });
  try {
    await until(() => echo !== undefined, 'PATCH applied');
    f.placements.set(30, raw(30, 'external'));
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'external') });
    await until(() => f.cachedPlacement(30)?.topic === 'external', 'real same-id refresh');
    await f.publish();
    assert.equal(f.sent.pop().topic, 'external', 'refresh established the new conversation');
    f.forgetPlacement(30);
    f.feed({ type: 'reaction', op: 'add', message_id: 30, user_id: 7, emoji_name: 'eyes' });
    await until(() => f.incoming.some(m => m.metadata?.reaction && m.metadata?.targetMessageId === '30'), 'reaction GET');
    assert.equal('moveOrder' in f.cachedPlacement(30), false, 'no placement proof survives reads');
    if (early) {
      const before = f.reconciled.length;
      f.feed(echo);
      await until(() => f.reconciled.length > before, 'early unproven echo');
      await assert.rejects(f.publish(), /unconfirmed topic/);
      assert.equal(f.cachedPlacement(30).placementUnknown, true, 'self-event can only invalidate cached placement');
    }
  } finally { release(); await moving; }
  if (!early) {
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'late unproven echo');
  }
  await assert.rejects(f.publish(), early ? /this conversation may have moved/ : /unconfirmed topic/);
  assert.equal(f.cachedPlacement(30).placementUnknown, true, 'self-event can only invalidate cached placement');
  assert.deepEqual([f.cachedPlacement(30).channelId, f.cachedPlacement(30).topic], [GENERAL, 'external']);
  assert.equal(f.hasReference(ARCHIVE), false);
  assert.ok(!f.sent.some(s => s.to === 'archive' && s.topic === 'external'), 'never fabricate archive/external');
  assert.deepEqual([f.placements.get(30).display_recipient, f.placements.get(30).subject], ['general', 'external']);
});

}

test('R3-fix3 early unknown-stream co-move stays unconfirmed after anchor discovery', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  f.onPatch(async event => {
    const before = f.reconciled.length;
    f.feed(event);
    await until(() => f.reconciled.length > before, 'early unresolved-stream echo');
    assert.equal(f.cachedPlacement(30).placementUnknown, true);
  });
  await f.callMove({ message_id: 21, stream_id: 9, topic: 'first', propagate_mode: 'change_all' });
  assert.equal(f.cachedPlacement(30).placementUnknown, true);
  assert.deepEqual([f.cachedPlacement(21).channelId, f.cachedPlacement(21).topic], ['zulip:new-stream', 'first'], 'anchor result still confirms');
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.equal(f.sent.length, 0);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(31, 'next') });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'real message clears refusal');
  await f.publish();
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['general', 'next']);
});

test('R3-fix4 older unknown-stream completion cannot certify an interleaved split-range move', async t => {
  const f = await fixture();
  t.after(() => f.close());
  for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  let release!: () => void;
  const held = new Promise<void>(resolve => { release = resolve; });
  let earlyEchoApplied = false;
  let secondEcho: any;
  f.onPatch(async event => {
    if (event.message_id !== 20) { secondEcho = event; return; }
    const before = f.reconciled.length;
    f.feed(event);
    await until(() => f.reconciled.length > before, 'first early unresolved echo');
    earlyEchoApplied = true;
    await held;
  });
  const first = f.tools.handleToolCall('move_messages', {
    message_id: 20, stream_id: 9, topic: 'first', propagate_mode: 'change_all',
  });
  try {
    await until(() => earlyEchoApplied, 'held first PATCH');
    assert.equal(f.cachedPlacement(30).placementUnknown, true);
    await f.callMove({ message_id: 21, stream_id: 8, topic: 'second', propagate_mode: 'change_later' });
    assert.ok(secondEcho, 'newer echo is delayed');
    assert.deepEqual([f.placements.get(30).display_recipient, f.placements.get(30).subject], ['archive', 'second']);
    assert.deepEqual([f.placements.get(20).display_recipient, f.placements.get(20).subject], ['new-stream', 'first']);
  } finally { release(); await first; }
  assert.equal(f.cachedPlacement(30).placementUnknown, true, 'completion cannot certify historical co-placement');
  await assert.rejects(f.publish(), /this conversation may have moved/);
  await f.publish('zulip:new-stream');
  assert.ok(!f.sent.some(s => s.to === 'new-stream' && s.topic === 'first'), 'never send to emptied historical co-placement');
});

for (const crossStream of [false, true]) for (const evictedAfterRead of [false, true]) {
  test(`R3-fix5 reaction GET cannot lend stale proof to an older echo (${crossStream ? 'cross-stream' : 'same-stream'}, ${evictedAfterRead ? 'evicted after read' : 'resident'})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    if (crossStream) await f.open(ARCHIVE);
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    let echo: any;
    f.onPatch(async event => { echo = event; });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all', ...(crossStream ? { stream_id: 8 } : {}) });
    assert.equal(f.cachedPlacement(30).placementUnknown, true);
    // No eviction: the later GET must not inherit the pending range's proof.
    f.placements.set(30, raw(30, 'external'));
    f.feed({ type: 'reaction', op: 'add', message_id: 30, user_id: 7, emoji_name: 'eyes' });
    await until(() => f.incoming.some(m => m.metadata?.reaction && m.metadata?.targetMessageId === '30'), 'reaction GET');
    assert.deepEqual([f.cachedPlacement(30).channelId, f.cachedPlacement(30).topic], [GENERAL, 'external']);
    if (evictedAfterRead) f.forgetPlacement(30);
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'delayed self echo');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    if (crossStream) assert.equal(f.hasReference(ARCHIVE), false);
    assert.equal(f.sent.length, 0, 'no historical placement is authorized');
    if (!evictedAfterRead) assert.deepEqual([f.cachedPlacement(30).channelId, f.cachedPlacement(30).topic], [GENERAL, 'external']);
  });
}

for (const observation of ['history', 'context', 'delivery', 'move-anchor', 'move-co'] as const) {
  test(`R3-fix5 independent ${observation} cannot authorize a historical echo`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    let echo: any;
    f.onPatch(async event => { echo = event; });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
    f.placements.set(30, raw(30, 'external'));
    if (observation === 'history' || observation === 'context') {
      f.historyWith([raw(30, 'external')]);
      if (observation === 'history') await f.readHistory();
      else await f.readContext();
    } else if (observation === 'delivery') {
      f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'external') });
      await until(() => f.cachedPlacement(30)?.topic === 'external', 'real refresh');
      await assert.rejects(f.publish(), /Conversation (?:placement unconfirmed|moved to)/, 'same-id delivery is not a new message');
    } else {
      const anchor = observation === 'move-anchor' ? 30 : 20;
      f.placements.set(anchor, raw(anchor, 'external'));
      f.feed(move([anchor, 30], { message_id: anchor, user_id: 7, orig_subject: 'first', subject: 'external' }));
      await until(() => f.cachedPlacement(30)?.topic === 'external', 'ordinary move');
    }
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'historical echo');
    await assert.rejects(f.publish(), /Conversation (?:placement unconfirmed|moved to)/);
    assert.equal(f.sent.length, 0);
    assert.equal(f.cachedPlacement(30).topic, 'external');
  });
}

for (const propagateMode of ['change_all', 'change_later'] as const) for (const crossStream of [false, true]) {
  test(`R3-fix6 unmatched self-move supersedes candidate-excluded proof (${propagateMode}, ${crossStream ? 'cross-stream' : 'same-stream'})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    if (crossStream) await f.open(ARCHIVE);
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) },
      { type: 'message', flags: ['mentioned'], message: raw(30, 'cached-old-topic') });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'stale but resident hint');
    f.placements.set(30, raw(30));
    let echo: any;
    f.onPatch(async event => { echo = event; });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: propagateMode,
      ...(crossStream ? { stream_id: 8 } : {}) });
    assert.ok(echo.message_ids.includes(30), 'actual range contains the omitted candidate');
    assert.equal(f.cachedPlacement(30).topic, 'cached-old-topic');
    const reads = f.placementReads.length;
    f.placements.set(30, raw(30, 'external'));
    let before = f.reconciled.length;
    f.feed(move([30], { message_id: 30, stream_id: crossStream ? 8 : 7,
      ...(crossStream ? { new_stream_id: 7 } : {}), orig_subject: 'first', subject: 'external' }));
    await until(() => f.reconciled.length > before, 'independent unmatched self-move');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    assert.equal(f.outstandingMoves(), 0, 'candidate omission must not be treated as disjointness proof');
    before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'older genuine echo');
    const source = await f.publish().then(() => null, error => error);
    // An unhinted destination may retain its pre-existing mcpl fallback, but
    // it must never receive the obsolete co-moved topic as a confirmed hint.
    if (crossStream) await f.publish(ARCHIVE).catch(() => {});
    assert.ok(!f.sent.some(s => s.topic === 'first'), `historical sends: ${JSON.stringify(f.sent)}`);
    assert.match(String(source), /unconfirmed topic/);
    assert.equal(f.cachedPlacement(30).placementUnknown, true);
    assert.equal(f.placementReads.length, reads, 'both self-event paths make zero placement GETs');
    assert.equal(f.placements.get(30).subject, 'external');
  });
}

for (const mode of ['change_later', 'change_all'] as const) for (const cross of [false, true]) {
  test(`R7 co-moved hints wait for a real message (${mode}, ${cross ? 'cross' : 'same'} stream)`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    if (cross) await f.open(ARCHIVE);
    for (const id of [21, 30]) f.feed({ type: 'message', flags: ['mentioned'], message: raw(id) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    let echo: any;
    f.onPatch(async event => { echo = event; });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: mode, ...(cross ? { stream_id: 8 } : {}) });
    const channel = cross ? ARCHIVE : GENERAL;
    await assert.rejects(f.publish(), /unconfirmed.*send_message.*list_streams.*get_stream_topics/);
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'own echo');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    await f.readHistory();
    await f.readContext();
    f.feed({ type: 'reaction', op: 'add', message_id: 30, user_id: 7, emoji_name: 'eyes' });
    await until(() => f.incoming.some(m => m.metadata?.reaction && m.metadata?.targetMessageId === '30'), 'reaction read');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    const next = { ...raw(31, 'first'), ...(cross ? { stream_id: 8, display_recipient: 'archive' } : {}) };
    f.historyWith([next]);
    await f.host.sendRequest(method.CHANNELS_OPEN, { channelId: channel, type: 'zulip', address: {}, history: { limit: 10 } });
    await assert.rejects(f.publish(), /unconfirmed topic/, 'history reads cannot clear the refusal');
    assert.equal(f.sent.length, 0);
    f.feed({ type: 'message', flags: ['mentioned'], message: { ...next, id: 32 } });
    await until(() => f.incoming.some(m => m.messageId === '32' && !m.metadata?.change), 'next real message');
    await f.publish(channel);
    assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], [cross ? 'archive' : 'general', 'first']);
    if (cross) await assert.rejects(f.publish(), /unconfirmed topic/);
  });
}

for (const mode of ['change_one', 'change_later', 'change_all'] as const) for (const cross of [false, true]) {
  test(`R7 tool-result anchor stays direct despite delayed echoes (${mode}, ${cross ? 'cross' : 'same'} stream)`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'anchor hint');
    let echo: any;
    f.onPatch(async event => { echo = event; });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: mode, ...(cross ? { stream_id: 8 } : {}) });
    const channel = cross ? ARCHIVE : GENERAL;
    let before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'anchor echo');
    await assert.doesNotReject(f.publish(channel), 'anchor route must stay direct');
    await f.callMove({ message_id: 21, topic: 'second', propagate_mode: mode });
    before = f.reconciled.length;
    f.forgetPlacement(21);
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'matching second anchor echo');
    await assert.doesNotReject(f.publish(channel), 'anchor route must stay direct');
    f.historyWith([raw(21, 'stale-history')]);
    await f.host.sendRequest(method.CHANNELS_OPEN, { channelId: GENERAL, type: 'zulip', address: {}, history: { limit: 10 } });
    if (cross) await assert.rejects(f.publish(), /zulip:archive.*second/);
    await assert.doesNotReject(f.publish(channel), 'anchor route must stay direct');
    assert.deepEqual(f.sent.map(m => m.topic), ['first', 'second', 'second']);
  });
}

for (const mode of ['change_later', 'change_all'] as const) {
  test(`R7 an earlier anchor cannot be confirmed as another moves co-hint (${mode})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    await f.callMove({ message_id: 30, topic: 'cached-old-topic' });
    // A missed external move makes the later actual co-ID absent from scope.
    f.placements.set(30, raw(30));
    let echo: any;
    f.onPatch(async event => { echo = event; });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: mode });
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'co-ID invalidation');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    assert.equal(f.sent.length, 0);
  });
}

for (const cross of [false, true]) {
  test(`anchor later genuine self move must invalidate (${cross})`, async (t) => {
    const f = await fixture();
    t.after(() => f.close());
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'hint');
    await f.callMove({ message_id: 21, topic: 'first' });
    f.placements.set(21, { ...raw(21, 'external'), ...(cross ? { stream_id: 8, display_recipient: 'archive' } : {}) });
    const before = f.reconciled.length;
    f.feed(move([21], { message_id: 21, orig_subject: 'first', subject: 'external', ...(cross ? { new_stream_id: 8 } : {}) }));
    await until(() => f.reconciled.length > before, 'genuine same-identity move');
    let refused = false;
    try {
      await f.publish();
    }
    catch {
      refused = true;
    }
    console.log('OBSERVED anchor later self move', cross, JSON.stringify(f.sent), 'actual', f.placements.get(21).subject);
    assert.equal(refused, true, 'must not send to obsolete first');
  });
}
test('delayed anchor read must not undo newer co-range refusal', async (t) => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  let release!: () => void;
  const held = new Promise<void>(r => release = r);
  let captured = false;
  f.readPlacementWith(async (id) => {
    const snapshot = { ...(f.placements.get(id) ?? raw(id)) };
    if (id === 30 && snapshot.subject === 'first' && !captured) {
      captured = true;
      await held;
    }
    return { result: 'success', message: snapshot };
  });
  // First tool moves all messages to first; its anchor result is delayed after reading first.
  const pending = f.tools.handleToolCall('move_messages', { message_id: 30, topic: 'first', propagate_mode: 'change_all' });
  try {
    await until(() => captured, 'old post-PATCH read captured');
    const before = f.reconciled.length;
    // A later move by the same bot identity uses a different anchor and co-moves 30.
    f.placements.set(30, raw(30, 'second'));
    f.feed(move([21, 30], { message_id: 21, orig_subject: 'first', subject: 'second', propagate_mode: 'change_later' }));
    await until(() => f.reconciled.length > before, 'newer co-range self event');
    await assert.rejects(f.publish(), /unconfirmed/i);
  }
  finally {
    release();
    await pending;
  }
  let refused = false;
  try {
    await f.publish();
  }
  catch {
    refused = true;
  }
  console.log('OBSERVED delayed result', JSON.stringify(f.sent), 'actual', f.placements.get(30).subject);
  assert.equal(refused, true, 'older completion must not re-confirm a co-moved reference');
});
for (const cross of [false, true]) {
  test(`moved copy cannot count as new message (${cross})`, async (t) => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    if (cross)
      await f.open(ARCHIVE);
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all', ...(cross ? { stream_id: 8 } : {}) });
    const ch = GENERAL;
    await assert.rejects(f.publish(ch), /unconfirmed/i);
    const before = f.incoming.length;
    f.feed({ type: 'message', flags: ['mentioned'], message: f.placements.get(30) });
    await until(() => f.incoming.length > before, 'old moved message delivered');
    let refused = false;
    try {
      await f.publish(ch);
    }
    catch {
      refused = true;
    }
    console.log('OBSERVED moved copy', cross, JSON.stringify(f.sent), 'id 30 is pre-existing');
    assert.equal(refused, true, 'moved copy is not a new real message');
  });
}
test('sender self echo does not clear refusal, new user message does', async (t) => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  f.feed({ type: 'message', flags: ['mentioned'], message: { ...raw(31, 'first'), sender_id: SELF } });
  await new Promise(r => setTimeout(r, 35));
  await assert.rejects(f.publish(), /unconfirmed/i);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(32, 'first') });
  await until(() => f.server.delivery.watermark(GENERAL) === 32, 'fresh user message');
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'first');
});
test('deletion and eviction cannot clear co refusal', async (t) => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  let echo: any;
  f.onPatch(async (e) => { echo = e; });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  f.forgetPlacement(30);
  f.feed({ type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 7, topic: 'first' });
  await until(() => f.incoming.some(m => m.metadata?.change === 'delete'), 'deletion');
  await assert.rejects(f.publish(), /deleted or moved where/);
  const before = f.reconciled.length;
  f.feed(echo);
  await until(() => f.reconciled.length > before, 'old echo');
  await assert.rejects(f.publish(), /deleted or moved where/);
});


for (const early of [false, true]) {
  test(`anchor exemption consumes only one exact ${early ? 'early' : 'late'} echo`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'anchor');
    let echo: any;
    f.onPatch(async event => {
      echo = event;
      if (early) {
        const before = f.reconciled.length;
        f.feed(event);
        await until(() => f.reconciled.length > before, 'early exact echo');
      }
    });
    await f.callMove({ message_id: 21, topic: 'first' });
    if (!early) {
      const before = f.reconciled.length;
      f.feed(echo);
      await until(() => f.reconciled.length > before, 'late exact echo');
    }
    await f.publish();
    assert.equal(f.sent.at(-1).topic, 'first', 'one exact own echo preserves the result');
    const before = f.reconciled.length;
    f.feed(echo);
    await until(() => f.reconciled.length > before, 'duplicate echo');
    await assert.rejects(f.publish(), /unconfirmed topic/, 'the exemption has been consumed');
    assert.equal(f.sent.length, 1);
  });
}

for (const change of ['ordinary move', 'deletion'] as const) {
  test(`delayed anchor result respects newer ${change} routing policy`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'anchor');
    let captured = false;
    let release!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    f.readPlacementWith(async id => {
      const snapshot = { ...(f.placements.get(id) ?? raw(id)) };
      if (id === 30 && snapshot.subject === 'first') { captured = true; await held; }
      return { result: 'success', message: snapshot };
    });
    const pending = f.tools.handleToolCall('move_messages', { message_id: 30, topic: 'first' });
    try {
      await until(() => captured, 'held result');
      f.placements.set(30, raw(30, 'external'));
      f.feed(change === 'ordinary move'
        ? move([30], { message_id: 30, user_id: 7, orig_subject: 'first', subject: 'external', content: 'edited too', orig_content: 'old' })
        : { type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 7, topic: 'first' });
      await until(() => f.incoming.some(m => m.metadata?.change), 'independent invalidation');
      await assert.rejects(f.publish(), change === 'deletion' ? /deleted or moved where/ : /Conversation moved to/);
    } finally { release(); await pending; }
    if (change === 'deletion') {
      await assert.rejects(f.publish(), /deleted or moved where/);
      assert.equal(f.sent.length, 0);
    } else {
      await assert.rejects(f.publish(), /Conversation (?:placement unconfirmed|moved to)/);
      assert.equal(f.sent.length, 0);
    }
  });
}

test('refusal floor includes known public, own, history and tool-read ids above the hint', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'hint');
  f.feed({ type: 'message', flags: [], message: { ...raw(100), stream_id: 9, display_recipient: 'random', flags: [] } },
    { type: 'message', flags: [], message: { ...raw(101), sender_id: SELF, flags: [] } });
  await until(() => f.cachedPlacement(101) !== undefined, 'filtered observations');
  f.historyWith([raw(102)]);
  await f.readHistory();
  f.historyWith([raw(104)]);
  await f.tools.handleToolCall('fetch_history', { channel: 'general' });
  f.historyWith([raw(108)]);
  await f.tools.handleToolCall('start_monitoring', { channels: ['general'] });
  await f.tools.readResource('zulip://unread/summary');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  const before = f.incoming.length;
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(107, 'first') });
  await until(() => f.incoming.length > before, 'existing id above the adapter and hint');
  await assert.rejects(f.publish(), /unconfirmed topic/);
  assert.equal(f.sent.length, 0);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(109, 'first') });
  await until(() => f.server.delivery.watermark(GENERAL) === 109, 'genuinely new id');
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'first');
});

for (const mismatch of ['anchor', 'origin stream', 'origin topic', 'destination stream', 'destination topic', 'mode'] as const) {
  test(`anchor ignores only the expected echo shape: mismatched ${mismatch}`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'anchor');
    await f.callMove({ message_id: 21, topic: 'first' });
    const difference = mismatch === 'anchor' ? { message_id: 20 }
      : mismatch === 'origin stream' ? { stream_id: 8 }
      : mismatch === 'origin topic' ? { orig_subject: 'elsewhere' }
      : mismatch === 'destination stream' ? { new_stream_id: 8 }
      : mismatch === 'destination topic' ? { subject: 'external' }
      : { propagate_mode: 'change_all' };
    const before = f.reconciled.length;
    f.feed(move([20, 21], { message_id: 21, subject: 'first', ...difference }));
    await until(() => f.reconciled.length > before, 'nonmatching self event');
    await assert.rejects(f.publish(), /unconfirmed topic/);
    assert.equal(f.sent.length, 0);
  });
}

test('expected echo destination agrees with the actual result, not only requested fields', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'anchor');
  let echo: any;
  f.onPatch(async event => {
    echo = event;
    // A later independent move is visible to the result GET, not yet to events.
    f.placements.set(21, { ...f.placements.get(21), stream_id: 8, display_recipient: 'archive' });
  });
  await f.callMove({ message_id: 21, topic: 'first' });
  const before = f.reconciled.length;
  f.feed(echo);
  await until(() => f.reconciled.length > before, 'echo with a different destination than the result');
  await assert.rejects(f.publish(ARCHIVE), /unconfirmed topic/);
  assert.equal(f.sent.length, 0);
});

for (const cached of [false, true]) {
  test(`failed pre-move read scopes range uncertainty without poisoning foreign references (cached=${cached})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    await f.open(ARCHIVE);
    f.feed(
      { type: 'message', flags: ['mentioned'], message: raw(21) },
      { type: 'message', flags: ['mentioned'], message: { ...raw(40, 'other'), stream_id: 8, display_recipient: 'archive' } },
      { type: 'message', flags: [], message: { ...raw(41), type: 'private', display_recipient: [
        { id: 7, email: 'ann@example.com', full_name: 'Ann' }, { id: SELF, email: 'bot@example.com', full_name: 'Bot' },
      ] } },
    );
    await until(() => f.server.delivery.watermark('zulip:dm:7') === 41, 'DM reference');
    await until(() => f.server.delivery.watermark(ARCHIVE) === 40, 'archive reference');
    if (!cached) f.forgetPlacement(21);
    let reads = 0;
    f.readPlacementWith(async id => {
      if (++reads === 1) throw new Error('transient pre-move failure');
      return { result: 'success', message: f.placements.get(id) ?? raw(id) };
    });
    await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
    await f.publish('zulip:dm:7');
    await f.publish(ARCHIVE);
    await f.publish();
    assert.equal(f.sent.at(-1).topic, 'first');
    assert.equal(f.cachedPlacement(41).placementUnknown, undefined);
    assert.equal(f.cachedPlacement(40).placementUnknown, undefined);
  });
}

for (const kind of ['deletion', 'rename', 'stream move']) {
  test(`third-party ${kind} preserves safe routing with authoritative hints`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    if (kind === 'deletion') f.feed({ type: 'message', flags: [], message: raw(20) });
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
    f.feed(kind === 'deletion'
      ? { type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 7, topic: 'deploys', user_id: 7 }
      : move([30], { message_id: 30, user_id: 7, subject: 'ops', propagate_mode: 'change_all',
        ...(kind === 'stream move' ? { new_stream_id: 8 } : {}) }));
    await until(() => f.incoming.some(m => m.metadata?.change === (kind === 'deletion' ? 'delete' : 'move')), 'change marker');
    if (kind === 'deletion') {
      await f.publish();
      assert.equal(f.sent.at(-1).topic, 'deploys');
    } else {
      await assert.rejects(f.publish(), error => {
        const e = error as any;
        assert.match(e.message, /Nothing was posted\./);
        assert.match(e.message, /send_message\(\{reply_to: 30, content\}\)/);
        assert.match(e.message, /> ops/);
        assert.equal(e.data.messageId, 30);
        assert.equal(e.data.stream, kind === 'stream move' ? 'archive' : 'general');
        return true;
      });
    }
  });
}

test('same-stream co-move refusal carries anchor likely topic and recovery data', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  const result = await f.tools.handleToolCall('move_messages', { message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  assert.equal(result.moved_to.stream, 'general');
  assert.equal(result.moved_to.topic, 'first');
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /Nothing was posted\..*likely first/);
    assert.equal(e.data.messageId, 30);
    assert.equal(e.data.stream, 'general');
    return true;
  });
});

test('stalled post-move placement read cannot fail a successful move', async t => {
  const f = await fixture({}, false, 20);
  t.after(() => f.close());
  let reads = 0;
  let release: (() => void) | undefined;
  f.readPlacementWith(async id => {
    if (++reads === 1) return { result: 'success', message: raw(id) };
    return new Promise(resolve => { release = () => resolve({ result: 'success', message: raw(id, 'first') }); });
  });
  const start = Date.now();
  let timer: NodeJS.Timeout | undefined;
  try {
    await Promise.race([
      f.callMove({ message_id: 21, topic: 'first' }),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('move stalled after success')), 500); }),
    ]);
  } finally { clearTimeout(timer); release?.(); }
  assert.ok(Date.now() - start < 500, 'follow-up read must be bounded');
  assert.equal(f.reconciled.length, 1);
});


for (const hint of [21, 30]) {
  test(`failed post-move read keeps requested destination likely, not old topic (hint=${hint})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(hint) });
    await until(() => f.server.delivery.watermark(GENERAL) === hint, 'reference');
    f.onPatch(async () => { f.failReads(true); });
    const result = await f.tools.handleToolCall('move_messages', { message_id: 21, topic: 'first', propagate_mode: 'change_all' });
    assert.equal(result.moved_to.topic, null);
    assert.match(result.routing_note, /succeeded/);
    await assert.rejects(f.publish(), (e: any) => {
      assert.match(e.message, /Nothing was posted.*likely first/);
      assert.doesNotMatch(e.message, /likely deploys|topic: "deploys"/);
      assert.equal(e.data.messageId, hint);
      assert.equal(e.data.stream, 'general');
      assert.deepEqual(e.data.movedTo, { channelId: GENERAL, topic: null, likelyTopic: 'first' });
      return true;
    });
    assert.equal(f.sent.length, 0);
  });
}

test('late unmatched self echo without cache hints its new subject', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  f.forgetPlacement(30);
  f.feed(move([21, 30], { message_id: 21, subject: 'first', propagate_mode: 'change_all' }));
  await until(() => f.reconciled.length === 1, 'late echo');
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /likely first/);
    assert.doesNotMatch(e.message, /likely deploys|topic: "deploys"/);
    assert.equal(e.data.messageId, 30);
    assert.equal(e.data.stream, 'general');
    assert.equal(e.data.movedTo.likelyTopic, 'first');
    return true;
  });
});

test('old-topic Notification Bot notice cannot clear refusal but destination breadcrumb can', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  f.feed(move([30], { message_id: 30, user_id: 7, subject: 'ops', propagate_mode: 'change_all' }));
  await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'move');
  await assert.rejects(f.publish(), /> ops/);
  const notice = (id: number, topic: string) => ({ type: 'message', flags: [], message: {
    ...raw(id, topic), sender_id: 100, sender_full_name: 'Notification Bot',
    sender_email: 'notification-bot@example.com', flags: [], content: 'This topic was moved to ops',
  } });
  f.feed(notice(31, 'deploys'));
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'old-topic notice');
  await assert.rejects(f.publish(), /> ops/);
  assert.equal(f.sent.length, 0);
  f.feed(notice(32, 'ops'));
  await until(() => f.server.delivery.watermark(GENERAL) === 32, 'destination breadcrumb');
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'ops');
});

test('hidden move old-topic Notification Bot notice cannot resume public replies', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  f.feed({ type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 7, topic: 'deploys' });
  await until(() => f.incoming.some(m => m.metadata?.change === 'delete'), 'lost visibility');
  await assert.rejects(f.publish(), /deleted or moved where/);
  f.feed({ type: 'message', flags: [], message: {
    ...raw(31), sender_id: 100, sender_full_name: 'Notification Bot',
    sender_email: 'notification-bot@example.com', flags: [], content: 'This topic was moved to a private stream',
  } });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'old-topic notice');
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /Nothing was posted.*deleted or moved where/);
    assert.equal(e.data.messageId, 30);
    assert.equal(e.data.stream, null);
    return true;
  });
  assert.equal(f.sent.length, 0, 'old-topic notice must not recreate the public conversation');
});

test('deletion of followed message refuses only when cached topic is empty', async t => {
  for (const remaining of [true, false]) {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    if (remaining) f.feed({ type: 'message', flags: [], message: raw(20) });
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
    f.feed({ type: 'delete_message', message_ids: remaining ? [30] : [20, 30], message_type: 'stream', stream_id: 7, topic: 'deploys' });
    await until(() => f.incoming.some(m => m.metadata?.change === 'delete'), 'deletion');
    if (remaining) {
      await f.publish();
      assert.equal(f.sent.at(-1).topic, 'deploys');
    } else {
      await assert.rejects(f.publish(), (e: any) => {
        assert.match(e.message, /Nothing was posted.*the message this conversation followed was deleted or moved where this bot can't see it/);
        assert.equal(e.data.messageId, 30);
        assert.equal(e.data.stream, null);
        assert.doesNotMatch(e.message, /alternatively|topic:|destination hint/);
        return true;
      });
      assert.equal(f.sent.length, 0);
    }
  }
});

test('stale cached anchor cannot poison another stream or displace destination reference', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  f.feed({ type: 'message', flags: [], message: raw(21) },
    { type: 'message', flags: ['mentioned'], message: { ...raw(25, 'other'), stream_id: 8, display_recipient: 'archive' } },
    { type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source reference');
  await until(() => f.server.delivery.watermark(ARCHIVE) === 25, 'destination reference');
  f.placements.set(21, { ...raw(21), stream_id: 8, display_recipient: 'archive' });
  let reads = 0;
  f.readPlacementWith(async id => {
    if (++reads === 1) throw new Error('failed pre-move read');
    return { result: 'success', message: f.placements.get(id) };
  });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await f.publish();
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['general', 'deploys']);
  await assert.rejects(f.publish(ARCHIVE), (e: any) => {
    assert.equal(e.data.messageId, 25);
    assert.equal(e.data.stream, null);
    assert.doesNotMatch(e.message, /likely first|alternatively/);
    return true;
  });
});

test('unknown source gives destination only to exact anchor, never unrelated stream or DM', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) },
    { type: 'message', flags: ['mentioned'], message: { ...raw(40, 'other'), stream_id: 8, display_recipient: 'archive' } },
    { type: 'message', flags: [], message: { ...raw(41), type: 'private', display_recipient: [
      { id: 7, email: 'ann@example.com', full_name: 'Ann' }, { id: SELF, email: 'bot@example.com', full_name: 'Bot' },
    ] } });
  await until(() => f.server.delivery.watermark('zulip:dm:7') === 41, 'DM');
  await until(() => f.server.delivery.watermark(ARCHIVE) === 40, 'archive');
  f.forgetPlacement(21);
  let reads = 0;
  f.readPlacementWith(async id => {
    if (++reads === 1) throw new Error('unknown source');
    return { result: 'success', message: f.placements.get(id) };
  });
  await f.callMove({ message_id: 21, stream_id: 8, topic: 'first', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), (e: any) => {
    assert.equal(e.data.messageId, 21);
    assert.equal(e.data.stream, 'archive');
    assert.equal(e.data.movedTo.topic, 'first');
    assert.match(e.message, /Conversation moved to zulip:archive > first/);
    return true;
  });
  await assert.rejects(f.publish(ARCHIVE), (e: any) => {
    assert.match(e.message, /this conversation may have moved; reply with send_message\(\{reply_to: 40, content\}\)/);
    assert.doesNotMatch(e.message, /alternatively|destination hint|likely|topic:/);
    assert.equal(e.data.messageId, 40);
    assert.equal(e.data.stream, null);
    assert.deepEqual(e.data.movedTo, { channelId: null, topic: null });
    return true;
  });
  await f.publish('zulip:dm:7');
  assert.equal(f.sent.at(-1).type, 'private');
});

test('failed rename reads do not perform a destination stream lookup', async t => {
  const f = await fixture();
  t.after(() => f.close());
  const before = f.streamReads();
  f.failReads(true);
  await f.callMove({ message_id: 21, topic: 'first' });
  assert.equal(f.streamReads(), before);
});

test('unresolved third-party stream move retains numeric stream hint', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  f.feed(move([30], { message_id: 30, user_id: 7, subject: 'ops', new_stream_id: 9 }));
  await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'move');
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /stream 9/);
    assert.equal(e.data.movedTo.streamId, 9);
    return true;
  });
});

test('unknown previous topic is wildcard for otherwise matching own anchor echo', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'reference');
  f.forgetPlacement(21);
  let reads = 0;
  f.readPlacementWith(async id => {
    if (++reads === 1) throw new Error('unknown previous topic');
    return { result: 'success', message: f.placements.get(id) };
  });
  await f.callMove({ message_id: 21, topic: 'first' });
  f.feed(move([21], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length === 2, 'own echo');
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'first');
});

for (const opened of [true, false]) {
  test(`R6-1 delete and repost in a fresh topic resumes replies (${opened ? 'open' : 'closed'})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    if (opened) await f.open();
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'help') });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'original question');
    f.markDeleted(30);
    f.feed({ type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 7, topic: 'help' });
    await until(() => f.routingReference()?.deletedOrHidden, 'deletion refusal');
    await assert.rejects(f.publish(), /deleted or moved where/);
    for (const id of [31, 32]) {
      f.feed({ type: 'message', flags: ['mentioned'], message: raw(id, 'help') });
      await until(() => f.server.delivery.watermark(GENERAL) === id, 'reposted question');
      await f.publish();
      assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['general', 'help']);
      assert.equal(f.routingReference().from, id);
    }
  });
}

test('R6-1 deletion refusal directs reply_to to a newer id, never the gone id', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'help') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'question');
  f.markDeleted(30);
  f.feed({ type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 7, topic: 'help' });
  await until(() => f.routingReference()?.deletedOrHidden, 'deletion refusal');
  // The replacement exists on Zulip, but has not reached this event queue.
  f.placements.set(31, raw(31, 'help'));
  const gone = await f.host.sendRequest('tools/call', { name: 'send_message', arguments: { reply_to: 30, content: 'Done' } }) as any;
  assert.equal(gone.isError, true);
  assert.match(gone.content[0].text, /Zulip refused reply_to 30 \(BAD_REQUEST\): Invalid message\(s\)/);
  await assert.rejects(f.publish(), (e: any) => {
    assert.equal(e.message, "RPC error -32602: Nothing was posted: the message this conversation followed was deleted or moved where this bot can't see it, so it can't be replied to. To answer a newer message use send_message({reply_to: <its id>, content}); otherwise choose an explicit destination.");
    assert.doesNotMatch(e.message, /reply_to: 30/);
    return true;
  });
  assert.equal(f.sent.length, 0);
  const reply = await f.host.sendRequest('tools/call', { name: 'send_message', arguments: { reply_to: 31, content: 'Done' } }) as any;
  assert.notEqual(reply.isError, true);
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], [7, 'help']);
  await assert.rejects(f.publish(), /deleted or moved where/, 'explicit reply bypasses, not clears, refusal');
});

test('R6-2 a person reusing a renamed topic starts a new conversation there', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'standup') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'old standup');
  f.feed(move([30], { message_id: 30, user_id: 7, orig_subject: 'standup', subject: 'standup 11-05', propagate_mode: 'change_all' }));
  await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'rename');
  await assert.rejects(f.publish(), /Conversation moved to zulip:general > standup 11-05/);
  f.feed({ type: 'message', flags: ['mentioned'], message: { ...raw(31, 'standup'), sender_id: 8, sender_full_name: 'Bob', sender_email: 'bob@example.com' } });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'new standup');
  await f.publish();
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['general', 'standup']);
});

for (const path of ['tool', 'third-party']) for (const sender of ['human', 'bot']) {
  test(`R6-2 case-only ${path} rename records no departure and follows later ${sender} traffic`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'question');
    if (path === 'tool') {
      await f.callMove({ message_id: 21, topic: 'Deploys', propagate_mode: 'change_all' });
      await assert.rejects(f.publish(), /unconfirmed topic/);
    } else {
      f.feed(move([30], { message_id: 30, user_id: 7, subject: 'Deploys', propagate_mode: 'change_all' }));
      await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'case-only rename');
      await assert.rejects(f.publish(), /Conversation moved to zulip:general > Deploys/);
    }
    assert.equal(f.routingReference().leftTopic, undefined);
    f.feed({ type: 'message', flags: [], message: { ...raw(31, 'Deploys'), flags: [],
      ...(sender === 'bot' ? { sender_id: 100, sender_email: 'notification-bot@example.com' } : {}) } });
    await until(() => f.server.delivery.watermark(GENERAL) === 31, 'later message');
    await f.publish();
    assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['general', 'Deploys']);
  });
}

test('R6-2 uncertain move back A to B to A preserves the older departed topic', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.placements.set(21, raw(21, 'A'));
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'A') });
  f.placements.set(30, raw(30, 'A'));
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'A conversation');
  await f.callMove({ message_id: 21, topic: 'B', propagate_mode: 'change_all' });
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'A' });
  await assert.rejects(f.publish(), /unconfirmed topic/);
  await f.callMove({ message_id: 21, topic: 'A', propagate_mode: 'change_all' });
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'A' },
    'unknown placement preserves the older barrier; a person can still clear refusal');
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(31, 'A') });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'back in A');
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'A');
});

test('N1 a reference arriving during PATCH gets no non-exact destination hint', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'initial reference');
  f.onPatch(async () => {
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(31) });
    await until(() => f.server.delivery.watermark(GENERAL) === 31, 'reference arriving after PATCH applied');
  });
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /this conversation may have moved; reply with send_message\(\{reply_to: 31, content\}\)/);
    assert.doesNotMatch(e.message, /likely|alternatively|topic:/);
    assert.deepEqual(e.data.movedTo, { channelId: null, topic: null });
    return true;
  });
});

test('N1 already-unknown placement cannot use its original hints as source proof', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), /likely first/);
  // Another anchor is in the original topic; 30's placement is already unknown.
  f.placements.set(21, raw(21));
  await f.callMove({ message_id: 21, topic: 'second', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /this conversation may have moved/);
    assert.doesNotMatch(e.message, /likely|alternatively|topic:/);
    assert.deepEqual(e.data.movedTo, { channelId: null, topic: null });
    return true;
  });
});

test('N2 deletion at destination revokes cross-channel references to that conversation', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'source');
  await f.callMove({ message_id: 30, stream_id: 8 });
  await assert.rejects(f.publish(), /Conversation moved to zulip:archive > deploys/);
  f.markDeleted(30);
  f.feed({ type: 'delete_message', message_ids: [30], message_type: 'stream', stream_id: 8, topic: 'deploys' });
  await until(() => f.incoming.some(m => m.channelId === ARCHIVE && m.metadata?.change === 'delete'), 'destination deletion');
  for (const channel of [GENERAL, ARCHIVE]) {
    await assert.rejects(f.publish(channel), (e: any) => {
      assert.match(e.message, /deleted or moved where/);
      assert.doesNotMatch(e.message, /reply_to: 30|alternatively|topic:/);
      assert.equal(e.data.stream, null);
      assert.deepEqual(e.data.movedTo, { channelId: null, topic: null });
      return true;
    });
  }
  assert.equal(f.sent.length, 0);
});

for (const bulk of [false, true]) {
  test(`N2 own deletion removes cached ids without surfacing its echo (bulk=${bulk})`, async t => {
    const f = await fixture();
    t.after(() => f.close());
    await f.open();
    f.feed({ type: 'message', flags: [], message: { ...raw(20), sender_id: SELF, flags: [] } },
      { type: 'message', flags: ['mentioned'], message: raw(30) });
    await until(() => f.server.delivery.watermark(GENERAL) === 30, 'question and cached own reply');
    assert.ok(f.cachedPlacement(20));
    const result = await f.host.sendRequest('tools/call', { name: 'delete_message', arguments: { message_id: 20 } }) as any;
    assert.notEqual(result.isError, true);
    if (!bulk) f.feed({ type: 'delete_message', message_ids: [20], message_type: 'stream', stream_id: 7, topic: 'deploys' });
    f.feed({ type: 'delete_message', message_ids: bulk ? [20, 30] : [30], message_type: 'stream', stream_id: 7, topic: 'deploys' });
    await until(() => f.incoming.some(m => m.metadata?.change === 'delete'), 'human deletion after self echo');
    assert.equal(f.cachedPlacement(20), undefined, 'self-deleted id must not keep the topic nonempty');
    const changes = f.incoming.filter(m => m.metadata?.change === 'delete');
    assert.equal(changes.length, 1, 'own delete remains silent as on main');
    assert.deepEqual(changes[0].metadata?.targetMessageIds, ['30']);
    await assert.rejects(f.publish(), /deleted or moved where/);
  });
}

test('N1 unknown channel cannot use the original channel as source proof', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30) });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'reference');
  f.placements.set(30, { ...raw(30), stream_id: 9, display_recipient: 'unresolved' });
  f.feed(move([30], { message_id: 30, user_id: 7, new_stream_id: 9, subject: 'deploys' }));
  await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'unresolved stream move');
  assert.equal(f.routingReference().movedTo.channelId, null);
  await f.callMove({ message_id: 21, topic: 'first', propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), (e: any) => {
    assert.match(e.message, /this conversation may have moved/);
    assert.doesNotMatch(e.message, /likely|alternatively|topic:/);
    assert.deepEqual(e.data.movedTo, { channelId: null, topic: null });
    return true;
  });
});

// R7-1a: a stream-only second move leaves the event's current topic, not
// the original hints (which have deliberately remained refused since A).
test('R7-1a stream-only second move keeps the current departed-topic barrier', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'A') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'A conversation');
  f.feed(move([30], { message_id: 30, user_id: 7, orig_subject: 'A', subject: 'B', propagate_mode: 'change_all' }));
  await until(() => f.incoming.filter(m => m.metadata?.change === 'move').length === 1, 'A to B');
  f.feed(move([30], { message_id: 30, user_id: 7, orig_subject: 'B', subject: undefined,
    new_stream_id: 8, propagate_mode: 'change_all' }));
  await until(() => f.incoming.filter(m => m.metadata?.change === 'move').length === 2, 'B to archive');
  await assert.rejects(f.publish(), /Conversation moved to zulip:archive > B/);
  f.feed({ type: 'message', flags: [], message: { ...raw(31, 'B'), flags: [],
    sender_id: 100, sender_email: 'notification-bot@example.com', content: 'This topic was moved to #archive>B' } });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'old-topic notice');
  await assert.rejects(f.publish(), /Conversation moved to zulip:archive > B/);
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'B' });
  assert.equal(f.sent.length, 0);
});

test('R7-1b unknown-source move preserves an earlier departed-topic barrier', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'A') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'A conversation');
  f.feed(move([30], { message_id: 30, user_id: 7, orig_subject: 'A', subject: 'B', propagate_mode: 'change_all' }));
  await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'A to B');
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'A' });
  assert.equal(f.cachedPlacement(21), undefined, 'the later anchor is uncached');
  f.failReads(true);
  await f.callMove({ message_id: 21, topic: 'elsewhere', propagate_mode: 'change_all' });
  f.failReads(false);
  assert.deepEqual(f.routingReference().movedTo, { channelId: null, topic: null });
  f.feed({ type: 'message', flags: [], message: { ...raw(31, 'A'), flags: [],
    sender_id: 100, sender_email: 'notification-bot@example.com', content: 'This topic was moved to B' } });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'late old-topic notice');
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'A' });
  assert.equal(f.sent.length, 0);
});

test('R7-1b a likely co-moved reference keeps its barrier after an unrelated range move', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.placements.set(21, raw(21, 'A'));
  f.placements.set(30, raw(30, 'A'));
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'A') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'A conversation');
  await f.callMove({ message_id: 21, topic: 'B', propagate_mode: 'change_all' });
  assert.deepEqual(f.routingReference().movedTo, { channelId: GENERAL, topic: null, likelyTopic: 'B' });
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'A' });
  f.placements.set(20, raw(20, 'unrelated'));
  await f.callMove({ message_id: 20, topic: 'elsewhere', propagate_mode: 'change_all' });
  assert.deepEqual(f.routingReference().movedTo, { channelId: null, topic: null });
  f.feed({ type: 'message', flags: [], message: { ...raw(31, 'A'), flags: [],
    sender_id: 100, sender_email: 'notification-bot@example.com', content: 'This topic was moved to B' } });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'late old-topic notice');
  await assert.rejects(f.publish(), /this conversation may have moved/);
  assert.deepEqual(f.routingReference().leftTopic, { channelId: GENERAL, topic: 'A' });
  assert.equal(f.sent.length, 0);
});

test('R7-2 an unverified requested id cannot raise the refusal floor', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(30, 'A') });
  await until(() => f.server.delivery.watermark(GENERAL) === 30, 'A conversation');
  // This callback runs before the pre-move GET/PATCH can reject an invalid id.
  // No successful Zulip read or event has observed this caller-supplied id.
  f.tools.onMoveStarted!();
  f.feed(move([30], { message_id: 30, user_id: 7, orig_subject: 'A', subject: 'B', propagate_mode: 'change_all' }));
  await until(() => f.incoming.some(m => m.metadata?.change === 'move'), 'rename refusal');
  await assert.rejects(f.publish(), /Conversation moved to zulip:general > B/);
  f.feed({ type: 'message', flags: [], message: { ...raw(31, 'support'), flags: [],
    sender_id: 8, sender_full_name: 'Bob', sender_email: 'bob@example.com' } });
  await until(() => f.server.delivery.watermark(GENERAL) === 31, 'new message');
  await f.publish();
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['general', 'support']);
  assert.equal(f.routingReference().from, 31);
});
