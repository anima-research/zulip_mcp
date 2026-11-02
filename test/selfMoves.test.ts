/** Real event loop, adapter, server and MCPL wire; only Zulip is a stand-in. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { createServer } from 'node:http';
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

async function fixture(policy: ZulipFilters = {}, invisibleDestination = false) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-self-move-'));
  const filtersPath = join(dir, 'filters.json');
  writeFileSync(filtersPath, JSON.stringify({ reactionChannels: [GENERAL, ARCHIVE], ...policy }));
  const filters = new FiltersPlane(filtersPath, {});
  filters.start();
  const pending: any[] = [];
  let eventId = 0;
  const sent: any[] = [];
  const typing: any[] = [];
  const placements = new Map<number, any>();
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
      if (n !== id && (mode === 'change_one' || m.subject !== original.subject ||
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
      message_id: id, stream_id: original.stream_id, orig_subject: original.subject,
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
  const client = {
    streams: { retrieve: async () => ({ result: 'success', streams: [
      { name: 'general', stream_id: 7 }, { name: 'archive', stream_id: 8 },
    ] }) },
    messages: {
      retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
      getById: async ({ message_id }: any) => {
        const message = placements.get(message_id) ?? raw(message_id);
        if (invisibleDestination && message.stream_id !== 7) return { result: 'error', msg: 'Not visible' };
        return { result: 'success', message };
      },
      send: async (args: any) => { sent.push(args); return { result: 'success', id: 1000 + sent.length }; },
    },
    typing: { send: async (args: any) => { typing.push(args); return { result: 'success' }; } },
    users: { me: { subscriptions: { add: async () => ({ result: 'success', already_subscribed: {} }) } } },
    queues: { register: async () => ({ queue_id: 'q', last_event_id: -1 }) },
    events: { retrieve: async () => {
      await new Promise(r => setTimeout(r, 5));
      return { events: pending.splice(0) };
    } },
  };
  const adapter = new ZulipAdapter(client, SELF, 'test', { filters, dmDiscoveryLimit: 0 });
  const tools = new ZulipToolRuntime({ client, selfUserId: SELF, sessionId: 'test', realm, authHeader: 'Basic dGVzdDp0ZXN0' }, dir);
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
    edit_timestamp: 1_700_002_000, stream_id: 7, orig_subject: 'deploys', subject: 'deploys-2026',
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
    // The acted-on message differs from the hint source: message_ids matters.
    f.feed(move());
    await until(() => f.reconciled.length === 1, 'self rename reconciled');
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
  f.feed(move([20, 21], { new_stream_id: 8 }));
  await until(() => f.reconciled.length === 1, 'cross-stream move reconciled');
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
    f.feed(move([20, 21], { new_stream_id: 8, subject: undefined }));
    await until(() => f.reconciled.length === 1, 'cross-stream move reconciled');
    await f.type('start');
    await assert.rejects(f.publish(), /moved.*zulip:archive.*deploys.*send_message/);
    await f.type('stop');
    assert.deepEqual(f.typing.map(s => [s.stream_id, s.op, s.topic]), [
      [7, 'start', 'deploys'], [7, 'stop', 'deploys'],
    ]);
    assert.deepEqual(f.sent, [], 'no fallback write to mcpl');
    if (busyDestination) {
      await f.publish(ARCHIVE);
      assert.equal(f.sent[0].topic, 'ops', 'an unrelated newer destination conversation is not overwritten');
    }
    await f.tools.handleToolCall('send_message', { type: 'stream', to: 'archive', topic: 'deploys', content: 'Done' });
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
      await f.tools.handleToolCall('move_messages', { message_id: 21, topic: '  renamed ', propagate_mode: mode });
      await f.publish();
      const affected = hintId === 21 || (mode === 'change_all' && hintId === 20);
      assert.equal(f.sent[0].topic, affected ? 'renamed' : message.subject);
    });
  }
}

test('cross-stream tool success blocks source immediately and re-seats co-moved reactions before any echo', async t => {
  const f = await fixture();
  t.after(() => f.close());
  await f.open();
  await f.open(ARCHIVE);
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.type('start');
  const result = await f.tools.handleToolCall('move_messages', { message_id: 20, stream_id: 8, propagate_mode: 'change_later' });
  assert.deepEqual(result.moved_to, { channelId: ARCHIVE, topic: 'deploys' });
  await assert.rejects(f.publish(), /moved.*zulip:archive.*deploys/);
  await f.type('start');
  f.feed({ type: 'reaction', op: 'add', message_id: 21, user_id: 7,
    emoji_name: 'thumbs_up', emoji_code: '1f44d', reaction_type: 'unicode_emoji' });
  await until(() => f.incoming.some(m => m.metadata?.reaction), 'reaction delivered');
  assert.equal(f.incoming.find(m => m.metadata?.reaction)!.channelId, ARCHIVE);
  f.feed(move([20, 21], { new_stream_id: 8, subject: undefined }));
  await until(() => f.reconciled.length >= 2, 'echo confirmation');
  await assert.rejects(f.publish(), /moved.*zulip:archive.*deploys/);
  await f.publish(ARCHIVE);
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['archive', 'deploys'], 'echo does not turn destination into a moved marker');
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
  await assert.rejects(f.publish(), /moved.*stream 9/);
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
  await assert.rejects(f.publish(), /moved.*stream 9/);
});

test('a successful placement read resolves a new stream for the later echo too', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.tools.handleToolCall('move_messages', { message_id: 20, stream_id: 9, propagate_mode: 'change_all' });
  await assert.rejects(f.publish(), /moved.*zulip:new-stream.*deploys/);
  f.feed(move([20, 21], { new_stream_id: 9, subject: undefined }));
  await until(() => f.reconciled.length === 2, 'echo confirmation');
  await assert.rejects(f.publish(), /moved.*zulip:new-stream.*deploys/);
  await f.publish('zulip:new-stream');
  assert.deepEqual([f.sent.at(-1).to, f.sent.at(-1).topic], ['new-stream', 'deploys']);
});

test('delayed first self echo cannot undo a second tool-confirmed rename', async t => {
  const f = await fixture();
  t.after(() => f.close());
  f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
  await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
  await f.callMove({ message_id: 21, topic: 'first' });
  await f.callMove({ message_id: 21, topic: 'second' });
  await f.publish();
  assert.equal(f.sent.at(-1).topic, 'second');
  f.feed(move([21], { message_id: 21, subject: 'first' }));
  await until(() => f.reconciled.length === 3, 'delayed first echo processed');
  await f.publish();
  assert.equal(f.placements.get(21).subject, 'second', 'stored placement did not change');
  assert.equal(f.sent.at(-1).topic, 'second', 'old echo must not roll back the current reply hint');
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
      await f.publish();
      assert.equal(f.sent.at(-1).topic, expected[2]);
      assert.equal(echoes[0].edit_timestamp, echoes[1].edit_timestamp, 'same-second operations are deliberate');
      if (order !== 'early') {
        for (const echo of order === 'reverse' ? [...delayed].reverse() : delayed) {
          const before = f.reconciled.length;
          f.feed(echo);
          await until(() => f.reconciled.length > before, 'delayed echo');
          assert.equal(f.reconciled.at(-1).confirmationOnly, true);
          await f.publish();
          assert.equal(f.sent.at(-1).topic, expected[2], 'hint must not follow an older operation');
        }
      }
      // Content-only edits read cached placement rather than the tool fixture's
      // stored placements. Check every side of each partial propagation range.
      for (const id of [20, 21, 30]) f.feed({ type: 'update_message', message_id: id, message_ids: [id],
        user_id: 7, stream_id: 7, edit_timestamp: 1_700_002_000,
        content: `edited ${id}`, orig_content: `old ${id}`, flags: ['mentioned'] });
      await until(() => f.incoming.filter(m => m.metadata?.change === 'edit').length === 3, 'placed edits');
      assert.deepEqual(f.incoming.filter(m => m.metadata?.change === 'edit').map(m => m.metadata?.topic), expected);
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
  await until(() => f.reconciled.length === 4, 'both echoes confirmed');
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
    f.feed({ type: 'message', flags: ['mentioned'], message: raw(21) });
    await until(() => f.server.delivery.watermark(GENERAL) === 21, 'mention accepted');
    await f.callMove({ message_id: 21, topic: 'first' });
    await f.callMove({ message_id: 21, topic: 'second' });
    // Same-second genuine change, including a content change not made by the
    // move tool: it must not be mistaken for one of its delayed echoes.
    f.placements.set(21, { ...raw(21, 'external'), stream_id: 8, display_recipient: 'archive' });
    f.feed(move([21], { message_id: 21, user_id: actor, orig_subject: 'second', subject: 'external', new_stream_id: 8 }),
      { type: 'reaction', op: 'add', message_id: 21, user_id: 7, emoji_name: 'eyes' },
      { type: 'update_message', message_id: 21, message_ids: [21], user_id: 7, stream_id: 8,
        edit_timestamp: 1_700_002_000, content: 'edited externally', orig_content: 'old', flags: ['mentioned'] });
    await until(() => f.incoming.some(m => m.metadata?.reaction) &&
      f.incoming.some(m => m.metadata?.change === 'edit'), 'external placement observed');
    assert.equal(f.incoming.find(m => m.metadata?.reaction)!.channelId, ARCHIVE);
    const edit = f.incoming.find(m => m.metadata?.change === 'edit')!;
    assert.deepEqual([edit.channelId, edit.metadata?.topic], [ARCHIVE, 'external']);
    if (actor === SELF) await assert.rejects(f.publish(), /moved.*zulip:archive.*external/);
    // Delayed tool echoes must not roll back even this later external move.
    f.feed(move([21], { message_id: 21, subject: 'first' }),
      move([21], { message_id: 21, orig_subject: 'first', subject: 'second' }));
    await until(() => f.reconciled.filter(c => c.confirmationOnly).length === 2, 'old operations confirmed');
    f.feed({ type: 'update_message', message_id: 21, message_ids: [21], user_id: 7,
      edit_timestamp: 1_700_002_000, content: 'edited again', orig_content: 'old', flags: ['mentioned'] });
    await until(() => f.incoming.filter(m => m.metadata?.change === 'edit').length === 2, 'later edit placed');
    const last = f.incoming.filter(m => m.metadata?.change === 'edit').at(-1)!;
    assert.deepEqual([last.channelId, last.metadata?.topic], [ARCHIVE, 'external']);
  });
}
