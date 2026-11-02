/** Real event loop, adapter, server and MCPL wire; only Zulip is a stand-in. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
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

async function fixture(policy: ZulipFilters = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-self-move-'));
  const filtersPath = join(dir, 'filters.json');
  writeFileSync(filtersPath, JSON.stringify({ reactionChannels: [GENERAL, ARCHIVE], ...policy }));
  const filters = new FiltersPlane(filtersPath, {});
  filters.start();
  const pending: any[] = [];
  let eventId = 0;
  const sent: any[] = [];
  const typing: any[] = [];
  const client = {
    streams: { retrieve: async () => ({ result: 'success', streams: [
      { name: 'general', stream_id: 7 }, { name: 'archive', stream_id: 8 },
    ] }) },
    messages: {
      retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
      getById: async ({ message_id }: any) => ({ result: 'success', message: raw(message_id) }),
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
  const tools = new ZulipToolRuntime({ client, selfUserId: SELF, sessionId: 'test' }, dir);
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
    host, server, tools, sent, typing, incoming, pushes, reconciled,
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
