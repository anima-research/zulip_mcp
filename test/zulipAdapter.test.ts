import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipAdapter, type FilterView } from '../src/platforms/zulip.ts';
import type { ChangeEntry, IncomingEntry, MessageChangeEvent } from '../src/platforms/adapter.ts';
import type { ZulipRawMessage } from '../src/history.ts';
import type { ChannelDescriptor } from '@animalabs/mcpl-core';
import { inTurn, standInApi } from './zulip-stand-in.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import { dmKey, streamKey } from '../src/channel-key.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';

const SELF = 790;
const GENERAL = streamKey(7);

function directoryOf(streams: Record<number, string> = { 7: 'general' }): StreamDirectory {
  const directory = new StreamDirectory();
  directory.registration(Object.entries(streams).map(([id, name]) => ({ stream_id: Number(id), name })));
  return directory;
}

function raw(id: number, over: Partial<ZulipRawMessage> = {}): ZulipRawMessage {
  return {
    id, sender_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
    display_recipient: 'general', stream_id: 7, subject: 'deploys', content: `m${id}`, timestamp: 1_700_000_000 + id, type: 'stream', flags: [],
    ...over,
  };
}

function dm(id: number, senderId: number): ZulipRawMessage {
  const people = [
    { id: SELF, full_name: 'Bot', email: 'bot@example.com' },
    { id: 7, full_name: 'Ann', email: 'ann@example.com' },
    { id: 42, full_name: 'Bo', email: 'bo@example.com' },
  ];
  const sender = people.find((p) => p.id === senderId)!;
  return raw(id, { type: 'private', display_recipient: people, sender_id: senderId, sender_full_name: sender.full_name, sender_email: sender.email });
}

function fakeClient(rows: ZulipRawMessage[], routes: Parameters<typeof standInApi>[0] = {}) {
  const calls: { retrieve: Record<string, unknown>[]; subscribe: unknown[] } = { retrieve: [], subscribe: [] };
  const client = standInApi({
    'GET /messages': async (params: Record<string, unknown>) => {
      calls.retrieve.push(params);
      const after = typeof params.anchor === 'number' && params.num_after ? params.anchor : null;
      const matching = rows
        .filter((m) => after === null || m.id > after)
        .filter((m) => {
          // Both of Zulip's narrow forms: [operator, operand] and {operator, operand}.
          const operand = (op: string) => (params.narrow as unknown[])
            .map((n) => (Array.isArray(n) ? (n[0] === op ? n[1] : undefined) : (n as { operator: string; operand: unknown }).operator === op ? (n as { operand: unknown }).operand : undefined))
            .find((v) => v !== undefined);
          const stream = operand('stream');
          const dmIds = operand('dm') as number[] | undefined;
          if (typeof stream === 'number') return m.type === 'stream' && m.stream_id === stream;
          if (stream) return m.type === 'stream' && m.display_recipient === stream;
          if (dmIds) return m.type === 'private';
          return true;
        });
      const limit = Number(params.num_after || params.num_before);
      const page = after !== null ? matching.slice(0, limit) : matching.slice(-limit);
      return { result: 'success', messages: page, found_newest: page[page.length - 1] === matching[matching.length - 1], found_oldest: false };
    },
    'POST /users/me/subscriptions': async (p: unknown) => { calls.subscribe.push(p); return { result: 'success', subscribed: {}, already_subscribed: { 'bot@example.com': ['general'] } }; },
    ...routes,
  }).api;
  return { client, calls };
}

const onlyDev: FilterView = { streamAllowed: (s) => s === 'dev', dmAllowed: (u) => u.id === 42 };

test('fetchHistory withholds the bot\'s own messages and disallowed senders, and reports the newest id it scanned', async () => {
  const { client } = fakeClient([raw(1, { sender_id: SELF }), raw(2, { sender_id: SELF }), raw(3), dm(4, 7), dm(5, 42), dm(6, SELF)]);
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: onlyDev, directory: directoryOf() });

  const none = await adapter.fetchHistory(GENERAL, { limit: 10 });
  assert.deepEqual(none, { messages: [], scannedThrough: null, reachedNewest: true });

  const page = await adapter.fetchHistory(dmKey([7, 42]), { limit: 10, afterMessageId: '3' });
  assert.deepEqual(page.messages.map((m) => m.messageId), ['5']);
  assert.equal(page.scannedThrough, 6, 'the cursor covers the withheld rows');
  assert.equal(page.reachedNewest, true);
  assert.equal((page.messages[0].metadata as { backscroll: boolean }).backscroll, true);

  const allowAll: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const wide = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: allowAll, directory: directoryOf() });
  const own = await wide.fetchHistory(GENERAL, { limit: 2, afterMessageId: '0' });
  assert.deepEqual(own.messages, []);
  assert.equal(own.scannedThrough, 2);
  assert.equal(own.reachedNewest, false);
});

test('fetchContext injects nothing for a stream outside the allowlist, and only allowed senders of a DM', async () => {
  const { client, calls } = fakeClient([raw(3), dm(4, 7), dm(5, 42), dm(6, SELF)]);
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: onlyDev, directory: directoryOf() });
  assert.equal(await adapter.fetchContext(GENERAL, undefined, 5), null);
  assert.equal(calls.retrieve.length, 0, 'not even fetched');
  const injection = await adapter.fetchContext(dmKey([7, 42]), undefined, 5);
  assert.ok(injection);
  assert.match(injection!.content, /Bo: m5/);
  const timed = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: onlyDev, formatTime: () => 'T', directory: directoryOf() });
  const lines = (await timed.fetchContext(dmKey([7, 42]), undefined, 5))!.content.split('\n');
  assert.deepEqual(lines.slice(1), ['[T id=5] [DM] Bo: m5', '[T id=6] [DM] Bot: m6'], 'injected history uses the shared line shape, ids included');
  assert.match(injection!.content, /Bot: m6/, 'the bot\'s own turns stay in context');
  assert.doesNotMatch(injection!.content, /Ann: m4/);
});

test('ensureSubscribed asks Zulip every time — a subscription dropped by unlisten is not remembered as live', async () => {
  const { client, calls } = fakeClient([]);
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { directory: directoryOf() });
  await adapter.ensureSubscribed(GENERAL);
  await adapter.ensureSubscribed(GENERAL);
  assert.equal(calls.subscribe.length, 2);
  await adapter.ensureSubscribed(dmKey([42]));
  assert.equal(calls.subscribe.length, 2, 'DMs need nothing');
});

test('publish uploads image blocks and links them after the text; without an uploader they are dropped as before', async () => {
  const sends: Record<string, unknown>[] = [];
  const client = standInApi({
    'POST /messages': async (p: Record<string, unknown>) => { sends.push(p); return { result: 'success', id: 77 }; },
  }).api;
  const uploaded: string[] = [];
  const uploader = {
    async upload(i: { name: string; data: Buffer }) {
      uploaded.push(i.name);
      return { name: i.name, path: `/user_uploads/1/${i.name}`, url: `https://z/user_uploads/1/${i.name}` };
    },
  };
  const blocks = [
    { type: 'text' as const, text: 'chart attached' },
    { type: 'image' as const, data: Buffer.from('png').toString('base64'), mimeType: 'image/png' },
  ];

  const uploadPolicy = { roots: new Map<string, string>(), maxBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };
  const withUploads = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { uploader, uploadPolicy });
  const res = await withUploads.publish(GENERAL, undefined, blocks, { threadId: 'deploys' });
  assert.deepEqual(res, { delivered: true, messageId: '77', messageIds: ['77'] });
  assert.deepEqual(uploaded, ['image-2.png']);
  assert.deepEqual(sends[0], { type: 'stream', to: 7, topic: 'deploys', content: 'chart attached\n\n[image-2.png](/user_uploads/1/image-2.png)' });

  await withUploads.publish(dmKey([42]), undefined, [blocks[1]]);
  assert.deepEqual(sends[1], { type: 'private', to: [42], content: '[image-1.png](/user_uploads/1/image-1.png)' });

  const failing = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { uploader: { async upload() { throw new Error('quota'); } }, uploadPolicy });
  await assert.rejects(failing.publish(GENERAL, undefined, blocks), /quota/);
  assert.equal(sends.length, 2);

  const plain = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' });
  await plain.publish(GENERAL, undefined, blocks);
  assert.equal(sends[2].content, 'chart attached');
  assert.equal((await plain.publish(GENERAL, undefined, [blocks[1]])).delivered, false);

  await plain.publish({ streamName: 'elsewhere' }, undefined, [blocks[0]]);
  assert.deepEqual(sends[3], { type: 'stream', to: 'elsewhere', topic: 'mcpl', content: 'chart attached' });
});

function eventingClient(rows: ZulipRawMessage[], events: { message: Record<string, unknown>; flags?: string[] }[], visible: { name: string; stream_id: number }[] = []) {
  let served = false;
  return fakeClient(rows, {
    'GET /streams': async () => ({ result: 'success', streams: visible }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      if (served) return new Promise(() => {});
      served = true;
      return { result: 'success', events: events.map((e, i) => ({ id: i + 1, type: 'message', message: e.message, flags: e.flags ?? [] })) };
    },
  }).client;
}

function streamEvent(id: number, stream: string, streamId: number | undefined, over: Record<string, unknown> = {}) {
  return {
    message: {
      id, sender_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
      display_recipient: stream, subject: 'incidents', content: 'hello', timestamp: 1_700_000_000,
      type: 'stream', ...(streamId === undefined ? {} : { stream_id: streamId }), ...over,
    },
    flags: ['mentioned'],
  };
}

test('every stream message carries its stream descriptor, so a stream joined after startup can be registered (#20)', async () => {
  const client = eventingClient([], [streamEvent(900, 'ops', 12), streamEvent(901, 'ops', 12), streamEvent(902, 'general', 7)], [{ name: 'ops', stream_id: 12 }, { name: 'general', stream_id: 7 }]);
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' });
  const seen: { key: string; descriptor?: string }[] = [];
  adapter.startEvents(inTurn(({ message: m, newChannel: descriptor }) => { seen.push({ key: m.key, descriptor: descriptor?.id }); }));
  await new Promise((r) => setTimeout(r, 50));
  adapter.stopEvents();

  assert.deepEqual(seen.map((s) => s.key), [streamKey(12), streamKey(12), GENERAL]);
  assert.deepEqual(seen.map((s) => s.descriptor), ['zulip:ops', 'zulip:ops', 'zulip:general']);
});

test('the descriptor a message carries matches the one discovery builds for the same stream (#20)', async () => {
  const client = eventingClient([], [streamEvent(900, 'general', 7)], [{ name: 'general', stream_id: 7 }]);
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' });
  const discovered = (await adapter.discoverChannels()).find((c) => c.id === 'zulip:general')!;
  let fromEvent: ChannelDescriptor | undefined;
  adapter.startEvents(inTurn(({ newChannel: descriptor }) => { fromEvent = descriptor; }));
  await new Promise((r) => setTimeout(r, 50));
  adapter.stopEvents();
  assert.ok(fromEvent);
  assert.equal(fromEvent!.id, discovered.id);
  assert.equal(fromEvent!.label, discovered.label);
  assert.deepEqual(fromEvent!.address, discovered.address);
  assert.deepEqual(fromEvent!.capabilities, discovered.capabilities);
});

test('a message without a stream_id is placed by its exact name, and its descriptor has the stream id (#20)', async () => {
  const client = eventingClient([], [streamEvent(900, 'ops', undefined)], [{ name: 'ops', stream_id: 12 }]);
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' });
  const seen: { id: string; address: unknown }[] = [];
  adapter.startEvents(inTurn(({ newChannel: descriptor }) => { if (descriptor) seen.push({ id: descriptor.id, address: descriptor.address }); }));
  await new Promise((r) => setTimeout(r, 50));
  adapter.stopEvents();
  assert.deepEqual(seen, [{ id: 'zulip:ops', address: { stream_name: 'ops', stream_id: 12 } }]);
});

test("an edit that also moves a later message, of a first message the cache doesn't hold, is read from Zulip and told as that message's edit", async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(6) },
    { id: 2, type: 'update_message', message_id: 5, message_ids: [5, 6], user_id: 7, edit_timestamp: 1_700_000_905, orig_content: 'm5', content: 'm5 fixed', orig_subject: 'deploys', subject: 'elsewhere', flags: [], stream_id: 7 },
  ];
  let polls = 0;
  const got: unknown[] = [];
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'GET /messages/:id': async (_p: unknown, call: { id?: number }) => { got.push(`GET /messages/${call.id}`); return { result: 'success', message: raw(call.id!) }; },
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => { polls++; if (polls === 1) return { events }; return new Promise(() => {}); },
  }).api;
  const filters: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(() => {}), undefined, undefined, (entry) => {
      void entry.ready().then((c) => { got.push(c && { kind: c.kind, messageId: c.messageId, content: c.content, topic: c.topic }); });
    });
    const deadline = performance.now() + 2000;
    while (!got.some((g) => typeof g !== 'string') && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(got, ['GET /messages/5', { kind: 'edit', messageId: '5', content: 'm5 fixed', topic: 'elsewhere' }]);
});

test('two moves of one cached message, the second arriving while the first waits, leave the cache at the second\'s stream', async () => {
  const first = [
    { id: 1, type: 'message', flags: [], message: raw(5) },
    { id: 2, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_903, orig_subject: 'deploys', stream_id: 7, new_stream_id: 8 },
    { id: 3, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_904, orig_subject: 'deploys', stream_id: 8, new_stream_id: 9 },
  ];
  const later = [
    { id: 4, type: 'update_message', message_id: 5, message_ids: [5], user_id: 7, edit_timestamp: 1_700_000_905, orig_content: 'm5', content: 'm5 fixed', flags: [], stream_id: 9 },
  ];
  let polls = 0;
  let laterSent!: () => void;
  const laterDue = new Promise<void>((r) => { laterSent = r; });
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }, { name: 'random', stream_id: 8, subscriber_count: 2 }, { name: 'ops', stream_id: 9, subscriber_count: 2 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events: first };
      if (polls === 2) {
        await laterDue;
        return { events: later };
      }
      return new Promise(() => {});
    },
  }).api;
  const filters: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  let openLane7!: () => void;
  const lanes = new Map<string, Promise<void>>([[String(streamKey(7)), new Promise<void>((r) => { openLane7 = r; })]]);
  const lines: string[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(() => {}), undefined, undefined, (entry) => {
      const lane = String(entry.key);
      lanes.set(lane, (lanes.get(lane) ?? Promise.resolve()).then(async () => {
        const c = await entry.ready();
        lines.push(`${c?.kind} entered ${lane}, line ${c?.key}`);
      }));
    });
    const deadline = performance.now() + 2000;
    while (polls < 2 && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await new Promise((r) => setTimeout(r, 20));
    openLane7();
    while (lines.length < 2 && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    laterSent();
    while (lines.length < 3 && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(lines, [
    `move entered ${streamKey(7)}, line ${streamKey(7)}`,
    `move entered ${streamKey(7)}, line ${streamKey(8)}`,
    `edit entered ${streamKey(9)}, line ${streamKey(9)}`,
  ]);
});

test("the bot's own move reaches the server marked as the bot's, its own content edit doesn't, and a move says whether its message left the stream", async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(5) },
    { id: 2, type: 'message', flags: [], message: raw(6) },
    { id: 3, type: 'update_message', message_id: 5, message_ids: [5], user_id: SELF, edit_timestamp: 1_700_000_900, orig_subject: 'deploys', subject: 'releases', propagate_mode: 'change_one', stream_id: 7 },
    { id: 4, type: 'update_message', message_id: 5, message_ids: [5], user_id: SELF, edit_timestamp: 1_700_000_901, orig_content: 'm5', content: 'm5, again', flags: [], stream_id: 7 },
    { id: 5, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_902, orig_subject: 'releases', stream_id: 7, new_stream_id: 8 },
    { id: 6, type: 'update_message', message_id: 6, message_ids: [6], user_id: 12, edit_timestamp: 1_700_000_903, orig_subject: 'deploys', stream_id: 7, new_stream_id: 99 },
  ];
  let polls = 0;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }, { name: 'other', stream_id: 8, subscriber_count: 2 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const filters: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(() => {}), undefined, undefined, inTurn((c) => { changes.push(c); }));
    const deadline = Date.now() + 2000;
    while (changes.length < 3 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await new Promise((r) => setTimeout(r, 20));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  const view = changes.map((c) => ({
    kind: c.kind, messageId: c.messageId, topic: c.topic, previousTopic: c.previousTopic, movedTo: c.movedToKey, leftChannel: c.leftChannel, byBot: c.byBot,
  }));
  assert.deepEqual(view, [
    { kind: 'move', messageId: '5', topic: 'releases', previousTopic: 'deploys', movedTo: null, leftChannel: false, byBot: true },
    { kind: 'move', messageId: '5', topic: 'releases', previousTopic: null, movedTo: streamKey(8), leftChannel: true, byBot: false },
    { kind: 'move', messageId: '6', topic: 'deploys', previousTopic: null, movedTo: null, leftChannel: true, byBot: false },
  ]);
});

test("whereSeen answers a seen message's place as its moves left it, and nothing for a message that moved to a stream the adapter doesn't know", async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(5) },
    { id: 2, type: 'message', flags: [], message: raw(6) },
    { id: 3, type: 'message', flags: [], message: raw(7) },
    { id: 4, type: 'update_message', message_id: 5, message_ids: [5, 6], user_id: 12, edit_timestamp: 1_700_000_900, orig_subject: 'deploys', subject: 'releases', propagate_mode: 'change_all', stream_id: 7 },
    { id: 5, type: 'update_message', message_id: 7, message_ids: [7], user_id: 12, edit_timestamp: 1_700_000_901, orig_subject: 'deploys', subject: 'elsewhere', stream_id: 7, new_stream_id: 99 },
  ];
  let polls = 0;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const filters: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(() => {}), undefined, undefined, inTurn((c) => { changes.push(c); }));
    const deadline = Date.now() + 2000;
    while (changes.length < 2 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await new Promise((r) => setTimeout(r, 20));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  const place = (id: string) => adapter.whereSeen?.(id) ?? 'none';
  assert.deepEqual(
    { 5: place('5'), 6: place('6'), 7: place('7'), 8: place('8') },
    { 5: { key: streamKey(7), topic: 'releases' }, 6: { key: streamKey(7), topic: 'releases' }, 7: 'none', 8: 'none' },
  );
});

test("a message seen again loses its mark of a move to an unknown stream: by a move back from that stream once it's known, or by a history read", async () => {
  const first = [
    { id: 1, type: 'message', flags: [], message: raw(5) },
    { id: 2, type: 'message', flags: [], message: raw(6) },
    { id: 3, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_900, orig_subject: 'deploys', stream_id: 7, new_stream_id: 99 },
    { id: 4, type: 'update_message', message_id: 6, message_ids: [6], user_id: 12, edit_timestamp: 1_700_000_901, orig_subject: 'deploys', stream_id: 7, new_stream_id: 99 },
  ];
  const second = [
    { id: 5, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_902, orig_subject: 'deploys', stream_id: 99, new_stream_id: 7 },
  ];
  let polls = 0;
  let known99 = false;
  let secondDue = false;
  const { client } = fakeClient([raw(6)], {
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }, ...(known99 ? [{ name: 'elsewhere', stream_id: 99, subscriber_count: 2 }] : [])] }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events: first };
      if (secondDue) { secondDue = false; return { events: second }; }
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  });
  const filters: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(() => {}), undefined, undefined, inTurn((c) => { changes.push(c); }));
    const wait = async (n: number) => {
      const deadline = Date.now() + 2000;
      while (changes.length < n && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    };
    await wait(2);
    known99 = true;
    await adapter.refreshStreams('the test', true);
    secondDue = true;
    await wait(3);
    await new Promise((r) => setTimeout(r, 20));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  await adapter.fetchHistory(GENERAL, { limit: 10 });
  const place = (id: string) => adapter.whereSeen?.(id) ?? 'none';
  assert.deepEqual({ 5: place('5'), 6: place('6') }, { 5: { key: GENERAL, topic: 'deploys' }, 6: { key: GENERAL, topic: 'deploys' } });
});

test('a history read of a message the cache already holds evicts nothing, even with the cache full', async () => {
  const rows = Array.from({ length: 2000 }, (_, i) => raw(i + 1));
  const { client } = fakeClient(rows);
  const filters: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, directory: directoryOf() });
  await adapter.fetchHistory(GENERAL, { limit: 2000 });
  await adapter.fetchHistory(GENERAL, { limit: 1, afterMessageId: '1499' });
  assert.deepEqual(adapter.whereSeen?.('1') ?? 'none', { key: GENERAL, topic: 'deploys' }, 'the oldest entry stays');
});

test('edits, moves and deletions are placed, cleaned and filtered before reaching the server (#22)', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: { ...raw(5), content: 'first draft' } },
    { id: 2, type: 'update_message', message_id: 5, message_ids: [5], user_id: 7, edit_timestamp: 1_700_000_900, orig_content: 'first draft', content: 'second draft @**Bot**', flags: ['mentioned'], stream_id: 7 },
    { id: 3, type: 'update_message', message_id: 5, message_ids: [5], user_id: SELF, edit_timestamp: 1_700_000_901, orig_content: 'x', content: 'y', flags: [], stream_id: 7 },
    { id: 4, type: 'update_message', message_id: 6, message_ids: [6], user_id: 7, edit_timestamp: 1_700_000_902, orig_content: 'm6', content: 'm6 fixed', flags: [], stream_id: 7 },
    { id: 5, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_903, orig_subject: 'deploys', subject: 'deploys-2', propagate_mode: 'change_one', stream_id: 7 },
    // A cross-stream move of a message the cache evicted: the GET finds it
    // in its NEW stream, but the agent only ever saw it in the old one, which
    // the event names.
    { id: 55, type: 'update_message', message_id: 700, message_ids: [700], user_id: 12, edit_timestamp: 1_700_000_904, orig_subject: 'deploys', stream_id: 7, new_stream_id: 8 },
    { id: 6, type: 'delete_message', message_ids: [5], message_type: 'stream', stream_id: 7, topic: 'deploys-2' },
    { id: 7, type: 'delete_message', message_ids: [900, 901], message_type: 'stream', stream_id: 7, topic: 'old' },
    { id: 8, type: 'delete_message', message_id: 902, message_type: 'private' },
    { id: 9, type: 'delete_message', message_ids: [903], message_type: 'stream', stream_id: 8, topic: 't' },
  ];
  let polls = 0;
  const getById: number[] = [];
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }, { name: 'secret', stream_id: 8, subscriber_count: 1 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
      getById.push(message_id);
      if (message_id === 6) await new Promise((r) => setTimeout(r, 40));
      return { result: 'success', message: message_id === 700 ? raw(700, { display_recipient: 'secret', stream_id: 8 }) : raw(message_id) };
    },
    'DELETE /messages/:id': async () => ({ result: 'success' }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const filters: FilterView = { streamAllowed: (s) => s === 'general', dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const delivered: string[] = [];
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(({ message: m }) => { delivered.push(m.messageId); }), undefined, undefined, inTurn((c) => { changes.push(c); }));
    const deadline = Date.now() + 2000;
    while (changes.length < 6 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(delivered, ['5'], 'the live message still reaches onMessage');
  assert.deepEqual(getById, [6, 700], 'only the uncached messages cost a GET');
  const view = changes.map((c) => ({
    kind: c.kind, key: c.key, messageId: c.messageId, messageIds: c.messageIds, authorName: c.authorName, actorId: c.actorId,
    topic: c.topic, previousTopic: c.previousTopic, movedTo: c.movedToKey, content: c.content, previousContent: c.previousContent,
    mentioned: c.mentioned, wasMentioned: c.previouslyMentioned, onOwnMessage: c.onOwnMessage,
  }));
  assert.deepEqual(view, [
    { kind: 'edit', key: GENERAL, messageId: '5', messageIds: ['5'], authorName: 'Ann', actorId: '7', topic: 'deploys', previousTopic: null, movedTo: null, content: 'second draft @**Bot**', previousContent: 'first draft', mentioned: true, wasMentioned: false, onOwnMessage: false },
    // Placed by GET: only the post-edit state is known, so no previous verdict.
    { kind: 'edit', key: GENERAL, messageId: '6', messageIds: ['6'], authorName: 'Ann', actorId: '7', topic: 'deploys', previousTopic: null, movedTo: null, content: 'm6 fixed', previousContent: 'm6', mentioned: false, wasMentioned: null, onOwnMessage: false },
    { kind: 'move', key: GENERAL, messageId: '5', messageIds: ['5'], authorName: 'Ann', actorId: '12', topic: 'deploys-2', previousTopic: 'deploys', movedTo: null, content: null, previousContent: null, mentioned: true, wasMentioned: true, onOwnMessage: false },
    { kind: 'move', key: GENERAL, messageId: '700', messageIds: ['700'], authorName: 'Ann', actorId: '12', topic: 'deploys', previousTopic: null, movedTo: streamKey(8), content: null, previousContent: null, mentioned: false, wasMentioned: null, onOwnMessage: false },
    { kind: 'delete', key: GENERAL, messageId: '5', messageIds: ['5'], authorName: 'Ann', actorId: null, topic: 'deploys-2', previousTopic: null, movedTo: null, content: null, previousContent: 'second draft @**Bot**', mentioned: true, wasMentioned: true, onOwnMessage: false },
    { kind: 'delete', key: GENERAL, messageId: '900', messageIds: ['900', '901'], authorName: null, actorId: null, topic: 'old', previousTopic: null, movedTo: null, content: null, previousContent: null, mentioned: false, wasMentioned: null, onOwnMessage: false },
  ]);
  assert.deepEqual(changes[0].timestamp, new Date(1_700_000_900_000), 'an edit is stamped with Zulip\'s edit time');
  assert.equal(changes[0].authorEmail, 'ann@example.com', 'the sender travels with the change for the DM allowlist');
});

test('the bot\'s own deletions (rollback, the delete tool) do not echo back as changes, a delete that got no answer included', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(5) },
    { id: 2, type: 'message', flags: [], message: raw(6) },
    { id: 3, type: 'message', flags: [], message: raw(7) },
  ];
  let polls = 0;
  let released: (() => void) | null = null;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => ({ result: 'success', message: raw(message_id) }),
    'DELETE /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
      if (message_id === 8) throw new ZulipNoAnswer('timeout', 'DELETE', '/messages/8', true, 'Zulip gave no answer to DELETE /messages/8 within 15 s.');
      return message_id === 7 ? { result: 'error', msg: 'nope' } : { result: 'success' };
    },
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      if (polls === 2) {
        await new Promise<void>((r) => { released = r; });
        return { events: [
          { id: 4, type: 'delete_message', message_ids: [5], message_type: 'stream', stream_id: 7, topic: 'deploys' },
          { id: 5, type: 'delete_message', message_ids: [6], message_type: 'stream', stream_id: 7, topic: 'deploys' },
          { id: 6, type: 'delete_message', message_ids: [7], message_type: 'stream', stream_id: 7, topic: 'deploys' },
          { id: 7, type: 'delete_message', message_ids: [8], message_type: 'stream', stream_id: 7, topic: 'deploys' },
          { id: 8, type: 'delete_message', message_ids: [9], message_type: 'stream', stream_id: 7, topic: 'deploys' },
        ] };
      }
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(() => {}, undefined, undefined, inTurn((c) => { changes.push(c); }));
    const deadline = Date.now() + 2000;
    while (!released && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await adapter.deleteMessage(GENERAL, '5');
    adapter.noteSelfDeleted(6);
    await assert.rejects(adapter.deleteMessage(GENERAL, '7'));
    await assert.rejects(adapter.deleteMessage(GENERAL, '8'));
    released!();
    while (!changes.some((c) => c.messageId === '9') && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(changes.map((c) => [c.kind, c.messageId]), [['delete', '7'], ['delete', '9']], 'only the deletions the bot did not make surface');
});

test('a delete noted again becomes the newest mark, and noting it again removes no other mark', async () => {
  let released: (() => void) | null = null;
  let polls = 0;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => ({ result: 'success', message: raw(message_id) }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) {
        await new Promise<void>((r) => { released = r; });
        return { events: [1000, 1001, 1002, 9].map((id, i) => ({ id: i + 1, type: 'delete_message', message_ids: [id], message_type: 'stream', stream_id: 7, topic: 'deploys' })) };
      }
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(() => {}, undefined, undefined, inTurn((c) => { changes.push(c); }));
    const deadline = Date.now() + 2000;
    while (!released && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.noteSelfDeleted(1000);
    adapter.noteSelfDeleted(1001);
    for (let id = 1002; id < 1500; id++) adapter.noteSelfDeleted(id);
    adapter.noteSelfDeleted(1001);
    adapter.noteSelfDeleted(2000);
    adapter.noteSelfDeleted(2001);
    released!();
    while (!changes.some((c) => c.messageId === '9') && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(changes.map((c) => c.messageId), ['1000', '1002', '9'], 'the two oldest marks went to the two new ids, and the mark noted again stayed');
});

test('bulk deletion, co-moved ids, unresolvable origins and vanishing moves (#22 round 2)', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(20) },
    { id: 2, type: 'message', flags: [], message: raw(21) },
    { id: 3, type: 'message', flags: [], message: raw(22) },
    { id: 4, type: 'update_message', message_id: 20, message_ids: [20, 21, 22], user_id: 12, edit_timestamp: 1_700_001_000, orig_subject: 'deploys', subject: 'archive', propagate_mode: 'change_all', stream_id: 7 },
    { id: 5, type: 'delete_message', message_ids: [19, 21], message_type: 'stream', stream_id: 7, topic: 'archive' },
    // A cross-stream move of an uncached message whose origin stream is
    // unknown: the GET only knows the destination, so it is dropped.
    { id: 6, type: 'update_message', message_id: 800, message_ids: [800], user_id: 12, edit_timestamp: 1_700_001_001, orig_subject: 'x', stream_id: 99, new_stream_id: 8 },
    // A move into a stream the bot cannot see, then Zulip's delete for lost
    // visibility.
    { id: 7, type: 'update_message', message_id: 22, message_ids: [22], user_id: 12, edit_timestamp: 1_700_001_002, orig_subject: 'archive', stream_id: 7, new_stream_id: 66 },
    { id: 8, type: 'delete_message', message_ids: [22], message_type: 'stream', stream_id: 66, topic: 'archive' },
  ];
  let polls = 0;
  let listCalls = 0;
  const client = standInApi({
    'GET /streams': async () => { listCalls++; return { result: 'success', streams: [{ name: 'general', stream_id: 7 }, { name: 'secret', stream_id: 8 }] }; },
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => ({ result: 'success', message: raw(message_id, { display_recipient: 'secret', stream_id: 8 }) }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(() => {}, undefined, undefined, inTurn((c) => { changes.push(c); }));
    const deadline = Date.now() + 2000;
    while (changes.length < 4 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await new Promise((r) => setTimeout(r, 30));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  const view = changes.map((c) => ({ kind: c.kind, key: c.key, messageId: c.messageId, messageIds: c.messageIds, authorName: c.authorName, topic: c.topic, movedTo: c.movedToKey, previousContent: c.previousContent, vanished: c.vanished }));
  assert.deepEqual(view, [
    { kind: 'move', key: GENERAL, messageId: '20', messageIds: ['20', '21', '22'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: null, vanished: false },
    { kind: 'delete', key: GENERAL, messageId: '21', messageIds: ['19', '21'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: 'm21', vanished: false },
    { kind: 'move', key: GENERAL, messageId: '22', messageIds: ['22'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: null, vanished: false },
    { kind: 'delete', key: GENERAL, messageId: '22', messageIds: ['22'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: 'm22', vanished: true },
  ]);
  assert.equal(listCalls, 2, 'unknown stream ids cost one list refresh, then a hold-off — not one per event');
});

test('a change to the bot\'s own DM message is judged by whom it was sent to', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: dm(30, SELF) },
    { id: 2, type: 'update_message', message_id: 30, message_ids: [30], user_id: 42, edit_timestamp: 1_700_002_000, orig_subject: '', subject: 'renamed', stream_id: undefined },
  ];
  let polls = 0;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'GET /messages/:id': async () => ({ result: 'error' }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => { polls++; if (polls === 1) return { events }; await new Promise((r) => setTimeout(r, 5)); return { events: [] }; },
  }).api;
  const run = async (filters: FilterView) => {
    polls = 0;
    const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, filters });
    const changes: MessageChangeEvent[] = [];
    const original = console.error;
    console.error = () => {};
    try {
      adapter.startEvents(() => {}, undefined, undefined, inTurn((c) => { changes.push(c); }));
      await new Promise((r) => setTimeout(r, 60));
      adapter.stopEvents();
    } finally {
      console.error = original;
    }
    return changes.length;
  };
  assert.equal(await run({ streamAllowed: () => true, dmAllowed: (u) => u.id === 42 || u.id === 7 }), 1, 'a counterpart is allowlisted');
  assert.equal(await run({ streamAllowed: () => true, dmAllowed: (u) => u.id === 999 }), 0, 'no counterpart is: the bot\'s own authorship is no pass');
});

test('a message from a stream created after startup carries its descriptor, with the stream id; a filtered stream carries nothing', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(10) },
    { id: 2, type: 'stream', op: 'create', streams: [{ stream_id: 9, name: 'late-stream' }] },
    { id: 3, type: 'message', flags: ['mentioned'], message: raw(11, { display_recipient: 'late-stream', stream_id: 9, content: '@**Bot** hi' }) },
    { id: 4, type: 'message', flags: [], message: raw(12, { display_recipient: 'late-stream', stream_id: 9 }) },
    { id: 5, type: 'message', flags: [], message: raw(13, { display_recipient: 'secret', stream_id: 8 }) },
    { id: 6, type: 'message', flags: [], message: raw(14) },
  ];
  let polls = 0;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }, { name: 'secret', stream_id: 8, subscriber_count: 2 }] }),
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const filters: FilterView = { streamAllowed: (s) => s !== 'secret', dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const seen: { id: string; key: string; newChannel?: string; address?: unknown }[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(({ message: m, newChannel: d }) => { seen.push({ id: m.messageId, key: m.key, newChannel: d?.id, address: d?.address }); }));
    const deadline = Date.now() + 2000;
    while (!seen.some((s) => s.id === '14') && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(seen, [
    { id: '10', key: GENERAL, newChannel: 'zulip:general', address: { stream_name: 'general', stream_id: 7 } },
    { id: '11', key: streamKey(9), newChannel: 'zulip:late-stream', address: { stream_name: 'late-stream', stream_id: 9 } },
    { id: '12', key: streamKey(9), newChannel: 'zulip:late-stream', address: { stream_name: 'late-stream', stream_id: 9 } },
    { id: '14', key: GENERAL, newChannel: 'zulip:general', address: { stream_name: 'general', stream_id: 7 } },
  ]);
});

test('a stream the directory does not hold costs one list read for that stream, so another unknown stream within the minute is read for too, and a failed read holds nothing off', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(30, { display_recipient: 'north', stream_id: 11 }) },
    { id: 2, type: 'message', flags: [], message: raw(31, { display_recipient: 'north', stream_id: 11 }) },
    { id: 3, type: 'message', flags: [], message: raw(32, { display_recipient: 'south', stream_id: 12 }) },
    { id: 4, type: 'message', flags: [], message: raw(33, { display_recipient: 'east', stream_id: 13 }) },
    { id: 5, type: 'message', flags: [], message: raw(34, { display_recipient: 'east', stream_id: 13 }) },
    { id: 6, type: 'message', flags: [], message: raw(35) },
  ];
  let polls = 0;
  let listCalls = 0;
  const streams = [{ name: 'general', stream_id: 7 }];
  const client = standInApi({
    'GET /streams': async () => {
      listCalls++;
      if (listCalls === 1) return { result: 'error', code: 'BAD_REQUEST', msg: 'try again' };
      return { result: 'success', streams: [...streams] };
    },
    'GET /messages': async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory: directoryOf() });
  const seen: string[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(inTurn(({ message: m }) => {
      seen.push(`${m.messageId}@${m.key}`);
      if (m.messageId === '31') streams.push({ name: 'south', stream_id: 12 });
    }));
    streams.push({ name: 'north', stream_id: 11 });
    const deadline = Date.now() + 2000;
    while (!seen.some((s) => s.startsWith('35@')) && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(seen, [
    `31@${streamKey(11)}`,
    `32@${streamKey(12)}`,
    `35@${GENERAL}`,
  ]);
  assert.equal(listCalls, 4);
});

test('a miss while a read of the stream list is in flight waits for that read', async () => {
  let release: (() => void) | undefined;
  let listCalls = 0;
  const client = standInApi({
    'GET /streams': async () => {
      listCalls++;
      await new Promise<void>((resolve) => { release = resolve; });
      return { result: 'success', streams: [{ name: 'general', stream_id: 7 }, { name: 'north', stream_id: 11 }] };
    },
  }).api;
  const directory = directoryOf();
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory });
  const first = adapter.refreshStreams('stream 11');
  let secondDone = false;
  const second = adapter.refreshStreams('#north').then(() => { secondDone = true; });
  const deadline = Date.now() + 2000;
  while (!release && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(secondDone, false, 'the second miss waits for the read in flight');
  release!();
  await Promise.all([first, second]);
  assert.equal(directory.nameOf(11), 'north');
  assert.equal(listCalls, 1);
});

test('a miss that joined a read already in flight is read for again at its next miss, since that read may have begun before its stream existed', async () => {
  let release: (() => void) | undefined;
  let listCalls = 0;
  const client = standInApi({
    'GET /streams': async () => {
      listCalls++;
      if (listCalls === 1) {
        await new Promise<void>((resolve) => { release = resolve; });
        return { result: 'success', streams: [{ name: 'general', stream_id: 7 }, { name: 'north', stream_id: 11 }] };
      }
      return { result: 'success', streams: [{ name: 'general', stream_id: 7 }, { name: 'north', stream_id: 11 }, { name: 'south', stream_id: 12 }] };
    },
  }).api;
  const directory = directoryOf();
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory });
  const first = adapter.refreshStreams('stream 11');
  const joined = adapter.refreshStreams('stream 12');
  const deadline = Date.now() + 2000;
  while (!release && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  release!();
  await Promise.all([first, joined]);
  assert.equal(directory.nameOf(12), undefined, 'the read in flight began before #south');
  await adapter.refreshStreams('stream 12');
  assert.equal(directory.nameOf(12), 'south');
  assert.equal(listCalls, 2);
});

test('a miss within its hold-off still waits for a read already in flight, which may hold its stream', async () => {
  let release: (() => void) | undefined;
  let listCalls = 0;
  const client = standInApi({
    'GET /streams': async () => {
      listCalls++;
      if (listCalls === 1) return { result: 'success', streams: [{ name: 'general', stream_id: 7 }] };
      await new Promise<void>((resolve) => { release = resolve; });
      return { result: 'success', streams: [{ name: 'general', stream_id: 7 }, { name: 'north', stream_id: 11 }, { name: 'south', stream_id: 12 }] };
    },
  }).api;
  const directory = directoryOf();
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory });
  await adapter.refreshStreams('stream 12');
  assert.equal(directory.nameOf(12), undefined, 'the first read has no #south, so stream 12 is held off');
  const other = adapter.refreshStreams('stream 11');
  const deadline = Date.now() + 2000;
  while (!release && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  let againDone = false;
  const again = adapter.refreshStreams('stream 12').then(() => { againDone = true; });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(againDone, false, 'the held-off miss waits for the read in flight');
  release!();
  await Promise.all([other, again]);
  assert.equal(directory.nameOf(12), 'south');
  assert.equal(listCalls, 2);
});

test('a stream-list answer without a streams array is no list: the directory keeps what it holds', async () => {
  let listCalls = 0;
  const client = standInApi({
    'GET /streams': async () => {
      listCalls++;
      return { result: 'success' };
    },
  }).api;
  const directory = new StreamDirectory();
  directory.restart({ streams: { '7': 'general', '11': 'north' } });
  const removed: number[] = [];
  directory.onChange((change) => removed.push(...change.removed.map((r) => r.id)));
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory });
  const original = console.error;
  console.error = () => {};
  try {
    await adapter.discoverChannels();
    await adapter.refreshStreams('stream 12');
  } finally {
    console.error = original;
  }
  assert.equal(listCalls, 2);
  assert.deepEqual(removed, []);
  assert.equal(directory.nameOf(7), 'general');
  assert.equal(directory.nameOf(11), 'north');
  assert.equal(directory.isFilled, false, 'no list has been read');
});

test('a stopped event loop\'s registration that answers late leaves the directory to the loop that runs now', async () => {
  let registrations = 0;
  let releaseFirst: (() => void) | undefined;
  const client = standInApi({
    'POST /register': async () => {
      registrations++;
      if (registrations === 1) {
        const answer = { queue_id: 'q1', last_event_id: -1, streams: [{ stream_id: 7, name: 'general' }] };
        await new Promise<void>((resolve) => { releaseFirst = resolve; });
        return answer;
      }
      return { queue_id: `q${registrations}`, last_event_id: -1, streams: [{ stream_id: 7, name: 'lobby' }] };
    },
    'GET /events': async () => {
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
    'DELETE /events': { result: 'success' },
  }).api;
  const directory = directoryOf();
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory });
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(() => {});
    let deadline = Date.now() + 2000;
    while (!releaseFirst && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
    adapter.startEvents(() => {});
    deadline = Date.now() + 2000;
    while (directory.nameOf(7) !== 'lobby' && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    assert.equal(directory.nameOf(7), 'lobby', 'the second loop registered');
    releaseFirst!();
    await new Promise((r) => setTimeout(r, 50));
    assert.equal(directory.nameOf(7), 'lobby');
    assert.equal(directory.formerOf('lobby'), undefined);
  } finally {
    adapter.stopEvents();
    console.error = original;
  }
});

test('while a change waits for the lookup that places it, later reactions and changes without a stream id are placed after it, in arrival order', async () => {
  let release!: () => void;
  const held = new Promise<void>((resolve) => { release = resolve; });
  let polls = 0;
  const { api: client, calls } = standInApi({
    'GET /messages/:id': async () => { await held; return { result: 'success', message: dm(30, 7) }; },
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls > 1) return new Promise(() => {});
      return { events: [
        { id: 1, type: 'message', flags: [], message: dm(31, 7) },
        { id: 2, type: 'update_message', message_id: 30, message_ids: [30], user_id: 7, edit_timestamp: 1_700_002_000, orig_content: 'm30', content: 'm30 fixed', flags: [] },
        { id: 3, type: 'reaction', op: 'add', emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', message_id: 31, user_id: 42 },
        { id: 4, type: 'update_message', message_id: 31, message_ids: [31], user_id: 7, edit_timestamp: 1_700_002_001, orig_content: 'm31', content: 'm31 fixed', flags: [] },
        { id: 5, type: 'delete_message', message_ids: [30], message_type: 'private' },
      ] };
    },
  });
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory: directoryOf() });
  const placed: string[] = [];
  adapter.startEvents(() => {}, undefined,
    (r) => { placed.push(`reaction ${r.messageId}`); },
    (entry) => {
      const at = placed.push('change') - 1;
      void entry.ready().then((c) => { placed[at] = c ? `${c.kind} ${c.messageId}` : 'none'; });
    });
  try {
    const deadline = performance.now() + 2000;
    while (!calls.some((c) => c.path === '/messages/30') && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    release();
    while ((placed.length < 4 || placed.includes('change')) && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(placed, ['edit 30', 'reaction 31', 'edit 31', 'delete 30']);
  } finally { adapter.stopEvents(); }
});

test("a stopped event loop's events that still wait for their placement are not looked up, so the next loop's events don't wait behind them", async () => {
  let releaseOld!: () => void;
  const oldLookup = new Promise<void>((resolve) => { releaseOld = resolve; });
  let registrations = 0;
  const served = new Set<string>();
  const reaction = (id: number, messageId: number) => ({ id, type: 'reaction', op: 'add', emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', message_id: messageId, user_id: 42 });
  const { api: client, calls } = standInApi({
    'POST /register': async () => ({ queue_id: `q${++registrations}`, last_event_id: -1 }),
    'GET /events': async (params) => {
      const queue = String(params.queue_id);
      if (served.has(queue)) return new Promise(() => {});
      served.add(queue);
      return { events: queue === 'q1' ? [reaction(1, 60), reaction(2, 61)] : [reaction(1, 62)] };
    },
    'GET /messages/:id': async (_params, call) => {
      if (call.id === 60) await oldLookup;
      if (call.id === 61) await new Promise(() => {});
      return { result: 'success', message: raw(call.id!) };
    },
  });
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { dmDiscoveryLimit: 0, directory: directoryOf() });
  const placed: string[] = [];
  const deadline = performance.now() + 2000;
  adapter.startEvents(() => {}, undefined, (r) => { placed.push(r.messageId); });
  while (!calls.some((c) => c.path === '/messages/60') && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  adapter.stopEvents();
  adapter.startEvents(() => {}, undefined, (r) => { placed.push(r.messageId); });
  try {
    while (!served.has('q2') && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    releaseOld();
    while (!placed.includes('62') && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    assert.deepEqual(placed, ['62']);
    assert.equal(calls.some((c) => c.path === '/messages/61'), false);
  } finally { adapter.stopEvents(); }
});

test("a DM's row is built at its turn: an edit that arrived while it waited gives it the new text and \"(edited)\", and a deletion takes it out", async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: { ...dm(40, 7), content: 'first words' } },
    { id: 2, type: 'update_message', message_id: 40, message_ids: [40], user_id: 7, edit_timestamp: 1_700_000_900, orig_content: 'first words', content: 'second words', flags: [] },
    { id: 3, type: 'message', flags: [], message: { ...dm(41, 7), content: 'soon gone' } },
    { id: 4, type: 'delete_message', message_ids: [41], message_type: 'private' },
  ];
  let polls = 0;
  const client = standInApi({
    'GET /streams': async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }] }),
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      await new Promise((r) => setTimeout(r, 5));
      return { events: [] };
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: { streamAllowed: () => true, dmAllowed: () => true }, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const messages: IncomingEntry[] = [];
  const changes: ChangeEntry[] = [];
  adapter.startEvents((entry) => { messages.push(entry); }, undefined, undefined, (entry) => { changes.push(entry); });
  const deadline = performance.now() + 2000;
  while (changes.length < 2 && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  adapter.stopEvents();
  const edited = await messages[0].ready([changes[0]]);
  assert.equal((edited!.message.content[0] as { text: string }).text, 'second words (edited)');
  assert.equal(await messages[1].ready([changes[1]]), null);
  assert.deepEqual(changes.map((c) => c.kind), ['edit', 'delete']);
});

test("a new DM conversation whose first message is taken out while it waits still carries its descriptor with the next message, so the server can register it", async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: dm(50, 42) },
    { id: 2, type: 'delete_message', message_ids: [50], message_type: 'private' },
    { id: 3, type: 'message', flags: [], message: dm(51, 42) },
  ];
  let polls = 0;
  const client = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events };
      return new Promise(() => {});
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: { streamAllowed: () => true, dmAllowed: () => true }, dmDiscoveryLimit: 0, directory: directoryOf() });
  const messages: IncomingEntry[] = [];
  const changes: ChangeEntry[] = [];
  adapter.startEvents((entry) => { messages.push(entry); }, undefined, undefined, (entry) => { changes.push(entry); });
  const deadline = performance.now() + 2000;
  while (messages.length < 2 && performance.now() < deadline) await new Promise((r) => setTimeout(r, 5));
  adapter.stopEvents();
  assert.equal(await messages[0].ready([changes[0]]), null);
  const next = await messages[1].ready();
  assert.equal(next?.newChannel?.id, 'zulip:dm:7+42', "the conversation with Ann and Bo, as the README names it");
});

test("a reaction to a direct message is judged by that message's sender, not by the conversation", async () => {
  let polls = 0;
  const reaction = (id: number, messageId: number, by: number) => ({ id, type: 'reaction', op: 'add', emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', message_id: messageId, user_id: by });
  const { api: client } = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls > 1) return new Promise(() => {});
      return { events: [
        { id: 1, type: 'message', flags: [], message: dm(31, 7) },
        { id: 2, type: 'message', flags: [], message: dm(32, 42) },
        reaction(3, 31, 42),
        reaction(4, 32, 7),
      ] };
    },
  });
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: onlyDev, dmDiscoveryLimit: 0, directory: directoryOf() });
  const delivered: string[] = [];
  const reacted: { messageId: string; snippet: string | null }[] = [];
  adapter.startEvents(inTurn(({ message: m }) => { delivered.push(m.messageId); }), undefined, (r) => { reacted.push({ messageId: r.messageId, snippet: r.messageSnippet }); });
  try {
    const deadline = performance.now() + 2000;
    while ((delivered.length < 1 || reacted.length < 1) && performance.now() < deadline) await new Promise((res) => setTimeout(res, 5));
    await new Promise((res) => setTimeout(res, 50));
    assert.deepEqual(delivered, ['32'], "Ann's message isn't delivered: she isn't in dmUsers");
    assert.deepEqual(reacted.map((r) => r.messageId), ['32'], "only the reaction to Bo's message is told; the one to Ann's would carry her text as its snippet");
  } finally { adapter.stopEvents(); }
});

test("the adapter passes on a reaction to the bot's own direct message, for the server's check of the conversation, where dmUsers names a participant by email alone", async () => {
  let polls = 0;
  const { api: client } = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls > 1) return new Promise(() => {});
      return { events: [
        { id: 1, type: 'message', flags: [], message: raw(51, { type: 'private', display_recipient: [{ id: SELF, full_name: 'Bot', email: 'bot@example.com' }, { id: 7, full_name: 'Ann', email: 'ann@example.com' }], sender_id: SELF, sender_full_name: 'Bot', sender_email: 'bot@example.com' }) },
        { id: 2, type: 'reaction', op: 'add', emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', message_id: 51, user_id: 7 },
      ] };
    },
  });
  const annByEmail: FilterView = { streamAllowed: () => true, dmAllowed: (u) => u.email === 'ann@example.com' };
  const adapter = new ZulipAdapter(client, { selfUserId: SELF, sessionId: 's' }, { filters: annByEmail, dmDiscoveryLimit: 0, directory: directoryOf() });
  const reacted: string[] = [];
  adapter.startEvents(inTurn(() => {}), undefined, (r) => { reacted.push(r.messageId); });
  try {
    const deadline = performance.now() + 2000;
    while (reacted.length < 1 && performance.now() < deadline) await new Promise((res) => setTimeout(res, 5));
    await new Promise((res) => setTimeout(res, 50));
    assert.deepEqual(reacted, ['51'], "Ann's reaction to the bot's message is passed on");
  } finally { adapter.stopEvents(); }
});
