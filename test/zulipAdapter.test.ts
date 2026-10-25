/**
 * ZulipAdapter against a fake zulip-js client — what the allowlists do to
 * history reads, what a history page reports about what it scanned, and
 * that opening a channel asks Zulip about the subscription every time.
 *
 * Run: node --import tsx --test test/zulipAdapter.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipAdapter, type FilterView } from '../src/platforms/zulip.ts';
import type { MessageChangeEvent } from '../src/platforms/adapter.ts';
import type { ZulipRawMessage } from '../src/history.ts';
import type { ChannelDescriptor } from '@animalabs/mcpl-core';

const SELF = 790;

function raw(id: number, over: Partial<ZulipRawMessage> = {}): ZulipRawMessage {
  return {
    id, sender_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
    display_recipient: 'general', subject: 'deploys', content: `m${id}`, timestamp: 1_700_000_000 + id, type: 'stream', flags: [],
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

function fakeClient(rows: ZulipRawMessage[]) {
  const calls: { retrieve: Record<string, unknown>[]; subscribe: unknown[] } = { retrieve: [], subscribe: [] };
  const client = {
    messages: {
      async retrieve(params: Record<string, unknown>) {
        calls.retrieve.push(params);
        const after = typeof params.anchor === 'number' && params.num_after ? params.anchor : null;
        const matching = rows
          .filter((m) => after === null || m.id > after)
          .filter((m) => {
            const narrow = params.narrow as unknown[][];
            const stream = narrow.find((n) => n[0] === 'stream')?.[1];
            const dmIds = narrow.find((n) => n[0] === 'dm')?.[1] as number[] | undefined;
            if (stream) return m.type === 'stream' && m.display_recipient === stream;
            if (dmIds) return m.type === 'private';
            return true;
          });
        const limit = Number(params.num_after || params.num_before);
        const page = after !== null ? matching.slice(0, limit) : matching.slice(-limit);
        return { result: 'success', messages: page, found_newest: page[page.length - 1] === matching[matching.length - 1], found_oldest: false };
      },
    },
    users: { me: { subscriptions: { add: async (p: unknown) => { calls.subscribe.push(p); return { result: 'success', subscribed: {}, already_subscribed: { 'bot@example.com': ['general'] } }; } } } },
  };
  return { client, calls };
}

const onlyDev: FilterView = { streamAllowed: (s) => s === 'dev', dmAllowed: (u) => u.id === 42 };

test('fetchHistory withholds the bot\'s own messages and disallowed senders, and reports the newest id it scanned', async () => {
  const { client } = fakeClient([raw(1, { sender_id: SELF }), raw(2, { sender_id: SELF }), raw(3), dm(4, 7), dm(5, 42), dm(6, SELF)]);
  const adapter = new ZulipAdapter(client, SELF, 's', { filters: onlyDev });

  // A stream outside the allowlist: nothing, without a fetch.
  const none = await adapter.fetchHistory('zulip:general', { limit: 10 });
  assert.deepEqual(none, { messages: [], scannedThrough: null, reachedNewest: true });

  // A DM conversation: the bot's own and the excluded sender's messages are
  // withheld, but the page scanned through all of them.
  const page = await adapter.fetchHistory('zulip:dm:7+42', { limit: 10, afterMessageId: '3' });
  assert.deepEqual(page.messages.map((m) => m.messageId), ['5']);
  assert.equal(page.scannedThrough, 6, 'the cursor covers the withheld rows');
  assert.equal(page.reachedNewest, true);
  assert.equal((page.messages[0].metadata as { backscroll: boolean }).backscroll, true);

  // A page of nothing but the bot's own messages is not the end of history.
  const allowAll: FilterView = { streamAllowed: () => true, dmAllowed: () => true };
  const wide = new ZulipAdapter(client, SELF, 's', { filters: allowAll });
  const own = await wide.fetchHistory('zulip:general', { limit: 2, afterMessageId: '0' });
  assert.deepEqual(own.messages, []);
  assert.equal(own.scannedThrough, 2);
  assert.equal(own.reachedNewest, false);
});

test('fetchContext injects nothing for a stream outside the allowlist, and only allowed senders of a DM', async () => {
  const { client, calls } = fakeClient([raw(3), dm(4, 7), dm(5, 42), dm(6, SELF)]);
  const adapter = new ZulipAdapter(client, SELF, 's', { filters: onlyDev });
  assert.equal(await adapter.fetchContext('zulip:general', undefined, 5), null);
  assert.equal(calls.retrieve.length, 0, 'not even fetched');
  const injection = await adapter.fetchContext('zulip:dm:7+42', undefined, 5);
  assert.ok(injection);
  assert.match(injection!.content, /Bo: m5/);
  const timed = new ZulipAdapter(client, SELF, 's', { filters: onlyDev, formatTime: () => 'T' });
  const lines = (await timed.fetchContext('zulip:dm:7+42', undefined, 5))!.content.split('\n');
  assert.deepEqual(lines.slice(1), ['[T id=5] [DM] Bo: m5', '[T id=6] [DM] Bot: m6'], 'injected history uses the shared line shape, ids included');
  assert.match(injection!.content, /Bot: m6/, 'the bot\'s own turns stay in context');
  assert.doesNotMatch(injection!.content, /Ann: m4/);
});

test('ensureSubscribed asks Zulip every time — a subscription dropped by unlisten is not remembered as live', async () => {
  const { client, calls } = fakeClient([]);
  const adapter = new ZulipAdapter(client, SELF, 's');
  await adapter.ensureSubscribed('zulip:general');
  await adapter.ensureSubscribed('zulip:general');
  assert.equal(calls.subscribe.length, 2);
  await adapter.ensureSubscribed('zulip:dm:42');
  assert.equal(calls.subscribe.length, 2, 'DMs need nothing');
});

test('publish uploads image blocks and links them after the text; without an uploader they are dropped as before', async () => {
  const sends: Record<string, unknown>[] = [];
  const client = { messages: { async send(p: Record<string, unknown>) { sends.push(p); return { result: 'success', id: 77 }; } } };
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
  const withUploads = new ZulipAdapter(client, SELF, 's', { uploader, uploadPolicy });
  const res = await withUploads.publish('zulip:general', undefined, blocks, { threadId: 'deploys' });
  assert.deepEqual(res, { delivered: true, messageId: '77', messageIds: ['77'] });
  assert.deepEqual(uploaded, ['image-2.png']);
  assert.deepEqual(sends[0], { type: 'stream', to: 'general', topic: 'deploys', content: 'chart attached\n\n[image-2.png](/user_uploads/1/image-2.png)' });

  // Only an image: the links are the body.
  await withUploads.publish('zulip:dm:42', undefined, [blocks[1]]);
  assert.deepEqual(sends[1], { type: 'private', to: [42], content: '[image-1.png](/user_uploads/1/image-1.png)' });

  // A failed upload fails the publish before anything is sent.
  const failing = new ZulipAdapter(client, SELF, 's', { uploader: { async upload() { throw new Error('quota'); } }, uploadPolicy });
  await assert.rejects(failing.publish('zulip:general', undefined, blocks), /quota/);
  assert.equal(sends.length, 2);

  const plain = new ZulipAdapter(client, SELF, 's');
  await plain.publish('zulip:general', undefined, blocks);
  assert.equal(sends[2].content, 'chart attached');
  assert.equal((await plain.publish('zulip:general', undefined, [blocks[1]])).delivered, false);
});

// ── Streams joined after startup (#20) ──

/** A client whose event queue delivers `events` once, then parks. `visible`
 *  is what stream discovery answers. */
function eventingClient(rows: ZulipRawMessage[], events: { message: Record<string, unknown>; flags?: string[] }[], visible: { name: string; stream_id: number }[] = []) {
  const { client } = fakeClient(rows);
  let served = false;
  return {
    ...client,
    streams: { retrieve: async () => ({ result: 'success', streams: visible }) },
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        if (served) return new Promise(() => {}); // park: the test is done polling
        served = true;
        return { result: 'success', events: events.map((e, i) => ({ id: i + 1, type: 'message', message: e.message, flags: e.flags ?? [] })) };
      },
    },
  };
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
  const client = eventingClient([], [streamEvent(900, 'ops', 12), streamEvent(901, 'ops', 12), streamEvent(902, 'general', 7)]);
  const adapter = new ZulipAdapter(client, SELF, 's');
  const seen: { channelId: string; descriptor?: string }[] = [];
  adapter.startEvents((m, descriptor) => seen.push({ channelId: m.channelId, descriptor: descriptor?.id }));
  await new Promise((r) => setTimeout(r, 50));
  adapter.stopEvents();

  assert.deepEqual(seen.map((s) => s.channelId), ['zulip:ops', 'zulip:ops', 'zulip:general']);
  // Every message, not just the first: what is already registered is the
  // server's decision, and its registry is cleared on every reconnect.
  assert.deepEqual(seen.map((s) => s.descriptor), ['zulip:ops', 'zulip:ops', 'zulip:general']);
});

test('the descriptor a message carries matches the one discovery builds for the same stream (#20)', async () => {
  const client = eventingClient([], [streamEvent(900, 'general', 7)], [{ name: 'general', stream_id: 7 }]);
  const adapter = new ZulipAdapter(client, SELF, 's');
  const discovered = (await adapter.discoverChannels()).find((c) => c.id === 'zulip:general')!;
  let fromEvent: ChannelDescriptor | undefined;
  adapter.startEvents((_m, descriptor) => { fromEvent = descriptor; });
  await new Promise((r) => setTimeout(r, 50));
  adapter.stopEvents();
  assert.ok(fromEvent);
  assert.equal(fromEvent!.id, discovered.id);
  assert.equal(fromEvent!.label, discovered.label);
  assert.deepEqual(fromEvent!.address, discovered.address);
  assert.deepEqual(fromEvent!.capabilities, discovered.capabilities);
});

test('a descriptor built from an event without a stream_id still addresses the stream (#20)', async () => {
  const client = eventingClient([], [streamEvent(900, 'ops', undefined)]);
  const adapter = new ZulipAdapter(client, SELF, 's');
  const seen: { id: string; address: unknown }[] = [];
  adapter.startEvents((_m, descriptor) => { if (descriptor) seen.push({ id: descriptor.id, address: descriptor.address }); });
  await new Promise((r) => setTimeout(r, 50));
  adapter.stopEvents();
  assert.deepEqual(seen, [{ id: 'zulip:ops', address: { stream_name: 'ops', stream_id: undefined } }]);
});

test('edits, moves and deletions are placed, cleaned and filtered before reaching the server (#22)', async () => {
  const events = [
    // Ann's message arrives live (cached), then she edits it to mention the bot.
    { id: 1, type: 'message', flags: [], message: { ...raw(5), content: 'first draft' } },
    { id: 2, type: 'update_message', message_id: 5, message_ids: [5], user_id: 7, edit_timestamp: 1_700_000_900, orig_content: 'first draft', content: 'second draft @**Bot**', flags: ['mentioned'], stream_id: 7 },
    // The bot's own edit is its own doing.
    { id: 3, type: 'update_message', message_id: 5, message_ids: [5], user_id: SELF, edit_timestamp: 1_700_000_901, orig_content: 'x', content: 'y', flags: [], stream_id: 7 },
    // A message the cache never saw: one GET places it.
    { id: 4, type: 'update_message', message_id: 6, message_ids: [6], user_id: 7, edit_timestamp: 1_700_000_902, orig_content: 'm6', content: 'm6 fixed', flags: [], stream_id: 7 },
    // A moderator moves Ann's message to another topic.
    { id: 5, type: 'update_message', message_id: 5, message_ids: [5], user_id: 12, edit_timestamp: 1_700_000_903, orig_subject: 'deploys', subject: 'deploys-2', propagate_mode: 'change_one', stream_id: 7 },
    // A cross-stream move of a message the cache evicted: the GET finds it
    // in its NEW stream, but the agent only ever saw it in the old one, which
    // the event names.
    { id: 55, type: 'update_message', message_id: 700, message_ids: [700], user_id: 12, edit_timestamp: 1_700_000_904, orig_subject: 'deploys', stream_id: 7, new_stream_id: 8 },
    // The (now edited, moved) message is deleted: placed from the cache.
    { id: 6, type: 'delete_message', message_ids: [5], message_type: 'stream', stream_id: 7, topic: 'deploys-2' },
    // A deletion the cache never saw: placed by the stream the event names.
    { id: 7, type: 'delete_message', message_ids: [900, 901], message_type: 'stream', stream_id: 7, topic: 'old' },
    // A deleted DM the cache never saw cannot be placed.
    { id: 8, type: 'delete_message', message_id: 902, message_type: 'private' },
    // A stream outside the allowlist: nothing.
    { id: 9, type: 'delete_message', message_ids: [903], message_type: 'stream', stream_id: 8, topic: 't' },
  ];
  let polls = 0;
  const getById: number[] = [];
  const client = {
    streams: { retrieve: async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }, { name: 'secret', stream_id: 8, subscriber_count: 1 }] }) },
    messages: {
      retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
      getById: async ({ message_id }: { message_id: number }) => {
        getById.push(message_id);
        // A slow GET must not let later changes overtake this one.
        if (message_id === 6) await new Promise((r) => setTimeout(r, 40));
        return { result: 'success', message: message_id === 700 ? raw(700, { display_recipient: 'secret', stream_id: 8 }) : raw(message_id) };
      },
      deleteById: async () => ({ result: 'success' }),
    },
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        polls++;
        if (polls === 1) return { events };
        await new Promise((r) => setTimeout(r, 5));
        return { events: [] };
      },
    },
  };
  const filters: FilterView = { streamAllowed: (s) => s === 'general', dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, SELF, 's', { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const delivered: string[] = [];
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents((m) => { delivered.push(m.messageId); }, undefined, undefined, (c) => { changes.push(c); });
    const deadline = Date.now() + 2000;
    while (changes.length < 6 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(delivered, ['5'], 'the live message still reaches onMessage');
  assert.deepEqual(getById, [6, 700], 'only the uncached messages cost a GET');
  const view = changes.map((c) => ({
    kind: c.kind, channelId: c.channelId, messageId: c.messageId, messageIds: c.messageIds, authorName: c.authorName, actorId: c.actorId,
    topic: c.topic, previousTopic: c.previousTopic, movedTo: c.movedToChannelId, content: c.content, previousContent: c.previousContent,
    mentioned: c.mentioned, wasMentioned: c.previouslyMentioned, onOwnMessage: c.onOwnMessage,
  }));
  assert.deepEqual(view, [
    { kind: 'edit', channelId: 'zulip:general', messageId: '5', messageIds: ['5'], authorName: 'Ann', actorId: '7', topic: 'deploys', previousTopic: null, movedTo: null, content: 'second draft @**Bot**', previousContent: 'first draft', mentioned: true, wasMentioned: false, onOwnMessage: false },
    // Placed by GET: only the post-edit state is known, so no previous verdict.
    { kind: 'edit', channelId: 'zulip:general', messageId: '6', messageIds: ['6'], authorName: 'Ann', actorId: '7', topic: 'deploys', previousTopic: null, movedTo: null, content: 'm6 fixed', previousContent: 'm6', mentioned: false, wasMentioned: null, onOwnMessage: false },
    // A move quotes nothing (the content did not change) and keeps the
    // mention it had: the mention the agent is answering moved topics.
    { kind: 'move', channelId: 'zulip:general', messageId: '5', messageIds: ['5'], authorName: 'Ann', actorId: '12', topic: 'deploys-2', previousTopic: 'deploys', movedTo: null, content: null, previousContent: null, mentioned: true, wasMentioned: true, onOwnMessage: false },
    // Placed on the stream it left, pointing at the one it went to; the
    // topic name did not change, so there is no previous topic.
    { kind: 'move', channelId: 'zulip:general', messageId: '700', messageIds: ['700'], authorName: 'Ann', actorId: '12', topic: 'deploys', previousTopic: null, movedTo: 'zulip:secret', content: null, previousContent: null, mentioned: false, wasMentioned: null, onOwnMessage: false },
    // The deleted message kept its last mention verdict: it is the one the agent was about to answer.
    { kind: 'delete', channelId: 'zulip:general', messageId: '5', messageIds: ['5'], authorName: 'Ann', actorId: null, topic: 'deploys-2', previousTopic: null, movedTo: null, content: null, previousContent: 'second draft @**Bot**', mentioned: true, wasMentioned: true, onOwnMessage: false },
    { kind: 'delete', channelId: 'zulip:general', messageId: '900', messageIds: ['900', '901'], authorName: null, actorId: null, topic: 'old', previousTopic: null, movedTo: null, content: null, previousContent: null, mentioned: false, wasMentioned: null, onOwnMessage: false },
  ]);
  assert.deepEqual(changes[0].timestamp, new Date(1_700_000_900_000), 'an edit is stamped with Zulip\'s edit time');
  assert.equal(changes[0].authorEmail, 'ann@example.com', 'the sender travels with the change for the DM allowlist');
});

test('the bot\'s own deletions (rollback, the delete tool) do not echo back as changes', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(5) },
    { id: 2, type: 'message', flags: [], message: raw(6) },
    { id: 3, type: 'message', flags: [], message: raw(7) },
  ];
  let polls = 0;
  let released: (() => void) | null = null;
  const client = {
    streams: { retrieve: async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7 }] }) },
    messages: {
      retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
      getById: async ({ message_id }: { message_id: number }) => ({ result: 'success', message: raw(message_id) }),
      deleteById: async ({ message_id }: { message_id: number }) => (message_id === 7 ? { result: 'error', msg: 'nope' } : { result: 'success' }),
    },
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        polls++;
        if (polls === 1) return { events };
        if (polls === 2) {
          // The second batch waits for the test to delete first.
          await new Promise<void>((r) => { released = r; });
          return { events: [
            { id: 4, type: 'delete_message', message_ids: [5], message_type: 'stream', stream_id: 7, topic: 'deploys' },   // rollback
            { id: 5, type: 'delete_message', message_ids: [6], message_type: 'stream', stream_id: 7, topic: 'deploys' },   // delete tool
            { id: 6, type: 'delete_message', message_ids: [7], message_type: 'stream', stream_id: 7, topic: 'deploys' },   // someone else: the bot's delete failed
          ] };
        }
        await new Promise((r) => setTimeout(r, 5));
        return { events: [] };
      },
    },
  };
  const adapter = new ZulipAdapter(client, SELF, 's', { dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(() => {}, undefined, undefined, (c) => { changes.push(c); });
    const deadline = Date.now() + 2000;
    while (!released && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await adapter.deleteMessage('zulip:general', '5');
    adapter.noteSelfDeleted(6);
    await assert.rejects(adapter.deleteMessage('zulip:general', '7'));
    released!();
    while (changes.length < 1 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await new Promise((r) => setTimeout(r, 30));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  assert.deepEqual(changes.map((c) => [c.kind, c.messageId]), [['delete', '7']], 'only the deletion the bot did not make surfaces');
});

test('bulk deletion, co-moved ids, unresolvable origins and vanishing moves (#22 round 2)', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(20) },
    { id: 2, type: 'message', flags: [], message: raw(21) },
    { id: 3, type: 'message', flags: [], message: raw(22) },
    // A change_all move re-seats every co-moved message, not only the one acted on.
    { id: 4, type: 'update_message', message_id: 20, message_ids: [20, 21, 22], user_id: 12, edit_timestamp: 1_700_001_000, orig_subject: 'deploys', subject: 'archive', propagate_mode: 'change_all', stream_id: 7 },
    // Bulk deletion where the oldest id (Bob's, never cached) comes first: the
    // line is reported under an id the cache can vouch for.
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
  const client = {
    streams: { retrieve: async () => { listCalls++; return { result: 'success', streams: [{ name: 'general', stream_id: 7 }, { name: 'secret', stream_id: 8 }] }; } },
    messages: {
      retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }),
      getById: async ({ message_id }: { message_id: number }) => ({ result: 'success', message: raw(message_id, { display_recipient: 'secret', stream_id: 8 }) }),
    },
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        polls++;
        if (polls === 1) return { events };
        await new Promise((r) => setTimeout(r, 5));
        return { events: [] };
      },
    },
  };
  const adapter = new ZulipAdapter(client, SELF, 's', { dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const changes: MessageChangeEvent[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents(() => {}, undefined, undefined, (c) => { changes.push(c); });
    const deadline = Date.now() + 2000;
    while (changes.length < 4 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    await new Promise((r) => setTimeout(r, 30));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  const view = changes.map((c) => ({ kind: c.kind, channelId: c.channelId, messageId: c.messageId, messageIds: c.messageIds, authorName: c.authorName, topic: c.topic, movedTo: c.movedToChannelId, previousContent: c.previousContent, vanished: c.vanished }));
  assert.deepEqual(view, [
    { kind: 'move', channelId: 'zulip:general', messageId: '20', messageIds: ['20', '21', '22'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: null, vanished: false },
    // Reported under 21 (cached, now in `archive`), with 19 still listed; never Ann's name on Bob's id.
    { kind: 'delete', channelId: 'zulip:general', messageId: '21', messageIds: ['19', '21'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: 'm21', vanished: false },
    { kind: 'move', channelId: 'zulip:general', messageId: '22', messageIds: ['22'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: null, vanished: false },
    { kind: 'delete', channelId: 'zulip:general', messageId: '22', messageIds: ['22'], authorName: 'Ann', topic: 'archive', movedTo: null, previousContent: 'm22', vanished: true },
  ]);
  assert.equal(listCalls, 2, 'unknown stream ids cost one list refresh, then a hold-off — not one per event');
});

test('a change to the bot\'s own DM message is judged by whom it was sent to', async () => {
  const events = [
    { id: 1, type: 'message', flags: [], message: dm(30, SELF) },
    { id: 2, type: 'update_message', message_id: 30, message_ids: [30], user_id: 42, edit_timestamp: 1_700_002_000, orig_subject: '', subject: 'renamed', stream_id: undefined },
  ];
  let polls = 0;
  const client = {
    streams: { retrieve: async () => ({ result: 'success', streams: [] }) },
    messages: { retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }), getById: async () => ({ result: 'error' }) },
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: { retrieve: async () => { polls++; if (polls === 1) return { events }; await new Promise((r) => setTimeout(r, 5)); return { events: [] }; } },
  };
  const run = async (filters: FilterView) => {
    polls = 0;
    const adapter = new ZulipAdapter(client, SELF, 's', { dmDiscoveryLimit: 0, filters });
    const changes: MessageChangeEvent[] = [];
    const original = console.error;
    console.error = () => {};
    try {
      adapter.startEvents(() => {}, undefined, undefined, (c) => { changes.push(c); });
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
  // Discovery saw only #general; #late-stream is created later and its
  // first message (a mention of the bot) arrives live.
  const events = [
    { id: 1, type: 'message', flags: [], message: raw(10) },
    { id: 2, type: 'message', flags: ['mentioned'], message: raw(11, { display_recipient: 'late-stream', stream_id: 9, content: '@**Bot** hi' }) },
    { id: 3, type: 'message', flags: [], message: raw(12, { display_recipient: 'late-stream', stream_id: 9 }) },
    // Outside the allowlist: filtered before any descriptor is made.
    { id: 4, type: 'message', flags: [], message: raw(13, { display_recipient: 'secret', stream_id: 8 }) },
  ];
  let polls = 0;
  const client = {
    streams: { retrieve: async () => ({ result: 'success', streams: [{ name: 'general', stream_id: 7, subscriber_count: 2 }] }) },
    messages: { retrieve: async () => ({ result: 'success', messages: [], found_newest: true, found_oldest: true }) },
    queues: { register: async () => ({ queue_id: 'q1', last_event_id: -1 }) },
    events: {
      retrieve: async () => {
        polls++;
        if (polls === 1) return { events };
        await new Promise((r) => setTimeout(r, 5));
        return { events: [] };
      },
    },
  };
  const filters: FilterView = { streamAllowed: (s) => s !== 'secret', dmAllowed: () => true };
  const adapter = new ZulipAdapter(client, SELF, 's', { filters, dmDiscoveryLimit: 0 });
  await adapter.discoverChannels();
  const seen: { id: string; channelId: string; newChannel?: string; address?: unknown }[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    adapter.startEvents((m, d) => { seen.push({ id: m.messageId, channelId: m.channelId, newChannel: d?.id, address: d?.address }); });
    const deadline = Date.now() + 2000;
    while (seen.length < 3 && Date.now() < deadline) await new Promise((r) => setTimeout(r, 5));
    adapter.stopEvents();
  } finally {
    console.error = original;
  }
  // Every allowed stream message carries its descriptor — whether it is new
  // is the server's registry's call, not the adapter's.
  assert.deepEqual(seen, [
    { id: '10', channelId: 'zulip:general', newChannel: 'zulip:general', address: { stream_name: 'general', stream_id: undefined } },
    // Described from the message, with the stream id typing needs.
    { id: '11', channelId: 'zulip:late-stream', newChannel: 'zulip:late-stream', address: { stream_name: 'late-stream', stream_id: 9 } },
    { id: '12', channelId: 'zulip:late-stream', newChannel: 'zulip:late-stream', address: { stream_name: 'late-stream', stream_id: 9 } },
  ]);
});
