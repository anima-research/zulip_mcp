import { test } from 'node:test';
import assert from 'node:assert/strict';
import { contractEnv } from './env.ts';
import { apis, newStream, uniqueName, until, type PlainApi } from './zulip.ts';

const env = contractEnv();
const { user, bot } = apis(env);

const AS_THE_SERVER_REGISTERS = {
  event_types: ['message', 'reaction', 'update_message', 'delete_message', 'stream'],
  all_public_streams: true,
  apply_markdown: false,
  client_capabilities: { notification_settings_null: false, bulk_message_deletion: true },
};

interface ZulipEvent {
  id: number;
  type: string;
  [field: string]: any;
}

class Queue {
  private readonly seen: ZulipEvent[] = [];
  private lastEventId: number;

  private constructor(
    private readonly api: PlainApi,
    readonly answer: any,
  ) {
    this.lastEventId = answer.last_event_id;
  }

  static async register(api: PlainApi, params: Record<string, unknown> = AS_THE_SERVER_REGISTERS): Promise<Queue> {
    return new Queue(api, await api.ok('POST', 'register', params));
  }

  async next(what: string, match: (event: ZulipEvent) => boolean): Promise<ZulipEvent> {
    return until(async () => {
      const already = this.seen.find(match);
      if (already) return already;
      const polled = await this.api.ok('GET', 'events', { queue_id: this.answer.queue_id, last_event_id: this.lastEventId, dont_block: true });
      for (const event of polled.events as ZulipEvent[]) {
        this.lastEventId = Math.max(this.lastEventId, event.id);
        this.seen.push(event);
      }
      return this.seen.find(match);
    }, what);
  }

  async close(): Promise<void> {
    await this.api.call('DELETE', 'events', { queue_id: this.answer.queue_id });
  }
}

async function idOf(stream: string): Promise<number> {
  return (await user.ok('GET', 'get_stream_id', { stream })).stream_id;
}

async function rename(id: number, to: string): Promise<void> {
  await user.ok('PATCH', `streams/${id}`, { new_name: to });
}

test('C1: a rename gives a stream event with op "update", property "name", the stream\'s id and the new name as value', async () => {
  const stream = await newStream(env, 'c1');
  const id = await idOf(stream);
  const queue = await Queue.register(bot);
  try {
    const renamed = uniqueName('c1-renamed');
    await rename(id, renamed);
    const event = await queue.next(`the rename of stream ${id}`, (e) => e.type === 'stream' && e.op === 'update' && e.stream_id === id);
    assert.equal(event.property, 'name');
    assert.equal(event.value, renamed);
  } finally {
    await queue.close();
  }
});

test('C2: with "stream" among the event types, the registration answers with streams, each with its id and name', async () => {
  const stream = await newStream(env, 'c2');
  const id = await idOf(stream);
  const queue = await Queue.register(bot);
  try {
    const streams = queue.answer.streams as { stream_id: unknown; name: unknown }[];
    assert.ok(Array.isArray(streams), 'the registration has a streams list');
    assert.ok(streams.every((s) => typeof s.stream_id === 'number' && typeof s.name === 'string'));
    assert.deepEqual(streams.filter((s) => s.stream_id === id).map((s) => s.name), [stream]);
  } finally {
    await queue.close();
  }
});

test('C3: a send with an integer `to` lands in that stream, also just after a rename', async () => {
  const stream = await newStream(env, 'c3');
  const id = await idOf(stream);
  const before = await bot.ok('POST', 'messages', { type: 'stream', to: id, topic: 'here', content: 'before the rename' });
  const readBefore = await user.message(before.id);
  assert.equal(readBefore.display_recipient, stream);
  assert.equal(readBefore.content, 'before the rename');
  const renamed = uniqueName('c3-renamed');
  await rename(id, renamed);
  const sent = await bot.ok('POST', 'messages', { type: 'stream', to: id, topic: 'here', content: 'just after the rename' });
  const read = await user.message(sent.id);
  assert.equal(read.display_recipient, renamed);
  assert.equal(read.content, 'just after the rename');
});

test('C4: a narrow of {operator: "stream", operand: <id>} reads that stream and no other, also after a rename', async () => {
  const stream = await newStream(env, 'c4');
  const other = await newStream(env, 'c4-other');
  const id = await idOf(stream);
  const mine = await user.ok('POST', 'messages', { type: 'stream', to: stream, topic: 'here', content: 'in the stream read' });
  await user.ok('POST', 'messages', { type: 'stream', to: other, topic: 'here', content: 'in another stream' });
  const readById = async () => (await bot.ok('GET', 'messages', {
    anchor: 'newest', num_before: 10, num_after: 0, narrow: [{ operator: 'stream', operand: id }, { operator: 'topic', operand: 'here' }], apply_markdown: false,
  })).messages.map((m: { id: number }) => m.id);
  assert.deepEqual(await readById(), [mine.id], 'before the rename');
  await rename(id, uniqueName('c4-renamed'));
  assert.deepEqual(await readById(), [mine.id], 'after the rename');
});

test('C5: a stream message carries stream_id, as an event and from GET /messages', async () => {
  const stream = await newStream(env, 'c5');
  const id = await idOf(stream);
  const queue = await Queue.register(bot);
  try {
    const sent = await user.ok('POST', 'messages', { type: 'stream', to: stream, topic: 'here', content: 'carry my stream id' });
    const event = await queue.next(`message ${sent.id} as an event`, (e) => e.type === 'message' && e.message?.id === sent.id);
    assert.equal(event.message.stream_id, id);
    const page = await bot.ok('GET', 'messages', {
      anchor: sent.id, num_before: 0, num_after: 0, narrow: [['stream', stream]], apply_markdown: false, include_anchor: true,
    });
    assert.deepEqual(page.messages.map((m: { id: number; stream_id: unknown }) => [m.id, m.stream_id]), [[sent.id, id]]);
  } finally {
    await queue.close();
  }
});

test('C6: an archive gives a stream event with op "delete" naming the stream\'s id, to a client that didn\'t declare archived_channels', async () => {
  const stream = await newStream(env, 'c6');
  const id = await idOf(stream);
  const queue = await Queue.register(bot);
  try {
    await user.ok('DELETE', `streams/${id}`);
    const event = await queue.next(
      `the archive of stream ${id}`,
      (e) => e.type === 'stream' && e.op === 'delete' && Array.isArray(e.stream_ids) && e.stream_ids.includes(id),
    );
    assert.equal(event.op, 'delete');
  } finally {
    await queue.close();
  }
});

test('C7: with all_public_streams, a message in a public stream the bot isn\'t subscribed to arrives as an event', async () => {
  const stream = uniqueName('c7');
  await user.ok('POST', 'users/me/subscriptions', { subscriptions: [{ name: stream }] });
  const subscribers = (await user.ok('GET', `streams/${await idOf(stream)}/members`)).subscribers as number[];
  assert.ok(!subscribers.includes(await bot.myId()), 'the bot isn\'t subscribed');
  const queue = await Queue.register(bot);
  try {
    const sent = await user.ok('POST', 'messages', { type: 'stream', to: stream, topic: 'here', content: 'unsubscribed bot, public stream' });
    const event = await queue.next(`message ${sent.id} as an event`, (e) => e.type === 'message' && e.message?.id === sent.id);
    assert.equal(event.message.content, 'unsubscribed bot, public stream');
  } finally {
    await queue.close();
  }
});

test('C8: a bot\'s send to a name no stream has is refused with STREAM_DOES_NOT_EXIST and its owner gets a direct message; a read narrowed to the name is refused with BAD_NARROW', async () => {
  const missing = uniqueName('c8-nowhere');
  const sent = await bot.call('POST', 'messages', { type: 'stream', to: missing, topic: 'here', content: 'to nowhere' });
  assert.equal(sent.result, 'error');
  assert.equal(sent.code, 'STREAM_DOES_NOT_EXIST');
  const notice = await until(async () => {
    const page = await user.ok('GET', 'messages', {
      anchor: 'newest', num_before: 20, num_after: 0, narrow: [{ operator: 'is', operand: 'dm' }], apply_markdown: false,
    });
    return (page.messages as { type: string; content: string }[]).find((m) => m.content.includes(missing));
  }, `the owner's direct message about ${missing}`);
  assert.equal(notice.type, 'private');
  const read = await bot.call('GET', 'messages', {
    anchor: 'newest', num_before: 1, num_after: 0, narrow: [['stream', missing]], apply_markdown: false,
  });
  assert.equal(read.result, 'error');
  assert.equal(read.code, 'BAD_NARROW');
});
