import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipEventLoop } from '../../src/platforms/zulip-events.ts';
import { ZulipToolRuntime } from '../../src/tool-runtime.ts';
import { initializeZulipClient, type ZulipSession } from '../../src/zulip-client.ts';
import { contractEnv } from './env.ts';
import { apis, newStream, until } from './zulip.ts';

const env = contractEnv();
const { user, bot } = apis(env);
let session: ZulipSession;
let tools: ZulipToolRuntime;
let botId: number;
let userId: number;
let stateDir: string;

before(async () => {
  session = await initializeZulipClient(process.env);
  stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-'));
  tools = new ZulipToolRuntime(session, stateDir);
  botId = await bot.myId();
  userId = await user.myId();
});

after(() => rmSync(stateDir, { recursive: true, force: true }));

test('bootstrap reads the bot\'s own id, and no upload cap, since server_settings carries none', async (t) => {
  assert.equal(session.selfUserId, botId);
  const settings = await user.ok('GET', 'server_settings');
  assert.equal(settings.max_file_upload_size_mib, undefined);
  assert.equal(session.maxUploadBytes, null);
  const registered = await user.ok('POST', 'register', { fetch_event_types: ['realm'] });
  t.diagnostic(`register's max_file_upload_size_mib: ${registered.max_file_upload_size_mib}`);
});

test('send_message posts a stream message the second user reads back', async () => {
  const stream = await newStream(env, 'send');
  const sent = (await tools.handleToolCall('send_message', { type: 'stream', to: stream, topic: 'a topic', content: 'hello from the bot' })) as { id: number };
  const read = await user.message(sent.id);
  assert.equal(read.sender_id, botId);
  assert.equal(read.display_recipient, stream);
  assert.equal(read.subject, 'a topic');
  assert.equal(read.content, 'hello from the bot');
});

test('send_dm posts a direct message the second user reads back', async () => {
  const sent = (await tools.handleToolCall('send_dm', { to: String(userId), content: 'a direct hello' })) as { id: number };
  const read = await user.message(sent.id);
  assert.equal(read.type, 'private');
  assert.deepEqual((read.display_recipient as { id: number }[]).map((r) => r.id).sort(), [userId, botId].sort());
  assert.equal(read.sender_id, botId);
  assert.equal(read.content, 'a direct hello');
});

test('upload_file uploads bytes that fetch_attachment fetches back unchanged', async () => {
  const bytes = randomBytes(2048);
  const uploaded = (await tools.handleToolCall('upload_file', { data: bytes.toString('base64'), name: 'contract.bin' })) as { path: string };
  const fetched = (await tools.handleToolCall('fetch_attachment', { path: uploaded.path })) as { base64: string; size: number };
  assert.equal(fetched.size, bytes.length);
  assert.equal(fetched.base64, bytes.toString('base64'));
});

test('the event loop registers a queue and a second user\'s message arrives on the long poll', async () => {
  const stream = await newStream(env, 'events');
  const arrived: { id: number; content: string }[] = [];
  const loop = new ZulipEventLoop();
  let registered = false;
  const realRegister = session.client.queues.register.bind(session.client.queues);
  const client = {
    ...session.client,
    queues: { register: async (params: Record<string, unknown>) => {
      const answer = await realRegister(params);
      registered = typeof answer?.queue_id === 'string';
      return answer;
    } },
  };
  void loop.start(client, async (_stream, msg) => {
    arrived.push({ id: msg.id as number, content: msg.content as string });
  });
  try {
    await until(() => (registered ? true : undefined), 'the event queue to register');
    const posted = await user.ok('POST', 'messages', { type: 'stream', to: stream, topic: 'events', content: 'on the long poll' });
    const got = await until(() => arrived.find((m) => m.id === posted.id), `message ${posted.id} on the long poll`);
    assert.equal(got.content, 'on the long poll');
  } finally {
    loop.stop();
  }
});

async function topicsPolicy(): Promise<string> {
  return (await user.ok('POST', 'register', { fetch_event_types: ['realm'] })).realm_topics_policy;
}

test('the server\'s send path: send_message to a stream with no topic lands in the topic "undefined"', async (t) => {
  t.diagnostic(`realm_topics_policy: ${await topicsPolicy()}`);
  const stream = await newStream(env, 'notopic');
  const sent = (await tools.handleToolCall('send_message', { type: 'stream', to: stream, content: 'no topic given' })) as { id: number };
  assert.equal((await user.message(sent.id)).subject, 'undefined');
});

test('Zulip itself: a stream message with no topic parameter is refused, and one with an empty topic is accepted', async (t) => {
  t.diagnostic(`realm_topics_policy: ${await topicsPolicy()}`);
  const stream = await newStream(env, 'notopic-zulip');
  const missing = await bot.call('POST', 'messages', { type: 'stream', to: stream, content: 'no topic parameter' });
  assert.equal(missing.result, 'error');
  assert.equal(missing.msg, 'Missing topic');
  const empty = await bot.ok('POST', 'messages', { type: 'stream', to: stream, topic: '', content: 'an empty topic' });
  assert.equal((await user.message(empty.id)).subject, '');
});

test('with empty topics disabled: the server\'s send path still lands in "undefined", and Zulip refuses both no topic and an empty one', async (t) => {
  const before = await topicsPolicy();
  try {
    await user.ok('PATCH', 'realm', { topics_policy: 'disable_empty_topic' });
    t.diagnostic(`realm_topics_policy: ${await topicsPolicy()}`);
    const stream = await newStream(env, 'notopic-required');
    const sent = (await tools.handleToolCall('send_message', { type: 'stream', to: stream, content: 'no topic given' })) as { id: number };
    assert.equal((await user.message(sent.id)).subject, 'undefined');
    const missing = await bot.call('POST', 'messages', { type: 'stream', to: stream, content: 'no topic parameter' });
    assert.equal(missing.result, 'error');
    assert.equal(missing.msg, 'Missing topic');
    const empty = await bot.call('POST', 'messages', { type: 'stream', to: stream, topic: '', content: 'an empty topic' });
    assert.equal(empty.result, 'error');
    assert.equal(empty.msg, 'Sending messages to the general chat is not allowed in this channel.');
  } finally {
    await user.ok('PATCH', 'realm', { topics_policy: before });
  }
});

test('edit_message applies a short edit the second user reads back', async () => {
  const stream = await newStream(env, 'edit');
  const sent = (await tools.handleToolCall('send_message', { type: 'stream', to: stream, topic: 'edits', content: 'before the edit' })) as { id: number };
  await tools.handleToolCall('edit_message', { message_id: sent.id, content: 'after the edit' });
  assert.equal((await user.message(sent.id)).content, 'after the edit');
});
