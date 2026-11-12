import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../../src/tool-runtime.ts';
import { initializeZulipClient, type ZulipSession } from '../../src/zulip-client.ts';
import type { Method, RequestOptions, ZulipApi } from '../../src/zulip-api.ts';
import { contractEnv } from './env.ts';
import { apis, newStream } from './zulip.ts';

const env = contractEnv();
const { user } = apis(env);
const answers: { call: string; answer: unknown }[] = [];
let api: ZulipApi;
let tools: ZulipToolRuntime;
let userId: number;
let stateDir: string;

function recording(inner: ZulipApi): ZulipApi {
  return {
    request: (async (method: Method, path: string, what: string, params?: Record<string, unknown>, opts?: RequestOptions) => {
      const answer = await inner.request(method, path, what, params, opts);
      answers.push({ call: `${method} ${path}`, answer });
      return answer;
    }) as ZulipApi['request'],
    requestAnswer: (async (method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions) => {
      const answer = await inner.requestAnswer(method, path, params, opts);
      answers.push({ call: `${method} ${path}`, answer });
      return answer;
    }) as ZulipApi['requestAnswer'],
  };
}

before(async () => {
  const session: ZulipSession = await initializeZulipClient(process.env);
  api = recording(session.api);
  stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-api-'));
  tools = new ZulipToolRuntime({ ...session, api }, stateDir);
  userId = await user.myId();
});

after(() => rmSync(stateDir, { recursive: true, force: true }));

test('a number for `to`, and text with & and =, reach a stream message as sent', async () => {
  const stream = await newStream(env, 'values');
  const { stream_id: streamId } = await api.request<{ stream_id: number }>('GET', '/get_stream_id', 'a stream id', { stream });
  const sent = await api.request<{ id: number }>('POST', '/messages', 'a stream message', {
    type: 'stream', to: streamId, topic: 'a & b = c', content: '**raw** & text = kept',
  });
  const read = await user.message(sent.id);
  assert.equal(read.display_recipient, stream);
  assert.equal(read.subject, 'a & b = c');
  assert.equal(read.content, '**raw** & text = kept');
});

test('a narrow of pairs, a string anchor, numbers and booleans on a GET find the message', async () => {
  const stream = await newStream(env, 'narrow');
  const sent = await api.request<{ id: number }>('POST', '/messages', 'a stream message', { type: 'stream', to: stream, topic: 'here', content: '**raw**' });
  const page = await api.request<{ messages: { id: number; content: string }[] }>('GET', '/messages', 'a stream page', {
    anchor: 'newest', num_before: 5, num_after: 0, narrow: [['stream', stream], ['topic', 'here']], apply_markdown: false, include_anchor: true,
  });
  assert.deepEqual(page.messages.map((m) => [m.id, m.content]), [[sent.id, '**raw**']]);
});

test('an array of numbers for `to` sends a direct message, and a narrow of objects with a numeric anchor finds it', async () => {
  const dm = await api.request<{ id: number }>('POST', '/messages', 'a direct message', { type: 'private', to: [userId], content: 'direct' });
  const page = await api.request<{ messages: { id: number }[] }>('GET', '/messages', 'a direct page', {
    anchor: dm.id, num_before: 0, num_after: 0, narrow: [{ operator: 'dm', operand: [userId] }], apply_markdown: false, include_anchor: true,
  });
  assert.deepEqual(page.messages.map((m) => m.id), [dm.id]);
});

test('subscribing takes an array of objects, and unsubscribing an array of names in a DELETE body', async () => {
  const first = await newStream(env, 'join');
  const second = await newStream(env, 'leave');
  const joined = await api.request<{ subscribed: Record<string, string[]>; already_subscribed: Record<string, string[]> }>(
    'POST', '/users/me/subscriptions', 'subscribing', { subscriptions: [{ name: first }, { name: second }] },
  );
  assert.deepEqual(Object.values({ ...joined.subscribed, ...joined.already_subscribed }).flat().sort(), [first, second].sort());
  const left = await api.request<{ removed: string[] }>('DELETE', '/users/me/subscriptions', 'unsubscribing', { subscriptions: [second] });
  assert.deepEqual(left.removed, [second]);
});

test('a reaction is added, then removed with its parameters in a DELETE body', async () => {
  const stream = await newStream(env, 'react');
  const sent = await api.request<{ id: number }>('POST', '/messages', 'a stream message', { type: 'stream', to: stream, topic: 'here', content: 'react to me' });
  await api.request('POST', `/messages/${sent.id}/reactions`, 'a reaction', { emoji_name: 'eyes', reaction_type: 'unicode_emoji' });
  await api.request('DELETE', `/messages/${sent.id}/reactions`, 'removing a reaction', { emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji' });
  const read = await api.request<{ message: { reactions: unknown[] } }>('GET', `/messages/${sent.id}`, 'the message', { message_id: sent.id });
  assert.deepEqual(read.message.reactions, []);
});

test('typing starts in a stream without `to`, and in a direct message to an array of ids', async () => {
  const stream = await newStream(env, 'typing');
  const { stream_id: streamId } = await api.request<{ stream_id: number }>('GET', '/get_stream_id', 'a stream id', { stream });
  await api.request('POST', '/typing', 'typing in a stream', { type: 'stream', stream_id: streamId, topic: 'here', op: 'start' });
  await api.request('POST', '/typing', 'typing in a direct message', { type: 'direct', to: [userId], op: 'start' });
});

test('a registration takes an array, booleans and an object', async () => {
  const queue = await api.request<{ queue_id: string }>('POST', '/register', 'registering a queue', {
    event_types: ['message'], all_public_streams: true, apply_markdown: false,
    client_capabilities: { notification_settings_null: false, bulk_message_deletion: true },
  });
  assert.equal(typeof queue.queue_id, 'string');
  await api.request('DELETE', '/events', 'deleting the queue', { queue_id: queue.queue_id });
});

test('a long edit_message, above 8,000 characters, is applied', async () => {
  const stream = await newStream(env, 'long-edit');
  const sent = (await tools.handleToolCall('send_message', { type: 'stream', to: stream, topic: 'edits', content: 'short' })) as { id: number };
  const long = 'é'.repeat(9_000);
  await tools.handleToolCall('edit_message', { message_id: sent.id, content: long });
  assert.equal((await user.message(sent.id)).content, long);
});

test('Zulip itself cuts an edit above its limit to its length and marks the cut, which is why edit_message refuses one', async (t) => {
  const stream = await newStream(env, 'over-limit');
  const sent = await api.request<{ id: number }>('POST', '/messages', 'a stream message', { type: 'stream', to: stream, topic: 'edits', content: 'short' });
  await api.request('PATCH', `/messages/${sent.id}`, 'an edit above the limit', { content: 'b'.repeat(10_050) });
  const stored = (await user.message(sent.id)).content;
  t.diagnostic(`stored ${[...stored].length} code points, ending ${JSON.stringify(stored.slice(-24))}`);
  assert.equal([...stored].length, 10_000);
  assert.match(stored, /\[message truncated\]$/);
});

test('send_message to a person with no topic sends none, and Zulip takes it', async () => {
  const sent = (await tools.handleToolCall('send_message', { type: 'private', to: [userId], content: 'no topic for a direct message' })) as { id: number };
  const read = await user.message(sent.id);
  assert.equal(read.type, 'private');
  assert.equal(read.content, 'no topic for a direct message');
});

test('no call in this file came back saying Zulip ignored a parameter', () => {
  assert.ok(answers.length > 10, `answers recorded: ${answers.length}`);
  const ignored = answers
    .filter(({ answer }) => (answer as { ignored_parameters_unsupported?: unknown[] }).ignored_parameters_unsupported?.length)
    .map(({ call, answer }) => `${call}: ${(answer as { ignored_parameters_unsupported: unknown[] }).ignored_parameters_unsupported.join(', ')}`);
  assert.deepEqual(ignored, []);
});
