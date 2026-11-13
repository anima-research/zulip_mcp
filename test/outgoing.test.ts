import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';
import { dmKey, streamKey } from '../src/channel-key.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import type { ZulipSession } from '../src/zulip-client.ts';
import type { UploadPolicy } from '../src/uploads.ts';
import { standInApi, type StandInCall } from './zulip-stand-in.ts';

type Sent = { messageId: string; channelId: string; content: string };
type Answer = (n: number) => unknown;

const ok: Answer = (n) => ({ result: 'success', msg: '', id: 900 + n });

function messagesApi(answer: Answer = ok) {
  let n = 0;
  return standInApi({ 'POST /messages': () => answer(++n) });
}

const uploader = {
  async upload(i: { name: string }) {
    return { name: i.name, path: `/user_uploads/1/ab/${i.name}`, url: `https://z/user_uploads/1/ab/${i.name}` };
  },
};
const uploadPolicy: UploadPolicy = { roots: new Map(), maxBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };

function tools(api: ZulipSession['api'], maxMessageLength?: number) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-outgoing-'));
  const session: ZulipSession = { api, selfUserId: 790, realm: 'https://z', authHeader: 'Basic x', sessionId: 't' };
  const original = console.error;
  console.error = () => {};
  try {
    const runtime = new ZulipToolRuntime(session, dir, { uploader, uploadPolicy, ...(maxMessageLength ? { maxMessageLength } : {}) } as never);
    const sent: Sent[] = [];
    runtime.onSent = (s) => sent.push(s);
    return { runtime, sent, done: () => rmSync(dir, { recursive: true, force: true }) };
  } finally {
    console.error = original;
  }
}

function adapter(api: ZulipSession['api'], options: Record<string, unknown> = {}) {
  const directory = new StreamDirectory();
  directory.registration([{ stream_id: 7, name: 'general' }]);
  const a = new ZulipAdapter(api, 790, 's', { directory, ...options } as never);
  const sent: Sent[] = [];
  (a as unknown as { onSent: (s: Sent) => void }).onSent = (s) => sent.push(s);
  const publish = (channelId: string, ...rest: Parameters<ZulipAdapter['publish']> extends [unknown, ...infer R] ? R : never) =>
    a.publish(channelId === 'zulip:dm:42' ? dmKey([42]) : streamKey(7), ...rest);
  return { adapter: a, sent, publish };
}

const text = (s: string) => [{ type: 'text' as const, text: s }];
const bodies = (calls: StandInCall[]) => calls.filter((c) => c.method === 'POST' && c.path === '/messages').map((c) => c.params.content);


test('R1: send_message, send_dm and publish split a long text at the configured length, into the same chunks', async () => {
  const para = (i: number) => `P${i} ${'a'.repeat(42)}`;
  const long = [1, 2, 3, 4, 5].map(para).join('\n\n');
  const expected = [`${para(1)}\n\n${para(2)}`, `${para(3)}\n\n${para(4)}`, para(5)];

  const sm = messagesApi();
  const t1 = tools(sm.api, 100);
  await t1.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: long });
  t1.done();

  const dm = messagesApi();
  const t2 = tools(dm.api, 100);
  await t2.runtime.handleToolCall('send_dm', { to: ['42'], content: long });
  t2.done();

  const pub = messagesApi();
  await adapter(pub.api, { maxMessageLength: 100 }).publish('zulip:general', undefined, text(long), { threadId: 't' });

  assert.deepEqual(bodies(sm.calls), expected, 'send_message');
  assert.deepEqual(bodies(dm.calls), expected, 'send_dm');
  assert.deepEqual(bodies(pub.calls), expected, 'publish');
});


const big = (i: number, n: number) => `p${i} ${'x'.repeat(n)}`;
const long3 = [big(1, 9000), big(2, 9000), big(3, 5000)].join('\n\n');
const long3Chunks = [big(1, 9000), big(2, 9000), big(3, 5000)];
const file = [{ data: 'aGk=', name: 'hi.txt' }];
const fileLink = '[hi.txt](/user_uploads/1/ab/hi.txt)';
const fileRecord = [{ name: 'hi.txt', path: '/user_uploads/1/ab/hi.txt', url: 'https://z/user_uploads/1/ab/hi.txt' }];

test('R2: send_message returns and posts as before, at one chunk, three chunks and with a file', async () => {
  const cases: [Record<string, unknown>, unknown, string[]][] = [
    [{ content: 'hello there' }, { result: 'success', msg: '', id: 901 }, ['hello there']],
    [{ content: long3 }, { result: 'success', msg: '', id: 903, ids: [901, 902, 903], note: "Sent as 3 messages (content exceeded the realm's message length)." }, long3Chunks],
    [{ content: 'hello there', attachments: file }, { result: 'success', msg: '', id: 901, attachments: fileRecord }, [`hello there\n\n${fileLink}`]],
    [{ content: '  ', attachments: file }, { result: 'success', msg: '', id: 901, attachments: fileRecord }, [fileLink]],
  ];
  for (const [args, result, posted] of cases) {
    const { api, calls } = messagesApi();
    const t = tools(api);
    assert.deepEqual(await t.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', ...args }), result);
    assert.deepEqual(calls.map((c) => c.params), posted.map((content) => ({ type: 'stream', to: 'general', topic: 't', content })));
    assert.deepEqual(t.sent, posted.map((content, i) => ({ messageId: String(901 + i), channelId: 'zulip:general', content })));
    t.done();
  }
});

test('R2: send_dm returns and posts as before, at one chunk, three chunks and with a file', async () => {
  const cases: [Record<string, unknown>, unknown, string[]][] = [
    [{ content: 'hello there' }, { result: 'success', msg: '', id: 901, to_user_ids: [42] }, ['hello there']],
    [{ content: long3 }, { result: 'success', msg: '', id: 903, to_user_ids: [42], ids: [901, 902, 903], note: 'Sent as 3 messages.' }, long3Chunks],
    [{ content: 'hello there', attachments: file }, { result: 'success', msg: '', id: 901, to_user_ids: [42], attachments: fileRecord }, [`hello there\n\n${fileLink}`]],
  ];
  for (const [args, result, posted] of cases) {
    const { api, calls } = messagesApi();
    const t = tools(api);
    assert.deepEqual(await t.runtime.handleToolCall('send_dm', { to: ['42'], ...args }), result);
    assert.deepEqual(calls.map((c) => c.params), posted.map((content) => ({ type: 'private', to: [42], content })));
    assert.deepEqual(t.sent.map((s) => s.content), posted);
    t.done();
  }
});

test('R2: publish returns and posts as before, at one chunk, three chunks, with an image, and with nothing to send', async () => {
  const image = { type: 'image' as const, data: Buffer.from('png').toString('base64'), mimeType: 'image/png' };
  const cases: [string, unknown[], unknown, Record<string, unknown>, string[], Record<string, unknown>][] = [
    ['zulip:general', text('hello there'), { threadId: 'deploys' }, { delivered: true, messageId: '901', messageIds: ['901'] }, ['hello there'], {}],
    ['zulip:general', text(long3), { threadId: 'deploys' }, { delivered: true, messageId: '903', messageIds: ['901', '902', '903'] }, long3Chunks, {}],
    ['zulip:general', [...text('hello there'), image], { threadId: 'deploys' }, { delivered: true, messageId: '901', messageIds: ['901'] }, ['hello there\n\n[image-2.png](/user_uploads/1/ab/image-2.png)'], { uploader, uploadPolicy }],
    ['zulip:dm:42', text(long3), undefined, { delivered: true, messageId: '903', messageIds: ['901', '902', '903'] }, long3Chunks, {}],
  ];
  for (const [channelId, blocks, hints, result, posted, options] of cases) {
    const { api, calls } = messagesApi();
    assert.deepEqual(await adapter(api, options).publish(channelId, undefined, blocks as never, hints as never), result);
    const target = channelId === 'zulip:dm:42' ? { type: 'private', to: [42] } : { type: 'stream', to: 7, topic: 'deploys' };
    assert.deepEqual(calls.map((c) => c.params), posted.map((content) => ({ ...target, content })));
  }
  const empty = messagesApi();
  assert.deepEqual(await adapter(empty.api).publish('zulip:general', undefined, text('')), { delivered: false });
  assert.equal(empty.calls.length, 0, 'nothing to send posts nothing');
});


const refuseSecond: Answer = (n) => (n === 2 ? { result: 'error', msg: 'nope', code: 'BAD_REQUEST' } : ok(n));
let noAnswer: ZulipNoAnswer | undefined;
const noAnswerSecond: Answer = (n) => {
  if (n === 2) throw (noAnswer = new ZulipNoAnswer('timeout', 'POST', '/messages', true, 'Zulip gave no answer to POST /messages within 15 s.'));
  return ok(n);
};
const threeChunks = [big(1, 9000), big(2, 9000), big(3, 5000)].join('\n\n');

test('R3: a refusal at the second of three chunks keeps each caller\'s error text, and the first chunk is on record', async () => {
  const sm = tools(messagesApi(refuseSecond).api);
  await assert.rejects(sm.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: threeChunks }), (e: Error) => e.message.endsWith('nope'));
  assert.deepEqual(sm.sent.map((s) => s.messageId), ['901']);
  sm.done();

  const dm = tools(messagesApi(refuseSecond).api);
  await assert.rejects(dm.runtime.handleToolCall('send_dm', { to: ['42'], content: threeChunks }), (e: Error) => e.message.endsWith('nope'));
  assert.deepEqual(dm.sent.map((s) => s.messageId), ['901']);
  dm.done();

  const pub = adapter(messagesApi(refuseSecond).api);
  await assert.rejects(pub.publish('zulip:general', undefined, text(threeChunks), { threadId: 't' }), (e: Error) => e.message === 'Zulip refused message to #general: nope');
  assert.deepEqual(pub.sent, [{ messageId: '901', channelId: 'zulip:general', content: big(1, 9000) }]);

  const pubDm = adapter(messagesApi(refuseSecond).api);
  await assert.rejects(pubDm.publish('zulip:dm:42', undefined, text(threeChunks)), (e: Error) => e.message === 'Zulip refused direct message to zulip:dm:42: nope');
  assert.deepEqual(pubDm.sent.map((s) => s.messageId), ['901']);
});

test('R3b: no answer at the second of three chunks throws that error, and the first chunk is on record', async () => {
  const sm = tools(messagesApi(noAnswerSecond).api);
  await assert.rejects(sm.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: threeChunks }), (e) => e === noAnswer);
  assert.deepEqual(sm.sent.map((s) => s.messageId), ['901']);
  sm.done();

  const dm = tools(messagesApi(noAnswerSecond).api);
  await assert.rejects(dm.runtime.handleToolCall('send_dm', { to: ['42'], content: threeChunks }), (e) => e === noAnswer);
  assert.deepEqual(dm.sent.map((s) => s.messageId), ['901']);
  dm.done();

  const pub = adapter(messagesApi(noAnswerSecond).api);
  await assert.rejects(pub.publish('zulip:general', undefined, text(threeChunks), { threadId: 't' }), (e) => e === noAnswer);
  assert.deepEqual(pub.sent.map((s) => s.messageId), ['901']);
});

test('R2 (109): a send that fails after a part was posted names that part by id, then Zulip\'s words or the error; a first part refused gives Zulip\'s words alone', async () => {
  for (const tool of ['send_message', 'send_dm'] as const) {
    const args = tool === 'send_message' ? { type: 'stream', to: 'general', topic: 't', content: threeChunks } : { to: ['42'], content: threeChunks };
    const refused = tools(messagesApi(refuseSecond).api);
    await assert.rejects(refused.runtime.handleToolCall(tool, args), (e: Error) => {
      assert.match(e.message, /\b901\b/, `${tool}: the posted part's id`);
      assert.ok(e.message.endsWith('nope'), `${tool}: Zulip's words last`);
      return true;
    });
    refused.done();
    const silent = tools(messagesApi(noAnswerSecond).api);
    await assert.rejects(silent.runtime.handleToolCall(tool, args), (e: Error) => {
      assert.equal(e, noAnswer, `${tool}: the same error`);
      assert.match(e.message, /\b901\b/, `${tool}: the posted part's id`);
      assert.match(e.message, /within 15 s/, `${tool}: the error's own words`);
      return true;
    });
    silent.done();
    const first = tools(messagesApi(() => ({ result: 'error', msg: 'nope', code: 'BAD_REQUEST' })).api);
    await assert.rejects(first.runtime.handleToolCall(tool, args), (e: Error) => e.message === 'nope');
    first.done();
  }
});
