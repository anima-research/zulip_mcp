/**
 * The tool runtime against a stand-in for Zulip's API — DM conversations in
 * fetch_history, reaction suppression on the legacy raw format, and API
 * errors that surface as errors.
 *
 * Run: node --import tsx --test test/toolRuntime.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { ZulipToolRuntime, stripSuppressedReactions } from '../src/tool-runtime.ts';
import { toolDefinitions } from '../src/tools.ts';
import type { ZulipSession } from '../src/zulip-client.ts';
import { LOCAL_FILES_SUPPORTED, type UploadPolicy } from '../src/uploads.ts';
import { httpTransport, zulipApi, type ZulipApi } from '../src/zulip-api.ts';
import { standInApi } from './zulip-stand-in.ts';

function runtime(api: ZulipApi): { tools: ZulipToolRuntime; dir: string } {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-tools-'));
  const session: ZulipSession = { api, selfUserId: 790, realm: 'https://z.example.com', authHeader: '', sessionId: 't' };
  const original = console.error;
  console.error = () => {};
  try {
    return { tools: new ZulipToolRuntime(session, dir), dir };
  } finally {
    console.error = original;
  }
}

test('fetch_history reads a DM conversation by its channel id, the form every catch-up note quotes', async () => {
  const calls: Record<string, unknown>[] = [];
  const client = standInApi({
    'GET /messages': async (params: Record<string, unknown>) => {
      calls.push(params);
      return { result: 'success', messages: [], found_newest: true, found_oldest: true };
    },
  }).api;
  const { tools, dir } = runtime(client);
  try {
    const dm = await tools.handleToolCall('fetch_history', { channel: 'zulip:dm:7+42', after: 100 });
    assert.deepEqual(calls[0].narrow, [{ operator: 'dm', operand: [7, 42] }]);
    assert.equal(dm.channelId, 'zulip:dm:7+42');
    const stream = await tools.handleToolCall('fetch_history', { channel: '#general', topic: 'deploys' });
    assert.deepEqual(calls[1].narrow, [['stream', 'general'], ['topic', 'deploys']]);
    assert.equal(stream.channelId, 'zulip:general');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('suppressed reactions are withheld from the raw format too', () => {
  const policy = { suppressed: (name: string, code?: string, type?: string) => name === 'biohazard' || (type === 'unicode_emoji' && code === '1f6d1') };
  const stripped = stripSuppressedReactions([
    { id: 1, reactions: [
      { emoji_name: 'biohazard', emoji_code: '2623', reaction_type: 'unicode_emoji', user_id: 1 },
      { emoji_name: 'octagonal_sign', emoji_code: '1f6d1', reaction_type: 'unicode_emoji', user_id: 1 },
      { emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 2 },
    ] },
    { id: 2 },
  ], policy);
  assert.deepEqual(stripped[0].reactions, [{ emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 2 }]);
  assert.deepEqual(stripped[1], { id: 2 });
});

test('get_channel_history hands the model a raw payload with suppressed reactions removed', async () => {
  const client = standInApi({
    'GET /messages': async () => {
      return { result: 'success', messages: [{ id: 5, subject: 'x', sender_full_name: 'Ann', content: 'hi', timestamp: 1_700_000_000, reactions: [
        { emoji_name: 'biohazard', emoji_code: '2623', reaction_type: 'unicode_emoji', user_id: 1 },
        { emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', user_id: 2 },
      ] }] };
    },
  }).api;
  const { tools, dir } = runtime(client);
  try {
    tools.setReactionPolicy({ suppressed: (name) => name === 'biohazard' });
    const out = await tools.handleToolCall('get_channel_history', { channel: 'general', format: 'raw', start_date: '2000-01-01', auto_monitor: false });
    assert.doesNotMatch(out.formatted_history, /biohazard/);
    assert.match(out.formatted_history, /eyes/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('find_user and list_emojis report a Zulip API error as an error, never as an empty result', async () => {
  const client = standInApi({
    'GET /users': async () => { return { result: 'error', msg: 'Invalid API key' }; },
    'GET /realm/emoji': async () => { return { result: 'error', msg: 'Invalid API key' }; },
  }).api;
  const { tools, dir } = runtime(client);
  try {
    await assert.rejects(tools.handleToolCall('find_user', { query: 'ann' }), /Zulip refused listing users: Invalid API key/);
    await assert.rejects(tools.handleToolCall('list_emojis', {}), /Zulip refused listing realm emoji: Invalid API key/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ── Outbound attachments ──

function fakeUploader() {
  const uploaded: { name: string; bytes: string; mimeType?: string }[] = [];
  const uploader = {
    async upload(i: { name: string; data: Buffer; mimeType?: string }) {
      uploaded.push({ name: i.name, bytes: i.data.toString(), mimeType: i.mimeType });
      return { name: i.name, path: `/user_uploads/1/ab/${i.name}`, url: `https://z.example.com/user_uploads/1/ab/${i.name}` };
    },
  };
  return { uploader, uploaded };
}

function runtimeWithUploads(api: ZulipApi, uploader: { upload: (i: any) => Promise<any> }, roots = new Map<string, string>(), selfUserId: number | null = 790) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-tools-'));
  const session: ZulipSession = { api, selfUserId, realm: 'https://z.example.com', authHeader: 'Basic x', sessionId: 't' };
  const uploadPolicy: UploadPolicy = { roots, maxBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };
  const original = console.error;
  console.error = () => {};
  try {
    return { tools: new ZulipToolRuntime(session, dir, { uploader, uploadPolicy }), dir };
  } finally {
    console.error = original;
  }
}

test('send_message uploads attachments first and links them after the text; content may be empty with files', async () => {
  const sends: Record<string, unknown>[] = [];
  const client = standInApi({
    'POST /messages': async (p: Record<string, unknown>) => { sends.push(p); return { result: 'success', id: 500 + sends.length }; },
  }).api;
  const { uploader, uploaded } = fakeUploader();
  const { tools, dir } = runtimeWithUploads(client, uploader);
  const sent: unknown[] = [];
  tools.onSent = (s) => sent.push(s);
  try {
    const res = await tools.handleToolCall('send_message', {
      type: 'stream', to: 'general', topic: 'reports', content: 'weekly numbers',
      attachments: [{ data: Buffer.from('a,b\n1,2').toString('base64'), name: 'numbers.csv', mime_type: 'text/csv' }],
    });
    assert.deepEqual(uploaded, [{ name: 'numbers.csv', bytes: 'a,b\n1,2', mimeType: 'text/csv' }]);
    assert.equal(sends[0].content, 'weekly numbers\n\n[numbers.csv](/user_uploads/1/ab/numbers.csv)');
    assert.equal(res.id, 501);
    assert.deepEqual(res.attachments, [{ name: 'numbers.csv', path: '/user_uploads/1/ab/numbers.csv', url: 'https://z.example.com/user_uploads/1/ab/numbers.csv' }]);
    assert.equal((sent[0] as { content: string }).content, sends[0].content, 'rollback sees the body as sent');

    const onlyFile = await tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 'reports', attachments: [{ data: 'aGk=', name: 'hi.txt' }] });
    assert.equal(sends[1].content, '[hi.txt](/user_uploads/1/ab/hi.txt)');
    assert.equal(onlyFile.id, 502);

    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 'x' }), /content is required \(or at least one attachment\)/);
    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 'x', content: 'x', attachments: [{ data: 'aGk=' }] }), /need a `name`/);
    assert.equal(sends.length, 2, 'a bad attachment sends nothing');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('send_dm resolves recipients, then uploads, then sends with the links; upload_file returns the markdown', async () => {
  const sends: Record<string, unknown>[] = [];
  const client = standInApi({
    'POST /messages': async (p: Record<string, unknown>) => { sends.push(p); return { result: 'success', id: 9 }; },
    'GET /users': async () => { return { result: 'success', members: [{ user_id: 42, full_name: 'Bo', email: 'bo@example.com', is_active: true, is_bot: false }] }; },
  }).api;
  const { uploader, uploaded } = fakeUploader();
  const { tools, dir } = runtimeWithUploads(client, uploader);
  try {
    const res = await tools.handleToolCall('send_dm', { to: ['42'], content: 'here', attachments: [{ data: 'aGk=', name: 'hi.txt' }] });
    assert.deepEqual(sends[0], { type: 'private', to: [42], content: 'here\n\n[hi.txt](/user_uploads/1/ab/hi.txt)' });
    assert.deepEqual(res.to_user_ids, [42]);
    assert.equal(res.attachments[0].path, '/user_uploads/1/ab/hi.txt');

    const up = await tools.handleToolCall('upload_file', { data: Buffer.from('%PDF').toString('base64'), name: 'doc.pdf', mime_type: 'application/pdf' });
    assert.deepEqual(up, { name: 'doc.pdf', path: '/user_uploads/1/ab/doc.pdf', url: 'https://z.example.com/user_uploads/1/ab/doc.pdf', size: 4, markdown: '[doc.pdf](/user_uploads/1/ab/doc.pdf)' });
    assert.equal(uploaded[1].mimeType, 'application/pdf');
    await assert.rejects(tools.handleToolCall('upload_file', { data: Buffer.alloc(2048).toString('base64'), name: 'big.bin' }), /over the 1KB upload ceiling/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('upload_file and the send tools read local files only through a configured root', { skip: LOCAL_FILES_SUPPORTED ? false : 'local-file attachments are Linux-only' }, async () => {
  const sends: Record<string, unknown>[] = [];
  const client = standInApi({
    'POST /messages': async (p: Record<string, unknown>) => { sends.push(p); return { result: 'success', id: 3 }; },
  }).api;
  const { uploader, uploaded } = fakeUploader();
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'zulip-root-')));
  writeFileSync(join(root, 'chart.png'), 'PNG');
  const { tools, dir } = runtimeWithUploads(client, uploader, new Map([['out', root]]));
  try {
    const up = await tools.handleToolCall('upload_file', { file: 'out/chart.png' });
    assert.equal(up.markdown, '[chart.png](/user_uploads/1/ab/chart.png)');
    assert.deepEqual(uploaded[0], { name: 'chart.png', bytes: 'PNG', mimeType: 'image/png' });
    await assert.rejects(tools.handleToolCall('upload_file', { file: join(root, 'chart.png') }), /paths are root-relative/);
    await assert.rejects(tools.handleToolCall('upload_file', { file: 'out/../chart.png' }), /outside upload root/);
    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', to: 'g', topic: 't', attachments: [{ file: 'home/.zuliprc' }] }), /unknown upload root "home"/);
    assert.equal(sends.length, 0);
  } finally {
    rmSync(dir, { recursive: true, force: true });
    rmSync(root, { recursive: true, force: true });
  }
});

test('without a realm there is no uploader: attachments error clearly and plain sends still work', async () => {
  const sends: Record<string, unknown>[] = [];
  const client = standInApi({
    'POST /messages': async (p: Record<string, unknown>) => { sends.push(p); return { result: 'success', id: 1 }; },
  }).api;
  const dir = mkdtempSync(join(tmpdir(), 'zulip-tools-'));
  const session: ZulipSession = { api: client, selfUserId: 790, realm: '', authHeader: '', sessionId: 't' };
  const original = console.error;
  console.error = () => {};
  const tools = new ZulipToolRuntime(session, dir);
  console.error = original;
  try {
    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', to: 'g', topic: 't', attachments: [{ data: 'aGk=', name: 'a' }] }), /need the realm URL and bot credentials/);
    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', to: 'g', topic: 't', attachments: [{ file: 'x/y' }] }), /need the realm URL/, 'the uploader is checked before any file is touched');
    await assert.rejects(tools.handleToolCall('upload_file', { file: 'x/y' }), /need the realm URL/);
    await tools.handleToolCall('send_message', { type: 'stream', to: 'g', topic: 't', content: 'ok' });
    await tools.handleToolCall('send_message', { type: 'stream', to: 'g', topic: 't', content: 'ok', attachments: [] });
    assert.equal(sends.length, 2, 'an empty attachments array needs no uploader');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

// ── listen registers what it joined (#20) ──

test('listen reports subscriptions to the server and carries the registration outcome back to the agent (#20)', async () => {
  const calls: unknown[] = [];
  const client = standInApi({
    'POST /users/me/subscriptions': async (p: Record<string, unknown>) => {
      calls.push(p);
      return { result: 'success', subscribed: { '790': ['ops'] }, already_subscribed: {}, unauthorized: ['secret'] };
    },
  }).api;
  const { tools, dir } = runtime(client);
  const seen: string[][] = [];
  try {
    tools.onSubscribed = (streams) => {
      seen.push(streams);
      return { announced: [], local: ['zulip:ops'], refused: [], reason: 'timed out' };
    };
    const out = await tools.handleToolCall('listen', { channels: ['ops', 'secret'] });
    assert.deepEqual(seen, [['ops']], 'a stream the bot was not authorized to join is not registered');
    assert.deepEqual(out.registration, { announced: [], local: ['zulip:ops'], refused: [], reason: 'timed out' });
    assert.match(out.note, /did not confirm zulip:ops \(timed out\)/, 'the agent is told, instead of it going to stderr');

    // The hook is awaited: `listen` answers after registration, not before.
    let settled = false;
    tools.onSubscribed = async () => {
      await new Promise((r) => setTimeout(r, 20));
      settled = true;
      return { announced: ['zulip:ops'], local: [], refused: [] };
    };
    const awaited = await tools.handleToolCall('listen', { channels: ['ops'] });
    assert.equal(settled, true);
    assert.match(awaited.note, /Registered 1 channel/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('listen does not register anything when Zulip refused the subscription (#20)', async () => {
  const client = standInApi({
    'POST /users/me/subscriptions': async () => ({ result: 'success', subscribed: {}, already_subscribed: {}, unauthorized: ['secret'] }),
  }).api;
  const { tools, dir } = runtime(client);
  let called = 0;
  try {
    tools.onSubscribed = () => { called++; };
    const out = await tools.handleToolCall('listen', { channels: ['secret'] });
    assert.equal(called, 0, 'nothing was joined, so there is nothing to register');
    assert.equal(out.registration, undefined);
    assert.deepEqual(out.unauthorized, ['secret']);

    // An error from Zulip is reported as such, and registers nothing.
    const failing = runtime(standInApi({
      'POST /users/me/subscriptions': async () => { throw new Error('nope'); },
    }).api);
    failing.tools.onSubscribed = () => { called++; };
    assert.match((await failing.tools.handleToolCall('listen', { channels: ['ops'] })).error, /nope/);
    assert.equal(called, 0);
    rmSync(failing.dir, { recursive: true, force: true });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('delete_message notes the id before the call and un-notes it when the call fails (#22)', async () => {
  const noted: string[] = [];
  const failed: string[] = [];
  const { tools } = runtime(standInApi({
    'DELETE /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) =>
      message_id === 7 ? { result: 'error', msg: 'You don\'t have permission to delete this message' } : { result: 'success' },
  }).api);
  tools.onDeleted = (id) => noted.push(id);
  tools.onDeleteFailed = (id) => failed.push(id);
  await tools.handleToolCall('delete_message', { message_id: 5 });
  await assert.rejects(tools.handleToolCall('delete_message', { message_id: 7 }));
  assert.deepEqual(noted, ['5', '7'], 'noted before the call on both');
  assert.deepEqual(failed, ['7'], 'only the refused one is un-noted');
});

// ── Replies derive their destination before any outbound side effect ──

const replyStream = { type: 'stream', stream_id: 12, display_recipient: 'General', subject: 'Deploy status' };
const replyParticipants = [
  { id: 790, email: 'bot@example.com', full_name: 'Bot' },
  { id: 42, email: 'bo@example.com', full_name: 'Bo' },
  { id: 7, email: 'ann@example.com', full_name: 'Ann' },
];

function replyRuntime(message: Record<string, unknown>, error?: Record<string, unknown>, selfUserId: number | null = 790) {
  const gets: unknown[] = [];
  const userLookups: unknown[] = [];
  const sends: unknown[] = [];
  const { uploader, uploaded } = fakeUploader();
  const result = { result: 'success', id: 501 };
  const client = standInApi({
    'GET /messages/:id': async (params: Record<string, unknown>, { id }: { id: number }) => { gets.push({ id, ...params }); return error ?? { result: 'success', message }; },
    'POST /messages': async (params: unknown) => { sends.push(params); return result; },
    'GET /users': async (params: unknown) => { userLookups.push(params); return { result: 'success', members: [] }; },
  }).api;
  return { ...runtimeWithUploads(client, uploader, new Map(), selfUserId), gets, userLookups, sends, uploaded, result };
}

test('send_message reply_to derives the current stream and topic', async () => {
  const { tools, dir, gets, sends, result } = replyRuntime(replyStream);
  const sent: unknown[] = [];
  tools.onSent = (s) => sent.push(s);
  try {
    assert.deepEqual(await tools.handleToolCall('send_message', { type: 'stream', reply_to: 123, content: 'reply' }), result);
    assert.deepEqual(gets, [{ id: 123 }]);
    assert.deepEqual(sends, [{ type: 'stream', to: 12, topic: 'Deploy status', content: 'reply' }]);
    assert.deepEqual(sent[0], { messageId: '501', channelId: 'zulip:General', content: 'reply' });
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const participants of [replyParticipants.slice(0, 2), replyParticipants]) {
  test(`send_message reply_to preserves all ${participants.length} DM participants`, async () => {
    const { tools, dir, gets, sends } = replyRuntime({ type: 'private', display_recipient: participants, subject: '' });
    const sent: { channelId: string }[] = [];
    tools.onSent = (s) => sent.push(s);
    try {
      const ids = participants.map((p) => p.id);
      await tools.handleToolCall('send_message', { type: 'private', reply_to: 123, content: 'reply' });
      assert.equal(gets.length, 1);
      assert.deepEqual(sends, [{ type: 'private', to: ids, topic: undefined, content: 'reply' }]);
      assert.deepEqual(sent.map((s) => s.channelId), [`zulip:dm:${ids.filter((id) => id !== 790).sort((a, b) => a - b).join('+')}`]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

for (const [label, message, destination] of [
  ['matching stream ID', replyStream, { to: 12 }],
  ['numeric-string stream ID', replyStream, { to: '12' }],
  ['matching stream name', replyStream, { to: 'general' }],
  ['different stream', replyStream, { to: 'Other' }],
  ['matching topic', replyStream, { topic: 'Deploy status' }],
  ['different topic', replyStream, { topic: 'Other' }],
  ['matching DM recipients', { type: 'private', display_recipient: replyParticipants }, { to: [790, 42, 7] }],
  ['partial DM recipients', { type: 'private', display_recipient: replyParticipants }, { to: [42] }],
  ['DM email', { type: 'private', display_recipient: replyParticipants }, { to: 'bo@example.com' }],
  ['empty DM topic', { type: 'private', display_recipient: replyParticipants }, { topic: '' }],
  ['empty self-DM recipients', { type: 'private', display_recipient: [replyParticipants[0]] }, { to: [] }],
] as const) {
  test(`send_message reply_to refuses supplied ${label} before lookups, uploads or sends`, async () => {
    const { tools, dir, gets, userLookups, sends, uploaded } = replyRuntime(message);
    try {
      await assert.rejects(tools.handleToolCall('send_message', {
        ...destination, reply_to: 123, content: 'reply', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
      }), { message: 'reply_to sets the destination; omit to/topic' });
      assert.deepEqual(gets, []);
      assert.deepEqual(userLookups, []);
      assert.deepEqual(sends, []);
      assert.deepEqual(uploaded, []);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

for (const code of ['BAD_REQUEST', 'UNAUTHORIZED']) {
  test(`send_message reply_to preserves API error code ${code} for missing/deleted/inaccessible ids`, async () => {
    const { tools, dir, gets, sends, uploaded } = replyRuntime({}, { result: 'error', code, msg: 'Invalid message(s)' });
    try {
      await assert.rejects(tools.handleToolCall('send_message', {
        type: 'stream', reply_to: 999, content: 'reply',
        attachments: [{ data: 'aGk=', name: 'hi.txt' }],
      }), (err: Error) => err.message.includes(code) && err.message.includes('Invalid message(s)'));
      assert.deepEqual(gets, [{ id: 999 }]);
      assert.deepEqual(sends, []);
      assert.deepEqual(uploaded, []);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

test('send_message reply_to accepts integer-valued strings without mutating arguments', async () => {
  const { tools, dir, gets, sends, userLookups } = replyRuntime(replyStream);
  try {
    for (const reply_to of ['123', ' 123 ', '123.0', '1.23e2']) {
      const args = { reply_to, content: 'reply' };
      await tools.handleToolCall('send_message', args);
      assert.deepEqual(args, { reply_to, content: 'reply' });
    }
    assert.deepEqual(gets, Array(4).fill({ id: 123 }));
    assert.deepEqual(sends, Array(4).fill({ type: 'stream', to: 12, topic: 'Deploy status', content: 'reply' }));
    assert.deepEqual(userLookups, []);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const [alias, message] of [
  ['channel', replyStream],
  ['direct', { type: 'private', display_recipient: replyParticipants }],
] as const) {
  test(`send_message reply_to normalizes the ${alias} type alias and still refuses a conflict`, async () => {
    const { tools, dir, gets, sends, uploaded } = replyRuntime(message);
    try {
      const args = { type: alias, reply_to: 123, content: 'reply' };
      await tools.handleToolCall('send_message', args);
      assert.deepEqual(args, { type: alias, reply_to: 123, content: 'reply' });
      assert.deepEqual(sends, [{
        type: message.type, to: message.type === 'stream' ? 12 : [790, 42, 7],
        topic: message.type === 'stream' ? replyStream.subject : undefined, content: 'reply',
      }]);
      await assert.rejects(tools.handleToolCall('send_message', {
        type: alias === 'channel' ? 'direct' : 'channel', reply_to: 123, content: 'reply',
        attachments: [{ data: 'aGk=', name: 'hi.txt' }],
      }), /type disagrees/);
      assert.deepEqual(gets, [{ id: 123 }, { id: 123 }]);
      assert.equal(sends.length, 1, 'a conflicting alias sends nothing');
      assert.deepEqual(uploaded, [], 'a conflicting alias uploads nothing');
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

test('send_message reply_to requires an integer id and a readable message destination', async () => {
  const { tools, dir, gets, sends, uploaded } = replyRuntime({});
  try {
    for (const reply_to of ['', ' ', 'abc', '123.5', 'Infinity', '9007199254740992', 1.5, NaN, Infinity, 9007199254740992, true, {}, []]) {
      await assert.rejects(tools.handleToolCall('send_message', {
        type: 'stream', reply_to, content: 'reply', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
      }), /reply_to.*integer/);
    }
    assert.deepEqual(gets, []);
    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', reply_to: 123, content: 'reply' }), /reply_to.*destination/);
    assert.deepEqual(sends, []);
    assert.deepEqual(uploaded, []);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('send_message without reply_to keeps the exact payload, response and callbacks without fetching', async () => {
  const { tools, dir, gets, sends, result } = replyRuntime(replyStream);
  const sent: unknown[] = [];
  tools.onSent = (s) => sent.push(s);
  try {
    const stream = { type: 'stream', to: ' general ', topic: ' topic ', content: ' original\n' };
    const dm = { type: 'private', to: 'bo@example.com,ann@example.com', topic: undefined, content: 'DM' };
    for (const args of [stream, dm]) assert.deepEqual(await tools.handleToolCall('send_message', args), result);
    assert.deepEqual(gets, []);
    assert.deepEqual(sends, [stream, dm]);
    assert.deepEqual(sent, [
      { messageId: '501', channelId: 'zulip: general ', content: stream.content },
      { messageId: '501', channelId: 'zulip:bo@example.com,ann@example.com', content: dm.content },
    ]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('send_message schema advertises string destinations and DM email addresses', () => {
  const schema = toolDefinitions.find((t) => t.name === 'send_message')!.inputSchema;
  const properties = schema.properties as Record<string, any>;
  assert.deepEqual(properties.to, {
    type: 'string',
    description: 'For stream: stream name. For private: comma-separated email addresses. Required without reply_to; omit with reply_to.',
  });
});

test('send_message schema leaves destination requirements to the runtime', () => {
  const schema = toolDefinitions.find((t) => t.name === 'send_message')!.inputSchema;
  assert.equal(schema.required, undefined);
  for (const combinator of ['anyOf', 'oneOf', 'allOf']) assert.equal(combinator in schema, false);
  const properties = schema.properties as Record<string, any>;
  assert.equal(properties.reply_to.type, 'integer');
  assert.match(properties.reply_to.description, /Omit to\/topic/);
});

test('send_message reply_to supports a canonical stream name fallback and an empty topic', async () => {
  const { tools, dir, sends } = replyRuntime({ type: 'stream', display_recipient: 'General', subject: '' });
  try {
    await tools.handleToolCall('send_message', { type: 'stream', reply_to: 123, content: 'reply' });
    assert.deepEqual(sends, [{ type: 'stream', to: 'General', topic: '', content: 'reply' }]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('send_message reply_to over HTTP takes numeric strings and aliases before POST and propagates HTTP API errors', async () => {
  const requests: { method?: string; url?: string; body: string }[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      requests.push({ method: req.method, url: req.url, body: Buffer.concat(chunks).toString() });
      res.setHeader('content-type', 'application/json');
      if (req.url?.startsWith('/api/v1/messages/999')) {
        res.statusCode = 400;
        res.end(JSON.stringify({ result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' }));
      } else {
        res.end(JSON.stringify(req.method === 'GET'
          ? { result: 'success', message: replyStream }
          : { result: 'success', id: 501 }));
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  let dir: string | undefined;
  try {
    const api = zulipApi(httpTransport({ apiUrl: `http://127.0.0.1:${port}/api/v1`, authHeader: 'Basic ' + Buffer.from('bot@example.com:test-only').toString('base64') }));
    const state = runtime(api);
    dir = state.dir;
    await state.tools.handleToolCall('send_message', { type: 'channel', reply_to: '123', content: 'reply' });
    assert.deepEqual(requests.map(({ method, url }) => ({ method, url })), [
      { method: 'GET', url: '/api/v1/messages/123' },
      { method: 'POST', url: '/api/v1/messages' },
    ]);
    const posted = new URLSearchParams(requests[1].body);
    assert.equal(posted.get('to'), '12');
    assert.equal(posted.get('topic'), 'Deploy status');
    assert.equal(posted.has('reply_to'), false);
    await assert.rejects(state.tools.handleToolCall('send_message', { type: 'stream', reply_to: 999, content: 'reply' }), /BAD_REQUEST.*Invalid message/);
    assert.equal(requests.length, 3, 'the failed lookup never posts');
  } finally {
    if (dir) rmSync(dir, { recursive: true, force: true });
    server.close();
  }
});

test('send_message refuses a missing destination before lookups, uploads or sends', async () => {
  const { tools, dir, gets, userLookups, sends, uploaded } = replyRuntime(replyStream);
  try {
    for (const destination of [{ to: 12 }, { type: 'stream' }, { type: 'private' }, {}]) {
      await assert.rejects(tools.handleToolCall('send_message', {
        ...destination, topic: 'topic', content: 'oops', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
      }), /type and to are required without reply_to/);
    }
    assert.deepEqual(gets, []);
    assert.deepEqual(userLookups, []);
    assert.deepEqual(uploaded, []);
    assert.deepEqual(sends, []);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const message of [replyStream, { type: 'private', display_recipient: replyParticipants, subject: '' }]) {
  test(`send_message infers omitted type from ${message.type} reply`, async () => {
    const { tools, dir, sends } = replyRuntime(message);
    try {
      await tools.handleToolCall('send_message', { reply_to: 123, content: 'reply' });
      assert.deepEqual(sends, [{ type: message.type, to: message.type === 'stream' ? 12 : [790, 42, 7], topic: message.type === 'stream' ? replyStream.subject : undefined, content: 'reply' }]);
      await assert.rejects(tools.handleToolCall('send_message', { reply_to: 123, type: message.type === 'stream' ? 'private' : 'stream', content: 'reply' }), /type disagrees/);
      assert.equal(sends.length, 1);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

test('send_message reply_to preserves DM participants when bot identity is unknown', async () => {
  const { tools, dir, sends } = replyRuntime({ type: 'private', display_recipient: replyParticipants.slice(0, 2), subject: '' }, undefined, null);
  try {
    await tools.handleToolCall('send_message', { type: 'private', reply_to: 123, content: 'reply' });
    assert.deepEqual(sends, [{ type: 'private', to: [790, 42], topic: undefined, content: 'reply' }]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('send_message keeps the bot in self-DM channel IDs and recipients', async () => {
  const { tools, dir, sends } = replyRuntime({ type: 'private', display_recipient: [replyParticipants[0]], subject: '' });
  const sent: { channelId: string }[] = [];
  tools.onSent = (s) => sent.push(s);
  try {
    await tools.handleToolCall('send_message', { type: 'private', reply_to: 123, content: 'reply' });
    assert.deepEqual(sends, [{ type: 'private', to: [790], topic: undefined, content: 'reply' }]);
    assert.equal(sent[0].channelId, 'zulip:dm:790');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const failure of ['transport', 'api'] as const) {
  test(`user resolution retries after a ${failure} error and caches only a successful directory`, async () => {
    let lookups = 0;
    const { tools, dir } = runtime(standInApi({
      'GET /users': async () => {
        lookups++;
        if (lookups === 1) {
          if (failure === 'transport') throw new Error('directory unavailable');
          return { result: 'error', code: 'RATE_LIMIT_HIT', msg: 'Too many requests' };
        }
        return { result: 'success', members: [{ user_id: 42, email: 'bo@example.com' }] };
      },
    }).api);
    try {
      await assert.rejects(tools.resolveUserId('bo@example.com'),
        failure === 'transport' ? /directory unavailable/ : /Zulip refused listing users: Too many requests/);
      assert.equal(lookups, 1);
      assert.equal(await tools.resolveUserId('bo@example.com'), 42);
      assert.equal(await tools.resolveUserId('BO@example.com'), 42);
      assert.equal(lookups, 2, 'retry failed lookups, then reuse the successful directory');
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

test('send_message reply_to needs only participant IDs and never reads the user directory', async () => {
  const { tools, dir, sends, userLookups } = replyRuntime({
    type: 'private', display_recipient: [{ id: 790 }, { id: 42 }], subject: '',
  });
  try {
    await tools.handleToolCall('send_message', { reply_to: 123, content: 'reply' });
    assert.deepEqual(sends, [{ type: 'private', to: [790, 42], topic: undefined, content: 'reply' }]);
    assert.deepEqual(userLookups, []);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const message of [replyStream, { type: 'private', display_recipient: replyParticipants, subject: '' }]) {
  test(`send_message treats null destination fields and type as absent in a ${message.type} reply`, async () => {
    const { tools, dir, gets, sends, userLookups } = replyRuntime(message);
    try {
      for (const optional of [{ to: null }, { topic: null }, { type: null }, { to: null, topic: null, type: null }]) {
        const args = { reply_to: 123, content: 'reply', ...optional };
        const original = { ...args };
        await tools.handleToolCall('send_message', args);
        assert.deepEqual(args, original, 'normalization does not mutate the caller');
      }
      assert.deepEqual(gets, Array(4).fill({ id: 123 }));
      assert.deepEqual(sends, Array(4).fill({
        type: message.type, to: message.type === 'stream' ? 12 : [790, 42, 7],
        topic: message.type === 'stream' ? replyStream.subject : undefined, content: 'reply',
      }));
      assert.deepEqual(userLookups, []);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}

test('send_message treats a null reply_to and topic as absent on a plain send', async () => {
  const { tools, dir, gets, sends, result } = replyRuntime(replyStream);
  try {
    assert.deepEqual(await tools.handleToolCall('send_message', {
      type: 'stream', to: 'General', topic: 'Deploy status', reply_to: null, content: 'stream',
    }), result);
    assert.deepEqual(await tools.handleToolCall('send_message', {
      type: 'private', to: [42], topic: null, reply_to: null, content: 'DM',
    }), result);
    assert.deepEqual(gets, []);
    assert.deepEqual(sends, [
      { type: 'stream', to: 'General', topic: 'Deploy status', content: 'stream' },
      { type: 'private', to: [42], topic: undefined, content: 'DM' },
    ]);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

for (const field of ['to', 'type']) {
  test(`send_message treats null ${field} as missing without a reply before uploads or sends`, async () => {
    const { tools, dir, gets, sends, uploaded } = replyRuntime(replyStream);
    try {
      await assert.rejects(tools.handleToolCall('send_message', {
        type: 'stream', to: 12, [field]: null, reply_to: null, content: 'reply',
        attachments: [{ data: 'aGk=', name: 'hi.txt' }],
      }), /type and to are required without reply_to/);
      assert.deepEqual(gets, []);
      assert.deepEqual(sends, []);
      assert.deepEqual(uploaded, []);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });
}
