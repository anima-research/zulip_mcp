/** Destination regressions using strict-shaped calls and the real HTTP client. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import zulipInit from 'zulip-js';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { toolDefinitions } from '../src/tools.ts';
import { assertStrictArguments } from './strictArguments.ts';

const stream = { type: 'stream', stream_id: 12, display_recipient: 'General', subject: 'Parent' };
const privateMessage = { type: 'private', display_recipient: [{ id: 790 }, { id: 42 }, { id: 7 }] };
const properties = toolDefinitions.find(t => t.name === 'send_message')!.inputSchema.properties!;
function strictCall(fields: Record<string, unknown>, unused: null | '') {
  // Every advertised property is present, as in a Codex strict-schema call.
  return { ...Object.fromEntries(Object.keys(properties).map(k => [k, k === 'attachments' ? [] : k === 'reply_to' && unused === '' ? 0 : unused])), ...fields };
}
function fixture(message: unknown = stream) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-receipt-'));
  const sends: any[] = [];
  let gets = 0, uploads = 0;
  const tools = new ZulipToolRuntime({
    client: { messages: {
      async getById() { gets++; return { result: 'success', message }; },
      async send(p: any) { sends.push(p); return { result: 'success', id: 501, msg: '' }; },
    }, users: { async retrieve() { throw new Error('No user-directory lookup expected'); } } },
    realm: 'https://example.invalid', authHeader: '', selfUserId: 790, sessionId: 'receipt',
  }, dir, { uploader: { async upload() { uploads++; throw new Error('Unexpected upload'); } } });
  return { tools, sends, gets: () => gets, uploads: () => uploads, close: () => rmSync(dir, { recursive: true, force: true }) };
}

for (const unused of [null, ''] as const) {
  for (const [label, fields, payload, destination] of [
    ['stream', { type: 'stream', to: 'General', topic: 'New' },
      { type: 'stream', to: 'General', topic: 'New' }, { type: 'stream', stream: 'General', topic: 'New', channelId: 'zulip:General' }],
    ['DM', { type: 'private', to: 'bo@example.com,ann@example.com' },
      { type: 'private', to: 'bo@example.com,ann@example.com', topic: undefined },
      { type: 'private', recipients: ['bo@example.com', 'ann@example.com'], channelId: 'zulip:bo@example.com,ann@example.com' }],
    ['reply', { reply_to: 123 }, { type: 'stream', to: 12, topic: 'Parent' },
      { type: 'stream', stream: 'General', topic: 'Parent', channelId: 'zulip:General' }],
    ['reply with topic', { reply_to: 123, topic: 'New' }, { type: 'stream', to: 12, topic: 'New' },
      { type: 'stream', stream: 'General', topic: 'New', channelId: 'zulip:General' }],
  ] as const) {
    test(`strict-shaped ${label} with unused ${JSON.stringify(unused)} reports the sent destination`, async t => {
      const f = fixture(); t.after(f.close);
      const args = strictCall({ ...fields, content: 'hello' }, unused);
      assertStrictArguments('send_message', args);
      const before = structuredClone(args);
      assert.deepEqual(Object.keys(args).sort(), Object.keys(properties).sort());
      const result = await f.tools.handleToolCall('send_message', args);
      assert.deepEqual(f.sends, [{ ...payload, content: 'hello' }]);
      assert.deepEqual(result, { result: 'success', id: 501, msg: '', ...destination });
      assert.equal(f.gets(), 'reply_to' in fields ? 1 : 0);
      assert.deepEqual(args, before, 'normalization does not mutate caller arguments');
    });
  }
  test(`strict-shaped private reply with unused ${JSON.stringify(unused)} reports all participants`, async t => {
    const f = fixture(privateMessage); t.after(f.close);
    const args = strictCall({ reply_to: 123, content: 'hello' }, unused);
    assertStrictArguments('send_message', args);
    const result = await f.tools.handleToolCall('send_message', args);
    assert.deepEqual(f.sends, [{ type: 'private', to: [790, 42, 7], topic: undefined, content: 'hello' }]);
    assert.deepEqual(result, { result: 'success', id: 501, msg: '', type: 'private', recipients: [790, 42, 7], channelId: 'zulip:dm:7+42' });
  });
}

test('private reply topic is refused before uploads or sends', async t => {
  const f = fixture(privateMessage); t.after(f.close);
  await assert.rejects(f.tools.handleToolCall('send_message', {
    reply_to: 123, topic: 'Wrong', content: 'hello', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
  }), /topic.*private reply_to/);
  assert.equal(f.gets(), 1); assert.equal(f.uploads(), 0); assert.deepEqual(f.sends, []);
});

test('explicitly empty stream topic without reply_to is refused before upload/send', async t => {
  const f = fixture(); t.after(f.close);
  await assert.rejects(f.tools.handleToolCall('send_message', {
    type: 'stream', to: 'General', topic: '', reply_to: '', content: 'hello', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
  }), /topic.*empty/);
  assert.equal(f.gets(), 0); assert.equal(f.uploads(), 0); assert.deepEqual(f.sends, []);
});

test('send_dm includes its destination alongside existing receipt fields', async t => {
  const f = fixture(); t.after(f.close);
  const result = await f.tools.handleToolCall('send_dm', { to: ['42', '7'], content: 'hello' });
  assert.deepEqual(result, { result: 'success', id: 501, msg: '', to_user_ids: [42, 7], type: 'private', recipients: [42, 7], channelId: 'zulip:dm:7+42' });
});

test('reply topic and receipt follow the real zulip-js HTTP send, including chunking', async t => {
  const requests: { method?: string; body: string }[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', c => chunks.push(c));
    req.on('end', () => {
      requests.push({ method: req.method, body: Buffer.concat(chunks).toString() });
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify(req.method === 'GET' ? { result: 'success', message: stream } : { result: 'success', id: 500 + requests.length }));
    });
  });
  server.listen(0, '127.0.0.1'); await once(server, 'listening');
  t.after(() => new Promise<void>(resolve => server.close(() => resolve())));
  const realm = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const client = await zulipInit({ realm, username: 'test@example.invalid', apiKey: 'test-only' });
  const dir = mkdtempSync(join(tmpdir(), 'zulip-receipt-http-')); t.after(() => rmSync(dir, { recursive: true, force: true }));
  const tools = new ZulipToolRuntime({ client, realm, authHeader: '', selfUserId: 790, sessionId: 'receipt-http' }, dir);
  const receipt = await tools.handleToolCall('send_message', strictCall({ reply_to: 123, topic: 'New', content: 'word '.repeat(5000) }, ''));
  assert.equal(requests[0].method, 'GET');
  assert.ok(requests.length > 2, 'long send is chunked');
  for (const request of requests.slice(1)) {
    assert.equal(request.method, 'POST');
    for (const [key, value] of [['type', 'stream'], ['to', '12'], ['topic', 'New']]) {
      assert.ok(request.body.includes(`name="${key}"\r\n\r\n${value}\r\n`), `${key} matches receipt`);
    }
  }
  assert.equal(receipt.type, 'stream'); assert.equal(receipt.stream, 'General');
  assert.equal(receipt.topic, 'New'); assert.equal(receipt.channelId, 'zulip:General');
  assert.equal(receipt.id, 500 + requests.length); assert.equal(receipt.ids.length, requests.length - 1);
});

for (const type of ['stream', 'channel']) {
  test(`stream reply topic accepts matching ${type} type and still refuses to`, async t => {
    const f = fixture(); t.after(f.close);
    const receipt = await f.tools.handleToolCall('send_message', { type, reply_to: 123, topic: 'New', content: 'hello' });
    assert.equal(receipt.type, 'stream'); assert.equal(receipt.topic, 'New');
    assert.deepEqual(f.sends, [{ type: 'stream', to: 12, topic: 'New', content: 'hello' }]);
    await assert.rejects(f.tools.handleToolCall('send_message', {
      type, reply_to: 123, to: 'Other', topic: 'New', content: 'hello', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
    }), /omit to/);
    assert.equal(f.gets(), 1); assert.equal(f.uploads(), 0); assert.equal(f.sends.length, 1);
  });
}

for (const field of ['type', 'to']) {
  test(`empty ${field} without reply_to is absent, not a send destination`, async t => {
    const f = fixture(); t.after(f.close);
    await assert.rejects(f.tools.handleToolCall('send_message', {
      type: 'stream', to: 'General', topic: 'New', content: 'hello', [field]: '',
    }), /type and to are required/);
    assert.equal(f.gets(), 0); assert.deepEqual(f.sends, []);
  });
}

test('schema descriptions document strict placeholders and stream-only reply topics', () => {
  for (const key of ['type', 'to', 'topic', 'reply_to']) {
    assert.match((properties[key] as any).description, /[Nn]ull or empty string means absent/);
  }
  assert.match((properties.topic as any).description, /private reply_to/);
  assert.match((properties.topic as any).description, /explicitly empty topic.*error/);
});

test('send_message publishes nullable destination types and a nullable type enum', () => {
  for (const key of ['type', 'to', 'topic', 'reply_to']) {
    assert.deepEqual((properties[key] as any).type, [key === 'reply_to' ? 'integer' : 'string', 'null']);
  }
  assert.deepEqual((properties.type as any).enum, ['stream', 'private', '', null]);
});

for (const reply_to of [null, '', 0]) {
  test(`absent reply_to ${JSON.stringify(reply_to)} cannot trigger a lookup without a destination`, async t => {
    const f = fixture(); t.after(f.close);
    await assert.rejects(f.tools.handleToolCall('send_message', {
      reply_to, type: null, to: '', topic: null, content: 'hello',
      attachments: [{ data: 'aGk=', name: 'hi.txt' }],
    }), /type and to are required without reply_to/);
    assert.equal(f.gets(), 0); assert.equal(f.uploads(), 0); assert.deepEqual(f.sends, []);
  });
}
