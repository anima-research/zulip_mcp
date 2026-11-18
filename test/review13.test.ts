import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { fetchAttachmentBytes, parseZulipAttachmentUrl } from '../src/content.ts';
import { fetchCapped, buildAttachmentBlocks } from '../src/attachments.ts';
import { MessageIds, messageIdError } from '../src/message-ids.ts';
import { httpTransport, selfIdGate, ZulipNoAnswer } from '../src/zulip-api.ts';
import { createZulipUploader } from '../src/uploads.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { renderMissedBlock } from '../src/delivery.ts';
import { standInApi } from './zulip-stand-in.ts';

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  const ids = new MessageIds('alpha', mode);
  test(`${mode} API-suffixed realms resolve attachment paths at the origin`, async t => {
    t.mock.method(console, 'error', () => {});
    const requested: string[] = [];
    t.mock.method(globalThis, 'fetch', async (url: string) => {
      requested.push(url);
      return new Response('hello', { headers: { 'content-type': 'text/plain' } });
    });
    for (const realm of ['https://alpha.example', 'https://alpha.example/', 'https://alpha.example/api', 'https://alpha.example/api/']) {
      for (const path of ['https://alpha.example/user_uploads/a.txt', '/user_uploads/a.txt', 'user_uploads/a.txt']) {
        const url = parseZulipAttachmentUrl(path, realm);
        assert.equal(url.href, 'https://alpha.example/user_uploads/a.txt');
        assert.equal((await fetchAttachmentBytes(url.href, 'a.txt')).buf.toString(), 'hello');
        assert.equal((await fetchCapped(url.href, {}, 10, new AbortController().signal)).buf.toString(), 'hello');
      }
    }
    assert.equal(requested.length, 24);
    for (const path of ['https://u:p@alpha.example/user_uploads/a', '/user_uploads/%2e%2e%2fapi/v1/users/me', '/user_uploads/..%5capi/v1/users/me']) {
      assert.throws(() => parseZulipAttachmentUrl(path, 'https://alpha.example/api'), /credentials|only serves/);
    }
  });

  test(`${mode} compact DM recovery gets a pageable channel from its anchor`, async t => {
    t.mock.method(console, 'error', () => {});
    const dir = mkdtempSync(join(tmpdir(), 'r13-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const users = [1, ...Array.from({ length: 399 }, (_, i) => i + 10000)];
    const channel = 'zulip:dm:' + users.slice(1).join('+');
    let paged = false;
    const raw = { id: 101, type: 'private',
      display_recipient: users.map(id => ({ id, full_name: `user${id}`, email: `${id}@example.com` })),
      sender_id: 10000, sender_full_name: 'sender', sender_email: 'sender@example.com',
      subject: '', timestamp: 1000, content: 'hello', flags: [] };
    const { api } = standInApi({
      'GET /messages/101': { result: 'success', message: raw },
      'GET /messages': params => {
        if (params.include_anchor === true) return { result: 'success', messages: [raw] };
        assert.deepEqual(params.narrow, [{ operator: 'dm', operand: users.slice(1) }]);
        paged = true;
        return { result: 'success', messages: [], found_oldest: true, found_newest: true };
      },
    });
    const tools = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example', authHeader: '', sessionId: 'r13' }, dir, { messageIds: ids });
    const notice = renderMissedBlock([{ id: 101, authorName: 'sender', timestamp: new Date(),
      topic: '', text: 'hello', attachmentNames: [], mentioned: false } as any], {
      streamName: channel, channelId: channel, reason: 'backscroll', count: 1,
      formatTime: () => '', maxChars: 1000, messageIds: ids,
    }).text;
    assert.ok(notice.length <= 1000);
    assert.match(notice, /returns channelId \(including for DMs\)/);
    const reference = JSON.parse(/fetch_around\(message_id=([^)]*)\)/.exec(notice)![1]);
    const anchor = await tools.handleToolCall('fetch_around', { message_id: reference }) as any;
    assert.equal(anchor.channelId, channel);
    await tools.handleToolCall('fetch_history', { channel: anchor.channelId, after: ids.format(101) });
    assert.ok(paged);
  });

  test(`${mode} original native errors survive transport and self-ID replacements`, async t => {
    const failure = new TypeError('private socket diagnostic', { cause: new Error('private native cause') });
    const stack = failure.stack;
    const logs: unknown[][] = [];
    t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args); });
    t.mock.method(globalThis, 'fetch', async () => { throw failure; });
    const transport = httpTransport({ apiUrl: 'https://alpha.example/api/v1', authHeader: '' });
    for (const inner of [transport, selfIdGate(transport).transport]) {
      await assert.rejects(inner('GET', '/messages', {}, {}), error => {
        assert.ok(error instanceof ZulipNoAnswer);
        const native = error.kind === 'no-self-id' ? (error.cause as Error).cause : error.cause;
        assert.equal(native, failure);
        const safe = messageIdError(error, ids);
        assert.doesNotMatch(safe.message + safe.stack, /private/);
        assert.equal(safe.cause, undefined);
        assert.equal(failure.stack, stack);
        assert.equal(logs.at(-1)?.[1], error);
        return true;
      });
    }
  });

  test(`${mode} inline body failures retain HTTP 206 and completed refusals do not advise blind retry`, async t => {
    t.mock.method(console, 'error', () => {});
    let status = 206;
    t.mock.method(globalThis, 'fetch', async () => status === 206 ? new Response(new ReadableStream({
      start(controller) { controller.error(new Error('private broken body')); },
    }), { status }) : new Response('', { status }));
    const blocks = await buildAttachmentBlocks([{ path: '/user_uploads/a.txt', name: 'a.txt', mimeType: 'text/plain', isImage: false }], {
      fetch: (path, max, signal) => fetchCapped(path, {}, max, signal),
    }, { inlineImages: false, maxImages: 0, inlineTextMaxBytes: 100 }, new AbortController().signal, ids);
    assert.match(JSON.stringify(blocks), /HTTP 206/);
    assert.doesNotMatch(JSON.stringify(blocks), /private broken body/);
    for (const refusalStatus of [403, 404]) {
      status = refusalStatus;
      await assert.rejects(fetchAttachmentBytes('https://alpha.example/user_uploads/a', 'a'), error => {
        const safe = messageIdError(error, ids);
        assert.match(safe.message, new RegExp(`refused.*HTTP ${status}`));
        assert.doesNotMatch(safe.message, /before a complete answer|Retry fetch_attachment/);
        return true;
      });
    }
  });

  test(`${mode} proxy JSON and malformed successful uploads retain uncertainty without leaking text`, async t => {
    t.mock.method(console, 'error', () => {});
    for (const [status, body] of [[502, { msg: 'private proxy text' }], [200, {}], [200, null], [200, { result: 'success' }]] as const) {
      const uploader = createZulipUploader({ realm: 'https://alpha.example', authHeader: 'test' }, async () => ({ ok: status === 200, status, text: async () => JSON.stringify(body), json: async () => body }));
      await assert.rejects(uploader.upload({ name: 'a', data: Buffer.from('a') }), error => {
        const safe = messageIdError(error, ids);
        assert.match(safe.message, /No uploaded path was confirmed.*duplicate/);
        assert.match(safe.message, new RegExp(`HTTP ${status}`));
        assert.doesNotMatch(safe.message, /private/);
        return true;
      });
    }
  });
}

test('non-JSON transport failures retain numeric and date Retry-After headers', async t => {
  t.mock.method(console, 'error', () => {});
  let header = '';
  t.mock.method(globalThis, 'fetch', async () => new Response('not JSON', { status: 503, headers: { 'Retry-After': header } }));
  t.mock.method(Date, 'now', () => Date.parse('2026-11-18T00:00:00Z'));
  for (const [after, seconds] of [['60', 60], ['0.5', 0.5], ['Wed, 18 Nov 2026 00:02:00 GMT', 120]] as const) {
    header = after;
    await assert.rejects(httpTransport({ apiUrl: 'https://alpha.example/api/v1', authHeader: '' })('GET', '/messages', {}, {}), error => {
      assert.ok(error instanceof ZulipNoAnswer);
      assert.equal(error.retryAfterSeconds, seconds);
      assert.match(messageIdError(error, new MessageIds('alpha', 'strict')).message, new RegExp(`HTTP 503.*Retry after ${seconds} seconds`));
      return true;
    });
  }
});

test('native socket resets retain response status and upload uncertainty', async t => {
  t.mock.method(console, 'error', () => {});
  const server = createServer((req, res) => {
    if (req.method === 'POST') { req.socket.destroy(); return; }
    res.writeHead(206, { 'content-type': 'text/plain', 'content-length': '100' });
    res.write('partial');
    setTimeout(() => res.destroy(), 30);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  t.after(async () => { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); });
  const realm = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  for (const mode of ['legacy', 'compat', 'strict'] as const) {
    const ids = new MessageIds('alpha', mode);
    await assert.rejects(fetchCapped(`${realm}/user_uploads/a`, {}, 200, new AbortController().signal), error => {
      assert.match(messageIdError(error, ids).message, /HTTP 206/);
      assert.ok((error as Error).cause, 'native fetch attaches a socket cause');
      return true;
    });
    const uploader = createZulipUploader({ realm, authHeader: 'test' });
    await assert.rejects(uploader.upload({ name: 'a', data: Buffer.from('a') }), error => {
      assert.match(messageIdError(error, ids).message, /No uploaded path was confirmed.*duplicate/);
      assert.ok((error as Error).cause);
      return true;
    });
  }
});

test('compat warnings preserve primitive, array and null API results', async t => {
  const logs: unknown[][] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args); });
  const dir = mkdtempSync(join(tmpdir(), 'r13-warn-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  for (const response of ['text', [1, 2], null]) {
    const { api } = standInApi({ 'PATCH /messages/7': response });
    const tools = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example', authHeader: '', sessionId: 'r13' }, dir,
      { messageIds: new MessageIds('alpha', 'compat') });
    assert.deepEqual(await tools.handleToolCall('edit_message', { message_id: 7, content: 'hello' }), response);
  }
  assert.equal(logs.filter(args => String(args[0]).includes('Warning: bare message_id')).length, 3);
});

test('completed upload refusals differ from truncated response bodies in every mode', async t => {
  t.mock.method(console, 'error', () => {});
  for (const mode of ['legacy', 'compat', 'strict'] as const) {
    const ids = new MessageIds('alpha', mode);
    for (const [status, text] of [[500, '{"result":"error","msg":"private server detail"}'], [413, '<html>private proxy detail</html>'], [503, '<html>unavailable</html>']] as const) {
      const uploader = createZulipUploader({ realm: 'https://alpha.example', authHeader: 'test' }, async () => new Response(text, { status, headers: { 'Retry-After': '120' } }));
      await assert.rejects(uploader.upload({ name: 'a', data: Buffer.from('a') }), error => {
        const safe = messageIdError(error, ids);
        assert.match(safe.message, status === 413 ? /Upload was refused by the server/ : /No uploaded path was confirmed.*duplicate/);
        assert.match(safe.message, new RegExp(`HTTP ${status}.*Retry after 120 seconds`));
        assert.doesNotMatch(safe.message, /before a complete|private|<html>/);
        return true;
      });
    }
    const uploader = createZulipUploader({ realm: 'https://alpha.example', authHeader: 'test' }, async () => ({
      ok: false, status: 503, headers: { get: () => '120' }, text: async () => { throw new TypeError('socket lost'); }, json: async () => ({}),
    }));
    await assert.rejects(uploader.upload({ name: 'a', data: Buffer.from('a') }), error => {
      assert.match(messageIdError(error, ids).message, /before a complete.*No uploaded path was confirmed.*HTTP 503.*Retry after 120 seconds/);
      return true;
    });
  }
});

test('rate-limited reads wait before recommending a retry', () => {
  const safe = messageIdError(new ZulipNoAnswer('not-json', 'GET', '/messages', false, 'private', 429, 60), new MessageIds('alpha', 'strict'), false);
  assert.match(safe.message, /Retry after 60 seconds, then retry the read/);
  assert.doesNotMatch(safe.message, /Retry the read\./);
});

test('a failed send names earlier confirmed uploads without implying none of the batch succeeded', async t => {
  const { markTransportError } = await import('../src/message-ids.ts');
  t.mock.method(console, 'error', () => {});
  const dir = mkdtempSync(join(tmpdir(), 'batch-upload-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  for (const mode of ['legacy', 'compat', 'strict'] as const) {
    const { api, calls } = standInApi({});
    let count = 0;
    const tools = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example', authHeader: '', sessionId: 'batch' }, dir, {
      messageIds: new MessageIds('alpha', mode),
      uploader: { upload: async () => {
        if (++count === 2) throw markTransportError(new TypeError('private socket text'), { operation: 'Upload' });
        return { name: 'first', path: '/user_uploads/1/confirmed', url: 'https://alpha.example/user_uploads/1/confirmed' };
      } },
    });
    await assert.rejects(tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'files',
      attachments: [{ name: 'first', data: 'YQ==' }, { name: 'second', data: 'Yg==' }],
    }), error => {
      assert.match((error as Error).message, /No uploaded path was confirmed for this attempt/);
      assert.match((error as Error).message, /Earlier attachment uploads confirmed:.*\/user_uploads\/1\/confirmed/);
      assert.match((error as Error).message, /No message was sent.*Retrying the original send uploads those attachments again/);
      assert.doesNotMatch((error as Error).message, /private socket/);
      return true;
    });
    assert.equal(count, 2);
    assert.equal(calls.length, 0, 'no message was posted');
  }
});
