/** Reviewer regressions through the real handlers and zulip-js HTTP client. */
import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import zulipInit from 'zulip-js';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { PassThrough } from 'node:stream';
import { McplConnection } from '@animalabs/mcpl-core';
import { ZulipMcplServer } from '../src/server.ts';
import type { PlatformAdapter } from '../src/platforms/adapter.ts';

const stream = { id: 101, type: 'stream', stream_id: 12,
  display_recipient: "R&D / café. (test)!*'", subject: "a/b .% #? [] (x)!*'💬" };
const streamPath = 'stream/12/topic/a.2Fb.20.2E.25.20.23.3F.20.5B.5D.20.28x.29.21.2A.27.F0.9F.92.AC/near/101';
const messages: Record<number, unknown> = {
  101: stream,
  102: { id: 102, type: 'private', display_recipient: [{ id: 790 }, { id: 42 }] },
  103: { id: 103, type: 'private', display_recipient: [{ id: 790 }, { id: 42 }, { id: 7 }] },
  104: { id: 104, type: 'private', display_recipient: [{ id: 790 }] },
  105: { id: 105, type: 'stream', stream_id: 12, display_recipient: 'general', subject: '' },
  996: { id: 996, type: 'stream' },
};

async function fixture(t: TestContext) {
  const requests: { method?: string; url: URL; content: string | null }[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const url = new URL(req.url!, 'http://127.0.0.1');
      const body = Buffer.concat(chunks).toString();
      const content = req.method === 'POST'
        ? body.match(/name="content"\r\n\r\n([\s\S]*?)\r\n--/)?.[1] ?? null
        : new URLSearchParams(body).get('content');
      requests.push({ method: req.method, url, content });
      res.setHeader('content-type', 'application/json');
      if (req.method === 'GET') {
        const id = Number(url.pathname.split('/').at(-1));
        if (id === 992) { res.end(JSON.stringify({ result: 'error', code: 'PRIVATE_BODY_MARKER', msg: 'PRIVATE_BODY_MARKER' })); return; }
        if (id === 991) { res.end(JSON.stringify({ message: stream })); return; }
        if (id === 995) { res.end('PRIVATE_BODY_MARKER not valid JSON'); return; }
        if (id === 994) { req.socket.destroy(); return; }
        if (id === 993) { res.statusCode=403; res.end(JSON.stringify({result:'error',code:'ACCESS_DENIED',msg:'No access'})); return; }
        if (id === 997) { res.statusCode = 502; res.end('not JSON'); return; }
        if (!messages[id]) {
          res.statusCode = id === 998 ? 404 : 400;
          res.end(JSON.stringify({ result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' }));
        } else res.end(JSON.stringify({ result: 'success', message: messages[id] }));
      } else res.end(JSON.stringify({ result: 'success', id: 501 }));
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const realm = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const dir = mkdtempSync(join(tmpdir(), 'zulip-message-links-'));
  t.after(async () => {
    rmSync(dir, { recursive: true, force: true });
    await new Promise<void>((resolve, reject) => server.close((err) => err ? reject(err) : resolve()));
  });
  const client = await zulipInit({ realm, username: 'bot@example.com', apiKey: 'test-only' });
  let uploads = 0;
  const tools = new ZulipToolRuntime({ client, selfUserId: 790, realm, authHeader: 'Basic dGVzdDpvbmx5', sessionId: 'test' }, dir, {
    uploader: { async upload({ name }) { uploads++; return { name, path: '/user_uploads/1/a/file', url: `${realm}/user_uploads/1/a/file` }; } },
  });
  return { tools, requests, realm, uploads: () => uploads,
    link: (path: string) => `${realm}/#narrow/${path}` };
}


test('send: large content with final bad reference cannot upload or partially chunk', async t => {
 const f=await fixture(t);
 await assert.rejects(f.tools.handleToolCall('send_message',{type:'stream',to:'general',topic:'links',content:'a'.repeat(50000)+' [[msg:101]] [[msg:999]]', attachments:[{data:'aGk=',name:'hi.txt'}]}));
 assert.equal(f.uploads(),0); assert.ok(f.requests.every(r=>r.method==='GET'));
});
test('saved upstream URL example: shortened stream slug and encoded topic', async t => {
 const f=await fixture(t);
 await f.tools.handleToolCall('edit_message',{message_id:500,content:'[[msg:101]]'});
 const expected=f.link(streamPath);
 assert.equal(f.requests.at(-1).content,expected);
});

test('malformed response must not leak raw body in error', async t => {
 const f=await fixture(t);
 let error;
 try { await f.tools.handleToolCall('edit_message',{message_id:500,content:'[[msg:995]]'}); } catch(e) {error=e;}
 assert.ok(error); assert.ok(f.requests.every(r=>r.method==='GET'));
 assert.doesNotMatch(String(error),/PRIVATE_BO/);
});
for (const id of [994,993]) test(`network/access failure ${id} is atomic`, async t => {
 const f=await fixture(t);
 await assert.rejects(f.tools.handleToolCall('send_message',{type:'stream',to:'general',topic:'links',content:'[[msg:101]] [[msg:'+id+']]',attachments:[{data:'aGk=',name:'hi.txt'}]}));
 assert.equal(f.uploads(),0); assert.ok(f.requests.every(r=>r.method==='GET'));
});

test('lookup errors allowlist API codes, never body text, arbitrary codes or exception text', async () => {
  const { resolveMessageLinks } = await import('../src/message-links.ts');
  for (const [result, description] of [
    [{result:'error',code:'ACCESS_DENIED',msg:'PRIVATE_BODY_MARKER'}, 'Zulip API error ACCESS_DENIED'],
    [{result:'error',code:'PRIVATE_BODY_MARKER',msg:'PRIVATE_BODY_MARKER'}, 'message not found or not accessible'],
  ] as const) {
    await assert.rejects(resolveMessageLinks('[[msg:999]]', {
      realm:'https://example.invalid',client:{messages:{async getById(){return result;}}} as any,
    }), {message:`Cannot resolve [[msg:999]]: ${description}`});
  }
  await assert.rejects(resolveMessageLinks('[[msg:999]]', {
    realm:'https://example.invalid',client:{messages:{async getById(){throw new Error('PRIVATE_BODY_MARKER');}}} as any,
  }), {message:'Cannot resolve [[msg:999]]: transport or parse error'});
});

test('stream slug shortening includes the id prefix and uses strict upstream thresholds', async () => {
  const { resolveMessageLinks } = await import('../src/message-links.ts');
  // Thresholds apply to the full id-name slug, not just the name.
  for (const [name, operand] of [
    ['aaa!!', '12-aaa.21.21'], // +4, exactly 1.5x: retain
    ['aa!!', '12'],           // +4, greater than 1.5x: omit
    ['a'.repeat(16)+'!!!!!', '12-'+ 'a'.repeat(16)+'.21'.repeat(5)], // +10: retain
    ['a'.repeat(16)+'!!!!!!', '12'], // +12, even though ratio <1.5: omit
    ['team space', '12-team-space'],
    ['a'.repeat(27), '12-'+ 'a'.repeat(27)],
  ]) {
    const actual = await resolveMessageLinks('[[msg:101]]', {
      realm:'https://example.invalid',client:{messages:{async getById(){return {result:'success',message:{...stream,display_recipient:name}};}}} as any,
    });
    assert.equal(actual,`https://example.invalid/#narrow/stream/${operand}/topic/${streamPath.split('/topic/')[1]}`);
  }
});

for (const id of [995, 994, 993, 997, 999, 996, 992, 991]) {
  test(`reply_to lookup failure ${id} is sanitized after successful token lookup`, async (t) => {
    const f = await fixture(t);
    await assert.rejects(f.tools.handleToolCall('send_message', {
      reply_to: id, content: '[[msg:102]]', attachments: [{ data: 'aGk=', name: 'hi.txt' }],
    }), (error: Error) => {
      assert.equal(error.message, `Cannot resolve reply_to ${id}: message not found or not accessible`);
      assert.equal(error.cause, undefined);
      assert.doesNotMatch(String(error), /PRIVATE_BO|FetchError|No access|not JSON/);
      return true;
    });
    assert.deepEqual(f.requests.map((r) => r.method), ['GET', 'GET']);
    assert.equal(f.requests[0].url.pathname, '/api/v1/messages/102');
    assert.equal(f.requests[1].url.pathname, `/api/v1/messages/${id}`);
    assert.equal(f.uploads(), 0);
  });
}

test('tools/call returns only the sanitized reply_to error for malformed client JSON', async (t) => {
  const f = await fixture(t);
  // No adapter operations occur in plain-MCP tool calls.
  const server = new ZulipMcplServer({ type: 'zulip' } as PlatformAdapter, f.tools, {
    serverInfo: { name: 'test', version: '0' }, mcplEnabled: false, stateDir: null,
  });
  const toServer = new PassThrough(), toHost = new PassThrough();
  const host = McplConnection.fromStreams(toHost, toServer);
  const served = server.serve(McplConnection.fromStreams(toServer, toHost));
  t.after(async () => {
    toServer.end();
    await served;
    await server.shutdown();
    host.close();
  });
  await host.sendRequest('initialize', {
    protocolVersion: '2025-03-26', capabilities: {}, clientInfo: { name: 'test', version: '0' },
  });
  host.sendNotification('notifications/initialized');
  const result = await host.sendRequest('tools/call', {
    name: 'send_message', arguments: { reply_to: 995, content: '[[msg:102]]' },
  });
  assert.deepEqual(result, {
    content: [{ type: 'text', text: 'Error: Cannot resolve reply_to 995: message not found or not accessible' }],
    isError: true,
  });
  assert.deepEqual(f.requests.map((r) => r.method), ['GET', 'GET']);
});
