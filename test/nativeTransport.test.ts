import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MessageIds } from '../src/message-ids.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { standInApi } from './zulip-stand-in.ts';

// Fail closed for new native fetch sites; the API converts to ZulipNoAnswer.
test('all native fetch sites have a transport error boundary', () => {
  let sites = 0;
  for (const file of readdirSync(new URL('../src/', import.meta.url), { recursive: true })) {
    if (!String(file).endsWith('.ts')) continue;
    const source = readFileSync(new URL(`../src/${file}`, import.meta.url), 'utf8');
    for (const match of source.matchAll(/\bawait (?:fetch|fetchImpl)\(/g)) {
      sites++;
      const statement = source.slice(match.index).split(/;[ \t]*\n/, 1)[0];
      if (file === 'zulip-api.ts') {
        assert.match(source, /catch \(err\) \{[\s\S]*throw new ZulipNoAnswer\('network'/);
      } else {
        assert.match(statement, /\.catch\(error => \{ throw markTransportError\(error(?:, \{ operation: '[^']+' \})?\);/, String(file));
      }
    }
  }
  assert.ok(sites >= 4);
});

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  for (const stage of ['fetch', 'stream', 'buffer', 'upload', 'upload-body'] as const) {
    test(`${mode} native transport failure at ${stage}`, async t => {
      const dir = mkdtempSync(join(tmpdir(), 'nt-'));
      t.after(() => rmSync(dir, { recursive: true, force: true }));
      const original = globalThis.fetch;
      t.after(() => { globalThis.fetch = original; });
      const failure = new TypeError('fetch failed: private socket reset');
      const originalLog = console.error;
      const logged: unknown[][] = [];
      console.error = (...args) => { logged.push(args); };
      t.after(() => { console.error = originalLog; });
      let requests = 0;
      globalThis.fetch = (async () => {
        requests++;
        if (stage === 'fetch' || stage === 'upload') throw failure;
        if (stage === 'upload-body') return { ok: true, status: 200, json: async () => { throw failure; } };
        return {
          ok: true, headers: new Headers(),
          body: stage === 'buffer' ? null : { getReader: () => ({ read: async () => { throw failure; } }) },
          arrayBuffer: async () => { throw failure; },
        };
      }) as typeof fetch;
      const { api } = standInApi({});
      const runtime = new ZulipToolRuntime({
        api, selfUserId: 1, realm: 'https://alpha.example', authHeader: 'Basic test', sessionId: 'test',
      }, dir, { messageIds: new MessageIds('alpha', mode) });
      const uploading = stage.startsWith('upload');
      await assert.rejects(runtime.handleToolCall(uploading ? 'upload_file' : 'fetch_attachment',
        uploading ? { name: 'a.txt', data: 'YQ==' } : { path: '/user_uploads/1/a.txt' }), (error: Error) => {
        if (mode === 'legacy') {
          if (stage === 'upload-body') assert.equal(error.message, 'upload of "a.txt" failed: HTTP 200');
          else assert.equal(error, failure);
        } else {
          assert.match(error.message, /retry reads, but verify the outcome before repeating writes/);
          assert.doesNotMatch(error.message, /private|fetch failed/);
          assert.equal(logged.at(-1)?.[1], failure);
        }
        return true;
      });
      assert.equal(requests, 1, 'never repeat a potentially applied upload');
    });
  }
}

test('native HTML upload responses preserve status and log the original body failure', async t => {
  const { createServer } = await import('node:http');
  const { once } = await import('node:events');
  const { createZulipUploader } = await import('../src/uploads.ts');
  const { messageIdError } = await import('../src/message-ids.ts');
  const logs: unknown[][] = [];
  const originalLog = console.error;
  console.error = (...args) => { logs.push(args); };
  t.after(() => { console.error = originalLog; });
  for (const status of [413, 502]) {
    const server = createServer((_req, res) => { res.writeHead(status); res.end('<html>private proxy path</html>'); });
    server.listen(0, '127.0.0.1');
    await once(server, 'listening');
    try {
      const address = server.address() as { port: number };
      const uploader = createZulipUploader({ realm: `http://127.0.0.1:${address.port}`, authHeader: 'test' });
      for (const mode of ['legacy', 'compat', 'strict'] as const) {
        await assert.rejects(uploader.upload({ name: 'a.txt', data: Buffer.from('a') }), error => {
          const safe = messageIdError(error, new MessageIds('alpha', mode));
          assert.match(safe.message, new RegExp(`HTTP ${status}`));
          if (mode === 'legacy') assert.equal(safe, error);
          else {
            assert.match(safe.message, /Upload/);
            assert.doesNotMatch(safe.message, /private|html|proxy/);
            assert.ok(logs.at(-1)?.[1] instanceof SyntaxError);
          }
          return true;
        });
      }
    } finally { server.closeAllConnections(); await new Promise<void>(resolve => server.close(() => resolve())); }
  }
});

test('attachment timeout and HTTP reason phrases expose only safe facts', async t => {
  const { fetchAttachmentBytes } = await import('../src/content.ts');
  const { messageIdError } = await import('../src/message-ids.ts');
  const originalFetch = globalThis.fetch;
  const originalLog = console.error;
  const logs: unknown[][] = [];
  console.error = (...args) => { logs.push(args); };
  t.after(() => { globalThis.fetch = originalFetch; console.error = originalLog; });
  globalThis.fetch = async () => new Response('', { status: 502, statusText: 'private proxy path' });
  await assert.rejects(fetchAttachmentBytes('https://example.invalid/a', 'a'), error => {
    for (const mode of ['compat', 'strict'] as const) {
      const safe = messageIdError(error, new MessageIds('alpha', mode));
      assert.match(safe.message, /Attachment download.*HTTP 502/);
      assert.doesNotMatch(safe.message, /private proxy/);
    }
    assert.match(messageIdError(error, new MessageIds('alpha', 'legacy')).message, /private proxy/);
    return true;
  });
  t.mock.timers.enable({ apis: ['setTimeout'] });
  let original: unknown;
  globalThis.fetch = async (_url, init) => new Promise((_resolve, reject) => {
    init!.signal!.addEventListener('abort', () => {
      original = new DOMException('private abort diagnostic', 'AbortError');
      reject(original);
    });
  });
  const pending = fetchAttachmentBytes('https://example.invalid/a', 'a');
  t.mock.timers.tick(15000);
  await assert.rejects(pending, error => {
    const safe = messageIdError(error, new MessageIds('alpha', 'strict'));
    assert.match(safe.message, /Attachment download.*Timeout: 15 seconds/);
    assert.doesNotMatch(safe.message, /private/);
    assert.equal(logs.at(-1)?.[1], original);
    return true;
  });
});
