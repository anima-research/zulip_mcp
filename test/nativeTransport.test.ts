import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readdirSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MessageIds } from '../src/message-ids.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { standInApi } from './zulip-stand-in.ts';

// A small source tripwire, not a control-flow proof. Enumerate direct native
// calls (including return/globalThis spellings) and check each local boundary.
function assertFetchBoundaries(file: string, source: string): number {
  const calls = [...source.matchAll(/(?:\b(?:await|return|void)\s+|[=({;]\s*|^\s*)(?:(?:globalThis\s*\.\s*)?fetch|fetchImpl)\s*\(/gm)];
  const invocations = calls.filter(call => !source.slice(call.index, call.index! + 80).includes('fetch(path: string'));
  for (const call of invocations) {
    const start = call.index!;
    const statement = source.slice(start).split(/;[ \t]*\n/, 1)[0];
    if (file === 'zulip-api.ts') {
      // The entire request + body read must be inside this specific try/catch;
      // a second call elsewhere cannot borrow the first call's catch.
      const boundary = /try\s*\{\s*const response = await fetch\(url, init\);\s*status = response.status;(?:(?!\bfetch\s*\()[\s\S])*?text = await response.text\(\);\s*\} catch \(err\) \{[\s\S]*?throw new ZulipNoAnswer\('network'[\s\S]*?\} finally/g;
      assert.ok([...source.matchAll(boundary)].some(m => start >= m.index! && start < m.index! + m[0].indexOf('} catch')), file);
    } else {
      assert.ok(['content.ts', 'attachments.ts', 'uploads.ts'].includes(file), file);
      assert.match(statement, /\.catch\(error => \{ throw markTransportError\(error(?:, \{ operation: '[^']+'(?:, attachmentName: input\.name)? \})?\);/, file);
    }
  }
  return invocations.length;
}

test('direct native fetch inventory checks each local transport boundary', () => {
  let sites = 0;
  for (const file of readdirSync(new URL('../src/', import.meta.url), { recursive: true })) {
    if (!String(file).endsWith('.ts')) continue;
    sites += assertFetchBoundaries(String(file), readFileSync(new URL(`../src/${file}`, import.meta.url), 'utf8'));
  }
  assert.equal(sites, 4, 'update the reviewed direct-fetch inventory deliberately');
});

test('fetch tripwire catches return/globalThis spellings and an extra API call', () => {
  for (const source of ['return fetch(url);', 'globalThis.fetch(url);', 'await globalThis . fetch (url);']) {
    assert.throws(() => assertFetchBoundaries('content.ts', source));
  }
  const api = readFileSync(new URL('../src/zulip-api.ts', import.meta.url), 'utf8');
  assert.throws(() => assertFetchBoundaries('zulip-api.ts', api + '\nasync function unguarded() { return fetch(url); }'));
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
        if (stage === 'upload-body') return { ok: true, status: 200, text: async () => { throw failure; }, json: async () => { throw failure; } };
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
        assert.match(error.message, uploading ? /No uploaded path was confirmed.*operator.*duplicate/ : /Retry fetch_attachment with the same path/);
        assert.doesNotMatch(error.message, /private|fetch failed/);
        assert.equal(logged.at(-1)?.[1], failure);
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
          {
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
    assert.doesNotMatch(messageIdError(error, new MessageIds('alpha', 'legacy')).message, /private proxy/);
    return true;
  });
  try {
    t.mock.timers.enable({ apis: ['setTimeout'] });
  } catch (error) {
    // Node 20.10 predates the options-object API; keep the engines floor covered.
    if (!(error instanceof TypeError) || (error as NodeJS.ErrnoException).code !== 'ERR_INVALID_ARG_TYPE') throw error;
    const legacyTimers = t.mock.timers as unknown as { enable(apis: string[]): void };
    legacyTimers.enable(['setTimeout']);
  }
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

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  test(`${mode} fetch_attachment rejects resolved foreign origins before downloading`, async t => {
    const dir = mkdtempSync(join(tmpdir(), 'origin-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const requests: { url: string; auth: string | undefined }[] = [];
    t.mock.method(globalThis, 'fetch', async (url: string, init: RequestInit) => {
      requests.push({ url, auth: (init.headers as Record<string, string>).Authorization });
      return new Response('safe', { headers: { 'content-type': 'text/plain' } });
    });
    const { api, calls } = standInApi({});
    const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example', authHeader: 'Basic secret', sessionId: 'test' }, dir,
      { messageIds: new MessageIds('alpha', mode) });
    for (const path of [
      '//foreign.example/user_uploads/a', '//alpha.example/user_uploads/a', 'HTTPS://alpha.example/user_uploads/a', 'https://foreign.example/user_uploads/a',
      String.raw`/\foreign.example/user_uploads/a`, String.raw`\\foreign.example\user_uploads\a`,
      String.raw`https:\\foreign.example\user_uploads\a`,
      '//%66oreign.example/user_uploads/a', 'https://%66oreign.example/user_uploads/a',
      'HTTPS://foreign.example/user_uploads/a',
      'http://alpha.example/user_uploads/a', '//alpha.example:444/user_uploads/a',
      '/%2f%2fforeign.example/user_uploads/a', '/%5c%5cforeign.example/user_uploads/a',
      '/user_uploads/%2e%2e/api/v1/users/me',
    ]) {
      await assert.rejects(runtime.handleToolCall('fetch_attachment', { path }), /refusing to fetch|only serves/);
    }
    assert.equal(requests.length, 0);
    assert.equal(calls.length, 0);
    for (const path of ['/user_uploads/a', 'user_uploads/a', 'https://alpha.example/user_uploads/a']) {
      const result = await runtime.handleToolCall('fetch_attachment', { path }) as { content_text: string };
      assert.equal(result.content_text, 'safe');
    }
    assert.equal(requests.length, 3);
    assert.ok(requests.every(r => r.url === 'https://alpha.example/user_uploads/a' && r.auth === 'Basic secret'));
  });

  for (const stage of ['stream', 'buffer'] as const) {
    for (const timeout of [false, true]) {
      test(`${mode} fetch_attachment retains HTTP 206 on ${stage} ${timeout ? 'timeout' : 'failure'}`, async t => {
        const dir = mkdtempSync(join(tmpdir(), 'body-'));
        t.after(() => rmSync(dir, { recursive: true, force: true }));
        t.mock.method(console, 'error', () => {});
        if (timeout) {
          try { t.mock.timers.enable({ apis: ['setTimeout'] }); }
          catch (error) {
            if (!(error instanceof TypeError) || (error as NodeJS.ErrnoException).code !== 'ERR_INVALID_ARG_TYPE') throw error;
            (t.mock.timers as unknown as { enable(apis: string[]): void }).enable(['setTimeout']);
          }
        }
        let reading!: () => void;
        const started = new Promise<void>(resolve => { reading = resolve; });
        t.mock.method(globalThis, 'fetch', async (_url: string, init: RequestInit) => {
          const read = async () => {
            reading();
            if (!timeout) throw new TypeError('private body failure');
            return await new Promise<never>((_resolve, reject) => {
              init.signal!.addEventListener('abort', () => reject(new DOMException('private abort', 'AbortError')));
            });
          };
          return { ok: true, status: 206, headers: new Headers(),
            body: stage === 'stream' ? { getReader: () => ({ read }) } : null,
            arrayBuffer: read } as unknown as Response;
        });
        const { api } = standInApi({});
        const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example', authHeader: 'Basic secret', sessionId: 'test' }, dir,
          { messageIds: new MessageIds('alpha', mode) });
        const checked = assert.rejects(runtime.handleToolCall('fetch_attachment', { path: '/user_uploads/a' }), (error: Error) => {
          assert.match(error.message, /Attachment download.*Retry fetch_attachment.*HTTP 206/);
          if (timeout) assert.match(error.message, /Timeout: 15 seconds/);
          else assert.doesNotMatch(error.message, /Timeout/);
          assert.doesNotMatch(error.message, /private|secret/);
          return true;
        });
        await started;
        if (timeout) t.mock.timers.tick(15000);
        await checked;
      });
    }
  }
}
