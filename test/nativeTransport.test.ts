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
        assert.match(statement, /\.catch\(error => \{ throw markTransportError\(error\);/, String(file));
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
        }
        return true;
      });
      assert.equal(requests, 1, 'never repeat a potentially applied upload');
    });
  }
}
