/**
 * The server's Zulip calls over HTTP, against a local server that records
 * each request: what goes in the URL and in the body, the bound on a call,
 * and what an error says when Zulip gives no JSON answer.
 *
 * Run: node --import tsx --test test/zulipApi.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { initializeZulipClient } from '../src/zulip-client.ts';

interface Recorded {
  method: string;
  path: string;
  query: URLSearchParams;
  body: string;
  contentType: string | undefined;
  authorization: string | undefined;
  closed: Promise<void>;
}

type Answer =
  | { json: unknown; status?: number }
  | { html: string; status: number }
  | 'hang';

/** A local stand-in for a realm: answers by "METHOD /path" below /api/v1, and records every request. */
async function realm(answers: Record<string, Answer | ((r: Recorded) => Answer)>) {
  const requests: Recorded[] = [];
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const url = new URL(req.url ?? '/', 'http://realm.test');
      const path = url.pathname.replace(/^\/api\/v1/, '');
      const recorded: Recorded = {
        method: req.method ?? '',
        path,
        query: url.searchParams,
        body: Buffer.concat(chunks).toString(),
        contentType: req.headers['content-type'],
        authorization: req.headers.authorization,
        closed: once(res, 'close').then(() => undefined),
      };
      requests.push(recorded);
      const found = answers[`${recorded.method} ${path}`] ?? defaults[`${recorded.method} ${path}`];
      const answer = typeof found === 'function' ? found(recorded) : found;
      if (answer === undefined) {
        res.statusCode = 404;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ result: 'error', msg: `no stand-in for ${recorded.method} ${path}` }));
      } else if (answer === 'hang') {
        // Never answers: the client's bound has to end it.
      } else if ('html' in answer) {
        res.statusCode = answer.status;
        res.setHeader('content-type', 'text/html');
        res.end(answer.html);
      } else {
        res.statusCode = answer.status ?? 200;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify(answer.json));
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: () => { server.closeAllConnections(); server.close(); },
  };
}

const defaults: Record<string, Answer> = {
  'GET /users/me': { json: { result: 'success', user_id: 790, email: 'bot@example.com' } },
  'GET /server_settings': { json: { result: 'success', max_file_upload_size_mib: 25 } },
};

/** The session as the server builds it, from the environment, and the tool runtime on it. */
async function serverOn(realmUrl: string, env: Record<string, string> = {}, options: Record<string, unknown> = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-api-'));
  const original = console.error;
  console.error = () => {};
  try {
    const session = await (initializeZulipClient as (e: NodeJS.ProcessEnv, o?: unknown) => ReturnType<typeof initializeZulipClient>)(
      { ZULIP_REALM: realmUrl, ZULIP_EMAIL: 'bot@example.com', ZULIP_API_KEY: 'key-for-tests', ...env },
      options,
    );
    return { tools: new ZulipToolRuntime(session, dir), session, dir, done: () => rmSync(dir, { recursive: true, force: true }) };
  } finally {
    console.error = original;
  }
}

test('a long edit_message sends its text in the body, and nothing in the URL', async () => {
  const content = 'é'.repeat(9000);
  const z = await realm({ 'PATCH /messages/55': { json: { result: 'success' } } });
  const s = await serverOn(z.url);
  try {
    await s.tools.handleToolCall('edit_message', { message_id: 55, content });
    const edit = z.requests.find((r) => r.method === 'PATCH');
    assert.ok(edit, 'the edit reached Zulip');
    assert.equal(edit.path, '/messages/55');
    assert.equal([...edit.query.keys()].length, 0, 'no parameter in the URL');
    assert.match(edit.contentType ?? '', /^application\/x-www-form-urlencoded/);
    assert.equal(new URLSearchParams(edit.body).get('content'), content);
  } finally { s.done(); z.close(); }
});

test('delete_message, remove_reaction and unlisten send their parameters in the body', async () => {
  const z = await realm({
    'DELETE /messages/55': { json: { result: 'success' } },
    'DELETE /messages/55/reactions': { json: { result: 'success' } },
    'DELETE /users/me/subscriptions': { json: { result: 'success', removed: ['general'], not_removed: [] } },
  });
  const s = await serverOn(z.url);
  try {
    await s.tools.handleToolCall('delete_message', { message_id: 55 });
    await s.tools.handleToolCall('remove_reaction', { message_id: 55, emoji_name: 'thumbs_up' });
    await s.tools.handleToolCall('unlisten', { channels: ['general'] });
    const deletes = z.requests.filter((r) => r.method === 'DELETE');
    assert.equal(deletes.length, 3);
    for (const d of deletes) assert.equal([...d.query.keys()].length, 0, `no parameter in the URL of ${d.path}`);
    assert.equal(new URLSearchParams(deletes[1].body).get('emoji_name'), 'thumbs_up');
    assert.equal(new URLSearchParams(deletes[2].body).get('subscriptions'), JSON.stringify(['general']));
  } finally { s.done(); z.close(); }
});

test('a call that gets no answer ends with an error at its bound, and its connection is closed', { timeout: 10_000 }, async () => {
  const z = await realm({ 'GET /streams': 'hang' });
  const s = await serverOn(z.url, {}, { timeoutMs: 300 });
  try {
    const started = Date.now();
    await assert.rejects(s.tools.handleToolCall('list_streams', {}), (err: Error) => {
      assert.match(err.message, /no answer/i);
      assert.match(err.message, /300 ?ms|0\.3 ?s/);
      return true;
    });
    assert.ok(Date.now() - started < 5_000, 'it ended at its bound, not later');
    const hung = z.requests.find((r) => r.path === '/streams');
    assert.ok(hung);
    await hung.closed;
  } finally { s.done(); z.close(); }
});

test('an answer that is not JSON names its status, and for a send says it may have been applied', async () => {
  const z = await realm({ 'POST /messages': { html: '<html><body>502 Bad Gateway</body></html>', status: 502 } });
  const s = await serverOn(z.url);
  try {
    await assert.rejects(
      s.tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 'deploys', content: 'hi' }),
      (err: Error & { mayHaveBeenApplied?: boolean }) => {
        assert.match(err.message, /502/);
        assert.match(err.message, /may have been (applied|sent)/i);
        assert.equal(err.mayHaveBeenApplied, true);
        return true;
      },
    );
  } finally { s.done(); z.close(); }
});

test('a refused connection says the request was not sent', async () => {
  const z = await realm({});
  z.close();
  const s = await serverOn(z.url);
  try {
    await assert.rejects(
      s.tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 'deploys', content: 'hi' }),
      (err: Error & { mayHaveBeenApplied?: boolean }) => {
        assert.match(err.message, /was not sent|nothing was sent|not applied/i);
        assert.equal(err.mayHaveBeenApplied, false);
        return true;
      },
    );
  } finally { s.done(); }
});

test('get_channel_history shows Zulip\'s refusal, not a TypeError', async () => {
  const z = await realm({ 'GET /messages': { status: 400, json: { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid narrow operator: stream' } } });
  const s = await serverOn(z.url);
  try {
    await assert.rejects(s.tools.handleToolCall('get_channel_history', { channel: 'general' }), (err: Error) => {
      assert.match(err.message, /^Zulip refused .*: Invalid narrow operator: stream$/);
      return true;
    });
  } finally { s.done(); z.close(); }
});

test('a password login makes one exchange for a key at start, and no error shows the password', async () => {
  const password = 'pw-that-must-not-show';
  const z = await realm({
    'POST /fetch_api_key': { json: { result: 'success', api_key: 'exchanged-key', email: 'bot@example.com' } },
    'GET /streams': { status: 400, json: { result: 'error', code: 'BAD_REQUEST', msg: 'refused for the test' } },
  });
  const s = await serverOn(z.url, { ZULIP_API_KEY: '', ZULIP_PASSWORD: password });
  try {
    await assert.rejects(s.tools.handleToolCall('list_streams', {}), (err: Error) => {
      assert.doesNotMatch(err.message + JSON.stringify(err), new RegExp(password));
      return true;
    });
    const exchanges = z.requests.filter((r) => r.path === '/fetch_api_key');
    assert.equal(exchanges.length, 1);
    assert.equal(exchanges[0].method, 'POST');
    const after = z.requests.filter((r) => r.path === '/streams');
    assert.equal(after[0].authorization, 'Basic ' + Buffer.from('bot@example.com:exchanged-key').toString('base64'));
  } finally { s.done(); z.close(); }
});

test('a zuliprc is read from its [api] section, quoted values unquoted', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, `[other]\nkey=wrong\n\n[api]\nemail=bot@example.com\nkey="rc-key"\nsite=${z.url}\n`);
  const s = await serverOn('', { ZULIP_RC_PATH: rc, ZULIP_EMAIL: '', ZULIP_API_KEY: '' });
  try {
    await s.tools.handleToolCall('list_streams', {});
    const call = z.requests.find((r) => r.path === '/streams');
    assert.ok(call);
    assert.equal(call.authorization, 'Basic ' + Buffer.from('bot@example.com:rc-key').toString('base64'));
  } finally { s.done(); z.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('a realm that ends with /api is addressed at /api/v1', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const s = await serverOn(`${z.url}/api`);
  try {
    await s.tools.handleToolCall('list_streams', {});
    assert.ok(z.requests.some((r) => r.path === '/streams'), 'the call reached /api/v1/streams');
  } finally { s.done(); z.close(); }
});

test('listen and unlisten return Zulip\'s refusal as data, as they do today', async () => {
  const refusal = { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid stream name' };
  const z = await realm({
    'POST /users/me/subscriptions': { status: 400, json: refusal },
    'DELETE /users/me/subscriptions': { status: 400, json: refusal },
  });
  const s = await serverOn(z.url);
  try {
    const joined = await s.tools.handleToolCall('listen', { channels: ['general'] });
    assert.equal(joined.result, 'error');
    assert.equal(joined.msg, 'Invalid stream name');
    const left = await s.tools.handleToolCall('unlisten', { channels: ['general'] });
    assert.equal(left.result, 'error');
    assert.equal(left.msg, 'Invalid stream name');
  } finally { s.done(); z.close(); }
});
