import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { initializeZulipClient, subscribeConfigured } from '../src/zulip-client.ts';
import type { StreamDirectory } from '../src/stream-directory.ts';
import { standInApi } from './zulip-stand-in.ts';
import { httpTransport, zulipApi, ZulipRefused } from '../src/zulip-api.ts';

interface Recorded {
  method: string;
  rawPath: string;
  path: string;
  query: URLSearchParams;
  body: string;
  contentType: string | undefined;
  authorization: string | undefined;
  closed: Promise<void>;
}

type Answer =
  | { json: unknown; status?: number; delayMs?: number }
  | { html: string; status: number }
  | 'hang'
  | 'stall-body'
  | { slow: string[]; everyMs: number };

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
        rawPath: url.pathname,
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
      } else if (answer === 'stall-body') {
        res.statusCode = 200;
        res.setHeader('content-type', 'application/json');
        res.write('{"result":"success","streams":[');
      } else if ('slow' in answer) {
        res.statusCode = 200;
        res.setHeader('content-type', 'text/plain');
        const parts = [...answer.slow];
        const next = () => {
          const part = parts.shift();
          if (part === undefined) { res.end(); return; }
          res.write(part);
          setTimeout(next, answer.everyMs);
        };
        next();
      } else if ('html' in answer) {
        res.statusCode = answer.status;
        res.setHeader('content-type', 'text/html');
        res.end(answer.html);
      } else {
        const send = () => {
          res.statusCode = answer.status ?? 200;
          res.setHeader('content-type', 'application/json');
          res.end(JSON.stringify(answer.json));
        };
        if (answer.delayMs) setTimeout(send, answer.delayMs);
        else send();
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

test('delete_message sends no parameter, and remove_reaction and unlisten send theirs in the body', async () => {
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
    assert.equal(deletes[0].body, '', 'delete_message sends no body');
    assert.equal(deletes[0].contentType, undefined, 'delete_message sets no body, so no content type is sent');
    assert.equal(new URLSearchParams(deletes[1].body).get('emoji_name'), 'thumbs_up');
    assert.equal(new URLSearchParams(deletes[2].body).get('subscriptions'), JSON.stringify(['general']));
  } finally { s.done(); z.close(); }
});

test('a call that gets no answer ends with an error at its bound, and its connection is closed', { timeout: 20_000 }, async () => {
  const z = await realm({ 'GET /streams': 'hang' });
  const s = await serverOn(z.url, {}, { timeoutMs: 2_000 });
  try {
    const started = Date.now();
    await assert.rejects(s.tools.handleToolCall('list_streams', {}), (err: Error & { kind?: string }) => {
      assert.match(err.message, /no answer/i);
      assert.match(err.message, /within 2 s/);
      assert.equal(err.kind, 'timeout');
      return true;
    });
    assert.ok(Date.now() - started < 10_000, 'it ended at its bound, not later');
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
      (err: Error & { mayHaveBeenApplied?: boolean; kind?: string }) => {
        assert.match(err.message, /502/);
        assert.match(err.message, /may have been (applied|sent)/i);
        assert.equal(err.mayHaveBeenApplied, true);
        assert.equal(err.kind, 'not-json');
        return true;
      },
    );
  } finally { s.done(); z.close(); }
});

test('a JSON refusal with a server status says once, on a write, that it may have been applied, and reaches a read as Zulip gave it', async () => {
  const z = await realm({
    'POST /messages': { json: { result: 'error', msg: 'Internal server error.' }, status: 500 },
    'PATCH /messages/55': { json: { result: 'error', msg: 'Busy' }, status: 503 },
    'PATCH /messages/56': { json: { result: 'error', msg: '' }, status: 500 },
    'GET /streams': { json: { result: 'error', msg: 'Internal server error.' }, status: 500 },
  });
  const s = await serverOn(z.url);
  const sentence = 'Zulip answered with a server error, so the request may have been applied: check before sending it again.';
  const once = (message: string) => message.split(sentence).length - 1;
  try {
    await assert.rejects(
      s.tools.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 'deploys', content: 'hi' }),
      (err: Error) => {
        assert.ok(err.message.endsWith(`Internal server error. ${sentence}`), err.message);
        assert.equal(once(err.message), 1);
        return true;
      },
    );
    await assert.rejects(s.tools.handleToolCall('edit_message', { message_id: 55, content: 'x' }), (err: Error) => {
      assert.ok(err.message.endsWith(`Busy. ${sentence}`), err.message);
      assert.equal(once(err.message), 1);
      return true;
    });
    await assert.rejects(s.tools.handleToolCall('edit_message', { message_id: 56, content: 'x' }), (err: Error) => {
      assert.ok(err.message.endsWith(`: unknown error. ${sentence}`), err.message);
      return true;
    });
    assert.deepEqual(await s.session.api.requestAnswer('GET', '/streams'), { result: 'error', msg: 'Internal server error.' });
  } finally { s.done(); z.close(); }
});

test('delete_message keeps its note of the delete while the delete may have been applied, and drops it once the delete surely failed', { timeout: 10_000 }, async () => {
  const failed: string[] = [];
  const z = await realm({
    'DELETE /messages/55': 'hang',
    'DELETE /messages/56': { json: { result: 'error', msg: 'Internal server error' }, status: 500 },
    'DELETE /messages/57': { json: { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' }, status: 400 },
  });
  const s = await serverOn(z.url, {}, { timeoutMs: 300 });
  s.tools.onDeleteFailed = (id) => failed.push(id);
  try {
    for (const message_id of [55, 56, 57]) await assert.rejects(s.tools.handleToolCall('delete_message', { message_id }));
    assert.deepEqual(z.requests.filter((r) => r.method === 'DELETE').map((r) => r.path), ['/messages/55', '/messages/56', '/messages/57'], 'each delete reached Zulip');
  } finally { s.done(); z.close(); }
  const gone = await realm({});
  gone.close();
  const refused = await serverOn(gone.url);
  refused.tools.onDeleteFailed = (id) => failed.push(id);
  try {
    await assert.rejects(refused.tools.handleToolCall('delete_message', { message_id: 58 }));
  } finally { refused.done(); }
  assert.deepEqual(failed, ['57', '58'], 'no answer and a server error keep the note; a refusal and a refused connection drop it');
});

test('send_message to a person with no topic sends no topic parameter', async () => {
  const z = await realm({ 'POST /messages': { json: { result: 'success', id: 501 } } });
  const s = await serverOn(z.url);
  try {
    await s.tools.handleToolCall('send_message', { type: 'private', to: 'ann@example.com', content: 'hi' });
    await s.tools.handleToolCall('send_message', { type: 'direct', to: [42], content: 'hi' });
    const sends = z.requests.filter((r) => r.method === 'POST' && r.path === '/messages');
    assert.equal(sends.length, 2);
    for (const send of sends) assert.equal(new URLSearchParams(send.body).has('topic'), false, send.body);
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
    assert.equal(new URLSearchParams(exchanges[0].body).get('username'), 'bot@example.com');
    assert.equal(new URLSearchParams(exchanges[0].body).get('password'), password);
    const after = z.requests.filter((r) => r.path === '/streams');
    assert.equal(after[0].authorization, 'Basic ' + Buffer.from('bot@example.com:exchanged-key').toString('base64'));
  } finally { s.done(); z.close(); }
});

test('a password exchange that gets no JSON answer fails the start, and its error shows no password', async () => {
  const password = 'pw-that-must-not-show';
  const z = await realm({ 'POST /fetch_api_key': { html: '<html><body>502 Bad Gateway</body></html>', status: 502 } });
  try {
    await assert.rejects(
      initializeZulipClient({ ZULIP_REALM: z.url, ZULIP_EMAIL: 'bot@example.com', ZULIP_PASSWORD: password }),
      (err: Error) => {
        assert.match(err.message, /fetch_api_key/);
        assert.doesNotMatch(err.message + JSON.stringify(err) + String(err.stack), new RegExp(password));
        return true;
      },
    );
    assert.equal(z.requests.filter((r) => r.path === '/fetch_api_key').length, 1);
  } finally { z.close(); }
});

async function startLogging(env: Record<string, string>) {
  const lines: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => { lines.push(args.map(String).join(' ')); };
  try {
    return { session: await initializeZulipClient(env), lines };
  } finally {
    console.error = original;
  }
}

test('a password exchange that Zulip refuses is logged with Zulip\'s words and the missing key, the start goes on, and no line shows the password', async () => {
  const password = 'pw-that-must-not-show';
  const z = await realm({
    'POST /fetch_api_key': { status: 403, json: { result: 'error', msg: 'Your username or password is incorrect.', code: 'AUTHENTICATION_FAILED' } },
  });
  try {
    const { session, lines } = await startLogging({ ZULIP_REALM: z.url, ZULIP_EMAIL: 'bot@example.com', ZULIP_PASSWORD: password });
    assert.equal(session.authHeader, '');
    assert.ok(lines.includes('Failed to get an API key with ZULIP_PASSWORD: Zulip refused POST /fetch_api_key: Your username or password is incorrect. (AUTHENTICATION_FAILED). The server has no API key for this run, so Zulip refuses each call that needs one.'), lines.join('\n'));
    assert.ok(lines.every((line) => !line.includes(password)));
  } finally { z.close(); }
});

test('a refused read of the bot\'s profile, or of the realm\'s settings, is logged with Zulip\'s words, and the profile\'s line says the self-filter is off', async () => {
  const z = await realm({
    'GET /users/me': { status: 429, json: { result: 'error', msg: 'API usage exceeded rate limit', code: 'RATE_LIMIT_HIT', 'retry-after': 0.2 } },
    'GET /server_settings': { status: 400, json: { result: 'error', msg: 'Invalid subdomain', code: 'BAD_REQUEST' } },
  });
  try {
    const { session, lines } = await startLogging({ ZULIP_REALM: z.url, ZULIP_EMAIL: 'bot@example.com', ZULIP_API_KEY: 'key-for-tests' });
    assert.equal(session.selfUserId, null);
    assert.equal(session.maxUploadBytes, null);
    assert.ok(lines.includes('Failed to fetch bot profile for self-filter: Zulip refused GET /users/me: API usage exceeded rate limit (RATE_LIMIT_HIT). The self-filter is off for this run, so the bot\'s own messages come back to the agent.'), lines.join('\n'));
    assert.ok(lines.includes('Failed to read realm server settings (upload cap): Zulip refused GET /server_settings: Invalid subdomain (BAD_REQUEST).'), lines.join('\n'));
  } finally { z.close(); }
});

test('a password exchange whose answer holds no API key, or an empty one, gets the same line', async () => {
  for (const answer of [{ result: 'success' }, { result: 'success', api_key: '' }]) {
    const z = await realm({ 'POST /fetch_api_key': { json: answer } });
    try {
      const { session, lines } = await startLogging({ ZULIP_REALM: z.url, ZULIP_EMAIL: 'bot@example.com', ZULIP_PASSWORD: 'pw' });
      assert.equal(session.authHeader, '');
      assert.ok(lines.includes("Failed to get an API key with ZULIP_PASSWORD: Zulip's answer to POST /fetch_api_key held no API key. The server has no API key for this run, so Zulip refuses each call that needs one."), lines.join('\n'));
    } finally { z.close(); }
  }
});

test('a profile answer with no user id, or a profile read with no answer, says that the self-filter is off', async () => {
  const noId = await realm({ 'GET /users/me': { json: { result: 'success', email: 'bot@example.com' } } });
  const unanswered = await realm({ 'GET /users/me': { html: '<html><body>502 Bad Gateway</body></html>', status: 502 } });
  try {
    for (const [z, why] of [
      [noId, "Zulip's answer to GET /users/me held no user id"],
      [unanswered, "Zulip's answer to GET /users/me wasn't JSON (HTTP 502)"],
    ] as const) {
      const { session, lines } = await startLogging({ ZULIP_REALM: z.url, ZULIP_EMAIL: 'bot@example.com', ZULIP_API_KEY: 'key-for-tests' });
      assert.equal(session.selfUserId, null);
      assert.ok(lines.includes(`Failed to fetch bot profile for self-filter: ${why}. The self-filter is off for this run, so the bot's own messages come back to the agent.`), lines.join('\n'));
    }
  } finally { noId.close(); unanswered.close(); }
});

test('a ZULIP_SUBSCRIBE that Zulip refuses gets the failure line, with Zulip\'s words, and no success line', async () => {
  const { api } = standInApi({
    'POST /users/me/subscriptions': { result: 'error', msg: 'Not allowed for guest users', code: 'BAD_REQUEST' },
  });
  const directory = { whenListed: async () => {}, formerOf: () => undefined } as unknown as StreamDirectory;
  const lines: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => { lines.push(args.map(String).join(' ')); };
  try {
    await subscribeConfigured(api, directory, 'general, dev');
  } finally {
    console.error = original;
  }
  assert.deepEqual(lines, ['Zulip MCP auto-subscribe failed for [general, dev]: Zulip refused POST /users/me/subscriptions: Not allowed for guest users (BAD_REQUEST)']);
});

test('a call\'s own bound ends a real HTTP call at it, below the default', { timeout: 10_000 }, async () => {
  const z = await realm({ 'GET /streams': 'hang' });
  try {
    const api = zulipApi(httpTransport({ apiUrl: `${z.url}/api/v1`, authHeader: '', timeoutMs: 15_000 }));
    const started = Date.now();
    await assert.rejects(api.request('GET', '/streams', 'listing streams', {}, { timeoutMs: 300 }), (err: Error & { kind?: string }) => {
      assert.equal(err.kind, 'timeout');
      assert.match(err.message, /within 300 ms\./);
      return true;
    });
    assert.ok(Date.now() - started < 5_000, 'it ended at its own bound');
  } finally { z.close(); }
});

test('a bound also ends a call whose answer stalls after its headers', { timeout: 10_000 }, async () => {
  const z = await realm({ 'GET /streams': 'stall-body' });
  try {
    const api = zulipApi(httpTransport({ apiUrl: `${z.url}/api/v1`, authHeader: '', timeoutMs: 300 }));
    await assert.rejects(api.request('GET', '/streams', 'listing streams'), (err: Error & { kind?: string }) => {
      assert.equal(err.kind, 'timeout');
      assert.match(err.message, /within 300 ms\./);
      return true;
    });
  } finally { z.close(); }
});

test('fetch_attachment ends a download that gets no data for 15 s, and a slow one that keeps moving finishes', { timeout: 10_000 }, async () => {
  const slow = Array.from({ length: 15 }, (_, i) => `part ${i + 1}; `);
  const z = await realm({
    'GET /user_uploads/2/ab/never.png': 'hang',
    'GET /user_uploads/2/ab/stalls.png': 'stall-body',
    'GET /user_uploads/2/ab/slow.txt': { slow, everyMs: 50 },
  });
  const s = await serverOn(z.url);
  const original = globalThis.setTimeout;
  globalThis.setTimeout = ((fn: () => void, ms?: number, ...rest: unknown[]) => original(fn, ms === 15_000 ? 500 : ms, ...rest)) as typeof setTimeout;
  try {
    for (const name of ['never.png', 'stalls.png']) {
      await assert.rejects(s.tools.handleToolCall('fetch_attachment', { path: `/user_uploads/2/ab/${name}` }), (err: Error) => {
        assert.equal(err.message, `The download of ${name} got no data for 15 s.`);
        return true;
      }, name);
    }
    const started = Date.now();
    const fetched = await s.tools.handleToolCall('fetch_attachment', { path: '/user_uploads/2/ab/slow.txt' }) as { content_text?: string };
    assert.equal(fetched.content_text, slow.join(''));
    assert.ok(Date.now() - started > 500, 'the download took longer than the bound as a whole');
  } finally { globalThis.setTimeout = original; s.done(); z.close(); }
});

test('a call with no bound of its own ends at 15 s', async () => {
  const z = await realm({ 'GET /streams': 'hang' });
  const s = await serverOn(z.url);
  const original = globalThis.setTimeout;
  globalThis.setTimeout = ((fn: () => void, ms?: number, ...rest: unknown[]) => original(fn, ms === 15_000 ? 50 : ms, ...rest)) as typeof setTimeout;
  try {
    await assert.rejects(s.tools.handleToolCall('list_streams', {}), (err: Error & { kind?: string }) => {
      assert.match(err.message, /within 15 s\./);
      assert.equal(err.kind, 'timeout');
      return true;
    });
  } finally { globalThis.setTimeout = original; s.done(); z.close(); }
});

test('the start reads the bot\'s profile and the realm\'s settings past the default bound, up to 90 s', async () => {
  const z = await realm({
    'GET /users/me': { json: { result: 'success', user_id: 790, email: 'bot@example.com' }, delayMs: 400 },
    'GET /server_settings': { json: { result: 'success', max_file_upload_size_mib: 25 }, delayMs: 400 },
  });
  const delays: number[] = [];
  const original = globalThis.setTimeout;
  globalThis.setTimeout = ((fn: () => void, ms?: number, ...rest: unknown[]) => { if (typeof ms === 'number') delays.push(ms); return original(fn, ms, ...rest); }) as typeof setTimeout;
  let s: Awaited<ReturnType<typeof serverOn>>;
  try { s = await serverOn(z.url, {}, { timeoutMs: 100 }); } finally { globalThis.setTimeout = original; }
  try {
    assert.equal(delays.filter((ms) => ms === 90_000).length, 2, 'the profile read and the settings read each have a 90 s bound');
    assert.equal(s.session.selfUserId, 790);
    assert.equal(s.session.maxUploadBytes, 25 * 1024 * 1024);
  } finally { s.done(); z.close(); }
});

test('a request that fails before a connection is made says it was not sent', async () => {
  const api = zulipApi(httpTransport({ apiUrl: 'http://[bad/api/v1', authHeader: '' }));
  await assert.rejects(api.request('DELETE', '/messages/5', 'deleting message 5'), (err: Error & { mayHaveBeenApplied?: boolean; kind?: string }) => {
    assert.equal(err.kind, 'network');
    assert.equal(err.mayHaveBeenApplied, false);
    assert.match(err.message, /was not sent/);
    return true;
  });
});

async function failingWith(code: string) {
  const original = globalThis.fetch;
  globalThis.fetch = (async () => { throw new TypeError('fetch failed', { cause: Object.assign(new Error(code), { code }) }); }) as typeof fetch;
  try {
    const api = zulipApi(httpTransport({ apiUrl: 'https://chat.example/api/v1', authHeader: '' }));
    return await api.request('DELETE', '/messages/5', 'deleting message 5').then(() => undefined, (err: Error & { mayHaveBeenApplied?: boolean; kind?: string }) => err);
  } finally { globalThis.fetch = original; }
}

test('a certificate that fails verification means the request was not sent', async () => {
  const err = await failingWith('DEPTH_ZERO_SELF_SIGNED_CERT');
  assert.equal(err?.kind, 'network');
  assert.equal(err?.mayHaveBeenApplied, false);
  assert.match(err?.message ?? '', /was not sent/);
});

test('a TLS failure that can come after the request was written leaves a write uncertain', async () => {
  for (const code of ['ERR_SSL_DECRYPTION_FAILED_OR_BAD_RECORD_MAC', 'ERR_SSL_WRONG_VERSION_NUMBER', 'ERR_TLS_SESSION_ATTACK']) {
    const err = await failingWith(code);
    assert.equal(err?.kind, 'network', code);
    assert.equal(err?.mayHaveBeenApplied, true, code);
    assert.match(err?.message ?? '', /may have been applied/, code);
  }
});

test('a zuliprc without a site takes ZULIP_REALM, for API calls too', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, '[api]\nemail=rc-bot@example.com\nkey=rc-key\n');
  const s = await serverOn(z.url, { ZULIP_RC_PATH: rc });
  try {
    await s.tools.handleToolCall('list_streams', {});
    const call = z.requests.find((r) => r.path === '/streams');
    assert.ok(call, 'the call reached ZULIP_REALM');
    assert.equal(call.authorization, 'Basic ' + Buffer.from('rc-bot@example.com:rc-key').toString('base64'));
    assert.equal(s.session.realm, z.url);
  } finally { s.done(); z.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('ZULIP_REALM that ends with /api gets /v1 with a zuliprc too', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, '[api]\nemail=rc-bot@example.com\nkey=rc-key\n');
  const s = await serverOn(`${z.url}/api`, { ZULIP_RC_PATH: rc });
  try {
    await s.tools.handleToolCall('list_streams', {});
    assert.deepEqual(z.requests.filter((r) => r.path === '/streams').map((r) => r.rawPath), ['/api/v1/streams']);
  } finally { s.done(); z.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('a zuliprc\'s own site comes before ZULIP_REALM', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, `[api]\nemail=rc-bot@example.com\nkey=rc-key\nsite=${z.url}\n`);
  const s = await serverOn('http://127.0.0.1:1', { ZULIP_RC_PATH: rc });
  try {
    await s.tools.handleToolCall('list_streams', {});
    assert.ok(z.requests.some((r) => r.path === '/streams'), 'the call reached the file\'s site');
  } finally { s.done(); z.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('a zuliprc value in a section other than [api] isn\'t used', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, `[api]\nemail=bot@example.com\nsite=${z.url}\n\n[other]\nkey=other-key\n`);
  const s = await serverOn('', { ZULIP_RC_PATH: rc, ZULIP_EMAIL: '', ZULIP_API_KEY: '' });
  try {
    await s.tools.handleToolCall('list_streams', {});
    const call = z.requests.find((r) => r.path === '/streams');
    assert.ok(call);
    assert.equal(call.authorization, 'Basic ' + Buffer.from('bot@example.com:').toString('base64'));
    assert.equal(s.session.authHeader, '');
  } finally { s.done(); z.close(); rmSync(dir, { recursive: true, force: true }); }
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

test('a zuliprc with no [api] section fails at start, saying so', async () => {
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, '[other]\nkey=wrong\n');
  try {
    await assert.rejects(initializeZulipClient({ ZULIP_RC_PATH: rc }), /has no \[api\] section/);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('a zuliprc without email or key takes them from ZULIP_EMAIL and ZULIP_API_KEY for its API calls', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const dir = mkdtempSync(join(tmpdir(), 'zuliprc-'));
  const rc = join(dir, 'zuliprc');
  writeFileSync(rc, `[api]\nsite=${z.url}\n`);
  const s = await serverOn('', { ZULIP_RC_PATH: rc, ZULIP_EMAIL: 'env-bot@example.com', ZULIP_API_KEY: 'env-key' });
  try {
    await s.tools.handleToolCall('list_streams', {});
    const call = z.requests.find((r) => r.path === '/streams');
    assert.ok(call);
    assert.equal(call.authorization, 'Basic ' + Buffer.from('env-bot@example.com:env-key').toString('base64'));
  } finally { s.done(); z.close(); rmSync(dir, { recursive: true, force: true }); }
});

test('a realm that ends with /api is addressed at /api/v1', async () => {
  const z = await realm({ 'GET /streams': { json: { result: 'success', streams: [] } } });
  const s = await serverOn(`${z.url}/api`);
  try {
    await s.tools.handleToolCall('list_streams', {});
    assert.deepEqual(z.requests.filter((r) => r.path === '/streams').map((r) => r.rawPath), ['/api/v1/streams']);
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

test('edit_message refuses an edit with no content, or a message_id that is not a positive integer, before asking Zulip', async () => {
  const z = await realm({ 'PATCH /messages/55': { json: { result: 'success' } } });
  const s = await serverOn(z.url);
  try {
    const before = z.requests.length;
    await assert.rejects(s.tools.handleToolCall('edit_message', { message_id: 55 }), /content/);
    for (const message_id of [0, -3, 1.5, 'abc', undefined, '0x37', ' 55 ', '55.0', '1e2', '']) {
      await assert.rejects(s.tools.handleToolCall('edit_message', { message_id, content: 'x' }), /message_id/, `message_id ${String(message_id)}`);
    }
    assert.equal(z.requests.length, before, 'Zulip was never asked');
    await s.tools.handleToolCall('edit_message', { message_id: '55', content: 'a string of digits still names the message' });
    await s.tools.handleToolCall('edit_message', { message_id: '055', content: 'as Zulip reads the path' });
    assert.deepEqual(z.requests.filter((r) => r.method === 'PATCH').map((r) => r.path), ['/messages/55', '/messages/55']);
  } finally { s.done(); z.close(); }
});

test('delete_message, add_reaction, remove_reaction and get_stream_topics refuse an id that is not a positive integer, before asking Zulip', async () => {
  const z = await realm({
    'GET /realm/emoji': { json: { result: 'success', emoji: {} } },
    'DELETE /messages/55': { json: { result: 'success' } },
    'POST /messages/55/reactions': { json: { result: 'success' } },
    'DELETE /messages/55/reactions': { json: { result: 'success' } },
    'GET /users/me/12/topics': { json: { result: 'success', topics: [] } },
  });
  const s = await serverOn(z.url);
  const started = z.requests.length;
  const noted: string[] = [];
  s.tools.onDeleted = (id) => noted.push(id);
  try {
    for (const message_id of [0, -3, 1.5, 'abc', undefined, '../users/me', '5/reactions', ' 55 ', '0x37', '']) {
      await assert.rejects(s.tools.handleToolCall('delete_message', { message_id }), /message_id/, `delete_message ${String(message_id)}`);
      await assert.rejects(s.tools.handleToolCall('add_reaction', { message_id, emoji_name: 'eyes' }), /message_id/, `add_reaction ${String(message_id)}`);
      await assert.rejects(s.tools.handleToolCall('remove_reaction', { message_id, emoji_name: 'eyes' }), /message_id/, `remove_reaction ${String(message_id)}`);
      await assert.rejects(s.tools.handleToolCall('get_stream_topics', { stream_id: message_id }), /stream_id/, `get_stream_topics ${String(message_id)}`);
    }
    assert.deepEqual(z.requests.slice(started).map((r) => `${r.method} ${r.path}`), [], 'Zulip was never asked');
    assert.deepEqual(noted, [], 'no delete was noted');
    for (const message_id of [55, '55']) {
      await s.tools.handleToolCall('add_reaction', { message_id, emoji_name: 'eyes' });
      await s.tools.handleToolCall('remove_reaction', { message_id, emoji_name: 'eyes' });
      await s.tools.handleToolCall('delete_message', { message_id });
    }
    for (const stream_id of [12, '12']) await s.tools.handleToolCall('get_stream_topics', { stream_id });
    assert.deepEqual(z.requests.filter((r) => r.path.endsWith('/topics')).map((r) => r.path), ['/users/me/12/topics', '/users/me/12/topics']);
    assert.deepEqual(z.requests.filter((r) => r.path.startsWith('/messages')).map((r) => `${r.method} ${r.path}`), [
      'POST /messages/55/reactions', 'DELETE /messages/55/reactions', 'DELETE /messages/55',
      'POST /messages/55/reactions', 'DELETE /messages/55/reactions', 'DELETE /messages/55',
    ]);
  } finally { s.done(); z.close(); }
});

test('edit_message refuses content above the configured message length', async () => {
  const z = await realm({ 'PATCH /messages/55': { json: { result: 'success' } } });
  const s = await serverOn(z.url);
  try {
    const tools = new ZulipToolRuntime(s.session, s.dir, { maxMessageLength: 50 });
    await assert.rejects(tools.handleToolCall('edit_message', { message_id: 55, content: 'é'.repeat(51) }), /51 characters, above the message length of 50/);
    assert.equal(z.requests.filter((r) => r.method === 'PATCH').length, 0, 'Zulip was never asked');
    await tools.handleToolCall('edit_message', { message_id: 55, content: 'é'.repeat(50) });
    assert.equal(z.requests.filter((r) => r.method === 'PATCH').length, 1);
  } finally { s.done(); z.close(); }
});

test('edit_message refuses content above the message length, counted in code points, with both lengths, before asking Zulip', async () => {
  const z = await realm({ 'PATCH /messages/55': { json: { result: 'success' } } });
  const s = await serverOn(z.url);
  try {
    await assert.rejects(s.tools.handleToolCall('edit_message', { message_id: 55, content: 'a'.repeat(10_001) }), (err: Error) => {
      assert.match(err.message, /10001/);
      assert.match(err.message, /10000/);
      return true;
    });
    assert.equal(z.requests.filter((r) => r.method === 'PATCH').length, 0, 'Zulip was never asked');
    await s.tools.handleToolCall('edit_message', { message_id: 55, content: '😀'.repeat(10_000) });
    assert.equal(z.requests.filter((r) => r.method === 'PATCH').length, 1, '10,000 code points are 20,000 UTF-16 units, and fit');
  } finally { s.done(); z.close(); }
});

test("a refusal carries Zulip's code, and a rate limit's retry-after in seconds", async () => {
  const z = await realm({
    'GET /messages': { json: { result: 'error', msg: 'API usage exceeded rate limit', code: 'RATE_LIMIT_HIT', 'retry-after': 1.5 }, status: 429 },
    'GET /streams': { json: { result: 'error', msg: 'Invalid narrow', code: 'BAD_REQUEST' }, status: 400 },
  });
  try {
    const api = zulipApi(httpTransport({ apiUrl: `${z.url}/api/v1`, authHeader: '' }));
    await assert.rejects(api.request('GET', '/messages', 'history'), (err: unknown) => {
      assert.ok(err instanceof ZulipRefused);
      assert.equal(err.code, 'RATE_LIMIT_HIT');
      assert.equal(err.retryAfterSeconds, 1.5);
      return true;
    });
    await assert.rejects(api.request('GET', '/streams', 'the stream list'), (err: unknown) => {
      assert.ok(err instanceof ZulipRefused);
      assert.equal(err.code, 'BAD_REQUEST');
      assert.equal(err.retryAfterSeconds, undefined);
      return true;
    });
  } finally { z.close(); }
});
