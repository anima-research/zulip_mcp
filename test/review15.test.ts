import test from 'node:test';
import assert from 'node:assert/strict';
import { MessageIds, messageIdError } from '../src/message-ids.ts';
import { ZulipNoAnswer, parseRetryAfter } from '../src/zulip-api.ts';
import { createZulipUploader } from '../src/uploads.ts';
import { namePosted } from '../src/outgoing.ts';
import { ZulipMcplServer } from '../src/server.ts';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';
import { standInApi } from './zulip-stand-in.ts';

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  test(`only proven upload rejections omit duplicate uncertainty in ${mode}`, async t => {
    t.mock.method(console, 'error', () => {});
    const ids = new MessageIds('alpha', mode);
    for (const status of [400, 401, 403, 404, 405, 413, 415, 422, 429, 408, 409, 500, 502, 503, 504]) {
      for (const text of ['<html>private proxy text</html>', '{"result":"error","msg":"private server text"}']) {
        const uploader = createZulipUploader({ realm: 'https://alpha.example', authHeader: 'test' },
          async () => new Response(text, { status, headers: { 'Retry-After': '120' } }));
        await assert.rejects(uploader.upload({ name: 'a', data: Buffer.from('a') }), error => {
          const safe = messageIdError(error, ids);
          if ([408, 409, 500, 502, 503, 504].includes(status)) {
            assert.match(safe.message, /No uploaded path was confirmed.*duplicate/);
            assert.match(safe.message, new RegExp(`HTTP ${status}.*Retry after 120 seconds`));
            assert.doesNotMatch(safe.message, /refused|before a complete|private/);
          } else {
            assert.doesNotMatch(safe.message, /duplicate/);
            assert.match(safe.message, /Retry after 120 seconds/);
            if (text.startsWith('<')) {
              assert.match(safe.message, new RegExp(`HTTP ${status}`));
              assert.match(safe.message, /Upload was refused/);
            }
          }
          return true;
        });
      }
    }
  });

  test(`publish error boundary logs the original once in ${mode}`, t => {
    const logs: unknown[][] = [];
    t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args); });
    const ids = new MessageIds('alpha', mode);
    const raw = new ZulipNoAnswer('network', 'POST', '/messages', true, 'private socket detail');
    const named = namePosted(raw, [7], ids);
    let message = '';
    (ZulipMcplServer.prototype as any).answerError.call({ messageIds: ids },
      { sendError: (_id: unknown, _code: number, text: string) => { message = text; } },
      { id: 1, method: 'channels/publish' }, named);
    assert.equal(logs.length, 1);
    assert.equal(logs[0][1], raw);
    assert.match(message, /Posted as message.*7.*twice/);
    assert.doesNotMatch(message, /private/);
    (ZulipMcplServer.prototype as any).answerError.call({ messageIds: ids },
      { sendError: () => {} }, { id: 2, method: 'channels/publish' }, new Error('local failure'));
    assert.equal(logs.length, 2, 'unlogged failures still get contextual diagnostics');
  });
}

for (const mode of ['compat', 'strict'] as const) {
  test(`non-boolean duplicate still invalidates normalized acceptance in ${mode}`, async t => {
    const logs: unknown[][] = [];
    t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args); });
    const context = {
      messageIds: new MessageIds('alpha', mode),
      conn: { sendRequest: async () => ({ results: [
        { messageId: 'alpha:7', accepted: true },
        { messageId: 'alpha:007', accepted: 'false' },
        { messageId: 'secret-token-123', accepted: true },
      ] }) },
    };
    const result = await (ZulipMcplServer.prototype as any).sendIncomingToHost.call(context, [{ messageId: '7' }]);
    assert.deepEqual(result.results, []);
    assert.doesNotMatch(JSON.stringify(logs), /secret-token-123/);
  });
}

test('Retry-After rejects numeric date lookalikes and accepts padded delta seconds', () => {
  for (const text of ['-1', '+1', '1e3', '1/2']) assert.equal(parseRetryAfter(text), undefined);
  assert.equal(parseRetryAfter(' 60 '), 60);
});

test('polling awaits bounded Retry-After sleep before another request', async t => {
  t.mock.method(console, 'error', () => {});
  let release!: () => void;
  let sleeping!: () => void;
  const entered = new Promise<void>(resolve => { sleeping = resolve; });
  let calls = 0;
  const loop = new ZulipEventLoop({ sleep: ms => {
    assert.equal(ms, 2 ** 31 - 1);
    sleeping();
    return new Promise<void>(resolve => { release = resolve; });
  } });
  const { api } = standInApi({
    'POST /register': { queue_id: 'q1', last_event_id: -1 },
    'GET /events': () => { calls++; throw new ZulipNoAnswer('not-json', 'GET', '/events', false, 'private', 429, 3000000); },
  });
  const running = loop.start(api, () => {});
  await entered;
  await new Promise<void>(resolve => setImmediate(resolve));
  assert.equal(calls, 1);
  loop.stop();
  release();
  await running;
});
