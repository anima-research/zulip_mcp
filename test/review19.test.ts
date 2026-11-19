import test from 'node:test';
import assert from 'node:assert/strict';
import { MessageIds, messageIdError, markTransportError } from '../src/message-ids.ts';
import { sendMessage, SendRefused } from '../src/outgoing.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';
import { standInApi } from './zulip-stand-in.ts';

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  test(`post refusal without msg retains its code alongside upload receipts in ${mode}`, async () => {
    const { api } = standInApi({ 'POST /messages': { result: 'error', code: 'BAD_REQUEST' } });
    await assert.rejects(sendMessage(api, { type: 'stream', to: 7, topic: 'test' }, 'hello', {
      messageIds: new MessageIds('alpha', mode),
      upload: async () => [{ name: 'a.txt', path: '/user_uploads/a.txt', url: 'https://alpha.example/user_uploads/a.txt' }],
    }), (error: Error) => {
      assert.ok(error instanceof SendRefused);
      assert.match(error.answer.msg, /^BAD_REQUEST Attachment uploads confirmed:/);
      assert.doesNotMatch(error.answer.msg, /undefined/);
      return true;
    });
  });

  test(`authorization Retry-After and upload uncertainty give consistent recovery advice in ${mode}`, t => {
    t.mock.method(console, 'error', () => {});
    const ids = new MessageIds('alpha', mode);
    const auth = messageIdError(new ZulipNoAnswer('not-json', 'GET', '/messages', false, 'private', 401, 30), ids).message;
    assert.match(auth, /HTTP 401.*Server Retry-After: 30 seconds; access must be corrected first/);
    assert.doesNotMatch(auth, /then retry the read/);
    const upload = messageIdError(markTransportError(new Error('private'), { operation: 'Upload' }), ids).message;
    assert.match(upload, /attachment upload in the original operation/);
    assert.doesNotMatch(upload, /retrying upload_file/);
  });
}

test('degraded polling authorization errors require operator repair', async t => {
  t.mock.method(console, 'error', () => {});
  const events: string[] = [];
  const loop = new ZulipEventLoop({ degradedThreshold: 1, sleep: async () => loop.stop() });
  await loop.start(standInApi({
    'POST /register': { queue_id: 'q', last_event_id: -1 },
    'GET /events': () => { throw new ZulipNoAnswer('not-json', 'GET', '/events', false, 'private', 401, 30); },
  }).api, () => {}, event => events.push(event.text));
  assert.match(events[0], /host operator must repair authorization; retries alone cannot fix access/);
});
