import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipMcplServer } from '../src/server.ts';
import { MessageIds } from '../src/message-ids.ts';

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  test(`${mode} incoming results reject malformed acceptance and normalized duplicates independently`, async t => {
    t.mock.method(console, 'error', () => {});
    const ids = new MessageIds('alpha', mode);
    const format = (n: number) => String(ids.format(n));
    const server = Object.assign(Object.create(ZulipMcplServer.prototype), {
      messageIds: ids, conn: { sendRequest: async () => ({ results: [
        { messageId: format(21), accepted: 'false' },
        { messageId: format(22), accepted: true },
        { messageId: format(22), accepted: false },
        { messageId: format(23), accepted: true },
      ] }) },
    });
    const result = await server.sendIncomingToHost([21, 22, 23].map(messageId => ({ messageId: String(messageId) })));
    assert.deepEqual(result.results, [{ messageId: '23', accepted: true }]);
  });
}

test('unreadable write authorization failures require repair, not waiting out Retry-After', async t => {
  t.mock.method(console, 'error', () => {});
  const { ZulipNoAnswer } = await import('../src/zulip-api.ts');
  const { messageIdError } = await import('../src/message-ids.ts');
  for (const mode of ['legacy', 'compat', 'strict'] as const) for (const status of [401, 403]) {
    const safe = messageIdError(new ZulipNoAnswer('not-json', 'POST', '/messages', true, 'private', status, 10), new MessageIds('alpha', mode));
    assert.match(safe.message, /host operator must repair authorization/);
    assert.match(safe.message, /Server Retry-After: 10 seconds; access must be corrected first/);
    assert.doesNotMatch(safe.message, / Retry after/);
  }
});
