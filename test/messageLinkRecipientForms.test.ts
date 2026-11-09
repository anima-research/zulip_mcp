/** Recipient arrays are atomic; only send_message's string form is a comma list. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { canonicalDestination } from '../src/message-links.ts';

function fixture(t: any) {
  const sent: any[] = [];
  const client = { users: { async retrieve() { return { result: 'success', members: [
    { user_id: 4, full_name: 'Doe, Jane', email: 'jane@example.com' },
    { user_id: 2, full_name: 'Doe', email: 'doe@example.com' },
    { user_id: 3, full_name: 'Jane', email: 'other@example.com' },
  ] }; } }, messages: {
    async getById() { return { result: 'success', message: { id: 101, type: 'private', display_recipient: [{ id: 1 }, { id: 4 }] } }; },
    async send(args: any) { sent.push(args); return { result: 'success', id: 501 }; },
  } };
  const dir = mkdtempSync(join(tmpdir(), 'recipient-forms-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return { sent, tools: new ZulipToolRuntime({ client, realm: 'https://example.invalid', selfUserId: 1, sessionId: 'forms' } as any, dir) };
}
for (const content of ['plain', '[[msg:101]]']) {
  for (const to of [['Doe, Jane'], 'Doe, Jane']) test(`send_dm atomic ${JSON.stringify(to)} / ${content}`, async t => {
    const f = fixture(t);
    await f.tools.handleToolCall('send_dm', { to, content });
    assert.deepEqual(f.sent[0].to, [4]);
  });
  test(`send_message array full name / ${content}`, async t => {
    const f = fixture(t);
    await f.tools.handleToolCall('send_message', { type: 'private', to: ['Doe, Jane'], content });
    assert.deepEqual(f.sent[0].to, content === 'plain' ? ['Doe, Jane'] : [4]);
  });
}
test('canonical private strings split, but arrays never split', async () => {
  const queries: string[] = [];
  const resolve = async (q: string) => { queries.push(q); return queries.length; };
  assert.deepEqual((await canonicalDestination({}, { type: 'private', to: ['Doe, Jane', 'alice@example.com'] }, resolve)).to, [1, 2]);
  assert.deepEqual(queries, ['Doe, Jane', 'alice@example.com']);
  queries.length = 0;
  await canonicalDestination({}, { type: 'private', to: ' doe@example.com, other@example.com ' }, resolve);
  assert.deepEqual(queries, ['doe@example.com', 'other@example.com']);
});
