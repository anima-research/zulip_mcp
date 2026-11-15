import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MessageIds, idToolDefinitions } from '../src/message-ids.ts';
import { ZulipToolRuntime, formatHistoryLines, formatMessages } from '../src/tool-runtime.ts';
import { normalizeMessage } from '../src/history.ts';
import { toolDefinitions } from '../src/tools.ts';
import { standInApi } from './zulip-stand-in.ts';
import { validateMessageLinks } from '../src/message-links.ts';
import { initializeZulipClient } from '../src/zulip-client.ts';

const message = { id: 7, type: 'stream', stream_id: 12, display_recipient: 'general', subject: 'topic',
  content: 'hello', sender_id: 9, sender_full_name: 'Test User', sender_email: 'user@example.invalid', timestamp: 1700000000 };

test('realm modes validate configuration and positive safe IDs', () => {
  assert.equal(MessageIds.fromEnv('https://alpha.example.invalid', { ZULIP_ID_MODE: 'strict' }).tag, 'alpha');
  for (const mode of ['compat', 'strict'] as const) {
    const ids = new MessageIds('alpha', mode);
    assert.equal(ids.parse('alpha:7'), 7);
    assert.equal(ids.format(7), 'alpha:7');
    for (const v of ['beta:7', 'alpha:0', 'alpha:9007199254740992', ' alpha:7']) assert.throws(() => ids.parse(v));
    let warnings = 0;
    if (mode === 'strict') assert.throws(() => ids.parse(7));
    else { assert.equal(ids.parse(7, () => warnings++), 7); assert.equal(warnings, 1); }
  }
  assert.throws(() => new MessageIds('BAD'));
  assert.throws(() => new MessageIds('alpha', 'bad' as any));
});

test('foreign and strict bare fields and tokens fail before any API or write', async t => {
  for (const mode of ['compat', 'strict'] as const) {
    const { api, calls } = standInApi({});
    const dir = mkdtempSync(join(tmpdir(), 'realm-ids-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const tools = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir, { messageIds: new MessageIds('alpha', mode) });
    for (const [name, field] of [['send_message', 'reply_to'], ['edit_message', 'message_id'], ['delete_message', 'message_id'], ['add_reaction', 'message_id'], ['remove_reaction', 'message_id'], ['fetch_around', 'message_id'], ['fetch_history', 'before'], ['fetch_history', 'after']]) {
      await assert.rejects(tools.handleToolCall(name, { [field]: 'beta:7', content: 'hello' }), /differs/);
      if (mode === 'strict') await assert.rejects(tools.handleToolCall(name, { [field]: 7 }), /bare/);
    }
    await assert.rejects(tools.handleToolCall('send_message', { reply_to: 'alpha:7', content: '[[msg:alpha:7]] [[msg:beta:8]]' }), /differs/);
    assert.equal(calls.length, 0);
  }
});

test('realm projection tags only message IDs; numeric API and persisted state stay unchanged', async t => {
  const { api, calls } = standInApi({
    'GET /messages/:id': { result: 'success', message },
    'POST /messages': { result: 'success', id: 8 },
  });
  const dir = mkdtempSync(join(tmpdir(), 'realm-ids-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const ids = new MessageIds('alpha', 'strict');
  const tools = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir, { messageIds: ids });
  const result = await tools.handleToolCall('send_message', { reply_to: 'alpha:7', content: '[[msg:alpha:7]]' });
  assert.equal(result.id, 'alpha:8');
  assert.equal(calls[0].path, '/messages/7');
  assert.equal(calls.at(-1)?.params.to, 12);
  assert.match(formatHistoryLines([normalizeMessage(message as any)], undefined, undefined, 1, () => '', ids), /id=alpha:7/);
  assert.match(formatMessages([message], 'raw', ids), /"id": "alpha:7"/);
  assert.deepEqual(ids.project({ message_id: 7, stream_id: 12, user_id: 1, content: '7', max_id: 8 }), { message_id: 'alpha:7', stream_id: 12, user_id: 1, content: '7', max_id: 'alpha:8' });
  const schema = idToolDefinitions(toolDefinitions, ids).find(t => t.name === 'send_message')!.inputSchema.properties.reply_to;
  assert.equal(schema.type, 'string');
  validateMessageLinks('`[[msg:beta:7]]` \\[[msg:literal]]', ids);
});

test('invalid realm config fails before profile, settings or password exchange', async t => {
  let calls = 0;
  t.mock.method(globalThis, 'fetch', async () => { calls++; throw new Error('unexpected request'); });
  await assert.rejects(initializeZulipClient({ ZULIP_REALM: 'https://alpha.example.invalid', ZULIP_EMAIL: 'bot@example.invalid', ZULIP_PASSWORD: 'test', ZULIP_ID_MODE: 'bad' }), /ZULIP_ID_MODE/);
  assert.equal(calls, 0);
});

test('legacy preserves schemas, payload identity and unused realm configuration', () => {
  const ids = MessageIds.fromEnv('not a URL', { ZULIP_REALM_TAG: 'INVALID' });
  assert.equal(ids.mode, 'legacy');
  assert.equal(idToolDefinitions(toolDefinitions, ids), toolDefinitions);
  const payload = { message_id: 7, content: '[[msg:literal]]' };
  assert.equal(ids.project(payload), payload);
  validateMessageLinks(payload.content, ids);
  assert.equal(formatMessages([message], 'raw', ids), JSON.stringify([message], null, 2));
});

test('reaction tools translate scoped IDs to numeric API paths; compat warns on bare IDs', async t => {
  for (const mode of ['strict', 'compat'] as const) {
    const { api, calls } = standInApi({
      'GET /realm/emoji': { result: 'success', emoji: {} },
      'POST /messages/:id/reactions': { result: 'success' },
      'DELETE /messages/:id/reactions': { result: 'success' },
    });
    const dir = mkdtempSync(join(tmpdir(), 'realm-reactions-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir, { messageIds: new MessageIds('alpha', mode) });
    for (const name of ['add_reaction', 'remove_reaction']) {
      const result = await runtime.handleToolCall(name, { message_id: 'alpha:7', emoji_name: 'warning' });
      assert.equal(result.result, 'success');
      assert.equal(calls.at(-1)?.path, '/messages/7/reactions');
    }
    if (mode === 'compat') {
      const result = await runtime.handleToolCall('add_reaction', { message_id: 7, emoji_name: 'warning' });
      assert.match(result.warnings[0], /bare message_id accepted/);
    }
  }
});
