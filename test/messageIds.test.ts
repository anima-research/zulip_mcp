import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { MessageIds, idToolDefinitions, messageIdServerInfo } from '../src/message-ids.ts';
import { ZulipToolRuntime, formatHistoryLines, formatMessages } from '../src/tool-runtime.ts';
import { normalizeMessage } from '../src/history.ts';
import { toolDefinitions } from '../src/tools.ts';
import { standInApi } from './zulip-stand-in.ts';
import { validateMessageLinks, linkMessages } from '../src/message-links.ts';
import { prepareOutgoing } from '../src/outgoing.ts';
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
    let writes = 0;
    const tools = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir, {
      messageIds: new MessageIds('alpha', mode),
      uploader: { async upload() { writes++; throw new Error('unexpected upload'); } },
    });
    tools.onSent = () => { writes++; };
    for (const [name, field] of [['send_message', 'reply_to'], ['edit_message', 'message_id'], ['move_messages', 'message_id'], ['delete_message', 'message_id'], ['add_reaction', 'message_id'], ['remove_reaction', 'message_id'], ['fetch_around', 'message_id'], ['fetch_history', 'before'], ['fetch_history', 'after']]) {
      await assert.rejects(tools.handleToolCall(name, { [field]: 'beta:7', content: 'hello' }), /differs/);
      if (mode === 'strict') await assert.rejects(tools.handleToolCall(name, { [field]: 7 }), /bare/);
    }
    await assert.rejects(tools.handleToolCall('send_message', { reply_to: 'alpha:7', content: '[[msg:alpha:7]] [[msg:beta:8]]', attachments: [{ name: 'a.txt', data: 'YQ==', mimeType: 'text/plain' }] }), /differs/);
    assert.equal(calls.length, 0);
    assert.equal(writes, 0);
  }
});

test('realm projection tags only message IDs; numeric API paths stay unchanged', async t => {
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
  await assert.rejects(initializeZulipClient({ ZULIP_REALM: 'file:///tmp/zulip', ZULIP_EMAIL: 'bot@example.invalid', ZULIP_PASSWORD: 'test', ZULIP_ID_MODE: 'strict' }), /hostname/);
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

test('projection preserves sentinels and stream-name maps, includes first_message_id', () => {
  const ids = new MessageIds('alpha', 'strict');
  assert.deepEqual(ids.project({ last_message_id: 0, first_message_id: 7, stream_ids: { max_id: 12, message_id: 13 } }),
    { last_message_id: 0, first_message_id: 'alpha:7', stream_ids: { max_id: 12, message_id: 13 } });
});

test('invalid bare IDs do not warn; errors identify field and value', () => {
  let warned = false;
  assert.throws(() => new MessageIds('alpha', 'compat').parse('oops', () => { warned = true; }, 'before'), /before="oops".*Nothing was written/);
  assert.equal(warned, false);
  assert.throws(() => new MessageIds('alpha', 'strict').parse(7, undefined, 'before'), /before=7.*bare/);
});

test('enabled link expansion preserves escaped literals and as-typed invalid tokens', async () => {
  const { api, calls } = standInApi({ 'GET /messages/:id': { result: 'success', message } });
  const audience = async () => ({ kind: 'stream' as const, id: 12 });
  for (const mode of ['compat', 'strict'] as const) {
    const ids = new MessageIds('alpha', mode);
    const literal = String.raw`\[[msg:alpha:7]] \[[msg:literal]]`;
    assert.equal(await linkMessages(api, literal, audience, 'refuse', ids), literal);
    const invalid = '[[msg:beta:7]] [[msg:not-an-id]]' + (mode === 'strict' ? ' [[msg:7]]' : '');
    assert.equal(await linkMessages(api, invalid, audience, 'as-typed', ids), invalid);
    assert.equal(calls.length, 0);
    assert.throws(() => validateMessageLinks(String.raw`\[[msg:literal [[msg:beta:7]]`, ids), /differs/);
  }
  const nested = String.raw`\[[msg:literal [[msg:7]]`;
  let warned = false;
  validateMessageLinks(nested, new MessageIds('alpha', 'compat'), () => { warned = true; });
  assert.equal(warned, true);
  const expanded = await linkMessages(api, nested, audience, 'refuse', new MessageIds('alpha', 'compat'));
  assert.equal(expanded, String.raw`\[[msg:literal [#general > topic @ 💬](#narrow/channel/12-general/topic/topic/near/7)`);
});

test('normalized Markdown validates before reply lookup; optional null IDs are omitted', async t => {
  const { api, calls } = standInApi({
    'POST /messages': { result: 'success', id: 8 },
    'GET /messages': { result: 'success', messages: [] },
  });
  const dir = mkdtempSync(join(tmpdir(), 'realm-null-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  for (const mode of ['compat', 'strict'] as const) {
    const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir, { messageIds: new MessageIds('alpha', mode) });
    const content = 'intro `\n| [[msg:beta:7]] |\n| --- |\n| end ` |';
    assert.throws(() => validateMessageLinks(prepareOutgoing(content), new MessageIds('alpha', mode)), /differs/);
    const before = calls.length;
    await assert.rejects(runtime.handleToolCall('send_message', { reply_to: 'alpha:7', content }), /differs/);
    assert.equal(calls.length, before);
    await runtime.handleToolCall('send_message', { type: 'stream', to: 12, topic: 'topic', reply_to: null, content: 'hello' });
    await runtime.handleToolCall('fetch_history', { channel: 'dm:9', before: null, after: null });
  }
});

test('monitor cursors persist numeric and reload across enabled modes', async t => {
  const { api } = standInApi({ 'GET /messages': { result: 'success', messages: [message] } });
  const dir = mkdtempSync(join(tmpdir(), 'realm-state-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const session = { api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' };
  const runtime = new ZulipToolRuntime(session, dir, { messageIds: new MessageIds('alpha', 'strict') });
  await runtime.handleToolCall('start_monitoring', { channels: ['general'] });
  const state = JSON.parse(readFileSync(join(dir, 'test.json'), 'utf8'));
  assert.equal(state.monitoredChannels.general.lastReadMessageId, 7);
  const restarted = new ZulipToolRuntime(session, dir, { messageIds: new MessageIds('beta', 'compat') });
  assert.equal((restarted as any).monitoredChannels.get('general').lastReadMessageId, 7);
});

test('strict invalid IDs recommend the usable tagged form', () => {
  for (const value of [7, '7', 0, 'oops', 'alpha:0', 'alpha:bad']) {
    assert.throws(() => new MessageIds('alpha', 'strict').parse(value), /use alpha:<n>/);
  }
});

test('enabled configuration identifies invalid mode and realm', () => {
  assert.throws(() => MessageIds.fromEnv('bad realm', { ZULIP_ID_MODE: 'bad' }), /ZULIP_ID_MODE/);
  assert.throws(() => MessageIds.fromEnv('bad realm', { ZULIP_ID_MODE: 'strict' }), /Zulip realm.*URL/);
  assert.throws(() => MessageIds.fromEnv('file:///tmp/zulip', { ZULIP_ID_MODE: 'strict' }), /hostname/);
  assert.equal(MessageIds.fromEnv('bad realm', { ZULIP_ID_MODE: 'strict', ZULIP_REALM_TAG: 'alpha' }).tag, 'alpha');
});

test('projection preserves opaque serialization and existing handshake metadata', () => {
  const ids = new MessageIds('alpha', 'strict');
  const date = new Date('2024-01-01T00:00:00Z');
  class Metadata { toJSON() { return 'metadata'; } }
  const input = { message_id: 7, date, custom: new Metadata() };
  assert.equal(ids.project(input).date, date);
  assert.equal(ids.project(input).custom, input.custom);
  for (const value of [new Number(7), Object.create(null), date]) {
    assert.equal(ids.project({ message_id: value }).message_id, value);
    assert.equal(ids.project({ message_ids: [value] }).message_ids[0], value);
  }
  assert.equal(JSON.stringify(ids.project(input)), JSON.stringify({ ...input, message_id: 'alpha:7' }));
  assert.deepEqual(messageIdServerInfo({ name: 'test', version: '1', _meta: { other: true } }, ids)._meta,
    { other: true, 'zulip_mcp/messageIds': { version: 1, mode: 'strict', realmTag: 'alpha' } });
});

test('malformed bracket tokens are rejected before tool side effects', async t => {
  const { api, calls } = standInApi({});
  const dir = mkdtempSync(join(tmpdir(), 'realm-brackets-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir,
    { messageIds: new MessageIds('alpha', 'strict') });
  for (const content of ['[[msg:other:7[bad]]]', '[[msg:alpha:7]bad]]']) {
    await assert.rejects(runtime.handleToolCall('send_message', { to: 12, type: 'stream', topic: 'topic', content }), /Nothing was written/);
  }
  assert.equal(calls.length, 0);
});

test('compat warnings identify every bare field and survive refusals', async t => {
  const { api } = standInApi({
    'GET /messages/:id': { result: 'success', message },
    'POST /messages': { result: 'success', id: 8 },
    'GET /messages': { result: 'success', messages: [] },
    'GET /realm/emoji': { result: 'success', emoji: {} },
    'POST /messages/:id/reactions': { result: 'error', msg: 'reaction refused' },
  });
  const dir = mkdtempSync(join(tmpdir(), 'realm-warnings-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir,
    { messageIds: new MessageIds('alpha', 'compat') });
  for (const [name, args, field] of [
    ['send_message', { reply_to: 7, content: 'hello' }, 'reply_to'],
    ['fetch_history', { channel: 'dm:9', before: 7 }, 'before'],
    ['fetch_history', { channel: 'dm:9', after: 7 }, 'after'],
    ['send_message', { reply_to: 'alpha:7', content: '[[msg:7]]' }, 'message-link token'],
  ] as const) {
    const result = await runtime.handleToolCall(name, args);
    assert.ok(result.warnings.some((w: string) => w.includes(field)), field);
  }
  const both = await runtime.handleToolCall('send_message', { reply_to: 7, content: '[[msg:7]]' });
  for (const field of ['reply_to', 'message-link token']) assert.ok(both.warnings.some((w: string) => w.includes(field)));
  await assert.rejects(runtime.handleToolCall('send_message', { reply_to: 7, content: '[[msg:7]] [[msg:beta:8]]' }), (err: Error) => {
    for (const field of ['reply_to', 'message-link token']) assert.ok(err.message.includes('bare ' + field));
    return true;
  });
  await assert.rejects(runtime.handleToolCall('add_reaction', { message_id: 7, emoji_name: 'warning' }),
    /reaction refused[\s\S]*Warning: bare message_id accepted/);
});

test('enabled transport errors retain tagged operation context and uncertainty', async t => {
  const { ZulipNoAnswer, zulipApi, mayHaveBeenApplied } = await import('../src/zulip-api.ts');
  const error = new ZulipNoAnswer('timeout', 'PATCH', '/messages/7', true, 'PATCH /messages/7 timed out.');
  const api = zulipApi(async () => { throw error; });
  const dir = mkdtempSync(join(tmpdir(), 'realm-transport-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir,
    { messageIds: new MessageIds('alpha', 'strict') });
  await assert.rejects(runtime.handleToolCall('edit_message', { message_id: 'alpha:7', content: 'hello' }), err => {
    assert.notEqual(err, error);
    assert.ok(err instanceof ZulipNoAnswer);
    assert.equal(mayHaveBeenApplied(err), true);
    assert.match((err as Error).message, /edit_message \(message_id=alpha:7\)/);
    assert.match((err as Error).message, /check before sending it again/);
    return true;
  });
  assert.doesNotMatch(error.message, /edit_message/);
  await assert.rejects(runtime.handleToolCall('edit_message', { message_id: 'alpha:8', content: 'hello' }), (err: Error) => {
    assert.match(err.message, /message_id=alpha:8/);
    assert.doesNotMatch(err.message, /message_id=alpha:7/);
    return true;
  });
});

test('move_messages read-back timeout retains success and adds context only in enabled modes', async t => {
  const { ZulipNoAnswer, zulipApi } = await import('../src/zulip-api.ts');
  for (const mode of ['legacy', 'compat', 'strict'] as const) {
    const error = new ZulipNoAnswer('timeout', 'GET', '/messages/7', false, 'GET /messages/7 timed out.');
    const calls: string[] = [];
    const api = zulipApi(async (method, path) => {
      calls.push(`${method} ${path}`);
      if (method === 'GET') throw error;
      return { status: 200, body: { result: 'success' } };
    });
    const dir = mkdtempSync(join(tmpdir(), 'move-timeout-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir,
      { messageIds: new MessageIds('alpha', mode) });
    const result = await runtime.handleToolCall('move_messages', {
      message_id: mode === 'legacy' ? 7 : 'alpha:7', topic: 'new', scope: 'one',
    });
    assert.deepEqual(result, {
      moved: true,
      asked: { topic: 'new', scope: 'one' },
      readBackError: mode === 'legacy' ? error.message : `move_messages (message_id=alpha:7): ${error.message}`,
      note: 'To answer this message where it is now, use send_message with reply_to.',
    });
    assert.match(result.readBackError, /GET \/messages\/7 timed out\./);
    assert.deepEqual(calls, ['PATCH /messages/7', 'GET /messages/7']);
  }
});

test('reused transport errors do not leak compat warnings into tagged calls', async t => {
  const { ZulipNoAnswer, zulipApi } = await import('../src/zulip-api.ts');
  const error = new ZulipNoAnswer('timeout', 'PATCH', '/messages/7', true, 'timeout');
  const original = error.message;
  const dir = mkdtempSync(join(tmpdir(), 'shared-error-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const runtime = new ZulipToolRuntime({ api: zulipApi(async () => { throw error; }), selfUserId: 1,
    realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir,
    { messageIds: new MessageIds('alpha', 'compat') });
  await assert.rejects(runtime.handleToolCall('edit_message', { message_id: 7, content: 'hello' }), /Warning: bare message_id/);
  await assert.rejects(runtime.handleToolCall('edit_message', { message_id: 'alpha:8', content: 'hello' }), (err: Error) => {
    assert.match(err.message, /message_id=alpha:8/);
    assert.doesNotMatch(err.message, /Warning:|alpha:7/);
    return true;
  });
  assert.equal(error.message, original);
});

test('malformed move read-back names the operation only in enabled modes', async t => {
  for (const mode of ['legacy', 'strict', 'compat'] as const) {
    const { api } = standInApi({
      'PATCH /messages/:id': { result: 'success' },
      'GET /messages/:id': { result: 'success', message: {} },
    });
    const dir = mkdtempSync(join(tmpdir(), 'move-malformed-'));
    t.after(() => rmSync(dir, { recursive: true, force: true }));
    const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example.invalid', authHeader: '', sessionId: 'test' }, dir,
      { messageIds: new MessageIds('alpha', mode) });
    const result = await runtime.handleToolCall('move_messages', { message_id: mode === 'legacy' ? 7 : 'alpha:7', topic: 'new', scope: 'one' });
    assert.equal(result.moved, true);
    assert.equal(result.readBackError, (mode === 'legacy' ? '' : 'move_messages: ') +
      "Zulip's answer gave no stream and topic for message " + (mode === 'legacy' ? '7' : 'alpha:7') + '.');
  }
});
