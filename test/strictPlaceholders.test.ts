/** Strict-schema placeholders must not change query scope or cursor defaults. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { toolDefinitions } from '../src/tools.ts';
import { assertStrictArguments } from './strictArguments.ts';

function fixture() {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-placeholders-'));
  const calls: any[] = [];
  const tools = new ZulipToolRuntime({ sessionId: 'strict', selfUserId: 790, client: { messages: {
    async retrieve(args: any) { calls.push(args); return { result: 'success', messages: [], found_oldest: true, found_newest: true }; },
  } } }, dir);
  return { tools, calls, close: () => rmSync(dir, { recursive: true, force: true }) };
}
for (const cursor of [0, null]) {
  test(`fetch_history before/after ${cursor} keeps newest anchor with strict-shaped arguments`, async t => {
    const f = fixture(); t.after(f.close);
    const args = { channel: 'General', topic: '', limit: 50, before: cursor, after: cursor };
    assertStrictArguments('fetch_history', args);
    await f.tools.handleToolCall('fetch_history', args);
    assert.equal(f.calls[0].anchor, 'newest');
  });
}
for (const channels of [[], null]) {
  test(`get_unread_messages channels ${JSON.stringify(channels)} checks all monitored streams`, async t => {
    const f = fixture(); t.after(f.close);
    await f.tools.handleToolCall('start_monitoring', { channels: ['General'] });
    f.calls.length = 0;
    const args = { channels, format: 'raw', mark_as_read: false };
    assertStrictArguments('get_unread_messages', args);
    const result = await f.tools.handleToolCall('get_unread_messages', args);
    assert.equal(result.channels_checked.length, 1);
    assert.equal(result.channels_checked[0].channel, 'General');
    assert.equal(f.calls.length, 1);
  });
}
test('move_messages schema distinguishes absent topic from general chat and permits null propagation', () => {
  const properties = toolDefinitions.find(t => t.name === 'move_messages')!.inputSchema.properties as any;
  assert.match(properties.topic.description, /[Nn]ull.*unchanged/);
  assert.match(properties.topic.description, /empty string.*unnamed/);
  assert.match(properties.propagate_mode.description, /[Nn]ull.*default/);
  assertStrictArguments('move_messages', { message_id: 123, topic: null, stream_id: 12, propagate_mode: null });
});
test('filters_update schema uses null for absence, never [] as an unused DM allowlist', () => {
  assertStrictArguments('filters_update', { addStreams: ['dev'], removeStreams: null, setDmUsers: null });
  const properties = toolDefinitions.find(t => t.name === 'filters_update')!.inputSchema.properties as any;
  assert.match(properties.setDmUsers.description, /[Nn]ull.*unchanged/);
  assert.match(properties.setDmUsers.description, /empty array means UNRESTRICTED/);
});

test('nullable history and unread defaults work with every property present', async t => {
  const f = fixture(); t.after(f.close);
  const history = { channel: 'General', topic: null, limit: null, before: null, after: null };
  assertStrictArguments('fetch_history', history);
  await f.tools.handleToolCall('fetch_history', history);
  assert.equal(f.calls[0].anchor, 'newest'); assert.equal(f.calls[0].num_before, 50);
  assert.deepEqual(f.calls[0].narrow, [['stream', 'General']]);
  await f.tools.handleToolCall('start_monitoring', { channels: ['General'] });
  const unread = { channels: null, format: null, mark_as_read: null };
  assertStrictArguments('get_unread_messages', unread);
  const result = await f.tools.handleToolCall('get_unread_messages', unread);
  assert.equal(result.channels_checked[0].channel, 'General');
});
