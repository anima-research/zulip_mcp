import test from 'node:test';
import assert from 'node:assert/strict';
import { maxMessageLengthFromEnv } from '../src/index.ts';

test('a ZULIP_MAX_MESSAGE_LENGTH of 0 is ignored at start, with a line saying so', () => {
  const saved = process.env.ZULIP_MAX_MESSAGE_LENGTH;
  const lines: string[] = [];
  const original = console.error;
  console.error = (line: string) => lines.push(line);
  try {
    process.env.ZULIP_MAX_MESSAGE_LENGTH = '0';
    assert.equal(maxMessageLengthFromEnv(), 10000);
    assert.deepEqual(lines, ['[zulip-mcp] ignoring ZULIP_MAX_MESSAGE_LENGTH="0" (not an integer of at least 1); using 10000']);
  } finally {
    console.error = original;
    if (saved === undefined) delete process.env.ZULIP_MAX_MESSAGE_LENGTH;
    else process.env.ZULIP_MAX_MESSAGE_LENGTH = saved;
  }
});
