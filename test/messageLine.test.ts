import test from 'node:test';
import assert from 'node:assert/strict';
import { headerField, messageLineHead } from '../src/message-line.ts';
import { formatHistoryLines, utcLineTime } from '../src/tool-runtime.ts';
import type { ZulipMessage } from '../src/history.ts';

test('messageLineHead: stream, DM, mention mark on streams only, empty time keeps the id', () => {
  const base = { id: 7, time: 'T', stream: 'qa', topic: 'router', author: 'Ann', mentioned: false };
  assert.equal(messageLineHead(base), '[T id=7] [#qa > router] Ann: ');
  assert.equal(messageLineHead({ ...base, mentioned: true }), '[T id=7] [#qa > router] Ann (mention): ');
  assert.equal(messageLineHead({ ...base, stream: null, mentioned: true }), '[T id=7] [DM] Ann: ', 'a DM is addressed by definition');
  assert.equal(messageLineHead({ ...base, time: '' }), '[id=7] [#qa > router] Ann: ');
  assert.equal(messageLineHead({ ...base, topic: '' }), '[T id=7] [#qa > ] Ann: ');
});

test('header fields are folded onto one line; the body is not the header\'s business', () => {
  const newline = String.fromCharCode(10);
  const tab = String.fromCharCode(9);
  const lineSeparator = String.fromCharCode(0x2028);
  assert.equal(headerField(`a${newline}${newline}b${tab}c${lineSeparator}d`), 'a b c d');
  assert.equal(headerField('plain ]: kept'), 'plain ]: kept', 'not escaped, only folded');
  const head = messageLineHead({ id: 1, time: `T${newline}`, stream: `s${newline}x`, topic: `t${newline}y`, author: `A${newline}B`, mentioned: false });
  assert.equal(head.includes(newline), false);
  assert.equal(head, '[T  id=1] [#s x > t y] A B: ');
});

function zmsg(over: Partial<ZulipMessage>): ZulipMessage {
  return {
    id: 1, streamName: 'qa', topic: 'router', isDm: false, recipients: [], authorId: 9, authorName: 'Ann',
    authorEmail: 'ann@example.com', timestamp: new Date('2026-09-14T08:42:52.123Z'), rawContent: 'hi', cleanContent: 'hi',
    mentioned: false, wildcardMentioned: false, attachments: [], reactions: [],
    ...over,
  } as ZulipMessage;
}

test('fetch_history lines use the same head as live delivery: agent time when given, UTC otherwise, no mention mark on DMs', () => {
  const lines = formatHistoryLines(
    [zmsg({ id: 1, mentioned: true }), zmsg({ id: 2, isDm: true, streamName: null, topic: '', mentioned: true, authorName: 'Bo', cleanContent: 'yo' })],
    2,
    undefined,
    null,
    () => 'T',
  ).split(String.fromCharCode(10));
  assert.deepEqual(lines, ['[T id=1] [#qa > router] Ann (mention): hi', '[T id=2] [DM] Bo: yo <<']);
  assert.equal(formatHistoryLines([zmsg({})]), '[2026-09-14T08:42:52Z id=1] [#qa > router] Ann: hi', 'direct callers without a formatter get UTC seconds');
  assert.equal(utcLineTime(new Date('nope')), '');
});
