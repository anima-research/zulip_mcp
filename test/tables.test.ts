import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { prepareOutgoing } from '../src/outgoing.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import type { ZulipSession } from '../src/zulip-client.ts';
import type { UploadPolicy } from '../src/uploads.ts';
import { standInApi, type StandInCall } from './zulip-stand-in.ts';

const table = '| a | b |\n| --- | --- |\n| 1 | 2 |';
const underText = `Results:\n${table}`;
const repaired = `Results:\n\n${table}`;

test('a table directly under a line of text gets one blank line above its header row, and nothing else changes', () => {
  assert.equal(prepareOutgoing(underText), repaired);
  assert.equal(prepareOutgoing(`Before.\nResults:\n${table}\nAfter the table.`), `Before.\nResults:\n\n${table}\nAfter the table.`);
  assert.equal(
    prepareOutgoing(`Intro:\n${table}\n\nMore:\n| c | d |\n|---|---|`),
    `Intro:\n\n${table}\n\nMore:\n\n| c | d |\n|---|---|`,
    'each such table in a message',
  );
});

test('the tables that the parser Zulip pins takes as tables are repaired, as a description of rows would miss', () => {
  for (const rows of [
    '| a | b |\n| : | - |',
    '| a | b |\n|   | --- |',
    '`x|y` | b\n---|---',
    'a | b\n     ---|---',
  ]) assert.equal(prepareOutgoing(`Results:\n${rows}`), `Results:\n\n${rows}`, rows);
});

test('a message with no table under a line of text is posted byte for byte', () => {
  for (const text of [
    `Results:\n\n${table}`,
    table,
    `${table}\nAfter the table.`,
    `${table}\n\nAfter the table.`,
    'a | b\n---',
    `Results:\na | b\n|---|---|\n1 | 2`,
    `Results:\n| a |\n| --- |\n| 1 |\nmore`,
    `Results:\n    | a | b |\n    | --- | --- |`,
    `Results:\n\t| a | b |\n\t| --- | --- |`,
    `| a | b |\n|---|---|\n| x | y |\n|---|---|\n| 1 | 2 |`,
    'plain text\nwith two lines',
    '',
  ]) assert.equal(prepareOutgoing(text), text, text);
});

test('a table directly under a hash heading or a horizontal rule is posted byte for byte, since Zulip shows it already', () => {
  for (const text of [
    `# Results\n${table}`,
    `Before.\n## Results\n${table}`,
    `Before.\n---\n${table}`,
    `Before.\n* * *\n${table}`,
  ]) assert.equal(prepareOutgoing(text), text, text);
});

test('the lines that Zulip splits off a block, at a heading, a rule or the end of an indented start, are judged as their own block', () => {
  assert.equal(prepareOutgoing(`Results:\n| a |\n| --- |\n---`), `Results:\n\n| a |\n| --- |\n---`, 'a one-column table that a rule ends');
  assert.equal(prepareOutgoing(`Results:\n| a |\n| --- |\n# Next`), `Results:\n\n| a |\n| --- |\n# Next`, 'a one-column table that a heading ends');
  assert.equal(prepareOutgoing(`    code\nResults:\n${table}`), `    code\nResults:\n\n${table}`, 'after an indented start');
  assert.equal(
    prepareOutgoing('Results:\n    | a | b |\n|---|---|\n| : | - |'),
    'Results:\n    | a | b |\n\n|---|---|\n| : | - |',
    'not above a line indented four, which would then be code, but above the table that follows',
  );
  assert.equal(prepareOutgoing(`${table}\n---\nMore:\n| c | d |\n|---|---|`), `${table}\n---\nMore:\n| c | d |\n|---|---|`, 'a block that is a table from its first line keeps its rule and the rows after it');
});

test('the blank line goes in only where Zulip then shows more tables, and every such place gets one', () => {
  const absorbs = 'Results:\n| a | b |\n| : | - |\n* * *\n| c | d |\n|---|---|';
  assert.equal(prepareOutgoing(absorbs), absorbs, 'a table above a rule would take the rule and the table after it as rows');
  const afterRule = 'x\n---\n| 1 | 2 |\n---|---\n1 | 2\n| 3 |\n| --- |';
  assert.equal(prepareOutgoing(afterRule), afterRule, 'a table that opens after a rule keeps its rows');
  assert.equal(
    prepareOutgoing('plain words here\n| 3 |\n| --- |\n| 1 | 2 |\n:-|-:\n| a | b |'),
    'plain words here\n\n| 3 |\n| --- |\n\n| 1 | 2 |\n:-|-:\n| a | b |',
    'a one-column table that a cut below it makes whole',
  );
});

test('nothing inside a code fence is read, and a fence closes only at a line equal to its opening', () => {
  for (const text of [
    `\`\`\`\n${underText}\n\`\`\``,
    `~~~\n${underText}\n~~~`,
    `\`\`\`\n${underText}`,
    `\`\`\`\n\`\`\`js\n${underText}\n\`\`\``,
    `\`\`\`\`\n\`\`\`\n${underText}\n\`\`\`\``,
    `\`\`\`quote\n~~~\n\`\`\`\n${underText}\n~~~\n\`\`\``,
  ]) assert.equal(prepareOutgoing(text), text, text);
  assert.equal(prepareOutgoing(`\`\`\`\ncode\n\`\`\`\n${underText}`), `\`\`\`\ncode\n\`\`\`\n${repaired}`, 'after the fence closes, the rule reads again');
});

test('inside a quote, quoted or spoiler fence, which Zulip reads as Markdown, a table under a line of text gets the blank line', () => {
  for (const [open, close] of [['```quote', '```'], ['```quoted', '```'], ['~~~ quote', '~~~'], ['```spoiler Results', '```'], ['````quote', '````']]) {
    assert.equal(prepareOutgoing(`${open}\n${underText}\n${close}`), `${open}\n${repaired}\n${close}`, open);
  }
  assert.equal(prepareOutgoing(`\`\`\`\`quote\n\`\`\`spoiler\n${underText}\n\`\`\`\n\`\`\`\``), `\`\`\`\`quote\n\`\`\`spoiler\n${repaired}\n\`\`\`\n\`\`\`\``, 'a spoiler inside a quote');
  for (const text of [`\`\`\`quote\n${table}\n\`\`\``, `Results:\n\`\`\`quote\n${table}\n\`\`\``, `\`\`\`quote\nResults:\n\`\`\`\n${table}`]) {
    assert.equal(prepareOutgoing(text), text, `a fence's own lines end the block above them: ${text}`);
  }
});

test('a table directly under a list item or a quoted line is left as it is', () => {
  for (const above of ['- item', '* item', '+ item', '1. item', '2) item', '> quote']) {
    for (const text of [`${above}\n${table}`, `${above}\nmore\n${table}`]) assert.equal(prepareOutgoing(text), text, text);
  }
});

const uploader = {
  async upload(i: { name: string }) {
    return { name: i.name, path: `/user_uploads/1/ab/${i.name}`, url: `https://z/user_uploads/1/ab/${i.name}` };
  },
};
const uploadPolicy: UploadPolicy = { roots: new Map(), maxBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };

function tools(api: ZulipSession['api'], maxMessageLength?: number) {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-tables-'));
  const session: ZulipSession = { api, selfUserId: 790, realm: 'https://z', authHeader: 'Basic x', sessionId: 't' };
  const original = console.error;
  console.error = () => {};
  try {
    const runtime = new ZulipToolRuntime(session, dir, { uploader, uploadPolicy, ...(maxMessageLength ? { maxMessageLength } : {}) } as never);
    return { runtime, done: () => rmSync(dir, { recursive: true, force: true }) };
  } finally {
    console.error = original;
  }
}

const posted = (calls: StandInCall[], method: string) => calls.filter((c) => c.method === method).map((c) => c.params.content);

test('send_message, send_dm and a plain answer each post the table with the blank line above it', async () => {
  const sm = standInApi({ 'POST /messages': { result: 'success', msg: '', id: 901 } });
  const t1 = tools(sm.api);
  await t1.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: underText });
  t1.done();
  const dm = standInApi({ 'POST /messages': { result: 'success', msg: '', id: 901 } });
  const t2 = tools(dm.api);
  await t2.runtime.handleToolCall('send_dm', { to: ['42'], content: underText });
  t2.done();
  const pub = standInApi({ 'POST /messages': { result: 'success', msg: '', id: 901 } });
  await new ZulipAdapter(pub.api, 790, 's', {} as never).publish('zulip:general', undefined, [{ type: 'text', text: underText }], { threadId: 't' });
  assert.deepEqual(posted(sm.calls, 'POST'), [repaired], 'send_message');
  assert.deepEqual(posted(dm.calls, 'POST'), [repaired], 'send_dm');
  assert.deepEqual(posted(pub.calls, 'POST'), [repaired], 'plain answer');
});

test('with an upload, the links come after the prepared text, untouched', async () => {
  const { api, calls } = standInApi({ 'POST /messages': { result: 'success', msg: '', id: 901 } });
  const t = tools(api);
  await t.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: underText, attachments: [{ data: 'aGk=', name: 'hi.txt' }] });
  t.done();
  assert.deepEqual(posted(calls, 'POST'), [`${repaired}\n\n[hi.txt](/user_uploads/1/ab/hi.txt)`]);
});

test('a send at exactly the length goes out as two messages, since the added line counts', async () => {
  const { api, calls } = standInApi({ 'POST /messages': { result: 'success', msg: '', id: 901 } });
  const t = tools(api, [...underText].length);
  await t.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: underText });
  t.done();
  assert.deepEqual(posted(calls, 'POST'), ['Results:', table]);
});

test('edit_message prepares its text before its length check', async () => {
  const z = standInApi({ 'PATCH /messages/:id': { result: 'success', msg: '' } });
  const t = tools(z.api);
  await t.runtime.handleToolCall('edit_message', { message_id: 55, content: underText });
  t.done();
  assert.deepEqual(posted(z.calls, 'PATCH'), [repaired]);

  const limit = [...underText].length;
  const refusing = standInApi({ 'PATCH /messages/:id': { result: 'success', msg: '' } });
  const tight = tools(refusing.api, limit);
  await assert.rejects(tight.runtime.handleToolCall('edit_message', { message_id: 55, content: underText }), new RegExp(`content is ${limit + 1} characters`));
  tight.done();
  assert.equal(refusing.calls.length, 0, 'Zulip is not asked');
});
