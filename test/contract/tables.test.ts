import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { prepareOutgoing } from '../../src/outgoing.ts';
import { ZulipToolRuntime } from '../../src/tool-runtime.ts';
import { initializeZulipClient } from '../../src/zulip-client.ts';
import { contractEnv } from './env.ts';
import { apis, newStream } from './zulip.ts';

const env = contractEnv();
const { user } = apis(env);
let tools: ZulipToolRuntime;
let stateDir: string;

before(async () => {
  stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-tables-'));
  tools = new ZulipToolRuntime(await initializeZulipClient(process.env), stateDir);
});

after(() => rmSync(stateDir, { recursive: true, force: true }));

async function tables(content: string): Promise<number> {
  const { rendered } = await user.ok('POST', 'messages/render', { content });
  return (rendered as string).split('<table>').length - 1;
}

const table = '| a | b |\n| --- | --- |\n| 1 | 2 |';

const cases: [string, string, number, number][] = [
  ['a table directly under a line of text', `Results:\n${table}`, 0, 1],
  ['a line of text directly under a table', `${table}\nAfter the table.`, 1, 1],
  ['a pipe line over a rule', 'a | b\n---', 0, 0],
  ['a table under text inside a code fence', `\`\`\`\nResults:\n${table}\n\`\`\``, 0, 0],
  ['a header without border pipes over a delimiter with them', 'Results:\na | b\n|---|---|\n1 | 2', 0, 0],
  ['one column, then a line of text', 'Results:\n| a |\n| --- |\n| 1 |\nmore', 0, 0],
  ['a delimiter cell that is a lone colon', 'Results:\n| a | b |\n| : | - |', 0, 1],
  ['a blank delimiter cell', 'Results:\n| a | b |\n|   | --- |', 0, 1],
  ['a pipe inside a code span in the header', 'Results:\n`x|y` | b\n---|---', 0, 1],
  ['a delimiter row five spaces deep', 'Results:\na | b\n     ---|---', 0, 1],
  ['a table directly under a hash heading', `# Results\n${table}`, 1, 1],
  ['a table directly under a rule', `Before.\n---\n${table}`, 1, 1],
  ['a table directly under a list item', `- item\n${table}`, 0, 0],
  ['a table directly under a quoted line', `> quote\n${table}`, 0, 0],
];

for (const [name, text, asWritten, asPosted] of cases) {
  test(`Zulip shows ${asWritten} table(s) for ${name} as written, and ${asPosted} as the server posts it`, async () => {
    assert.equal(await tables(text), asWritten, 'as written');
    assert.equal(await tables(prepareOutgoing(text)), asPosted, 'as posted');
  });
}

test('with a blank line added, a table under a list item or a quoted line would leave the list or the quote, which is why the server adds none', async () => {
  assert.equal(await tables(`- item\n\n${table}`), 1);
  assert.equal(await tables(`> quote\n\n${table}`), 1);
});

test('send_message posts a table under a line of text with the blank line, and Zulip shows a table', async () => {
  const stream = await newStream(env, 'tables');
  const sent = (await tools.handleToolCall('send_message', { type: 'stream', to: stream, topic: 'a table', content: `Results:\n${table}` })) as { id: number };
  assert.equal((await user.message(sent.id)).content, `Results:\n\n${table}`);
  const { message } = await user.ok('GET', `messages/${sent.id}`, { apply_markdown: true });
  assert.match(message.content, /<table>/);
});
