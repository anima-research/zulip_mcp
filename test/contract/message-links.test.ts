import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../../src/tool-runtime.ts';
import { initializeZulipClient } from '../../src/zulip-client.ts';
import { contractEnv } from './env.ts';
import { apis, newStream, uniqueName } from './zulip.ts';

const env = contractEnv();
const { user, bot } = apis(env);
let tools: ZulipToolRuntime;
let stateDir: string;

before(async () => {
  const session = await initializeZulipClient(process.env);
  stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-links-'));
  tools = new ZulipToolRuntime(session, stateDir);
});

after(() => rmSync(stateDir, { recursive: true, force: true }));

const post = async (to: string, topic: string, content: string): Promise<number> => (await user.ok('POST', 'messages', { type: 'stream', to, topic, content })).id;
const rendered = async (id: number): Promise<string> => (await user.ok('GET', `messages/${id}`, { apply_markdown: true })).message.content;
const linksTo = (html: string, id: number) => new RegExp(`<a href="[^"]*/near/${id}"`).test(html);
const send = async (to: string, content: string): Promise<number> =>
  ((await tools.handleToolCall('send_message', { type: 'stream', to, topic: 'refs', content })) as { id: number }).id;

test('a link from the server shows as a link to its message, for plain names, names with special characters and the empty topic', async (t) => {
  const plain = await newStream(env, 'links');
  const special = await newStream(env, 'l&[k]*`>');
  const ids = [
    await post(plain, 'plain', 'pointed at'),
    await post(plain, 'a `b` > *c* & [d] $$e', 'pointed at'),
    await post(special, 't', 'pointed at'),
    await post(plain, '', 'pointed at'),
  ];
  const html = await rendered(await send(plain, ids.map((id) => `[[msg:${id}]]`).join('\n\n')));
  t.diagnostic(html);
  for (const id of ids) assert.ok(linksTo(html, id), `a link to ${id}`);
  assert.ok(!html.includes('[[msg:'), 'no token left');
});

const PLACES: [string, string][] = [
  ['a paragraph', 'see {T} here'],
  ['a code fence', '```\n{T}\n```'],
  ['a tilde fence', '~~~\n{T}\n~~~'],
  ['a math fence', '```math\n{T}\n```'],
  ['a code fence inside a quote fence', '```quote\nquoted\n```python\n{T}\n```\n```'],
  ['a fence never closed', '```\nnever closed\n{T}'],
  ['inline code', 'inline `{T}` code'],
  ['a span of two backticks', 'two ``a ` {T}`` ticks'],
  ['a span across a line break', 'a span `that crosses\n{T} a line` break'],
  ['a quote fence', '```quote\nas {T} said\n```'],
  ['a spoiler fence', '```spoiler Plans\nas {T} said\n```'],
  ["past a heading's backtick", '# a `b\n{T} c` d'],
  ["past another table row's backtick", '| `a | b |\n|---|---|\n| {T} | c` |'],
  ["past another list item's backtick", '- a `b\n- {T} c` d'],
  ["past an indented start's backtick", '    code `x\n{T} y`'],
  ['past a rule', 'a `b\n***\n{T} c`'],
  ['after an escaped backtick', 'a \\` then {T} and `x`'],
];

test('the server puts a link where Zulip itself reads Markdown, and only there', async (t) => {
  const stream = await newStream(env, 'where');
  const target = await post(stream, 'target', 'pointed at');
  const token = `[[msg:${target}]]`;
  const plain = (await user.message(await send(stream, `see ${token} here`))).content;
  const link = plain.slice('see '.length, -' here'.length);
  assert.match(link, new RegExp(`^\\[#.*\\]\\(#narrow/channel/.*/near/${target}\\)$`));
  for (const [place, text] of PLACES) {
    const typed = text.replace('{T}', token);
    const linked = text.replace('{T}', link);
    const raw = (await user.message(await send(stream, typed))).content;
    assert.ok(raw === typed || raw === linked, `${place}: posted as typed or with the link, got ${JSON.stringify(raw)}`);
    const zulipLinks = linksTo(await rendered(await post(stream, 'probe', linked)), target);
    t.diagnostic(`${place}: server ${raw === linked ? 'linked' : 'kept the token'}, Zulip ${zulipLinks ? 'shows a link' : 'shows no link'}`);
    assert.equal(raw === linked, zulipLinks, place);
  }
});

test('a message in a private stream links within its own stream, and is refused toward another', async () => {
  const secret = uniqueName('secret');
  await user.ok('POST', 'users/me/subscriptions', { subscriptions: [{ name: secret }], invite_only: true, principals: [await user.myId(), await bot.myId()] });
  const open = await newStream(env, 'open');
  const id = await post(secret, 'plans', 'private');
  await assert.rejects(send(open, `[[msg:${id}]]`), /is in a private stream, and a link would show that stream's and topic's names to this conversation's readers\. Nothing was sent\./);
  assert.ok(linksTo(await rendered(await send(secret.toUpperCase(), `[[msg:${id}]]`)), id), 'toward its own stream, given by name in another case');
});
