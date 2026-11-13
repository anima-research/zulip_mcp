import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import { toolDefinitions } from '../src/tools.ts';
import type { ZulipSession } from '../src/zulip-client.ts';
import type { UploadPolicy } from '../src/uploads.ts';
import { standInApi, type StandInCall } from './zulip-stand-in.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';

// What Zulip 12.4's own web client functions write for each case: on-gh run
// 20261112T122955Z-9f66f64b, zulip/zulip at da085bbb5c90, by_stream_topic_url
// (web/src/internal_url.ts) and html_escape_markdown_syntax_characters
// (web/src/topic_link_util.ts), with "general chat" for the empty topic.
const R1: { name: string; streamId: number; stream: string; topic: string; id: number; link: string }[] = [
  { name: 'plain names', streamId: 9, stream: 'general', topic: 'greetings', id: 123, link: '[#general > greetings @ 💬](#narrow/channel/9-general/topic/greetings/near/123)' },
  { name: 'topic with a backtick', streamId: 9, stream: 'general', topic: 'a`b', id: 124, link: '[#general > a&#96;b @ 💬](#narrow/channel/9-general/topic/a.60b/near/124)' },
  { name: 'topic with >', streamId: 9, stream: 'general', topic: 'a>b', id: 125, link: '[#general > a&gt;b @ 💬](#narrow/channel/9-general/topic/a.3Eb/near/125)' },
  { name: 'topic with *', streamId: 9, stream: 'general', topic: 'a*b', id: 126, link: '[#general > a&#42;b @ 💬](#narrow/channel/9-general/topic/a.2Ab/near/126)' },
  { name: 'topic with &', streamId: 9, stream: 'general', topic: 'a&b', id: 127, link: '[#general > a&amp;b @ 💬](#narrow/channel/9-general/topic/a.26b/near/127)' },
  { name: 'topic with [', streamId: 9, stream: 'general', topic: 'a[b', id: 128, link: '[#general > a&#91;b @ 💬](#narrow/channel/9-general/topic/a.5Bb/near/128)' },
  { name: 'topic with ]', streamId: 9, stream: 'general', topic: 'a]b', id: 129, link: '[#general > a&#93;b @ 💬](#narrow/channel/9-general/topic/a.5Db/near/129)' },
  { name: 'topic with $$', streamId: 9, stream: 'general', topic: 'cost $$5', id: 130, link: '[#general > cost &#36;&#36;5 @ 💬](#narrow/channel/9-general/topic/cost.20.24.245/near/130)' },
  { name: 'stream with each special character', streamId: 31, stream: 'q`>*&[]$$', topic: 't', id: 131, link: '[#q&#96;&gt;&#42;&amp;&#91;&#93;&#36;&#36; > t @ 💬](#narrow/channel/31/topic/t/near/131)' },
  { name: 'topic with a space, a dot, a percent sign and non-ASCII', streamId: 9, stream: 'general', topic: 'v1.2 at 100% — café ☕', id: 132, link: '[#general > v1.2 at 100% — café ☕ @ 💬](#narrow/channel/9-general/topic/v1.2E2.20at.20100.25.20.E2.80.94.20caf.C3.A9.20.E2.98.95/near/132)' },
  { name: 'stream with a space', streamId: 12, stream: 'design team', topic: 't', id: 133, link: '[#design team > t @ 💬](#narrow/channel/12-design-team/topic/t/near/133)' },
  { name: 'stream in a non-Latin script, long encoding', streamId: 44, stream: '日本語チャンネル', topic: '話題', id: 134, link: '[#日本語チャンネル > 話題 @ 💬](#narrow/channel/44/topic/.E8.A9.B1.E9.A1.8C/near/134)' },
  { name: 'stream in a non-Latin script, Cyrillic', streamId: 45, stream: 'новости', topic: 't', id: 135, link: '[#новости > t @ 💬](#narrow/channel/45/topic/t/near/135)' },
  { name: "stream and topic with ( ) ! ' and a dot", streamId: 46, stream: 'q&a (beta)!', topic: "don't (yet).", id: 136, link: "[#q&amp;a (beta)! > don't (yet). @ 💬](#narrow/channel/46/topic/don.27t.20.28yet.29.2E/near/136)" },
  { name: 'empty topic', streamId: 9, stream: 'general', topic: '', id: 137, link: '[#general > general chat @ 💬](#narrow/channel/9-general/topic//near/137)' },
];

type Stream = { name: string; invite_only: boolean };
type Message = { type: 'stream'; stream_id: number; display_recipient: string; subject: string } | { type: 'private'; display_recipient: { id: number }[] };

function zulip(streams: Record<number, Stream>, messages: Record<number, Message>, limit?: number, opts: { noAnswer?: number[]; rateLimited?: boolean } = {}) {
  let posted = 0;
  const { api, calls } = standInApi({
    'GET /messages/:id': (_p: unknown, call: StandInCall) => {
      if (opts.noAnswer?.includes(call.id!)) throw new ZulipNoAnswer('timeout', 'GET', `/messages/${call.id}`, false, `Zulip gave no answer to GET /messages/${call.id} within 15 s.`);
      return messages[call.id!] ? { result: 'success', msg: '', message: { id: call.id, ...messages[call.id!] } } : { result: 'error', msg: 'Invalid message(s)', code: 'BAD_REQUEST' };
    },
    'GET /streams/:id': (_p: unknown, call: StandInCall) =>
      streams[call.id!] ? { result: 'success', msg: '', stream: { stream_id: call.id, ...streams[call.id!] } } : { result: 'error', msg: 'Invalid channel ID', code: 'BAD_REQUEST' },
    'GET /get_stream_id': (p: Record<string, unknown>) => {
      if (opts.rateLimited) return { result: 'error', msg: 'API usage exceeded rate limit', code: 'RATE_LIMIT_HIT' };
      const found = Object.entries(streams).find(([, s]) => s.name.toLowerCase() === String(p.stream).toLowerCase());
      return found ? { result: 'success', msg: '', stream_id: Number(found[0]) } : { result: 'error', msg: `Invalid channel name '${p.stream}'`, code: 'BAD_REQUEST' };
    },
    'POST /messages': () => ({ result: 'success', msg: '', id: 900 + ++posted }),
    'PATCH /messages/:id': { result: 'success', msg: '' },
  });
  const uploads: string[] = [];
  const uploader = {
    async upload(i: { name: string }) {
      uploads.push(i.name);
      return { name: i.name, path: `/user_uploads/1/ab/${i.name}`, url: `https://z/user_uploads/1/ab/${i.name}` };
    },
  };
  const uploadPolicy: UploadPolicy = { roots: new Map(), maxBytes: 1024, maxTotalBytes: 4096, maxCount: 10 };
  const dir = mkdtempSync(join(tmpdir(), 'zulip-links-'));
  const session: ZulipSession = { api, selfUserId: 790, realm: 'https://z', authHeader: 'Basic x', sessionId: 't' };
  const original = console.error;
  console.error = () => {};
  let runtime: ZulipToolRuntime;
  try {
    runtime = new ZulipToolRuntime(session, dir, { uploader, uploadPolicy, ...(limit ? { maxMessageLength: limit } : {}) } as never);
  } finally {
    console.error = original;
  }
  const adapter = new ZulipAdapter(api, { selfUserId: 790, sessionId: 's' }, { uploader, uploadPolicy, ...(limit ? { maxMessageLength: limit } : {}) } as never);
  const posts = () => calls.filter((c) => c.method === 'POST' && c.path === '/messages').map((c) => c.params.content);
  const edits = () => calls.filter((c) => c.method === 'PATCH').map((c) => c.params.content);
  const reads = (prefix: string) => calls.filter((c) => c.method === 'GET' && c.path.startsWith(prefix));
  const done = () => rmSync(dir, { recursive: true, force: true });
  return { runtime: runtime!, adapter, calls, uploads, posts, edits, reads, done };
}

const PUBLIC: Record<number, Stream> = { 9: { name: 'general', invite_only: false }, 20: { name: 'secret', invite_only: true } };
const MESSAGES: Record<number, Message> = {
  1001: { type: 'stream', stream_id: 9, display_recipient: 'general', subject: 'greetings' },
  2001: { type: 'stream', stream_id: 20, display_recipient: 'secret', subject: 'plans' },
  3001: { type: 'private', display_recipient: [{ id: 790 }, { id: 42 }] },
};
const PUBLIC_LINK = '[#general > greetings @ 💬](#narrow/channel/9-general/topic/greetings/near/1001)';
const SECRET_LINK = '[#secret > plans @ 💬](#narrow/channel/20-secret/topic/plans/near/2001)';
const file = [{ data: 'aGk=', name: 'hi.txt' }];
const codePoints = (s: string) => [...s].length;

test('R1: a token becomes the link Zulip\'s own client writes, for each case', async () => {
  for (const c of R1) {
    const z = zulip({ ...PUBLIC, [c.streamId]: { name: c.stream, invite_only: false } }, { [c.id]: { type: 'stream', stream_id: c.streamId, display_recipient: c.stream, subject: c.topic } });
    await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: `see [[msg:${c.id}]] here` });
    assert.deepEqual(z.posts(), [`see ${c.link} here`], c.name);
    z.done();
  }
});

test('R1: a | in a name is written &#124; in the label, so a link in a table cell keeps its row; the one departure from Zulip\'s client', async () => {
  const z = zulip({ ...PUBLIC, 47: { name: 'ops|infra', invite_only: false } }, { 138: { type: 'stream', stream_id: 47, display_recipient: 'ops|infra', subject: 'a|b' } });
  await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: '| ref | x |\n|---|---|\n| [[msg:138]] | kept |' });
  assert.deepEqual(z.posts(), ['| ref | x |\n|---|---|\n| [#ops&#124;infra > a&#124;b @ 💬](#narrow/channel/47-ops.7Cinfra/topic/a.7Cb/near/138) | kept |']);
  z.done();
});

test('R1: send_message, send_dm, a plain answer and edit_message each put the link in place of the token', async () => {
  const z = zulip(PUBLIC, MESSAGES);
  await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'as [[msg:1001]] said' });
  await z.runtime.handleToolCall('send_dm', { to: ['42'], content: 'as [[msg:1001]] said' });
  await z.adapter.publish('zulip:general', undefined, [{ type: 'text', text: 'as [[msg:1001]] said' }], { threadId: 't' });
  await z.runtime.handleToolCall('edit_message', { message_id: 1001, content: 'as [[msg:1001]] said' });
  assert.deepEqual(z.posts(), [`as ${PUBLIC_LINK} said`, `as ${PUBLIC_LINK} said`, `as ${PUBLIC_LINK} said`]);
  assert.deepEqual(z.edits(), [`as ${PUBLIC_LINK} said`]);
  z.done();
});

test('R2: a token Zulip reads as code, or no token at all, is posted byte for byte and reads nothing', async () => {
  const unchanged = [
    'no token here',
    '```\n[[msg:1001]]\n```',
    '~~~\n[[msg:1001]]\n~~~',
    '```math\n[[msg:1001]]\n```',
    '```quote\nquoted\n```python\n[[msg:1001]]\n```\n```',
    '```\nnever closed\n[[msg:1001]]',
    'inline `[[msg:1001]]` code',
    'two ``a ` [[msg:1001]]`` ticks',
    'a span `that crosses\n[[msg:1001]] a line` break',
    '[[msg:abc]]',
    '[[msg:]]',
    'a span `across a\n1) [[msg:1001]] line` that Zulip takes as text',
    'a span `across a\n-\n[[msg:1001]] lone dash`',
    'a span `across a\n10. [[msg:1001]] line` in a paragraph',
    '```spoiler See [[msg:1001]]\nbody\n```',
  ];
  for (const content of unchanged) {
    const z = zulip(PUBLIC, MESSAGES);
    await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content });
    assert.deepEqual(z.posts(), [content], JSON.stringify(content));
    assert.equal(z.reads('/').length, 0, `no read for ${JSON.stringify(content)}`);
    z.done();
  }
});

test('R2: a token inside a quote fence, inside a spoiler fence, after an escaped backtick, or past a backtick that another element holds is replaced', async () => {
  const replaced: [string, string][] = [
    ['```quote\nas [[msg:1001]] said\n```', `\`\`\`quote\nas ${PUBLIC_LINK} said\n\`\`\``],
    ['```spoiler Plans\nas [[msg:1001]] said\n```', `\`\`\`spoiler Plans\nas ${PUBLIC_LINK} said\n\`\`\``],
    ['# a `b\n[[msg:1001]] c` d', `# a \`b\n${PUBLIC_LINK} c\` d`],
    ['| `a | b |\n|---|---|\n| [[msg:1001]] | c` |', `| \`a | b |\n|---|---|\n| ${PUBLIC_LINK} | c\` |`],
    ['- a `b\n- [[msg:1001]] c` d', `- a \`b\n- ${PUBLIC_LINK} c\` d`],
    ['    code `x\n[[msg:1001]] y`', `    code \`x\n${PUBLIC_LINK} y\``],
    ['a `b\n***\n[[msg:1001]] c`', `a \`b\n***\n${PUBLIC_LINK} c\``],
    ['a \\` then [[msg:1001]] and `x`', `a \\\` then ${PUBLIC_LINK} and \`x\``],
    ['one \\[[msg:1001]] backslash', `one \\${PUBLIC_LINK} backslash`],
    ['two \\\\[[msg:1001]] backslashes', `two \\\\${PUBLIC_LINK} backslashes`],
  ];
  for (const [content, expected] of replaced) {
    const z = zulip(PUBLIC, MESSAGES);
    await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content });
    assert.deepEqual(z.posts(), [expected], JSON.stringify(content));
    z.done();
  }
});

test('R2: a token is replaced in an indented code block, in inline math, inside another link, and in inline code across a list line in a spoiler fence', async () => {
  const replaced: [string, string][] = [
    ['    [[msg:1001]]', `    ${PUBLIC_LINK}`],
    ['a $$x [[msg:1001]]$$ b', `a $$x ${PUBLIC_LINK}$$ b`],
    ['[see [[msg:1001]]](https://x)', `[see ${PUBLIC_LINK}](https://x)`],
    ['[see](https://x/[[msg:1001]])', `[see](https://x/${PUBLIC_LINK})`],
    ['```spoiler S\na `b\n- [[msg:1001]] c` d\n```', `\`\`\`spoiler S\na \`b\n- ${PUBLIC_LINK} c\` d\n\`\`\``],
  ];
  for (const [content, expected] of replaced) {
    const z = zulip(PUBLIC, MESSAGES);
    await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content });
    assert.deepEqual(z.posts(), [expected], JSON.stringify(content));
    z.done();
  }
});

test('R2: a long message of thousands of headings, rules or table rows in one block gets its link, not a refusal', async () => {
  const shapes = [
    Array.from({ length: 5000 }, (_, i) => `# h${i}`),
    Array.from({ length: 20000 }, (_, i) => (i % 2 ? '---' : 'text')),
    ['| a |', '| --- |', ...Array.from({ length: 16000 }, (_, i) => `| ${i} |`)],
  ];
  for (const lines of shapes) {
    const z = zulip(PUBLIC, MESSAGES, 10_000_000);
    const content = [...lines, '', 'see [[msg:1001]]'].join('\n');
    await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content });
    assert.deepEqual(z.posts(), [[...lines, '', `see ${PUBLIC_LINK}`].join('\n')], lines[0]);
    z.done();
  }
});

test('R3: a tool refuses a token for a message Zulip doesn\'t show, or for a direct message, before any upload or post', async () => {
  const cases: [string, RegExp][] = [
    ['[[msg:404]]', /^\[\[msg:404\]\] can't become a link: Zulip refused to show message 404 to this bot \(Invalid message\(s\)\)\./],
    ['[[msg:3001]]', /^\[\[msg:3001\]\] can't become a link: message 3001 is a direct message, and only a stream message can be linked\./],
  ];
  for (const [token, reason] of cases) {
    const z = zulip(PUBLIC, MESSAGES);
    await assert.rejects(z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: `see ${token}`, attachments: file }), (err: Error) => {
      assert.match(err.message, reason);
      assert.match(err.message, / Nothing was sent\.$/);
      return true;
    });
    await assert.rejects(z.runtime.handleToolCall('send_dm', { to: ['42'], content: `see ${token}`, attachments: file }), (err: Error) => reason.test(err.message) && / Nothing was sent\.$/.test(err.message));
    await assert.rejects(z.runtime.handleToolCall('edit_message', { message_id: 1001, content: `see ${token}` }), (err: Error) => reason.test(err.message) && / The message wasn't edited\.$/.test(err.message));
    assert.deepEqual(z.posts(), [], `${token}: no post`);
    assert.deepEqual(z.edits(), [], `${token}: no edit`);
    assert.deepEqual(z.uploads, [], `${token}: no upload`);
    z.done();
  }
});

test('R3: a read that gets no answer, or any refused read, refuses the call and says nothing was sent or edited', async () => {
  const z = zulip(PUBLIC, MESSAGES, undefined, { noAnswer: [5005] });
  const noAnswer = "[[msg:5005]] can't become a link: Zulip gave no answer to GET /messages/5005 within 15 s.";
  await assert.rejects(z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'see [[msg:5005]]', attachments: file }), (err: Error) => err.message === `${noAnswer} Nothing was sent.`);
  await assert.rejects(z.runtime.handleToolCall('edit_message', { message_id: 1001, content: 'see [[msg:5005]]' }), (err: Error) => err.message === `${noAnswer} The message wasn't edited.`);
  await assert.rejects(z.runtime.handleToolCall('edit_message', { message_id: 6006, content: 'see [[msg:1001]]' }), (err: Error) => err.message === "[[msg:1001]] can't become a link: Zulip refused reading message 6006: Invalid message(s). The message wasn't edited.");
  assert.deepEqual([z.posts(), z.edits(), z.uploads], [[], [], []]);
  z.done();
});

test('R3b: in a plain answer, after a read that gets no answer, the other tokens stay as typed, unread', async () => {
  const z = zulip(PUBLIC, MESSAGES, undefined, { noAnswer: [5005] });
  await z.adapter.publish('zulip:general', undefined, [{ type: 'text', text: 'see [[msg:5005]] and [[msg:1001]]' }], { threadId: 't' });
  assert.deepEqual(z.posts(), ['see [[msg:5005]] and [[msg:1001]]']);
  assert.deepEqual(z.reads('/messages/').map((c) => c.path), ['/messages/5005']);
  z.done();
});

test('R3b: a plain answer posts such a token as typed, and the rest of the answer goes out', async () => {
  const z = zulip(PUBLIC, MESSAGES);
  const result = await z.adapter.publish('zulip:general', undefined, [{ type: 'text', text: 'see [[msg:404]], [[msg:3001]] and [[msg:1001]]' }], { threadId: 't' });
  assert.equal(result.delivered, true);
  assert.deepEqual(z.posts(), [`see [[msg:404]], [[msg:3001]] and ${PUBLIC_LINK}`]);
  z.done();
});

test('R4: a message in a private stream links only within its own stream', async () => {
  const privateReason = /^\[\[msg:2001\]\] can't become a link: message 2001 is in a private stream, and a link would show that stream's and topic's names to this conversation's readers\. Nothing was sent\.$/;
  const z = zulip(PUBLIC, MESSAGES);
  await assert.rejects(z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'see [[msg:2001]]' }), (err: Error) => privateReason.test(err.message));
  await assert.rejects(z.runtime.handleToolCall('send_dm', { to: ['42'], content: 'see [[msg:2001]]' }), (err: Error) => privateReason.test(err.message));
  assert.deepEqual(z.posts(), []);

  await z.runtime.handleToolCall('send_message', { type: 'stream', to: 20, topic: 't', content: 'see [[msg:2001]]' });
  assert.equal(z.reads('/streams/').length, 2, 'the two refusals read the privacy; a destination given by its id reads none');
  await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'Secret', topic: 't', content: 'see [[msg:2001]]' });
  assert.equal(z.reads('/get_stream_id').length, 2, 'each destination given by name, general and Secret, is resolved by Zulip');
  await z.runtime.handleToolCall('send_message', { reply_to: 2001, content: 'see [[msg:2001]]' });
  await z.adapter.publish('zulip:general', undefined, [{ type: 'text', text: 'see [[msg:2001]]' }], { threadId: 't' });
  await z.runtime.handleToolCall('send_dm', { to: ['42'], content: 'see [[msg:1001]]' });
  assert.deepEqual(z.posts(), [`see ${SECRET_LINK}`, `see ${SECRET_LINK}`, `see ${SECRET_LINK}`, 'see [[msg:2001]]', `see ${PUBLIC_LINK}`]);
  z.done();
});

test('R4: a refused lookup of the destination\'s name, or of a stream\'s privacy, gives its own reason', async () => {
  const limited = zulip(PUBLIC, MESSAGES, undefined, { rateLimited: true });
  await assert.rejects(limited.runtime.handleToolCall('send_message', { type: 'stream', to: 'Secret', topic: 't', content: 'see [[msg:2001]]' }), (err: Error) => err.message === "[[msg:2001]] can't become a link: Zulip refused the lookup of the destination stream Secret (API usage exceeded rate limit). Nothing was sent.");
  limited.done();
  const unknown = zulip(PUBLIC, { 4001: { type: 'stream', stream_id: 40, display_recipient: 'gone', subject: 'x' } });
  await assert.rejects(unknown.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'see [[msg:4001]]' }), (err: Error) => err.message === "[[msg:4001]] can't become a link: Zulip didn't say whether message 4001's stream is private (Invalid channel ID). Nothing was sent.");
  unknown.done();
});

test('R4: a stream destination is read as Zulip reads `to`, JSON first', async () => {
  const z = zulip(PUBLIC, MESSAGES);
  const own: unknown[] = [20, '20', '[20]', [20], '"secret"', '["Secret"]', 'secret'];
  for (const to of own) await z.runtime.handleToolCall('send_message', { type: 'stream', to, topic: 't', content: 'see [[msg:2001]]' });
  assert.deepEqual(z.posts(), own.map(() => `see ${SECRET_LINK}`));
  for (const to of ['[9]', '"general"', '[20, 9]']) {
    await assert.rejects(z.runtime.handleToolCall('send_message', { type: 'stream', to, topic: 't', content: 'see [[msg:2001]]' }), /is in a private stream/, to);
  }
  z.done();
});

test('R4: a stream made private after the server started is refused at the next call', async () => {
  const streams: Record<number, Stream> = { 9: { name: 'general', invite_only: false }, 30: { name: 'later', invite_only: false } };
  const z = zulip(streams, { 3002: { type: 'stream', stream_id: 30, display_recipient: 'later', subject: 'x' } });
  await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: '[[msg:3002]]' });
  streams[30] = { name: 'later', invite_only: true };
  await assert.rejects(z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: '[[msg:3002]]' }), /message 3002 is in a private stream/);
  assert.deepEqual(z.posts(), ['[#later > x @ 💬](#narrow/channel/30-later/topic/x/near/3002)']);
  z.done();
});

test('R5: one id in two tokens costs one read, asked with no rendered content, and one stream costs one privacy read', async () => {
  const z = zulip(PUBLIC, { ...MESSAGES, 1002: { type: 'stream', stream_id: 9, display_recipient: 'general', subject: 'other' } });
  await z.runtime.handleToolCall('send_dm', { to: ['42'], content: '[[msg:1001]], [[msg:1001]] and [[msg:1002]]' });
  assert.deepEqual(z.reads('/messages/').map((c) => [c.path, c.params]), [
    ['/messages/1001', { apply_markdown: false, allow_empty_topic_name: true }],
    ['/messages/1002', { apply_markdown: false, allow_empty_topic_name: true }],
  ]);
  assert.deepEqual(z.reads('/streams/').map((c) => c.path), ['/streams/9']);
  z.done();
});

test('R6: edit_message counts the link against the length, and holds the audience rule to the edited message\'s conversation', async () => {
  const short = zulip(PUBLIC, MESSAGES, codePoints(PUBLIC_LINK) + 3);
  await short.runtime.handleToolCall('edit_message', { message_id: 1001, content: 'as [[msg:1001]]' });
  await assert.rejects(short.runtime.handleToolCall('edit_message', { message_id: 1001, content: 'as [[msg:1001]]!' }), (err: Error) => err.message === `content is ${codePoints(PUBLIC_LINK) + 4} characters as it would be posted, with each [[msg:ID]] as its link (16 as written), above the message length of ${codePoints(PUBLIC_LINK) + 3}; an edit can't be split as a send is`);
  const table = '| a | b |\n|---|---|';
  const both = `[[msg:1001]]\n${table}`;
  await assert.rejects(short.runtime.handleToolCall('edit_message', { message_id: 1001, content: both }), (err: Error) => err.message === `content is ${codePoints(`${PUBLIC_LINK}\n\n${table}`)} characters as it would be posted, with a blank line above each table under a line of text and each [[msg:ID]] as its link (${codePoints(both)} as written), above the message length of ${codePoints(PUBLIC_LINK) + 3}; an edit can't be split as a send is`);
  assert.deepEqual(short.edits(), [`as ${PUBLIC_LINK}`]);
  short.done();

  const z = zulip(PUBLIC, MESSAGES);
  await assert.rejects(z.runtime.handleToolCall('edit_message', { message_id: 1001, content: 'see [[msg:2001]]' }), /message 2001 is in a private stream.* The message wasn't edited\.$/);
  await z.runtime.handleToolCall('edit_message', { message_id: 2001, content: 'see [[msg:2001]]' });
  await z.runtime.handleToolCall('edit_message', { message_id: 1001, content: 'no token' });
  assert.deepEqual(z.edits(), [`see ${SECRET_LINK}`, 'no token']);
  assert.deepEqual(z.reads('/streams/').map((c) => c.path), ['/streams/20'], 'the edit in its own stream reads no privacy');
  assert.equal(z.reads('/messages/1001').length, 1, 'the edited message is read only when its new text holds a token');
  z.done();
});

test('R7: the link goes in before the upload links and the split, and the split counts it', async () => {
  const z = zulip(PUBLIC, MESSAGES, PUBLIC_LINK.length + 1);
  await z.runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'a\n\n[[msg:1001]]', attachments: file });
  assert.deepEqual(z.posts(), ['a', PUBLIC_LINK, '[hi.txt](/user_uploads/1/ab/hi.txt)']);
  z.done();
});

test('R8: send_message\'s text tells the agent about the token', () => {
  const tool = toolDefinitions.find((t) => t.name === 'send_message')!;
  assert.ok(tool.description.endsWith(" Supports attachments. In a message's text, [[msg:ID]] becomes a link to the stream message with that id."));
});
