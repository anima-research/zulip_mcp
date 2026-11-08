/** Message-link tokens through the real tool handlers and zulip-js HTTP client. */
import test, { type TestContext } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import zulipInit from 'zulip-js';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';

const stream = { id: 101, type: 'stream', stream_id: 12,
  display_recipient: "R&D / café. (test)!*'", subject: "a/b .% #? [] (x)!*'💬" };
const streamPath = 'channel/12/topic/a.2Fb.20.2E.25.20.23.3F.20.5B.5D.20.28x.29.21.2A.27.F0.9F.92.AC/near/101';
const messages: Record<number, unknown> = {
  101: stream,
  102: { id: 102, type: 'private', display_recipient: [{ id: 790 }, { id: 42 }] },
  103: { id: 103, type: 'private', display_recipient: [{ id: 790 }, { id: 42 }, { id: 7 }] },
  104: { id: 104, type: 'private', display_recipient: [{ id: 790 }] },
  105: { id: 105, type: 'stream', stream_id: 12, display_recipient: 'general', subject: '' },
  996: { id: 996, type: 'stream' },
};

async function fixture(t: TestContext) {
  const requests: { method?: string; url: URL; content: string | null }[] = [];
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const url = new URL(req.url!, 'http://127.0.0.1');
      const body = Buffer.concat(chunks).toString();
      const content = req.method === 'POST'
        ? body.match(/name="content"\r\n\r\n([\s\S]*?)\r\n--/)?.[1] ?? null
        : new URLSearchParams(body).get('content');
      requests.push({ method: req.method, url, content });
      res.setHeader('content-type', 'application/json');
      if (req.method === 'GET') {
        const id = Number(url.pathname.split('/').at(-1));
        if (id === 997) { res.statusCode = 502; res.end('not JSON'); return; }
        if (!messages[id]) {
          res.statusCode = id === 998 ? 404 : 400;
          res.end(JSON.stringify({ result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' }));
        } else res.end(JSON.stringify({ result: 'success', message: messages[id] }));
      } else res.end(JSON.stringify({ result: 'success', id: 501 }));
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const realm = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const dir = mkdtempSync(join(tmpdir(), 'zulip-message-links-'));
  t.after(async () => {
    rmSync(dir, { recursive: true, force: true });
    await new Promise<void>((resolve, reject) => server.close((err) => err ? reject(err) : resolve()));
  });
  const client = await zulipInit({ realm, username: 'bot@example.com', apiKey: 'test-only' });
  let uploads = 0;
  const tools = new ZulipToolRuntime({ client, selfUserId: 790, realm, authHeader: 'Basic dGVzdDpvbmx5', sessionId: 'test' }, dir, {
    uploader: { async upload({ name }) { uploads++; return { name, path: '/user_uploads/1/a/file', url: `${realm}/user_uploads/1/a/file` }; } },
  });
  return { tools, requests, realm, uploads: () => uploads,
    link: (path: string, code = false) => {
      const url = `${realm}/#narrow/${path}`;
      if (code) return url;
      const escape = (value: string) => value.replace(/[\\`*_$~|[\]<>]/g, '\\$&');
      const label = path.includes('/near/105') ? '#general >  @ 💬' : path.startsWith('dm/') ? 'DM @ 💬' : `#${escape(stream.display_recipient)} > ${escape(stream.subject)} @ 💬`;
      return `[${label}](${url})`;
    } };
}

for (const tool of ['send_message', 'edit_message']) {
  const args = tool === 'send_message' ? { type: 'stream', to: 'general', topic: 'links', allow_private_references: true } : { message_id: 500, allow_private_references: true };
  for (const [id, path] of [
    [101, streamPath], [102, 'dm/42,790-dm/near/102'],
    [103, 'dm/7,42,790-group/near/103'], [104, 'dm/790-dm/near/104'],
    [105, 'channel/12-general/topic//near/105'],
  ] as const) {
    test(`${tool}: builds the Zulip narrow for message ${id}`, async (t) => {
      const f = await fixture(t);
      await f.tools.handleToolCall(tool, { ...args, content: `See [[msg:${id}]]!` });
      assert.deepEqual(f.requests.map((r) => r.method), ['GET', tool === 'send_message' ? 'POST' : 'PATCH']);
      assert.equal(f.requests[1].content, `See ${f.link(path)}!`);
    });
  }

  test(`${tool}: multiple tokens, canonical duplicates resolved once per call`, async (t) => {
    const f = await fixture(t);
    const input = { ...args, content: '[[msg:101]] + [[msg:102]] + [[msg:101]] + [[msg:00102]]' };
    await f.tools.handleToolCall(tool, input);
    assert.deepEqual(f.requests.filter((r) => r.method === 'GET').map((r) => r.url.pathname), ['/api/v1/messages/101', '/api/v1/messages/102']);
    const a = f.link(streamPath), b = f.link('dm/42,790-dm/near/102');
    assert.equal(f.requests.at(-1)!.content, `${a} + ${b} + ${a} + ${b}`);
    assert.equal(input.content, '[[msg:101]] + [[msg:102]] + [[msg:101]] + [[msg:00102]]', 'caller input is not mutated');
    await f.tools.handleToolCall(tool, { ...args, content: '[[msg:101]]' });
    assert.equal(f.requests.filter((r) => r.method === 'GET').length, 3, 'cache does not survive calls or permission changes');
  });

  for (const [label, content] of [
    ['inline code', '`[[msg:101]]`'],
    ['multiline inline code', '``first\n[[msg:101]]\nlast``'],
    ['code fence', '```ts\n[[msg:101]]\n```'],
    ['unclosed tilde fence', '~~~\n[[msg:101]]'],
    ['spoiler body', '```spoiler details\n[[msg:101]]\n```'],
    ['spoiler title', '```spoiler [[msg:101]]\nbody\n```'],
    ['quote block', '```quote\n[[msg:101]]\n```'],
    ['blockquote fence', '> ~~~\n> [[msg:101]]\n> ~~~'],
    ['nested list fence', '- > ```\n  > [[msg:101]]\n  > ```'],
    ['table cells', '| left | target | right |\n| --- | --- | --- |\n| ` | [[msg:101]] | ` |'],
  ]) {
    test(`${tool}: tokens in ${label} resolve`, async (t) => {
      const f = await fixture(t);
      await f.tools.handleToolCall(tool, { ...args, content });
      assert.equal(f.requests.at(-1)!.content, content.replace('[[msg:101]]', f.link(streamPath, ['inline code','multiline inline code','code fence','unclosed tilde fence','blockquote fence','nested list fence'].includes(label))));
      assert.equal(f.requests.filter((r) => r.method === 'GET').length, 1);
    });
    test(`${tool}: unreadable tokens in ${label} fail before mutations`, async (t) => {
      const f = await fixture(t);
      await assert.rejects(f.tools.handleToolCall(tool, { ...args, content: content.replace('101', '999'),
        ...(tool === 'send_message' ? { attachments: [{ data: 'aGk=', name: 'hi.txt' }] } : {}),
      }), /Cannot resolve \[\[msg:999\]\]/);
      assert.deepEqual(f.requests.map((r) => r.method), ['GET']);
      assert.equal(f.uploads(), 0);
    });
  }

  test(`${tool}: backslash-escaped tokens stay literal with zero reads`, async (t) => {
    const f = await fixture(t);
    const content = String.raw`\[[msg:999]] \[[msg:0]] \[[msg:9007199254740993]]` +
      '\n`\\[[msg:999]]`\n```\n\\[[msg:999]]\n```\n| \\[[msg:999]] |';
    await f.tools.handleToolCall(tool, { ...args, content });
    assert.equal(f.requests.at(-1)!.content, content.replaceAll('\\[[', '[['));
    assert.equal(f.requests.filter((r) => r.method === 'GET').length, 0);
  });

  test(`${tool}: mixed escaped/unescaped tokens remove only the immediate escape`, async (t) => {
    const f = await fixture(t);
    const content = String.raw`\[[msg:101]] [[msg:101]] \\[[msg:999]] [[msg:102]]`;
    await f.tools.handleToolCall(tool, { ...args, content });
    assert.equal(f.requests.at(-1)!.content,
      '[[msg:101]] ' + f.link(streamPath) + String.raw` \[[msg:999]] ` + f.link('dm/42,790-dm/near/102'));
    assert.equal(f.requests.filter((r) => r.method === 'GET').length, 2);
  });

  for (const id of [999, 998, 997, 996]) {
    test(`${tool}: unreadable ${id} fails the whole call naming the token, before mutations`, async (t) => {
      const f = await fixture(t);
      await assert.rejects(f.tools.handleToolCall(tool, { ...args, content: `[[msg:101]] [[msg:${id}]]`,
        ...(tool === 'send_message' ? { attachments: [{ data: 'aGk=', name: 'hi.txt' }] } : {}),
      }), new RegExp(`\\[\\[msg:${id}\\]\\]`));
      assert.equal(f.requests.some((r) => r.method !== 'GET'), false, 'no sends or edits');
      assert.equal(f.uploads(), 0, 'no uploads');
    });
  }

  test(`${tool}: cap counts unescaped occurrences everywhere, and fails before any HTTP`, async (t) => {
    const f = await fixture(t);
    await assert.rejects(f.tools.handleToolCall(tool, { ...args, content: '```\n' + '[[msg:101]] '.repeat(21) + '\n```' }), /20/);
    assert.deepEqual(f.requests, []);
    await f.tools.handleToolCall(tool, { ...args, content: String.raw`\[[msg:999]] `.repeat(25) + '[[msg:101]] '.repeat(20) });
    assert.equal(f.requests.filter((r) => r.method === 'GET').length, 1);
    assert.equal(f.requests.at(-1)!.content, '[[msg:999]] '.repeat(25) + (f.link(streamPath) + ' ').repeat(20));
  });

  test(`${tool}: no exact tokens means byte-identical content and no reads`, async (t) => {
    const f = await fixture(t);
    const content = '  123 999 [[msg:abc]] [[msg:-1]] [[msg:1.0]] [[msg: 101]] [[MSG:101]]\r\nexisting https://z.example/#narrow/stream/12-general/near/101\n';
    await f.tools.handleToolCall(tool, { ...args, content });
    assert.equal(f.requests.length, 1);
    assert.equal(f.requests[0].content, content);
  });

  test(`${tool}: decimal id beyond safe integer range fails without rounding or writing`, async (t) => {
    const f = await fixture(t);
    await assert.rejects(f.tools.handleToolCall(tool, { ...args, content: '[[msg:9007199254740993]]' }), /\[\[msg:9007199254740993\]\]/);
    assert.deepEqual(f.requests, []);
  });
}

test('send_message: tokens compose with reply_to and attachments before content is processed', async (t) => {
  const f = await fixture(t);
  await f.tools.handleToolCall('send_message', { allow_private_references: true, reply_to: 101, content: '[[msg:102]]', attachments: [{ data: 'aGk=', name: 'hi.txt' }] });
  assert.deepEqual(f.requests.map((r) => r.method), ['GET', 'GET', 'POST']);
  assert.equal(f.requests[0].url.pathname, '/api/v1/messages/102');
  assert.equal(f.requests[1].url.pathname, '/api/v1/messages/101');
  assert.ok(f.requests[2].content!.startsWith(f.link('dm/42,790-dm/near/102')));
  assert.match(f.requests[2].content!, /user_uploads/);
  assert.equal(f.uploads(), 1);
});
