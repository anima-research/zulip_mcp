import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import zulipInit from 'zulip-js';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { toolDefinitions } from '../src/tools.ts';

/** Exercise the real client and runtime against a local HTTP stand-in. */
async function fixture(response: unknown, status = 200) {
  const calls: { method?: string; path?: string; body: string; auth?: string; contentType?: string }[] = [];
  const server = createServer(async (req, res) => {
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk);
    calls.push({
      method: req.method, path: req.url, body: Buffer.concat(chunks).toString(),
      auth: req.headers.authorization, contentType: req.headers['content-type'],
    });
    res.writeHead(status, { 'Content-Type': typeof response === 'string' ? 'text/html' : 'application/json' });
    res.end(typeof response === 'string' ? response : JSON.stringify(response));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const realm = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  const client = await zulipInit({ realm, username: 'test@example.com', apiKey: 'test-key' });
  const authHeader = `Basic ${Buffer.from('test@example.com:test-key').toString('base64')}`;
  const dir = mkdtempSync(join(tmpdir(), 'zulip-edit-'));
  const tools = new ZulipToolRuntime({ client, realm, authHeader, selfUserId: 1, sessionId: 'test' }, dir);
  return {
    tools, calls, authHeader,
    async close() {
      server.close();
      server.closeAllConnections();
      await once(server, 'close');
      rmSync(dir, { recursive: true, force: true });
    },
  };
}

test('edit_message PATCHes the id in the path and only content in the form body', async () => {
  const response = { result: 'success', msg: '', ignored_parameters_unsupported: [] };
  const f = await fixture(response);
  try {
    const content = 'Updated **text** & + = \n✓';
    assert.deepEqual(await f.tools.handleToolCall('edit_message', { message_id: 42, content, extra: 'not sent' }), response);
    assert.deepEqual(f.calls, [{
      method: 'PATCH', path: '/api/v1/messages/42',
      body: new URLSearchParams({ content }).toString(), auth: f.authHeader,
      contentType: 'application/x-www-form-urlencoded;charset=UTF-8',
    }]);
  } finally {
    await f.close();
  }
});

test('edit_message rejects success responses with any unsupported parameters', async () => {
  for (const parameter of ['message_id', 'content', 'future_field']) {
    const f = await fixture({ result: 'success', msg: '', ignored_parameters_unsupported: [parameter] });
    try {
      await assert.rejects(
        f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated' }),
        new RegExp(`editing message 42.*ignored_parameters_unsupported.*${parameter}`),
      );
    } finally {
      await f.close();
    }
  }
});

test('edit_message preserves Zulip API errors', async () => {
  const f = await fixture({ result: 'error', msg: 'Message edit time limit exceeded' }, 400);
  try {
    await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated' }), /HTTP 400.*Message edit time limit exceeded/);
  } finally {
    await f.close();
  }
});

test('edit_message reports the status and a safe snippet for HTML HTTP errors', async () => {
  const auth = Buffer.from('test@example.com:test-key').toString('base64');
  const f = await fixture(`<html>Service unavailable: Basic ${auth} test-key ${'x'.repeat(500)}</html>`, 503);
  try {
    await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated' }), (error: Error) => {
      assert.match(error.message, /HTTP 503.*Service unavailable/);
      assert.ok(!error.message.includes(auth));
      assert.doesNotMatch(error.message, /test-key|test@example.com/);
      assert.ok(error.message.length < 250, 'the response snippet is bounded');
      return true;
    });
  } finally {
    await f.close();
  }
});

test('edit_message redacts credentials interleaved with markup before truncating diagnostics', async () => {
  const auth = Buffer.from('test@example.com:test-key').toString('base64');
  const fragments = [auth.slice(0, 8), auth.slice(8), 'test@', 'example.com', 'test-', 'key'];
  const f = await fixture(
    `<html>Service unavailable: Basic <b>${fragments[0]}</b><i>${fragments[1]}</i> ` +
    `<b>test@</b><i>example.com</i> <b>test-</b><i>key</i></html>`, 503,
  );
  try {
    await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated' }), (error: Error) => {
      assert.match(error.message, /HTTP 503.*Service unavailable/);
      for (const fragment of fragments) assert.ok(!error.message.includes(fragment), 'no credential fragment survives');
      assert.match(error.message, /Service unavailable: \[redacted\] \[redacted\] \[redacted\]$/);
      return true;
    });
  } finally {
    await f.close();
  }
});

test('edit_message rejects non-string content without making a request', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const args of [null, 123, false, {}].map(content => ({ message_id: 42, content, topic: 'new topic' }))) {
      await assert.rejects(f.tools.handleToolCall('edit_message', args), /^Error: content must be a string$/);
      assert.deepEqual(f.calls, []);
    }
  } finally {
    await f.close();
  }
});

test('edit_message rejects missing or invalid message_id without making a request', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    const invalid = [undefined, null, 0, -1, 1.5, '42', false, NaN, Infinity, {}];
    for (const message_id of invalid) {
      await assert.rejects(f.tools.handleToolCall('edit_message', { message_id, content: 'updated' }), /^Error: message_id is required$/);
      assert.deepEqual(f.calls, []);
    }
  } finally {
    await f.close();
  }
});

test('edit_message reports the HTTP status for an empty error body', async () => {
  const f = await fixture('', 502);
  try {
    await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated' }), /HTTP 502/);
  } finally {
    await f.close();
  }
});

const topic = 'New topic & + = \n✓';
const content = 'Updated **text** & + = \n✓';
const edits = [
  { label: 'topic only', fields: { topic } },
  ...['change_one', 'change_later', 'change_all'].map(propagate_mode => ({
    label: `topic with ${propagate_mode}`, fields: { topic, propagate_mode },
  })),
  { label: 'stream only', fields: { stream_id: 17 } },
  { label: 'stream and topic', fields: { topic, stream_id: 17 } },
  { label: 'content and topic', fields: { content, topic } },
  { label: 'stream with propagation', fields: { propagate_mode: 'change_later', stream_id: 17 } },
  { label: 'empty topic', fields: { topic: '' } },
  { label: 'empty content', fields: { content: '' } },
];
for (const { label, fields } of edits) {
  test(`edit_message encodes ${label} in the PATCH body`, async () => {
    const response = { result: 'success', msg: '' };
    const f = await fixture(response);
    try {
      assert.deepEqual(await f.tools.handleToolCall('edit_message', {
        message_id: 42, ...fields, extra: 'not sent',
      }), response);
      const expected = new URLSearchParams(Object.entries(fields).map(([key, value]) => [key, String(value)]));
      assert.deepEqual(f.calls, [{
        method: 'PATCH', path: '/api/v1/messages/42', body: expected.toString(),
        auth: f.authHeader, contentType: 'application/x-www-form-urlencoded;charset=UTF-8',
      }]);
    } finally {
      await f.close();
    }
  });
}

test('edit_message requires content, topic, or stream_id without making a request', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const fields of [{}, { propagate_mode: 'change_all' }]) {
      await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, ...fields }),
        /^Error: At least one of content, topic, or stream_id is required$/);
    }
    assert.deepEqual(f.calls, []);
  } finally {
    await f.close();
  }
});

for (const { field, invalid, error } of [
  { field: 'propagate_mode', invalid: ['all', 'CHANGE_ALL', '', null, 1, false, {}],
    error: /^Error: propagate_mode must be change_one, change_later, or change_all$/ },
  { field: 'stream_id', invalid: [null, 0, -1, 1.5, '17', false, NaN, Infinity, {}],
    error: /^Error: stream_id must be a positive integer$/ },
  { field: 'topic', invalid: [null, 1, false, {}], error: /^Error: topic must be a string$/ },
]) {
  test(`edit_message rejects invalid ${field} without making a request`, async () => {
    const f = await fixture({ result: 'success', msg: '' });
    try {
      for (const value of invalid) {
        await assert.rejects(f.tools.handleToolCall('edit_message', {
          message_id: 42, content: 'updated', [field]: value,
        }), error);
      }
      assert.deepEqual(f.calls, []);
    } finally {
      await f.close();
    }
  });
}

test('edit_message surfaces realm permission errors for topic moves', async () => {
  const f = await fixture({ result: 'error', msg: 'You do not have permission to move this message' }, 400);
  try {
    await assert.rejects(f.tools.handleToolCall('edit_message', {
      message_id: 42, topic, stream_id: 17, propagate_mode: 'change_all',
    }), /HTTP 400.*You do not have permission to move this message/);
    assert.equal(f.calls.length, 1, 'permissions are checked by Zulip, not locally');
  } finally {
    await f.close();
  }
});

test('edit_message schema exposes optional topic, propagation, and stream fields', () => {
  const schema = toolDefinitions.find(tool => tool.name === 'edit_message')!.inputSchema;
  assert.deepEqual(schema.required, ['message_id']);
  const properties = schema.properties as Record<string, Record<string, unknown>>;
  assert.equal(properties.content.type, 'string');
  assert.equal(properties.topic.type, 'string');
  assert.equal(properties.stream_id.type, 'integer');
  assert.equal(properties.stream_id.minimum, 1);
  assert.deepEqual(properties.propagate_mode.enum, ['change_one', 'change_later', 'change_all']);
  assert.equal(properties.propagate_mode.default, undefined, 'Zulip chooses the default');
});


test('edit_message rejects content with a stream move without making a request', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const fields of [
      { content, stream_id: 17 },
      { content, topic, propagate_mode: 'change_all', stream_id: 17 },
      { content: '', stream_id: 17 },
    ]) {
      await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, ...fields }),
        /^Error: a stream move cannot be combined with a content edit; send two calls$/);
    }
    assert.deepEqual(f.calls, []);
  } finally {
    await f.close();
  }
});

test('edit_message rejects propagation on content-only edits without making a request', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const propagate_mode of ['change_later', 'change_all']) {
      await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content, propagate_mode }),
        /^Error: propagate_mode requires topic or stream_id$/);
    }
    assert.deepEqual(f.calls, []);
  } finally {
    await f.close();
  }
});

test('edit_message validates message_id before edit fields', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const args of [{}, { message_id: 0, content: null, topic: false, stream_id: -1, propagate_mode: 'invalid' }]) {
      await assert.rejects(f.tools.handleToolCall('edit_message', args), /^Error: message_id is required$/);
    }
    assert.deepEqual(f.calls, []);
  } finally {
    await f.close();
  }
});

test('edit_message describes move constraints and server propagation defaults', () => {
  const tool = toolDefinitions.find(tool => tool.name === 'edit_message')!;
  assert.match(tool.description, /stream move cannot be combined with a content edit; send two calls/);
  const properties = tool.inputSchema.properties as Record<string, Record<string, unknown>>;
  const description = String(properties.propagate_mode.description);
  assert.match(description, /Requires topic or stream_id/);
  assert.match(description, /Zulip defaults to change_one \(this message only\)/);
  assert.match(description, /change_later moves this and later messages/);
  assert.match(description, /change_all renames\/moves the whole topic/);
});

test('edit_message accepts explicit default propagation on a content edit', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    await f.tools.handleToolCall('edit_message', { message_id: 42, content, propagate_mode: 'change_one' });
    assert.equal(f.calls.length, 1);
    assert.equal(new URLSearchParams(f.calls[0].body).get('content'), content);
  } finally {
    await f.close();
  }
});
