import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import zulipInit from 'zulip-js';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { TOOL_CLASSES } from '../src/tool-classes.ts';
import { featureSetForTool } from '../src/feature-sets.ts';
import { toolDefinitions } from '../src/tools.ts';
import { assertStrictArguments } from './strictArguments.ts';

/** Exercise the real client and runtime against a local HTTP stand-in. */
async function fixture(response: unknown, status = 200, loseVisibility = false, routingUnavailable = false) {
  const calls: { method?: string; path?: string; body: string; auth?: string; contentType?: string }[] = [];
  let currentTopic = 'deploys';
  let currentStream = 7;
  const lookups: string[] = [];
  const server = createServer(async (req, res) => {
    if (req.method === 'GET') {
      lookups.push(req.url!);
      if (routingUnavailable) {
        res.writeHead(503, { 'Content-Type': 'text/html' });
        res.end('temporarily unavailable');
        return;
      }
      const payload = req.url!.startsWith('/api/v1/streams')
        ? { result: 'success', streams: [{ stream_id: 17, name: 'archive' }] }
        : loseVisibility && currentStream === 17
          ? { result: 'error', msg: 'Not visible' }
          : { result: 'success', message: { stream_id: currentStream, display_recipient: currentStream === 17 ? 'archive' : 'general', subject: currentTopic } };
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(payload));
      return;
    }
    const chunks: Buffer[] = [];
    for await (const chunk of req) chunks.push(chunk);
    calls.push({
      method: req.method, path: req.url, body: Buffer.concat(chunks).toString(),
      auth: req.headers.authorization, contentType: req.headers['content-type'],
    });
    const fields = new URLSearchParams(calls.at(-1)!.body);
    if (status === 200 && typeof response === 'object' && response !== null && (response as any).result === 'success') {
      if (fields.has('topic')) currentTopic = fields.get('topic')!.trim();
      if (fields.has('stream_id')) currentStream = Number(fields.get('stream_id'));
    }
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
    tools, calls, authHeader, lookups,
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
const edits = [
  { label: 'topic only', fields: { topic } },
  ...['change_one', 'change_later', 'change_all'].map(propagate_mode => ({
    label: `topic with ${propagate_mode}`, fields: { topic, propagate_mode },
  })),
  { label: 'stream only', fields: { stream_id: 17 } },
  { label: 'stream and topic', fields: { topic, stream_id: 17 } },
  { label: 'stream with propagation', fields: { propagate_mode: 'change_later', stream_id: 17 } },
  { label: 'empty topic', fields: { topic: '' } },
];
for (const { label, fields } of edits) {
  test(`move_messages encodes ${label} in the PATCH body`, async () => {
    const response = { result: 'success', msg: '' };
    const f = await fixture(response);
    try {
      assert.deepEqual(await f.tools.handleToolCall('move_messages', {
        message_id: 42, ...fields, extra: 'not sent',
      }), 'stream_id' in fields ? { ...response, moved_to: { channelId: 'zulip:archive', stream: 'archive', topic: 'topic' in fields ? fields.topic.trim() : 'deploys' } } : { ...response, moved_to: { channelId: 'zulip:general', stream: 'general', topic: 'topic' in fields ? fields.topic : 'deploys' } });
      if ('stream_id' in fields) {
        assert.equal(f.lookups.length, 2, 'all moves read original and resulting placement');
        assert.ok(f.lookups.every(path => path.startsWith('/api/v1/messages/')), 'readable destinations need no stream enumeration');
      } else {
        assert.equal(f.lookups.length, 2, 'same-stream moves also reconcile from tool success');
      }
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

test('move_messages requires a topic or stream_id without making a request', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const fields of [{}, { propagate_mode: 'change_all' }]) {
      await assert.rejects(f.tools.handleToolCall('move_messages', { message_id: 42, ...fields }),
        /^Error: topic or stream_id is required$/);
    }
    assert.deepEqual(f.calls, []);
  } finally {
    await f.close();
  }
});

for (const { field, invalid, error } of [
  { field: 'propagate_mode', invalid: ['all', 'CHANGE_ALL', '', 1, false, {}],
    error: /^Error: propagate_mode must be change_one, change_later, or change_all$/ },
  { field: 'stream_id', invalid: [0, -1, 1.5, '17', false, NaN, Infinity, {}],
    error: /^Error: stream_id must be a positive integer$/ },
  { field: 'topic', invalid: [1, false, {}], error: /^Error: topic must be a string$/ },
]) {
  test(`move_messages rejects invalid ${field} without making a request`, async () => {
    const f = await fixture({ result: 'success', msg: '' });
    try {
      for (const value of invalid) {
        await assert.rejects(f.tools.handleToolCall('move_messages', {
          message_id: 42, topic, [field]: value,
        }), error);
      }
      assert.deepEqual(f.calls, []);
    } finally {
      await f.close();
    }
  });
}

test('move_messages surfaces realm permission errors for topic moves', async () => {
  const f = await fixture({ result: 'error', msg: 'You do not have permission to move this message' }, 400);
  try {
    await assert.rejects(f.tools.handleToolCall('move_messages', {
      message_id: 42, topic, stream_id: 17, propagate_mode: 'change_all',
    }), /HTTP 400.*You do not have permission to move this message/);
    assert.equal(f.calls.length, 1, 'permissions are checked by Zulip, not locally');
  } finally {
    await f.close();
  }
});

test('move_messages schema exposes optional topic, propagation, and stream fields', () => {
  const schema = toolDefinitions.find(tool => tool.name === 'move_messages')!.inputSchema;
  assert.deepEqual(schema.required, ['message_id']);
  const properties = schema.properties as Record<string, Record<string, unknown>>;
  assert.equal(properties.content, undefined);
  assert.deepEqual(properties.topic.type, ['string', 'null']);
  assert.deepEqual(properties.stream_id.type, ['integer', 'null']);
  assert.equal(properties.stream_id.minimum, 1);
  assert.deepEqual(properties.propagate_mode.enum, ['change_one', 'change_later', 'change_all', null]);
  assert.equal(properties.propagate_mode.default, undefined, 'Zulip chooses the default');
});


test('move_messages validates message_id before edit fields', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const args of [{}, { message_id: 0, content: null, topic: false, stream_id: -1, propagate_mode: 'invalid' }]) {
      await assert.rejects(f.tools.handleToolCall('move_messages', args), /^Error: message_id is required$/);
    }
    assert.deepEqual(f.calls, []);
  } finally {
    await f.close();
  }
});


test('edit_message stays content-only and rejects moves without a request', async () => {
  const tool = toolDefinitions.find(tool => tool.name === 'edit_message')!;
  assert.deepEqual(tool.inputSchema.required, ['message_id', 'content']);
  assert.deepEqual(Object.keys(tool.inputSchema.properties), ['message_id', 'content']);
  assert.match(tool.description, /your own/);
  const f = await fixture({ result: 'success', msg: '' });
  try {
    for (const fields of [{ topic }, { stream_id: 17 }]) {
      await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated', ...fields }), /use move_messages/);
    }
    await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42 }), /content must be a string/);
    for (const propagate_mode of ['change_later', 'change_all']) {
      await assert.rejects(f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated', propagate_mode }), /propagate_mode requires topic or stream_id/);
    }
    await assert.rejects(f.tools.handleToolCall('move_messages', { message_id: 42, content: 'updated', topic }), /use edit_message/);
    assert.deepEqual(f.calls, []);
  } finally { await f.close(); }
});

test('edit_message accepts explicit default propagation on a content edit', async () => {
  const f = await fixture({ result: 'success', msg: '' });
  try {
    await f.tools.handleToolCall('edit_message', { message_id: 42, content: 'updated', propagate_mode: 'change_one' });
    assert.equal(f.calls.length, 1);
    assert.equal(f.calls[0].body, 'content=updated');
  } finally { await f.close(); }
});

test('move_messages is independently named and classed as a messaging tool', () => {
  assert.deepEqual(TOOL_CLASSES.move_messages, ['comms']);
  assert.equal(featureSetForTool('move_messages'), 'zulip.messaging');
});

test('move_messages describes visible breadcrumbs and resolving a whole topic', () => {
  const tool = toolDefinitions.find(tool => tool.name === 'move_messages')!;
  assert.match(tool.description, /Notification Bot breadcrumb in the destination topic by default/);
  assert.match(tool.description, /resolve a topic.*✔ <topic> with change_all/);
  assert.match(tool.description, /For a move refusal, reply with send_message\(\{reply_to: <messageId>, content\}\)/);
  assert.match(tool.description, /deletion\/hidden-move refusal needs a newer readable message id with reply_to, or an explicit destination/);
  const properties = tool.inputSchema.properties as Record<string, Record<string, unknown>>;
  assert.match(String(properties.propagate_mode.description), /Zulip defaults to change_one/);
  assert.match(String(properties.propagate_mode.description), /change_later moves this and later messages/);
});

test('move_messages preserves structured move limits for a partial-move retry', async () => {
  for (const status of [400, 200]) {
    const f = await fixture({
      result: 'error', msg: 'You only have permission to move the 3/10 most recent messages in this topic.',
      code: 'MOVE_MESSAGES_TIME_LIMIT_EXCEEDED',
      first_message_id_allowed_to_move: 123, total_messages_in_topic: 10, total_messages_allowed_to_move: 3,
    }, status);
    try {
      await assert.rejects(f.tools.handleToolCall('move_messages', { message_id: 42, topic, propagate_mode: 'change_all' }), (error: Error) => {
        assert.match(error.message, /3\/10 most recent messages/);
        assert.match(error.message, /code=MOVE_MESSAGES_TIME_LIMIT_EXCEEDED/);
        assert.match(error.message, /first_message_id_allowed_to_move=123/);
        assert.match(error.message, /total_messages_in_topic=10/);
        assert.match(error.message, /total_messages_allowed_to_move=3/);
        return true;
      });
    } finally { await f.close(); }
  }
});

test('move_messages resolves destination names only as a fallback after losing visibility', async () => {
  for (const topic of [undefined, 'destination']) {
    const f = await fixture({ result: 'success', msg: '' }, 200, true);
    try {
      const result = await f.tools.handleToolCall('move_messages', { message_id: 42, stream_id: 17, topic, propagate_mode: 'change_all' });
      assert.deepEqual(result.moved_to, { channelId: 'zulip:archive', stream: 'archive', topic: topic === undefined ? 'deploys' : null });
      assert.equal(f.calls.length, 1);
      assert.equal(f.lookups.length, 3, 'pre-move, post-move, and fallback stream lookup');
      assert.ok(f.lookups.at(-1)!.startsWith('/api/v1/streams'), 'stream names are requested only after the placement read fails');
      assert.equal(f.lookups.filter(path => path.startsWith('/api/v1/streams')).length, 1);
    } finally { await f.close(); }
  }
});

test('structured error codes retain credential redaction and ignore nonnumeric retry fields', async () => {
  const f = await fixture({
    result: 'error', msg: 'Permission denied', code: '<b>test-</b><i>key</i>',
    first_message_id_allowed_to_move: 'test-key',
  }, 400);
  try {
    await assert.rejects(f.tools.handleToolCall('move_messages', { message_id: 42, topic }), (error: Error) => {
      assert.match(error.message, /code=\[redacted\]/);
      assert.doesNotMatch(error.message, /test-|key|<b>|first_message_id_allowed_to_move/);
      return true;
    });
  } finally { await f.close(); }
});

test('successful moves remain successful when destination lookups are unavailable', async () => {
  const f = await fixture({ result: 'success', msg: '' }, 200, false, true);
  try {
    const result = await f.tools.handleToolCall('move_messages', { message_id: 42, stream_id: 17 });
    assert.equal(result.result, 'success');
    assert.deepEqual(result.moved_to, { channelId: null, stream: null, topic: null });
    assert.match(result.routing_note, /Move succeeded.*before publishing/);
    assert.equal(f.calls.length, 1, 'the move is not retried or reported as a failure');
  } finally { await f.close(); }
});

for (const [name, verb, fields] of [
  ['edit_message', 'editing', { content: 'updated' }],
  ['move_messages', 'moving', { topic: 'destination' }],
] as const) {
  for (const status of [200, 400]) {
    test(`${name} names the operation and ${status === 200 ? 'omits a successful HTTP status from' : 'preserves a failed HTTP status in'} API errors`, async () => {
      const f = await fixture({ result: 'error', msg: 'Permission denied' }, status);
      try {
        await assert.rejects(f.tools.handleToolCall(name, { message_id: 42, ...fields }), (error: Error) => {
          assert.equal(error.message, `Zulip refused ${verb} message 42: ${status === 200 ? '' : 'HTTP 400: '}Permission denied`);
          return true;
        });
      } finally { await f.close(); }
    });
  }
}

test('edit_message keeps live-status anti-flood and expired-window guidance', () => {
  const description = toolDefinitions.find(tool => tool.name === 'edit_message')!.description;
  assert.match(description, /an expired window returns an error/);
  assert.match(description, /post once with send_message, then update it in place instead of flooding the topic/);
});

test('move_messages preserves a null partial-retry target', async () => {
  const f = await fixture({ result: 'error', msg: 'No messages may move',
    code: 'MOVE_MESSAGES_TIME_LIMIT_EXCEEDED', first_message_id_allowed_to_move: null,
    total_messages_in_topic: 10, total_messages_allowed_to_move: 0 }, 400);
  try {
    await assert.rejects(f.tools.handleToolCall('move_messages', { message_id: 42, topic: 'next' }),
      /first_message_id_allowed_to_move=null/);
  } finally { await f.close(); }
});

test('a requested topic is unconfirmed when post-move placement cannot be read', async () => {
  const f = await fixture({ result: 'success', msg: '' }, 200, true);
  try {
    const result = await f.tools.handleToolCall('move_messages', { message_id: 42, stream_id: 17, topic: '  deploys ' });
    assert.deepEqual(result.moved_to, { channelId: 'zulip:archive', stream: 'archive', topic: null });
    assert.match(result.routing_note, /Move succeeded.*before publishing/);
  } finally { await f.close(); }
});

for (const success of [false, true]) {
  test(`move result confirms placement ${success ? 'once on success' : 'never on failure'}`, async () => {
    const f = await fixture(success ? { result: 'success', msg: '' } : { result: 'error', msg: 'Permission denied' }, success ? 200 : 400);
    const completions: any[] = [];
    f.tools.onMoved = completed => { completions.push(completed); };
    try {
      const call = f.tools.handleToolCall('move_messages', { message_id: 42, topic: ' normalized ', propagate_mode: 'change_all' });
      if (success) await call;
      else await assert.rejects(call, /Permission denied/);
      assert.equal(completions.length, success ? 1 : 0);
      if (success) {
        assert.equal(completions[0].topic, 'normalized');
        assert.equal(completions[0].destinationChannelId, 'zulip:general');
      } // Failed writes emit no placement confirmation.
    } finally { await f.close(); }
  });
}

for (const fields of [{ topic: 'Next' }, { stream_id: 17 }, { topic: 'Next', stream_id: 17 }]) {
  test(`strict move_messages accepts unused null destination: ${JSON.stringify(fields)}`, async () => {
    const f = await fixture({ result: 'success' });
    try {
      const args = { message_id: 42, topic: null, stream_id: null, propagate_mode: 'change_one', ...fields };
      const before = structuredClone(args);
      assertStrictArguments('move_messages', args);
      await f.tools.handleToolCall('move_messages', args);
      assert.deepEqual(args, before, 'normalization must not mutate the caller');
      assert.deepEqual(Object.fromEntries(new URLSearchParams(f.calls[0].body)), {
        propagate_mode: 'change_one', ...Object.fromEntries(Object.entries(fields).map(([k, v]) => [k, String(v)])),
      });
    } finally { await f.close(); }
  });
}

test('move_messages treats null destinations as absent before requests', async () => {
  const f = await fixture({ result: 'success' });
  try {
    await assert.rejects(f.tools.handleToolCall('move_messages', {
      message_id: 42, topic: null, stream_id: null, propagate_mode: 'change_one',
    }), /^Error: topic or stream_id is required$/);
    assert.deepEqual(f.calls, []); assert.deepEqual(f.lookups, []);
  } finally { await f.close(); }
});

test('move_messages null propagation leaves the server default out of the PATCH', async () => {
  const f = await fixture({ result: 'success' });
  try {
    const args = { message_id: 42, topic: null, stream_id: 17, propagate_mode: null };
    assertStrictArguments('move_messages', args);
    await f.tools.handleToolCall('move_messages', args);
    assert.deepEqual(Object.fromEntries(new URLSearchParams(f.calls[0].body)), { stream_id: '17' });
  } finally { await f.close(); }
});
