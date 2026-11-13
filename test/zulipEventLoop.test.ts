import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipEventLoop, type ZulipMessageChange } from '../src/platforms/zulip-events.ts';
import type { PlatformSystemEvent } from '../src/platforms/adapter.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';
import { standInApi } from './zulip-stand-in.ts';

function html502Error(): Error {
  return new ZulipNoAnswer('not-json', 'GET', '/events', false, "Zulip's answer to GET /events wasn't JSON (HTTP 502).", 502);
}

function econnresetError(): Error {
  return new ZulipNoAnswer('network', 'GET', '/events', false, 'The connection to Zulip failed during GET /events (ECONNRESET).');
}

function makeSleepRecorder(loop: () => ZulipEventLoop, stopAfter: number) {
  const delays: number[] = [];
  const sleep = async (ms: number) => {
    delays.push(ms);
    if (delays.length >= stopAfter) loop().stop();
  };
  return { delays, sleep };
}

test('an HTML/proxy 502 backs off exponentially', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 6);
  const systemEvents: PlatformSystemEvent[] = [];

  loop = new ZulipEventLoop({ sleep });

  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => { throw html502Error(); },
  }).api;

  await loop.start(api, () => {}, (e) => systemEvents.push(e));

  assert.deepEqual(delays, [2000, 4000, 8000, 16000, 32000, 60000]);
  for (let i = 1; i < delays.length - 1; i++) {
    assert.ok(delays[i] > delays[i - 1], `delay ${i} should grow`);
  }

  const degraded = systemEvents.filter(e => e.kind === 'degraded');
  assert.equal(degraded.length, 1);
  assert.match(degraded[0].text, /non-JSON/);

  assert.equal(systemEvents.filter(e => e.kind === 'gap').length, 0);
});

test('a dropped connection is NOT treated as expiry', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 4);
  const systemEvents: PlatformSystemEvent[] = [];
  let registerCalls = 0;

  loop = new ZulipEventLoop({ sleep });

  const api = standInApi({
    'POST /register': async () => { registerCalls++; return { queue_id: 'q1', last_event_id: -1 }; },
    'GET /events': async () => { throw econnresetError(); },
  }).api;

  await loop.start(api, () => {}, (e) => systemEvents.push(e));

  assert.equal(registerCalls, 1, 'must not re-register the live queue on a transient error');
  assert.equal(systemEvents.filter(e => e.kind === 'gap').length, 0);
  assert.deepEqual(delays, [2000, 4000, 8000, 16000]);
});

test('a poll unanswered past its bound fails that poll: the loop backs off, asks again and marks itself degraded', { timeout: 5000 }, async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 3);
  const systemEvents: PlatformSystemEvent[] = [];
  let registerCalls = 0;

  loop = new ZulipEventLoop({ sleep, pollTimeoutMs: 20 });

  const { api, calls } = standInApi({
    'POST /register': async () => { registerCalls++; return { queue_id: 'q1', last_event_id: -1 }; },
    'GET /events': () => new Promise((resolve) => setTimeout(() => resolve({ result: 'success', events: [] }), 200)),
  });

  await loop.start(api, () => {}, (e) => systemEvents.push(e));

  assert.equal(calls.filter((c) => c.path === '/events').length, 3, 'each failed poll is asked again');
  assert.equal(registerCalls, 1, 'an unanswered poll is not queue expiry');
  assert.deepEqual(delays, [2000, 4000, 8000]);
  const degraded = systemEvents.filter((e) => e.kind === 'degraded');
  assert.equal(degraded.length, 1);
  assert.match(degraded[0].text, /no answer/i);
  assert.doesNotMatch(degraded[0].text, /\.\)/, 'no period inside the parentheses');
});

test('a long poll waits up to 90 s by default', async () => {
  const bounds: (number | undefined)[] = [];
  const loop = new ZulipEventLoop({ sleep: async () => {} });
  const { api } = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => { loop.stop(); return { result: 'success', events: [] }; },
  });
  await loop.start({
    ...api,
    requestAnswer: ((verb, path, params, opts) => {
      if (path === '/events') bounds.push(opts?.timeoutMs);
      return api.requestAnswer(verb, path, params, opts);
    }) as typeof api.requestAnswer,
  }, () => {});
  assert.deepEqual(bounds, [90_000]);
});

test('backoff caps at maxBackoffMs', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 5);

  loop = new ZulipEventLoop({ sleep, baseBackoffMs: 100, maxBackoffMs: 250 });

  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => { throw econnresetError(); },
  }).api;

  await loop.start(api, () => {});
  assert.deepEqual(delays, [100, 200, 250, 250, 250]);
});

test('queue expiry is a RETURNED error object; re-registers and emits a message-anchored gap marker', async () => {
  const systemEvents: PlatformSystemEvent[] = [];
  let registerCalls = 0;
  let retrieveCalls = 0;

  const loop = new ZulipEventLoop({ sleep: async () => {} });

  const api = standInApi({
    'POST /register': async () => {
      registerCalls++;
      return { queue_id: `q${registerCalls}`, last_event_id: -1 };
    },
    'GET /events': async () => {
      retrieveCalls++;
      if (retrieveCalls === 1) {
        return {
          events: [{
            id: 42,
            type: 'message',
            flags: [],
            message: {
              id: 9001,
              timestamp: 1_710_000_000,
              display_recipient: 'general',
              type: 'stream',
              subject: 'topic',
              content: 'hi',
              sender_id: 7,
              sender_full_name: 'Someone',
              sender_email: 's@example.com',
            },
          }],
        };
      }
      if (retrieveCalls === 2) {
        return { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: 'Bad event queue ID: q1' };
      }
      loop.stop();
      return { events: [] };
    },
  }).api;

  await loop.start(api, () => {}, (e) => systemEvents.push(e));

  assert.equal(registerCalls, 2, 'should re-register after expiry');

  const gaps = systemEvents.filter(e => e.kind === 'gap');
  assert.equal(gaps.length, 1, 'exactly one gap marker');
  assert.match(gaps[0].text, /may have been missed/);
  assert.equal(gaps[0].metadata?.expiredQueueId, 'q1');
  // The anchor is the last delivered MESSAGE id/timestamp, not the queue-local
  // event id — that's what makes "check history" actionable.
  assert.equal(gaps[0].metadata?.lastMessageId, 9001);
  assert.equal(gaps[0].metadata?.lastMessageTimestamp, 1_710_000_000);
  assert.equal(gaps[0].metadata?.newQueueId, 'q2');
  assert.match(gaps[0].text, /message id 9001/);
});

test('malformed non-throwing response (no events, no error code) sleeps instead of hot-spinning', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 3);
  let retrieveCalls = 0;

  loop = new ZulipEventLoop({ sleep });

  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      retrieveCalls++;
      return {};
    },
  }).api;

  await loop.start(api, () => {});

  assert.equal(retrieveCalls, 3);
  assert.deepEqual(delays, [2000, 4000, 8000]);
});

test('a refused poll, other than an expired queue, is logged and marked degraded with Zulip\'s words, and backs off as a malformed one does', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 3);
  const systemEvents: PlatformSystemEvent[] = [];
  loop = new ZulipEventLoop({ sleep, degradedThreshold: 3 });
  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': { result: 'error', msg: 'Invalid API key', code: 'INVALID_API_KEY' },
  }).api;
  const logged: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => { logged.push(args.map(String).join(' ')); };
  try {
    await loop.start(api, () => {}, (e) => systemEvents.push(e));
  } finally {
    console.error = original;
  }
  assert.deepEqual(delays, [2000, 4000, 8000]);
  assert.deepEqual(logged.filter((l) => /events\.retrieve/.test(l)), [1, 2, 3].map((n) =>
    `Zulip refused events.retrieve: Invalid API key (INVALID_API_KEY) (attempt ${n}); retrying in ${[2000, 4000, 8000][n - 1]}ms`));
  const degraded = systemEvents.filter((e) => e.kind === 'degraded');
  assert.equal(degraded.length, 1);
  assert.equal((degraded[0].metadata as { reason?: string }).reason, 'events.retrieve was refused: Invalid API key (INVALID_API_KEY)');
});

test('well-formed response resets the backoff counter', async () => {
  let loop: ZulipEventLoop;
  const { delays, sleep } = makeSleepRecorder(() => loop, 4);
  let retrieveCalls = 0;

  loop = new ZulipEventLoop({ sleep });

  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      retrieveCalls++;
      if (retrieveCalls === 3) return { events: [] };
      throw econnresetError();
    },
  }).api;

  await loop.start(api, () => {});
  assert.deepEqual(delays, [2000, 4000, 2000, 4000]);
});

test('degraded is followed by a recovered event once polling succeeds again', async () => {
  const systemEvents: PlatformSystemEvent[] = [];
  let loop: ZulipEventLoop;
  let retrieveCalls = 0;

  loop = new ZulipEventLoop({
    sleep: async () => {},
    degradedThreshold: 3,
  });

  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      retrieveCalls++;
      if (retrieveCalls <= 3) throw econnresetError();
      loop.stop();
      return { events: [] };
    },
  }).api;

  await loop.start(api, () => {}, (e) => systemEvents.push(e));

  const degraded = systemEvents.filter(e => e.kind === 'degraded');
  const recovered = systemEvents.filter(e => e.kind === 'recovered');
  assert.equal(degraded.length, 1, 'one degraded');
  assert.equal(recovered.length, 1, 'one recovered');
  assert.match(recovered[0].text, /healthy again/);
  assert.equal(recovered[0].metadata?.recoveredAfter, 3);
});

test('a throwing onMessage does not abort the batch or inflate the failure counter', async () => {
  const systemEvents: PlatformSystemEvent[] = [];
  const delivered: number[] = [];
  let loop: ZulipEventLoop;
  let retrieveCalls = 0;

  loop = new ZulipEventLoop({ sleep: async () => {}, degradedThreshold: 3 });

  const makeMessageEvent = (eventId: number, msgId: number) => ({
    id: eventId,
    type: 'message',
    flags: [],
    message: {
      id: msgId,
      timestamp: 1_710_000_000 + msgId,
      display_recipient: 'general',
      type: 'stream',
      subject: 't',
      content: 'c',
      sender_id: 1,
      sender_full_name: 'A',
      sender_email: 'a@example.com',
    },
  });

  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      retrieveCalls++;
      if (retrieveCalls === 1) {
        return { events: [makeMessageEvent(1, 100), makeMessageEvent(2, 101)] };
      }
      loop.stop();
      return { events: [] };
    },
  }).api;

  const onMessage = (_stream: string, msg: { id: number }) => {
    if (msg.id === 100) throw new Error('handler blew up');
    delivered.push(msg.id);
  };

  await loop.start(api, onMessage, (e) => systemEvents.push(e));

  assert.deepEqual(delivered, [101], 'rest of the batch survives a throwing handler');
  assert.equal(systemEvents.filter(e => e.kind === 'degraded').length, 0);
});

test('reaction events are forwarded to the reaction handler and never to onMessage', async () => {
  let loop: ZulipEventLoop;
  const { sleep } = makeSleepRecorder(() => loop, 1);
  loop = new ZulipEventLoop({ sleep });
  const registered: Record<string, unknown>[] = [];
  let polls = 0;
  const api = standInApi({
    'POST /register': async (params: Record<string, unknown>) => { registered.push(params); return { queue_id: 'q1', last_event_id: -1 }; },
    'GET /events': async () => {
      polls++;
      if (polls > 1) { loop.stop(); return { events: [] }; }
      return {
        events: [
          { id: 1, type: 'reaction', op: 'add', emoji_name: 'thumbs_up', emoji_code: '1f44d', reaction_type: 'unicode_emoji', message_id: 77, user_id: 9, user: { user_id: 9, full_name: 'Ann', email: 'ann@example.com' } },
          { id: 2, type: 'reaction', op: 'remove', emoji_name: 'eyes', emoji_code: '1f440', reaction_type: 'unicode_emoji', message_id: 78, user_id: 9 },
          { id: 3, type: 'heartbeat' },
        ],
      };
    },
  }).api;
  const messages: unknown[] = [];
  const reactions: { op: string; emoji_name: string; message_id: number; name?: string }[] = [];
  await loop.start(api, (_s, m) => { messages.push(m); }, undefined, (ev) => {
    reactions.push({ op: ev.op, emoji_name: ev.emoji_name, message_id: ev.message_id, name: ev.user?.full_name });
  });
  assert.deepEqual(registered[0].event_types, ['message', 'reaction', 'update_message', 'delete_message', 'stream'], 'the queue asks for reactions, edits, deletions and stream changes');
  assert.equal(registered[0].apply_markdown, false, 'the queue asks for raw markdown; cleanMarkdown depends on it (#24)');
  // bulk_message_deletion: a topic deletion arrives as one event, not N.
  // notification_settings_null at its default: Zulip 12 refuses the
  // registration when it is missing.
  assert.deepEqual(registered[0].client_capabilities, { notification_settings_null: false, bulk_message_deletion: true });
  assert.deepEqual(messages, []);
  assert.deepEqual(reactions, [
    { op: 'add', emoji_name: 'thumbs_up', message_id: 77, name: 'Ann' },
    { op: 'remove', emoji_name: 'eyes', message_id: 78, name: undefined },
  ]);
});

test('edits, topic moves and deletions reach onChange; re-renders and unchanged updates do not (#22)', async () => {
  let loop: ZulipEventLoop;
  const { sleep } = makeSleepRecorder(() => loop, 1);
  loop = new ZulipEventLoop({ sleep });
  let polls = 0;
  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls > 1) { loop.stop(); return { events: [] }; }
      return {
        events: [
          { id: 1, type: 'update_message', message_id: 77, message_ids: [77], user_id: 9, edit_timestamp: 1_710_000_100, rendering_only: false, orig_content: 'old', content: 'new @**bot**', flags: ['mentioned'], stream_id: 7 },
          { id: 2, type: 'update_message', message_id: 78, message_ids: [78], user_id: null, edit_timestamp: 1_710_000_101, rendering_only: true, content: 'with preview', stream_id: 7 },
          { id: 3, type: 'update_message', message_id: 79, message_ids: [79, 80, 81], user_id: 12, edit_timestamp: 1_710_000_102, orig_subject: 'old topic', subject: 'new topic', propagate_mode: 'change_all', stream_id: 7 },
          // A move to another stream under the same topic name: Zulip sends
          // orig_subject for every move but subject only for a rename.
          { id: 4, type: 'update_message', message_id: 82, message_ids: [82], user_id: 12, edit_timestamp: 1_710_000_103, stream_id: 7, new_stream_id: 8, orig_subject: 'same' },
          { id: 5, type: 'update_message', message_id: 83, message_ids: [83], user_id: 9, edit_timestamp: 1_710_000_104, stream_id: 7 },
          { id: 6, type: 'delete_message', message_ids: [84, 85], message_type: 'stream', stream_id: 7, topic: 'deploys' },
          { id: 7, type: 'delete_message', message_id: 86, message_type: 'private' },
          { id: 8, type: 'heartbeat' },
        ],
      };
    },
  }).api;
  const messages: unknown[] = [];
  const changes: ZulipMessageChange[] = [];
  await loop.start(api, (_s, m) => { messages.push(m); }, undefined, undefined, (c) => { changes.push(c); });
  assert.deepEqual(messages, [], 'changes never reach onMessage');
  assert.deepEqual(changes, [
    { kind: 'edit', messageId: 77, messageIds: [77], actorId: 9, editedAt: 1_710_000_100, content: 'new @**bot**', origContent: 'old', topic: null, origTopic: null, streamId: 7, newStreamId: null, flags: ['mentioned'] },
    { kind: 'edit', messageId: 79, messageIds: [79, 80, 81], actorId: 12, editedAt: 1_710_000_102, content: null, origContent: null, topic: 'new topic', origTopic: 'old topic', streamId: 7, newStreamId: null, flags: [] },
    { kind: 'edit', messageId: 82, messageIds: [82], actorId: 12, editedAt: 1_710_000_103, content: null, origContent: null, topic: null, origTopic: 'same', streamId: 7, newStreamId: 8, flags: [] },
    { kind: 'delete', messageIds: [84, 85], messageType: 'stream', streamId: 7, topic: 'deploys' },
    { kind: 'delete', messageIds: [86], messageType: 'private', streamId: null, topic: null },
  ]);
});

test('a throwing onChange handler does not abort the batch', async () => {
  let loop: ZulipEventLoop;
  const { sleep } = makeSleepRecorder(() => loop, 1);
  loop = new ZulipEventLoop({ sleep });
  let polls = 0;
  const api = standInApi({
    'POST /register': async () => ({ queue_id: 'q1', last_event_id: -1 }),
    'GET /events': async () => {
      polls++;
      if (polls > 1) { loop.stop(); return { events: [] }; }
      return {
        events: [
          { id: 1, type: 'update_message', message_id: 1, user_id: 9, edit_timestamp: 1, orig_content: 'a', content: 'b' },
          { id: 2, type: 'delete_message', message_id: 2, message_type: 'stream', stream_id: 7 },
        ],
      };
    },
  }).api;
  const seen: string[] = [];
  const original = console.error;
  console.error = () => {};
  try {
    await loop.start(api, () => {}, undefined, undefined, (c) => {
      if (c.kind === 'edit') throw new Error('handler blew up');
      seen.push(c.kind);
    });
  } finally {
    console.error = original;
  }
  assert.deepEqual(seen, ['delete']);
});

test('a refused queue registration is surfaced and retried, never polled as queue "undefined"', async () => {
  let loop: ZulipEventLoop;
  const pauses: number[] = [];
  loop = new ZulipEventLoop({ sleep: async (ms: number) => { pauses.push(ms); } });
  let registers = 0;
  const polledQueues: unknown[] = [];
  const api = standInApi({
    'POST /register': async () => {
      registers++;
      // What Zulip 12 answered live: an error body, returned as a value.
      if (registers === 1) {
        return { result: 'error', msg: 'client_capabilities["notification_settings_null"] field is missing: Field required', code: 'BAD_REQUEST' } as unknown as { queue_id: string; last_event_id: number };
      }
      return { queue_id: 'q2', last_event_id: -1 };
    },
    'GET /events': async (p: { queue_id: string }) => {
      polledQueues.push(p.queue_id);
      loop.stop();
      return { events: [] };
    },
  }).api;
  const logged: string[] = [];
  const original = console.error;
  console.error = (...args: unknown[]) => { logged.push(args.map(String).join(' ')); };
  try {
    await loop.start(api, () => {});
  } finally {
    console.error = original;
  }
  assert.equal(registers, 2, 're-registered after the refusal');
  assert.deepEqual(polledQueues, ['q2'], 'only the real queue is polled');
  assert.ok(logged.some((l) => /registration failed: client_capabilities\["notification_settings_null"\] field is missing.*BAD_REQUEST/.test(l)), 'Zulip\'s reason is logged');
  assert.ok(!logged.some((l) => /no events array/.test(l)), 'not misreported as a malformed poll');
  assert.deepEqual(pauses, [5000], 'the outer loop pauses before retrying');
});

test('a long poll completed after stop delivers none of its events', async () => {
  const loop = new ZulipEventLoop();
  let release!: (response: unknown) => void;
  let polling!: () => void;
  const polled = new Promise<void>((resolve) => { polling = resolve; });
  const seen: string[] = [];
  const running = loop.start(standInApi({
    'POST /register': async () => ({ queue_id: 'q', last_event_id: -1 }),
    'GET /events': () => new Promise((resolve) => { release = resolve; polling(); }),
  }).api, () => { seen.push('message'); }, undefined, () => { seen.push('reaction'); }, () => { seen.push('change'); });
  await polled;
  loop.stop();
  release({ events: [
    { id: 1, type: 'reaction', message_id: 1, user_id: 101 },
    { id: 2, type: 'delete_message', message_ids: [1], message_type: 'stream', stream_id: 7 },
  ] });
  await running;
  assert.deepEqual(seen, []);
});

test('stopping in a message handler drops all subsequent events in its batch', async () => {
  const loop = new ZulipEventLoop();
  const seen: string[] = [];
  await loop.start(standInApi({
    'POST /register': async () => ({ queue_id: 'q', last_event_id: -1 }),
    'GET /events': async () => ({ events: [
      { id: 1, type: 'message', message: { id: 1, type: 'stream', display_recipient: 'general', timestamp: 1 } },
      { id: 2, type: 'reaction', message_id: 1, user_id: 101 },
      { id: 3, type: 'delete_message', message_ids: [1], message_type: 'stream', stream_id: 7 },
    ] }),
  }).api, () => {
    seen.push('message');
    loop.stop();
  }, undefined, () => { seen.push('reaction'); }, () => { seen.push('change'); });
  assert.deepEqual(seen, ['message']);
});
