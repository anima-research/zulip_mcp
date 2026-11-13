import test, { beforeEach } from 'node:test';
import assert from 'node:assert/strict';
import type { KeyedMessage } from '../src/channel-key.ts';
import type { IncomingEntry, MessageChangeEvent } from '../src/platforms/adapter.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import type { ZulipRawMessage } from '../src/history.ts';
import { inTurn, oneLane, standInApi } from './zulip-stand-in.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import { dmKey, streamKey, type ChannelKey } from '../src/channel-key.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';

beforeEach(() => {
  (ZulipAdapter as unknown as {
    quoteBudget: { tokens: number; updatedAt: number };
  }).quoteBudget = { tokens: 30, updatedAt: performance.now() };
});

const SELF = 100;
const STREAMS = [{ stream_id: 1, name: 'general' }];

const KNOWN = [
  ...STREAMS,
  { stream_id: 2, name: 'other' },
  ...Array.from({ length: 16 }, (_, ch) => ({ stream_id: 100 + ch, name: String(ch) })),
  ...Array.from({ length: 30 }, (_, i) => ({ stream_id: 1500 + i, name: `channel-${500 + i}` })),
];

function knownStreams(): StreamDirectory {
  const directory = new StreamDirectory();
  directory.registration(KNOWN);
  return directory;
}

function key(channelId: string): ChannelKey {
  const name = channelId.slice('zulip:'.length);
  if (name.startsWith('dm:')) return dmKey(name.slice(3).split('+').map(Number));
  return streamKey(KNOWN.find((s) => s.name === name)!.stream_id);
}
const QUOTED = 200;

function raw(id: number, content: string, over: Partial<ZulipRawMessage> = {}): ZulipRawMessage {
  return {
    id, content, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
    display_recipient: 'general', stream_id: 1, subject: 'topic', timestamp: 1_700_000_000,
    type: 'stream', flags: [], ...over,
  };
}

function quote(id = QUOTED, namedUser = SELF): string {
  return `@_**User|${namedUser}** [said](/#narrow/channel/1-general/near/${id}):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`;
}

async function receive(
  rows: ZulipRawMessage[],
  getById: (params: Record<string, unknown>) => Promise<unknown>,
  selfUserId: number | null = SELF,
): Promise<KeyedMessage[]> {
  const received: KeyedMessage[] = [];
  const sentinel = 999;
  let finish!: () => void;
  const finished = new Promise<void>((resolve) => { finish = resolve; });
  let served = false;
  const client = standInApi({
    'GET /messages/:id': getById,
    'POST /register': async () => ({ queue_id: 'q', last_event_id: -1, streams: STREAMS }),
    'GET /events': async () => {
      if (served) return new Promise(() => {});
      served = true;
      return { result: 'success', events: [...rows, raw(sentinel, 'done')].map((message, i) => ({
        id: i + 1, type: 'message', message, flags: message.flags ?? [],
      })) };
    },
  }).api;
  const adapter = new ZulipAdapter(client, { selfUserId, sessionId: 'session' });
  adapter.startEvents(inTurn(({ message }) => {
    if (message.messageId === String(sentinel)) finish();
    else received.push(message);
  }));
  try {
    await finished;
    return received;
  } finally {
    adapter.stopEvents();
  }
}

test('own-message quote is tagged as a reply without changing the mention verdict', { timeout: 2000 }, async () => {
  const calls: Record<string, unknown>[] = [];
  const messages = await receive([raw(300, quote(QUOTED, 102))], async (params, { id }) => {
    calls.push({ id, ...params });
    return { result: 'success', message: raw(QUOTED, 'original', { sender_id: SELF }) };
  });
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
  assert.ok(messages[0].tags?.includes('chat:reply'));
  assert.deepEqual(calls, [{ id: QUOTED, apply_markdown: false }]);
});

test('group DMs stay addressed without any quote lookup', { timeout: 2000 }, async () => {
  let gets = 0;
  const rows = [quote(), `@_**User|${SELF}** hello`, quote(201), 'plain message'].map((content, i) => raw(300 + i, content, {
    type: 'private', display_recipient: [SELF, 101, 102].map((id) => ({ id, full_name: 'User', email: `${id}@example.invalid` })),
  }));
  const messages = await receive(rows, async () => { gets++; throw new Error('unexpected GET'); });
  assert.equal(messages.length, rows.length);
  for (const message of messages) {
    assert.equal(message.metadata?.isDM, true);
    assert.equal(message.metadata?.mentioned, false);
    assert.ok(message.tags?.includes('chat:dm'));
  }
  assert.equal(gets, 0);
});

test('quote authors already seen locally need no GET; self messages remain ignored', { timeout: 2000 }, async () => {
  let gets = 0;
  const messages = await receive([
    raw(QUOTED, quote(), { sender_id: SELF }),
    raw(300, quote()),
  ], async () => { gets++; throw new Error('unexpected GET'); });
  assert.deepEqual(messages.map((m) => m.messageId), ['300']);
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
  assert.equal(gets, 0);
});

for (const [label, content] of [
  ['silent mention only', `@_**User|${SELF}** hello`],
  ['unrelated near link', `[message](/#narrow/channel/1-general/near/${QUOTED})`],
  ['said link without quote', quote().replace('```quote', '```text')],
  ['intervening text', quote().replace(':\n\n', ':\nnot a quote header\n')],
  ['quote syntax inside a code example', `\`\`\`\`text\n${quote()}\n\`\`\`\``],
  ['near link elsewhere before a different quote', `[message](/#narrow/near/${QUOTED})\n${quote(201)}`],
] as const) {
  test(`${label} does not address the connector`, { timeout: 2000 }, async () => {
    const calls: Record<string, unknown>[] = [];
    const messages = await receive([raw(300, content)], async (params, { id }) => {
      calls.push({ id, ...params });
      return { result: 'success', message: raw(id, 'original') };
    });
    assert.equal(messages[0].metadata?.mentioned, false);
    assert.ok(messages[0].tags?.includes('chat:ambient'));
    assert.deepEqual(calls, label === 'near link elsewhere before a different quote'
      ? [{ id: 201, apply_markdown: false }] : []);
  });
}

test('a quote of another sender is not addressed even when the silent mention names self', { timeout: 2000 }, async () => {
  const messages = await receive([raw(300, quote())], async () => ({ result: 'success', message: raw(QUOTED, 'original') }));
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.ok(messages[0].tags?.includes('chat:ambient'));
});

for (const failure of ['throw', 'deleted', 'missing'] as const) {
  test(`quote lookup fails closed on ${failure}, without losing later events`, { timeout: 2000 }, async () => {
    let gets = 0;
    const messages = await receive([raw(300, quote()), raw(301, quote()), raw(302, 'later')], async () => {
      gets++;
      if (failure === 'throw') throw new Error('unavailable');
      return failure === 'deleted' ? { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' } : { result: 'success' };
    });
    assert.deepEqual(messages.map((m) => m.messageId), ['300', '301', '302']);
    assert.equal(messages[0].metadata?.mentioned, false);
    assert.equal(gets, failure === 'deleted' ? 1 : 2);
  });
}

test('unknown self identity makes no quote lookup', { timeout: 2000 }, async () => {
  let gets = 0;
  const messages = await receive([raw(300, quote())], async () => { gets++; return {}; }, null);
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(gets, 0);
});

test('multiple quotes resolve only their own headers, once per id, in delivery order', { timeout: 2000 }, async () => {
  const calls: number[] = [];
  const messages = await receive([
    raw(300, `${quote(201)}\n${quote(201)}\n${quote()}`), raw(301, 'later'),
  ], async (_params, { id }) => {
    calls.push(id);
    await new Promise((resolve) => setTimeout(resolve, 5));
    if (id === 201) throw new Error('unavailable');
    return { result: 'success', message: raw(QUOTED, 'original', { sender_id: SELF }) };
  });
  assert.deepEqual(calls, [201, QUOTED]);
  assert.deepEqual(messages.map((m) => m.messageId), ['300', '301']);
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
});

test('absolute quote links, CRLF and longer quote fences are recognized', { timeout: 2000 }, async () => {
  const content = quote().replace('](/', '](https://example.invalid/').replaceAll('```', '````').replaceAll('\n', '\r\n');
  const messages = await receive([raw(300, content)], async () => ({
    result: 'success', message: raw(QUOTED, 'original', { sender_id: SELF }),
  }));
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
});

test('an existing mention needs no quote lookup and retains its addressing', { timeout: 2000 }, async () => {
  let gets = 0;
  const messages = await receive([raw(300, quote(), { flags: ['mentioned'] })], async () => { gets++; return {}; });
  assert.equal(messages[0].metadata?.mentioned, true);
  assert.ok(messages[0].tags?.includes('chat:mention'));
  assert.equal(gets, 0);
});

test('R2-2: a live timeout bounds waiting without poisoning a later lookup', { timeout: 5000 }, async () => {
  let gets = 0;
  let release!: (value: unknown) => void;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const started = performance.now();
  const receiving = receive([raw(300, quote()), raw(301, 'unrelated'), raw(302, quote())], async () => {
    if (++gets === 1) return new Promise((resolve) => { release = resolve; });
    return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
  });
  try {
    const messages = await Promise.race([
      receiving,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('unrelated message blocked beyond quote budget')), 3500);
      }),
    ]);
    assert.deepEqual(messages.map((m) => m.messageId), ['300', '301', '302']);
    assert.equal(messages[0].metadata?.mentioned, false);
    assert.equal(gets, 2);
    assert.equal(messages[2].metadata?.replyToSelf, true);
    assert.ok(performance.now() - started < 3500);
  } finally {
    if (timer) clearTimeout(timer);
    release({ result: 'error', msg: 'unavailable' });
    await receiving;
  }
});

test('many quote headings look up only the first two distinct ids', { timeout: 2000 }, async () => {
  const calls: number[] = [];
  const content = [201, 201, 202, 203, QUOTED, 204].map((id) => quote(id)).join('\n');
  const messages = await receive([raw(300, content)], async (_params, { id }) => {
    calls.push(id);
    return { result: 'success', message: raw(id, 'original', { sender_id: id === QUOTED ? SELF : 101 }) };
  });
  assert.deepEqual(calls, [201, 202]);
  assert.equal(messages[0].metadata?.mentioned, false, 'later targets deliberately do not affect addressing');
});

test('translated quote heading link text is recognized', { timeout: 2000 }, async () => {
  const messages = await receive([raw(300, quote().replace('[said]', '[dijo]'))], async () => ({
    result: 'success', message: raw(QUOTED, 'original', { sender_id: SELF }),
  }));
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
});

test('tilde quote fences are recognized', { timeout: 2000 }, async () => {
  const messages = await receive([raw(300, quote().replaceAll('```', '~~~'))], async () => ({
    result: 'success', message: raw(QUOTED, 'original', { sender_id: SELF }),
  }));
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
});

test('history quote replies resolve page authors before the seen cache or GETs', async () => {
  const rows = [raw(300, quote()), raw(QUOTED, 'own original', { sender_id: SELF })];
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: rows, found_newest: true }),
    'GET /messages/:id': async () => { throw new Error('page target must not need a GET'); },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 500 });
  assert.deepEqual(page.messages.map((m) => m.messageId), ['300']);
  assert.equal(page.messages[0].metadata?.replyToSelf, true);
  assert.equal(page.messages[0].metadata?.mentioned, false);
  assert.deepEqual(page.messages[0].tags, ['chat:reply', 'chat:from-human']);
  assert.equal(page.scannedThrough, 300);
});

test('history quote lookups share a page cap and still use cached authors after it is exhausted', async () => {
  let rows = [raw(QUOTED, 'own original', { sender_id: SELF })];
  const gets: number[] = [];
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: rows, found_newest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
      gets.push(message_id);
      return { message: raw(message_id, 'another author') };
    },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  await adapter.fetchHistory(key('zulip:general'), { limit: 500 });
  rows = Array.from({ length: 20 }, (_, i) => raw(300 + i, quote(500 + i)));
  rows.push(raw(400, quote()));
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 500 });
  assert.equal(gets.length, 8);
  assert.deepEqual(page.unverifiedMessageIds, Array.from({ length: 12 }, (_, i) => String(308 + i)));
  assert.ok(page.messages.slice(0, -1).every((m) => m.metadata?.replyToSelf === false));
  assert.equal(page.messages.at(-1)?.metadata?.replyToSelf, true);
});

test('history verifies an off-page own-message quote with a raw-message GET', async () => {
  const gets: unknown[] = [];
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: [raw(300, quote())], found_newest: true }),
    'GET /messages/:id': async (params: Record<string, unknown>, { id }: { id: number }) => {
      gets.push({ id, ...params });
      return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
    },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 500 });
  assert.equal(page.messages[0].metadata?.replyToSelf, true);
  assert.deepEqual(gets, [{ id: QUOTED, apply_markdown: false }]);
});

for (const [label, before, after, wasReply, isReply] of [
  ['keeps', quote(), quote().replace('reply', 'corrected reply'), true, true],
  ['removes', quote(), 'no quote now', true, false],
  ['adds', 'no quote yet', quote(), false, true],
] as const) {
  test(`an edit that ${label} a quote recomputes reply addressing independently of mentions`, { timeout: 2000 }, async () => {
    const changes: MessageChangeEvent[] = [];
    const incoming: KeyedMessage[] = [];
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => { finish = resolve; });
    let served = false;
    const adapter = new ZulipAdapter(standInApi({
      'POST /register': async () => ({ queue_id: 'q', last_event_id: -1, streams: STREAMS }),
      'GET /events': async () => {
        if (served) return new Promise(() => {});
        served = true;
        return { events: [
          { id: 1, type: 'message', message: raw(QUOTED, 'original', { sender_id: SELF }), flags: [] },
          { id: 2, type: 'message', message: raw(300, before), flags: [] },
          { id: 3, type: 'update_message', message_id: 300, user_id: 101, orig_content: before, content: after, flags: [] },
          { id: 4, type: 'delete_message', message_ids: [300], message_type: 'stream', stream_id: 1 },
        ] };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const lane = oneLane();
    adapter.startEvents(lane(({ message: m }) => { incoming.push(m); }), undefined, undefined, lane((c) => {
      changes.push(c);
      if (changes.length === 2) finish();
    }));
    try {
      await finished;
      assert.equal(incoming[0].metadata?.mentioned, false);
      assert.equal(incoming[0].metadata?.replyToSelf, wasReply);
      assert.equal(changes[0].kind, 'edit');
      assert.equal(changes[0].mentioned, false);
      assert.equal(changes[0].previouslyMentioned, false);
      assert.equal(changes[0].replyToSelf, isReply);
      assert.equal(changes[0].replyToSelfVerified, true);
      assert.equal(changes[0].previouslyReplyToSelf, wasReply);
      assert.equal(changes[1].kind, 'delete');
      assert.equal(changes[1].replyToSelf, isReply, 'deletions use the edited verdict');
    } finally { adapter.stopEvents(); }
  });
}

for (const [label, content] of [
  ['non-breaking space before the colon', quote().replace('):', ')\u00a0:')],
  ['full-width colon', quote().replace('):', ')：')],
  ['space between link label and URL', quote().replace('](', '] (')],
] as const) {
  test(`a quote heading with ${label} is recognized`, { timeout: 2000 }, async () => {
    const messages = await receive([raw(300, content)], async () => ({
      message: raw(QUOTED, 'original', { sender_id: SELF }),
    }));
    assert.equal(messages[0].metadata?.replyToSelf, true);
    assert.equal(messages[0].metadata?.mentioned, false);
  });
}

for (const suffix of [' in #**general>topic**:', ' to **User**:']) {
  test(`a forwarding-style quote heading ending${suffix} is not reply addressing`, { timeout: 2000 }, async () => {
    let gets = 0;
    const messages = await receive([raw(300, quote().replace('):', `)${suffix}`))], async () => { gets++; return {}; });
    assert.equal(messages[0].metadata?.replyToSelf, false);
    assert.equal(gets, 0);
  });
}

test('an ordinary later quote still counts after an excluded multi-quote heading', { timeout: 2000 }, async () => {
  const calls: number[] = [];
  const messages = await receive([raw(300, `${quote(201).replace('):', ') in #**general>topic**:')}\n${quote()}`)], async (_params, { id }) => {
    calls.push(id);
    return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
  });
  assert.equal(messages[0].metadata?.replyToSelf, true);
  assert.deepEqual(calls, [QUOTED]);
});

test('a bot-authored quote does not infer reply addressing or spend a lookup', { timeout: 2000 }, async () => {
  let gets = 0;
  const messages = await receive([raw(300, quote(), { sender_email: 'another-bot@example.invalid' })], async () => { gets++; return {}; });
  assert.equal(messages[0].metadata?.replyToSelf, false);
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.deepEqual(messages[0].tags, ['chat:ambient', 'chat:from-bot']);
  assert.equal(gets, 0);
});

test('a bot-authored explicit mention remains addressed without a quote lookup', { timeout: 2000 }, async () => {
  let gets = 0;
  const messages = await receive([raw(300, quote(), { sender_email: 'another-bot@example.invalid', flags: ['mentioned'] })], async () => { gets++; return {}; });
  assert.equal(messages[0].metadata?.mentioned, true);
  assert.deepEqual(messages[0].tags, ['chat:mention', 'chat:from-bot']);
  assert.equal(gets, 0);
});

test('history also withholds inferred addressing for bot-authored quotes', async () => {
  let gets = 0;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: [raw(300, quote(), { sender_email: 'another-bot@example.invalid' })], found_newest: true }),
    'GET /messages/:id': async () => { gets++; return { message: raw(QUOTED, 'original', { sender_id: SELF }) }; },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 500 });
  assert.equal(page.messages[0].metadata?.replyToSelf, false);
  assert.equal(gets, 0);
});

for (const restart of [false, true]) {
  test(`a quote lookup waits in its message's turn: the rest of the batch is handed at once, and a stop leaves the lookup to answer${restart ? ' across a restart' : ''}`, { timeout: 2000 }, async () => {
    let release!: (value: unknown) => void;
    let lookupStarted!: () => void;
    const started = new Promise<void>((resolve) => { lookupStarted = resolve; });
    let registrations = 0;
    const served = new Set<string>();
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages/:id': () => new Promise((resolve) => { release = resolve; lookupStarted(); }),
      'POST /register': async () => ({ queue_id: `q${++registrations}`, last_event_id: -1, streams: STREAMS }),
      'GET /events': async ({ queue_id }: { queue_id: string }) => {
        if (served.has(queue_id)) return new Promise(() => {});
        served.add(queue_id);
        const rows = queue_id === 'q1' ? [raw(300, quote()), raw(301, 'later in old batch')] : [raw(400, 'new connection')];
        return { events: rows.map((message, i) => ({ id: i + 1, type: 'message', message, flags: [] })) };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const oldEntries: IncomingEntry[] = [];
    const newMessages: string[] = [];
    try {
      let handed!: () => void;
      const bothHanded = new Promise<void>((resolve) => { handed = resolve; });
      adapter.startEvents((entry) => { if (oldEntries.push(entry) === 2) handed(); });
      await bothHanded;
      assert.deepEqual(oldEntries.map((entry) => entry.messageId), ['300', '301']);
      const quoting = oldEntries[0].ready();
      await started;
      adapter.stopEvents();
      if (restart) {
        let received!: () => void;
        const done = new Promise<void>((resolve) => { received = resolve; });
        adapter.startEvents(inTurn(({ message: m }) => { newMessages.push(m.messageId); received(); }));
        await done;
      }
      release({ message: raw(QUOTED, 'original', { sender_id: SELF }) });
      const ready = await quoting;
      assert.equal(ready?.message.messageId, '300');
      assert.equal(ready?.message.metadata?.replyToSelf, true);
      assert.deepEqual(newMessages, restart ? ['400'] : []);
    } finally {
      adapter.stopEvents();
      release?.({ result: 'error' });
    }
  });
}

for (const failure of ['deleted'] as const) {
  test(`history caches a ${failure} quote target until its retry window expires`, async (t) => {
    let now = Date.now();
    t.mock.method(Date, 'now', () => now);
    let gets = 0;
    let succeed = false;
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages': async () => ({ messages: [raw(300, quote())], found_newest: true }),
      'GET /messages/:id': async () => {
        gets++;
        if (succeed) return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
        return failure === 'deleted' ? { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' } : { result: 'success' };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const page = () => adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.equal((await page()).messages[0].metadata?.replyToSelf, false);
    now += 2 * 60_000;
    assert.equal((await page()).messages[0].metadata?.replyToSelf, false);
    assert.equal(gets, 1, 'repeated pages share the failed-target cache');
    now += 60_001;
    succeed = true;
    assert.equal((await page()).messages[0].metadata?.replyToSelf, true);
    assert.equal(gets, 2, 'expired failures can recover');
    assert.equal((await page()).messages[0].metadata?.replyToSelf, true);
    assert.equal(gets, 2, 'the successful author is now cached');
  });
}

test('negative-cache hits do not consume the history page GET allowance', async () => {
  let rows = Array.from({ length: 8 }, (_, i) => raw(300 + i, quote(500 + i)));
  let gets = 0;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: rows, found_newest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
      gets++;
      return message_id === QUOTED
        ? { message: raw(QUOTED, 'original', { sender_id: SELF }) }
        : { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' };
    },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  assert.equal(gets, 8);
  rows = [...rows, raw(400, quote())];
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  assert.equal(gets, 9, 'only the new target costs a GET');
  assert.equal(page.messages.at(-1)?.metadata?.replyToSelf, true);
});

for (const kind of ['reaction', 'edit'] as const) {
  test(`a ${kind} retries placement after a transient failed quote-target lookup`, { timeout: 2000 }, async () => {
    let gets = 0;
    const placed: string[] = [];
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => { finish = resolve; });
    let polls = 0;
    let quoted!: () => void;
    const quoteAnswered = new Promise<void>((resolve) => { quoted = resolve; });
    const following = kind === 'reaction'
      ? { type: 'reaction', op: 'add', emoji_name: 'eyes', message_id: QUOTED, user_id: 101 }
      : { type: 'update_message', message_id: QUOTED, user_id: 101, orig_content: 'original', content: 'corrected original', flags: [] };
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages/:id': async () => {
        gets++;
        if (gets === 1) throw new Error('503 transient');
        return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
      },
      'POST /register': async () => ({ queue_id: 'q', last_event_id: -1, streams: STREAMS }),
      'GET /events': async () => {
        polls++;
        if (polls === 1) return { events: [{ id: 1, type: 'message', message: raw(300, quote()), flags: [] }] };
        if (polls > 2) return new Promise(() => {});
        await quoteAnswered;
        return { events: [
          { id: 2, ...following },
          { id: 3, type: 'message', message: raw(999, 'done'), flags: [] },
        ] };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    adapter.startEvents(
      (entry) => {
        if (entry.messageId === '300') void entry.ready().then(() => quoted());
        if (entry.messageId === '999') finish();
      }, undefined,
      (ev) => { placed.push(ev.messageId); }, inTurn((ev) => { placed.push(ev.messageId); }),
    );
    try {
      await finished;
      await new Promise((resolve) => setImmediate(resolve));
      assert.equal(gets, 2, 'placement retries independently of the quote failure cache');
      assert.deepEqual(placed, [String(QUOTED)]);
    } finally { adapter.stopEvents(); }
  });
}

test('each history page gets a fresh lookup wait budget, even on another channel', async (t) => {
  let now = 0;
  t.mock.method(performance, 'now', () => now);
  let gets = 0;
  let target = 500;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: [raw(300, quote(target))], found_newest: true }),
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
      gets++;
      now += 3_001;
      return { message: raw(message_id, 'original', { sender_id: message_id === 501 ? SELF : 101 }) };
    },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  target = 501;
  const page = await adapter.fetchHistory(key('zulip:other'), { limit: 100 });
  assert.equal(gets, 2, 'the next page is not starved by the previous deadline');
  assert.equal(page.messages[0].metadata?.replyToSelf, true);
});

test('sixteen history channels all verify off-page quote targets', async (t) => {
  let now = 0;
  t.mock.method(performance, 'now', () => now);
  let gets = 0;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async ({ narrow }: { narrow: { operand: number }[] }) => {
      now += 300;
      return { messages: [raw(300, quote(500 + narrow[0].operand - 100))], found_newest: true };
    },
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => { gets++; return { message: raw(message_id, 'original', { sender_id: SELF }) }; },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const verdicts: boolean[] = [];
  for (let ch = 0; ch < 16; ch++) {
    const page = await adapter.fetchHistory(key(`zulip:${ch}`), { limit: 100 });
    verdicts.push(page.messages[0].metadata?.replyToSelf === true);
  }
  assert.equal(gets, 16);
  assert.equal(verdicts.filter(Boolean).length, 16);
});

test('history reports a quote skipped after the page wait deadline as unverified', async (t) => {
  let now = 0;
  t.mock.method(performance, 'now', () => now);
  let gets = 0;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: [raw(300, quote(500)), raw(301, quote(501))], found_newest: true }),
    'GET /messages/:id': async () => {
      gets++;
      now += 3_001;
      return { message: raw(500, 'another author') };
    },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  assert.equal(gets, 1);
  assert.deepEqual(page.unverifiedMessageIds, ['301']);
  assert.equal(page.messages[1].metadata?.replyToSelf, false);
});

for (const resolvedOther of [false, true]) {
  test(`an intact quote edit ${resolvedOther ? 'resolved to another author is a confident negative verdict' : 'with an unavailable target is not a confident negative verdict'}`, { timeout: 2000 }, async () => {
  let targetGets = 0;
  let polls = 0;
  let classified!: () => void;
  const verdictIn = new Promise<void>((resolve) => { classified = resolve; });
  let finish!: (change: MessageChangeEvent) => void;
  const changed = new Promise<MessageChangeEvent>((resolve) => { finish = resolve; });
  const before = quote();
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages/:id': async (_params: unknown, { id: message_id }: { id: number }) => {
      if (message_id === 300) return { message: raw(300, before) };
      if (++targetGets > 1) {
        if (!resolvedOther) throw new Error('target unavailable after eviction');
        return { message: raw(QUOTED, 'original', { sender_id: 102 }) };
      }
      return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
    },
    'POST /register': async () => ({ queue_id: 'q', last_event_id: -1, streams: STREAMS }),
    'GET /events': async () => {
      polls++;
      if (polls === 1) return { events: [{ id: 0, type: 'message', message: raw(300, before), flags: [] }] };
      if (polls > 2) return new Promise(() => {});
      await verdictIn;
      const fills = Array.from({ length: 2001 }, (_, i) => raw(1000 + i, 'cache fill', { sender_id: SELF }));
      return { events: [
        ...fills.map((message, i) => ({ id: i + 1, type: 'message', message, flags: [] })),
        { id: 3000, type: 'update_message', message_id: 300, user_id: 102, orig_content: before, content: before + ' corrected', flags: [] },
      ] };
    },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const lane = oneLane();
  adapter.startEvents(lane(({ message: m }) => { assert.equal(m.metadata?.replyToSelf, true); classified(); }), undefined, undefined, lane(finish));
  try {
    const edit = await changed;
    assert.equal(edit.replyToSelf, false);
    assert.equal(edit.replyToSelfVerified, resolvedOther);
    assert.equal(targetGets, 2);
  } finally { adapter.stopEvents(); }
});
}

test('R2-5: quote GETs share one process-wide bucket across pages, adapters and live events', async (t) => {
  let now = 0;
  t.mock.method(performance, 'now', () => now);
  let target = 500;
  let gets = 0;
  const getById = async (_params: unknown, { id: message_id }: { id: number }) => {
    gets++;
    return { message: raw(Number(message_id), 'original', { sender_id: SELF }) };
  };
  const client = standInApi({
    'GET /messages': async () => ({ messages: [raw(2000 + target, quote(target))], found_newest: true }),
    'GET /messages/:id': getById,
  }).api;
  const adapters = [new ZulipAdapter(client, { selfUserId: SELF, sessionId: 'one' }, { directory: knownStreams() }), new ZulipAdapter(client, { selfUserId: SELF, sessionId: 'two' }, { directory: knownStreams() })];
  for (; target < 530; target++) {
    const page = await adapters[target % 2].fetchHistory(key(`zulip:channel-${target}`), { limit: 100 });
    assert.equal(page.messages[0].metadata?.replyToSelf, true);
  }
  assert.equal(gets, 30);
  target = 999;
  const blocked = () => adapters[0].fetchHistory(key('zulip:other'), { limit: 100 });
  assert.deepEqual((await blocked()).unverifiedMessageIds, ['2999']);
  assert.equal(gets, 30, 'a fresh page or adapter cannot reset the process allowance');
  const live = await receive([raw(1800, quote(1801))], getById);
  assert.equal(live[0].metadata?.replyToSelf, false);
  assert.equal(gets, 30, 'live lookups share the same bucket');
  target = 500;
  assert.equal((await blocked()).messages[0].metadata?.replyToSelf, true, 'cache hits stay usable without a token');
  target = 999;
  now = 1999;
  assert.deepEqual((await blocked()).unverifiedMessageIds, ['2999']);
  now = 2000;
  assert.equal((await blocked()).messages[0].metadata?.replyToSelf, true, 'one token refills every two seconds; rate skips are not negative-cached');
  assert.equal(gets, 31);
  now = 3_600_000;
  for (target = 10_000; target < 10_030; target++) {
    assert.equal((await blocked()).messages[0].metadata?.replyToSelf, true);
  }
  assert.deepEqual((await blocked()).unverifiedMessageIds, ['12030']);
  assert.equal(gets, 61, 'long idle periods refill at most the 30-token burst');
});

for (const failure of ['rejecting promise', 'RATE_LIMIT_HIT', 'non-JSON response'] as const) {
  test(`R3-2: a ${failure} quote lookup is deferred and retried without negative caching`, async () => {
    let gets = 0;
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages': async () => ({ messages: [raw(300, quote())], found_newest: true }),
      'GET /messages/:id': async () => {
        if (++gets === 1) {
          if (failure === 'rejecting promise') throw new Error('ECONNRESET');
          if (failure === 'non-JSON response') throw new ZulipNoAnswer('not-json', 'GET', `/messages/${QUOTED}`, false, `Zulip's answer to GET /messages/${QUOTED} wasn't JSON (HTTP 502).`, 502);
          return { result: 'error', code: 'RATE_LIMIT_HIT', msg: 'Too many requests' };
        }
        return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const first = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.deepEqual(first.unverifiedMessageIds, ['300'], 'transient failures participate in the retry pass');
    const second = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.equal(second.messages[0].metadata?.replyToSelf, true);
    assert.equal(gets, 2, 'the retry must issue a fresh GET');
  });
}

test('R3-4: channel messages expose umbrella and specific addressing metadata', async () => {
  const rows = [raw(300, quote()), raw(301, quote(), { flags: ['mentioned'] }), raw(302, 'ambient')];
  const messages = await receive(rows, async () => ({ message: raw(QUOTED, 'original', { sender_id: SELF }) }));
  for (const [i, [explicit, reply]] of [[false, true], [true, false], [false, false]].entries()) {
    assert.equal(messages[i].metadata?.isMention, explicit || reply);
    assert.equal(messages[i].metadata?.isExplicitMention, explicit);
    assert.equal(messages[i].metadata?.isReplyToBot, reply);
  }
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: rows, found_newest: true }),
    'GET /messages/:id': async () => ({ message: raw(QUOTED, 'original', { sender_id: SELF }) }),
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const page = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  assert.equal(page.messages[0].metadata?.isMention, true);
  assert.equal(page.messages[0].metadata?.isReplyToBot, true);
  assert.equal(page.messages[1].metadata?.isExplicitMention, true);
  assert.equal(page.messages[1].metadata?.isReplyToBot, false, 'explicit mentions do not infer quote addressing');
});

for (const malformed of ['wrong id', 'missing sender'] as const) {
  test(`R3-2 follow-up: a target response with ${malformed} is uncached and retry-eligible`, async () => {
    let gets = 0;
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages': async () => ({ messages: [raw(300, quote())], found_newest: true }),
      'GET /messages/:id': async () => {
        if (++gets === 1) return { message: raw(malformed === 'wrong id' ? 9999 : QUOTED, 'original',
          { sender_id: malformed === 'missing sender' ? undefined : SELF }) };
        return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const first = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.deepEqual(first.unverifiedMessageIds, ['300']);
    assert.equal((adapter as unknown as { seen: Map<number, unknown> }).seen.has(9999), false);
    const second = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.equal(second.messages[0].metadata?.replyToSelf, true);
    assert.equal(gets, 2, 'retry performs a fresh target GET');
  });
}

test('R4-4: translated BAD_REQUEST is a cached definite miss', async () => {
  let gets = 0;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: [raw(300, quote())], found_newest: true }),
    'GET /messages/:id': async () => { gets++; return { result: 'error', code: 'BAD_REQUEST', msg: 'Ungültige Nachricht(en)' }; },
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  for (let i = 0; i < 2; i++) {
    const page = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.deepEqual(page.unverifiedMessageIds ?? [], []);
    assert.equal(page.messages[0].metadata?.replyToSelf, false);
  }
  assert.equal(gets, 1);
});

for (const outcome of ['budget-skipped', 'unverified'] as const) {
  test(`R4-6/R5-1: history keeps a seen reply only for unchanged targets when ${outcome}`, async () => {
    let content = quote();
    let initial = true;
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages': async () => ({ messages: [raw(300, content)], found_newest: true }),
      'GET /messages/:id': async (_params: unknown, { id }: { id: number }) => initial && id === QUOTED
        ? { message: raw(QUOTED, 'original', { sender_id: SELF }) }
        : { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const first = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
    assert.equal(first.messages[0].metadata?.replyToSelf, true);
    initial = false;
    const internal = adapter as unknown as { seen: Map<number, { replyToSelf?: boolean }> };
    internal.seen.delete(QUOTED);
    content = quote().replace('reply', 'corrected reply');
    const query = { limit: 100, ...(outcome === 'budget-skipped' ? { quoteLookupBudget: { remainingMs: 0 } } : {}) };
    const unchanged = await adapter.fetchHistory(key('zulip:general'), query);
    assert.equal(unchanged.messages[0].metadata?.replyToSelf, true, 'unchanged targets retain the confident verdict');
    content = quote(201);
    const page = await adapter.fetchHistory(key('zulip:general'), query);
    assert.equal(internal.seen.get(300)?.replyToSelf, false, 'changed targets invalidate the cached verdict');
    assert.equal(page.messages[0].metadata?.replyToSelf, false);
  });
}

for (const kind of ['explicit mention', 'DM', 'self author', 'bot author'] as const) {
  test(`R5-1: history clears a seen reply after the message changes to ${kind}`, async () => {
    let row = raw(300, quote());
    let gets = 0;
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages': async () => ({ messages: [row], found_newest: true }),
      'GET /messages/:id': async () => { gets++; return { message: raw(QUOTED, 'original', { sender_id: SELF }) }; },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    assert.equal((await adapter.fetchHistory(key('zulip:general'), { limit: 100 })).messages[0].metadata?.replyToSelf, true);
    const overrides: Partial<ZulipRawMessage> = kind === 'explicit mention' ? { flags: ['mentioned'] }
      : kind === 'DM' ? { type: 'private', display_recipient: [SELF, 101].map((id) => ({ id, full_name: 'User', email: `${id}@example.invalid` })) }
      : kind === 'self author' ? { sender_id: SELF }
      : { sender_email: 'other-bot@example.invalid' };
    row = raw(300, quote(), overrides);
    const page = await adapter.fetchHistory(key(kind === 'DM' ? 'zulip:dm:101' : 'zulip:general'), { limit: 100 });
    assert.equal((adapter as unknown as { seen: Map<number, { replyToSelf: boolean }> }).seen.get(300)?.replyToSelf, false);
    if (kind === 'self author') assert.equal(page.messages.length, 0);
    else {
      assert.equal(page.messages[0].metadata?.isReplyToBot, false);
      assert.equal(page.messages[0].metadata?.isExplicitMention, kind === 'explicit mention');
    }
    assert.equal(gets, 1, 'ineligible messages do not look up quote targets');
  });
}

for (const outcome of ['budget-skipped', 'unverified'] as const) {
  test(`R5-1: ${outcome} typo edit keeps the verified reply for its deletion`, { timeout: 2000 }, async (t) => {
    t.mock.method(performance, 'now', () => 0);
    const changes: MessageChangeEvent[] = [];
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => { finish = resolve; });
    let served = false;
    let gets = 0;
    const adapter = new ZulipAdapter(standInApi({
      'GET /messages/:id': async () => { gets++; return { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' }; },
      'POST /register': async () => ({ queue_id: 'q', last_event_id: -1, streams: STREAMS }),
      'GET /events': async () => {
        if (served) return new Promise(() => {});
        served = true;
        return { events: [
          { id: 1, type: 'message', message: raw(QUOTED, 'original', { sender_id: SELF }), flags: [] },
          { id: 2, type: 'message', message: raw(300, quote()), flags: [] },
          { id: 3, type: 'update_message', message_id: 300, user_id: 101, orig_content: quote(), content: quote() + ' typo fixed', flags: [] },
          { id: 4, type: 'delete_message', message_ids: [300], message_type: 'stream', stream_id: 1 },
        ] };
      },
    }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
    const lane = oneLane();
    adapter.startEvents(lane(({ message }) => {
      assert.equal(message.metadata?.replyToSelf, true);
      (adapter as unknown as { seen: Map<number, unknown> }).seen.delete(QUOTED);
      if (outcome === 'budget-skipped') {
        (ZulipAdapter as unknown as { quoteBudget: { tokens: number; updatedAt: number } }).quoteBudget = { tokens: 0, updatedAt: 0 };
      }
    }), undefined, undefined, lane((change) => {
      changes.push(change);
      if (changes.length === 2) finish();
    }));
    try {
      await finished;
      assert.deepEqual(changes.map((change) => change.kind), ['edit', 'delete']);
      assert.equal(changes[0].replyToSelf, true, 'the typo edit retains the verified unchanged target');
      assert.equal(changes[0].replyToSelfVerified, false, 'no fresh verification was possible');
      assert.equal(changes[1].replyToSelf, true, 'deletion reads the retained cache verdict');
      assert.equal(gets, outcome === 'budget-skipped' ? 0 : 1);
    } finally { adapter.stopEvents(); }
  });
}

test('a quote target in a stream the stream list doesn\'t show yet is not remembered as unreachable', async (t) => {
  let now = Date.now();
  t.mock.method(Date, 'now', () => now);
  let gets = 0;
  let listed = false;
  const adapter = new ZulipAdapter(standInApi({
    'GET /messages': async () => ({ messages: [raw(300, quote())], found_newest: true }),
    'GET /messages/:id': async () => {
      gets++;
      return { message: raw(QUOTED, 'original', { sender_id: SELF, stream_id: 900, display_recipient: 'late' }) };
    },
    'GET /streams': async () => ({ result: 'success', streams: listed ? [...KNOWN, { stream_id: 900, name: 'late' }] : KNOWN }),
  }).api, { selfUserId: SELF, sessionId: 'session' }, { directory: knownStreams() });
  const first = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  assert.deepEqual(first.unverifiedMessageIds, ['300'], 'unverified, not a definite miss');
  listed = true;
  now += 61_000;
  const second = await adapter.fetchHistory(key('zulip:general'), { limit: 100 });
  assert.equal(second.messages[0].metadata?.replyToSelf, true);
  assert.equal(gets, 2, 'the retry reads the target again');
});
