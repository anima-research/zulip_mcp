import test from 'node:test';
import assert from 'node:assert/strict';
import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import type { MessageChangeEvent } from '../src/platforms/adapter.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import type { ZulipRawMessage } from '../src/history.ts';

const SELF = 100;
const QUOTED = 200;

function raw(id: number, content: string, over: Partial<ZulipRawMessage> = {}): ZulipRawMessage {
  return {
    id, content, sender_id: 101, sender_full_name: 'User', sender_email: 'user@example.invalid',
    display_recipient: 'general', subject: 'topic', timestamp: 1_700_000_000,
    type: 'stream', flags: [], ...over,
  };
}

function quote(id = QUOTED, namedUser = SELF): string {
  return `@_**User|${namedUser}** [said](/#narrow/channel/1-general/near/${id}):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`;
}

/** Exercise the real queue -> adapter -> incoming-message path. */
async function receive(
  rows: ZulipRawMessage[],
  getById: (params: Record<string, unknown>) => Promise<unknown>,
  selfUserId: number | null = SELF,
): Promise<IncomingChannelMessage[]> {
  const received: IncomingChannelMessage[] = [];
  const sentinel = 999;
  let finish!: () => void;
  const finished = new Promise<void>((resolve) => { finish = resolve; });
  let served = false;
  const client = {
    messages: { getById },
    queues: { register: async () => ({ queue_id: 'q', last_event_id: -1 }) },
    events: { retrieve: async () => {
      if (served) return new Promise(() => {});
      served = true;
      return { result: 'success', events: [...rows, raw(sentinel, 'done')].map((message, i) => ({
        id: i + 1, type: 'message', message, flags: message.flags ?? [],
      })) };
    } },
  };
  const adapter = new ZulipAdapter(client, selfUserId, 'session');
  adapter.startEvents((message) => {
    if (message.messageId === String(sentinel)) finish();
    else received.push(message);
  });
  try {
    await finished;
    return received;
  } finally {
    adapter.stopEvents();
  }
}

test('own-message quote is tagged as a reply without changing the mention verdict', { timeout: 2000 }, async () => {
  const calls: Record<string, unknown>[] = [];
  // The silent mention's id is deliberately not SELF: the quoted sender decides.
  const messages = await receive([raw(300, quote(QUOTED, 102))], async (params) => {
    calls.push(params);
    return { result: 'success', message: raw(QUOTED, 'original', { sender_id: SELF }) };
  });
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.equal(messages[0].metadata?.replyToSelf, true);
  assert.ok(messages[0].tags?.includes('chat:reply'));
  assert.deepEqual(calls, [{ message_id: QUOTED, apply_markdown: false }]);
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
    const messages = await receive([raw(300, content)], async (params) => {
      calls.push(params);
      return { result: 'success', message: raw(Number(params.message_id), 'original') };
    });
    assert.equal(messages[0].metadata?.mentioned, false);
    assert.ok(messages[0].tags?.includes('chat:ambient'));
    assert.deepEqual(calls, label === 'near link elsewhere before a different quote'
      ? [{ message_id: 201, apply_markdown: false }] : []);
  });
}

test('a quote of another sender is not addressed even when the silent mention names self', { timeout: 2000 }, async () => {
  const messages = await receive([raw(300, quote())], async () => ({ result: 'success', message: raw(QUOTED, 'original') }));
  assert.equal(messages[0].metadata?.mentioned, false);
  assert.ok(messages[0].tags?.includes('chat:ambient'));
});

for (const failure of ['throw', 'deleted', 'missing'] as const) {
  test(`quote lookup fails closed on ${failure}, caching repeated targets without losing later events`, { timeout: 2000 }, async () => {
    let gets = 0;
    const messages = await receive([raw(300, quote()), raw(301, quote()), raw(302, 'later')], async () => {
      gets++;
      if (failure === 'throw') throw new Error('unavailable');
      return failure === 'deleted' ? { result: 'error', msg: 'not found' } : { result: 'success' };
    });
    assert.deepEqual(messages.map((m) => m.messageId), ['300', '301', '302']);
    assert.equal(messages[0].metadata?.mentioned, false);
    assert.equal(gets, 1);
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
  ], async (params) => {
    const id = Number(params.message_id);
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

test('a hung quote lookup delays an unrelated message by at most the three-second wait budget and caches the timeout', { timeout: 5000 }, async () => {
  let gets = 0;
  let release!: (value: unknown) => void;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const started = performance.now();
  const receiving = receive([raw(300, quote()), raw(301, 'unrelated'), raw(302, quote())], () => new Promise((resolve) => { release = resolve; gets++; }));
  try {
    const messages = await Promise.race([
      receiving,
      new Promise<never>((_, reject) => {
        // Allow scheduler overhead beyond the dedicated three-second budget.
        timer = setTimeout(() => reject(new Error('unrelated message blocked beyond quote budget')), 3500);
      }),
    ]);
    assert.deepEqual(messages.map((m) => m.messageId), ['300', '301', '302']);
    assert.equal(messages[0].metadata?.mentioned, false);
    assert.equal(gets, 1);
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
  const messages = await receive([raw(300, content)], async (params) => {
    const id = Number(params.message_id);
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
  const adapter = new ZulipAdapter({ messages: {
    retrieve: async () => ({ messages: rows, found_newest: true }),
    getById: async () => { throw new Error('page target must not need a GET'); },
  } }, SELF, 'session');
  const page = await adapter.fetchHistory('zulip:general', { limit: 500 });
  assert.deepEqual(page.messages.map((m) => m.messageId), ['300']);
  assert.equal(page.messages[0].metadata?.replyToSelf, true);
  assert.equal(page.messages[0].metadata?.mentioned, false);
  assert.deepEqual(page.messages[0].tags, ['chat:reply', 'chat:from-human']);
  assert.equal(page.scannedThrough, 300);
});

test('history quote lookups share a page cap and still use cached authors after it is exhausted', async () => {
  let rows = [raw(QUOTED, 'own original', { sender_id: SELF })];
  const gets: number[] = [];
  const adapter = new ZulipAdapter({ messages: {
    retrieve: async () => ({ messages: rows, found_newest: true }),
    getById: async ({ message_id }: { message_id: number }) => {
      gets.push(message_id);
      return { message: raw(message_id, 'another author') };
    },
  } }, SELF, 'session');
  await adapter.fetchHistory('zulip:general', { limit: 500 });
  rows = Array.from({ length: 20 }, (_, i) => raw(300 + i, quote(500 + i)));
  rows.push(raw(400, quote()));
  const page = await adapter.fetchHistory('zulip:general', { limit: 500 });
  assert.equal(gets.length, 8);
  assert.ok(page.messages.slice(0, -1).every((m) => m.metadata?.replyToSelf === false));
  assert.equal(page.messages.at(-1)?.metadata?.replyToSelf, true);
});

test('history verifies an off-page own-message quote with a raw-message GET', async () => {
  const gets: unknown[] = [];
  const adapter = new ZulipAdapter({ messages: {
    retrieve: async () => ({ messages: [raw(300, quote())], found_newest: true }),
    getById: async (params: unknown) => {
      gets.push(params);
      return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
    },
  } }, SELF, 'session');
  const page = await adapter.fetchHistory('zulip:general', { limit: 500 });
  assert.equal(page.messages[0].metadata?.replyToSelf, true);
  assert.deepEqual(gets, [{ message_id: QUOTED, apply_markdown: false }]);
});

for (const [label, before, after, wasReply, isReply] of [
  ['keeps', quote(), quote().replace('reply', 'corrected reply'), true, true],
  ['removes', quote(), 'no quote now', true, false],
  ['adds', 'no quote yet', quote(), false, true],
] as const) {
  test(`an edit that ${label} a quote recomputes reply addressing independently of mentions`, { timeout: 2000 }, async () => {
    const changes: MessageChangeEvent[] = [];
    const incoming: IncomingChannelMessage[] = [];
    let finish!: () => void;
    const finished = new Promise<void>((resolve) => { finish = resolve; });
    let served = false;
    const adapter = new ZulipAdapter({
      queues: { register: async () => ({ queue_id: 'q', last_event_id: -1 }) },
      events: { retrieve: async () => {
        if (served) return new Promise(() => {});
        served = true;
        return { events: [
          { id: 1, type: 'message', message: raw(QUOTED, 'original', { sender_id: SELF }), flags: [] },
          { id: 2, type: 'message', message: raw(300, before), flags: [] },
          { id: 3, type: 'update_message', message_id: 300, user_id: 101, orig_content: before, content: after, flags: [] },
          { id: 4, type: 'delete_message', message_ids: [300], message_type: 'stream', stream_id: 1 },
        ] };
      } },
    }, SELF, 'session');
    adapter.startEvents((m) => incoming.push(m), undefined, undefined, (c) => {
      changes.push(c);
      if (changes.length === 2) finish();
    });
    try {
      await finished;
      assert.equal(incoming[0].metadata?.mentioned, false);
      assert.equal(incoming[0].metadata?.replyToSelf, wasReply);
      assert.equal(changes[0].kind, 'edit');
      assert.equal(changes[0].mentioned, false);
      assert.equal(changes[0].previouslyMentioned, false);
      assert.equal(changes[0].replyToSelf, isReply);
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
  const messages = await receive([raw(300, `${quote(201).replace('):', ') in #**general>topic**:')}\n${quote()}`)], async (params) => {
    calls.push(Number(params.message_id));
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
  const adapter = new ZulipAdapter({ messages: {
    retrieve: async () => ({ messages: [raw(300, quote(), { sender_email: 'another-bot@example.invalid' })], found_newest: true }),
    getById: async () => { gets++; return { message: raw(QUOTED, 'original', { sender_id: SELF }) }; },
  } }, SELF, 'session');
  const page = await adapter.fetchHistory('zulip:general', { limit: 500 });
  assert.equal(page.messages[0].metadata?.replyToSelf, false);
  assert.equal(gets, 0);
});

for (const restart of [false, true]) {
  test(`stopping during a quote lookup drops the message and remaining batch${restart ? ' across a restart' : ''}`, { timeout: 2000 }, async () => {
    let release!: (value: unknown) => void;
    let lookupStarted!: () => void;
    const started = new Promise<void>((resolve) => { lookupStarted = resolve; });
    let registrations = 0;
    const served = new Set<string>();
    const adapter = new ZulipAdapter({
      messages: { getById: () => new Promise((resolve) => { release = resolve; lookupStarted(); }) },
      queues: { register: async () => ({ queue_id: `q${++registrations}`, last_event_id: -1 }) },
      events: { retrieve: async ({ queue_id }: { queue_id: string }) => {
        if (served.has(queue_id)) return new Promise(() => {});
        served.add(queue_id);
        const rows = queue_id === 'q1' ? [raw(300, quote()), raw(301, 'later in old batch')] : [raw(400, 'new connection')];
        return { events: rows.map((message, i) => ({ id: i + 1, type: 'message', message, flags: [] })) };
      } },
    }, SELF, 'session');
    const oldMessages: string[] = [];
    const newMessages: string[] = [];
    try {
      adapter.startEvents((m) => oldMessages.push(m.messageId));
      await started;
      adapter.stopEvents();
      if (restart) {
        let received!: () => void;
        const done = new Promise<void>((resolve) => { received = resolve; });
        adapter.startEvents((m) => { newMessages.push(m.messageId); received(); });
        await done;
      }
      release({ message: raw(QUOTED, 'original', { sender_id: SELF }) });
      await new Promise((resolve) => setImmediate(resolve));
      assert.deepEqual(oldMessages, []);
      assert.deepEqual(newMessages, restart ? ['400'] : []);
    } finally {
      adapter.stopEvents();
      release?.({ result: 'error' });
    }
  });
}

for (const failure of ['throw', 'deleted', 'missing'] as const) {
  test(`history caches a ${failure} quote target until its retry window expires`, async (t) => {
    let now = Date.now();
    t.mock.method(Date, 'now', () => now);
    let gets = 0;
    let succeed = false;
    const adapter = new ZulipAdapter({ messages: {
      retrieve: async () => ({ messages: [raw(300, quote())], found_newest: true }),
      getById: async () => {
        gets++;
        if (succeed) return { message: raw(QUOTED, 'original', { sender_id: SELF }) };
        if (failure === 'throw') throw new Error('unavailable');
        return failure === 'deleted' ? { result: 'error', msg: 'not found' } : { result: 'success' };
      },
    } }, SELF, 'session');
    const page = () => adapter.fetchHistory('zulip:general', { limit: 100 });
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
  const adapter = new ZulipAdapter({ messages: {
    retrieve: async () => ({ messages: rows, found_newest: true }),
    getById: async ({ message_id }: { message_id: number }) => {
      gets++;
      return message_id === QUOTED
        ? { message: raw(QUOTED, 'original', { sender_id: SELF }) }
        : { result: 'error', msg: 'not found' };
    },
  } }, SELF, 'session');
  await adapter.fetchHistory('zulip:general', { limit: 100 });
  assert.equal(gets, 8);
  rows = [...rows, raw(400, quote())];
  const page = await adapter.fetchHistory('zulip:general', { limit: 100 });
  assert.equal(gets, 9, 'only the new target costs a GET');
  assert.equal(page.messages.at(-1)?.metadata?.replyToSelf, true);
});
