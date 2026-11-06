import test from 'node:test';
import assert from 'node:assert/strict';
import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import { ActiveTopicsCache } from '../src/active-topics.ts';

const turn = () => new Promise<void>(resolve => setImmediate(resolve));
function message(id = 30, topic = 'deploys', dm = false): IncomingChannelMessage {
  return {
    channelId: dm ? 'zulip:dm:7' : 'zulip:general', messageId: String(id), threadId: topic,
    author: { id: '7', name: 'Ann' }, timestamp: new Date(1_700_000_000_000 + id * 1000).toISOString(),
    content: [{ type: 'text', text: 'hello' }], metadata: { topic, isDM: dm },
  };
}
function fixture(limit = 6) {
  let now = 1_700_000_031_000, calls = 0;
  let topics = [{ name: 'deploys', max_id: 30 }, { name: 'planning', max_id: 10 }];
  let retrieve = async () => ({ result: 'success', topics });
  const client = {
    streams: { topics: { retrieve: async ({ stream_id }: { stream_id: number }) => { assert.ok(stream_id > 0); calls++; return retrieve(); } } },
    messages: { getById: async ({ message_id }: { message_id: number }) => ({ result: 'success', message: { id: message_id, timestamp: 1_700_000_000 + message_id } }) },
  };
  const cache = new ActiveTopicsCache(client, limit, () => now);
  return { cache, client, calls: () => calls, advance: () => { now += 60_001; },
    setTopics: (value: typeof topics) => { topics = value; }, setRetrieve: (fn: typeof retrieve) => { retrieve = fn; } };
}
function hints(m: IncomingChannelMessage): any[] | undefined {
  return (m.metadata as any)?.activeTopics;
}

test('cold cache returns the exact message while the API is unresolved; one background request per stream', async () => {
  const f = fixture();
  let release!: (value: any) => void;
  f.setRetrieve(() => new Promise(resolve => { release = resolve; }));
  const m = message();
  assert.equal(f.cache.decorate(m, 7), m);
  assert.equal(f.cache.decorate(message(31), 7).metadata?.activeTopics, undefined);
  await turn();
  assert.equal(f.calls(), 1);
  release({ result: 'success', topics: [{ name: 'planning', max_id: 10 }] });
  await turn();
  assert.deepEqual(hints(f.cache.decorate(message(32), 7))?.map(t => t.name), ['deploys', 'planning']);
});

test('warm cache has recent topics, ISO last-message times and a marked current topic; new topics update immediately', async () => {
  const f = fixture(2);
  f.cache.decorate(message(), 7);
  await turn();
  const warm = f.cache.decorate(message(31, 'new\n\u2028topic'), 7);
  assert.deepEqual(hints(warm), [
    { name: 'new topic', lastMessageTime: message(31).timestamp, current: true },
    { name: 'deploys', lastMessageTime: message().timestamp, current: false },
  ]);
  assert.equal((warm.content[1] as any).text, '[active topics] *"new topic" (0s); "deploys" (1s)');
  assert.equal(f.calls(), 1);
});

test('TTL refresh serves stale hints without awaiting refresh; refresh merges newer live events', async () => {
  const f = fixture(3);
  f.cache.decorate(message(), 7); await turn();
  f.advance();
  let release!: (value: any) => void;
  f.setRetrieve(() => new Promise(resolve => { release = resolve; }));
  assert.ok(hints(f.cache.decorate(message(40), 7)));
  await turn();
  assert.equal(f.calls(), 2);
  f.cache.decorate(message(41, 'new'), 7);
  release({ result: 'success', topics: [{ name: 'planning', max_id: 10 }] }); await turn();
  assert.deepEqual(hints(f.cache.decorate(message(42), 7))?.map(t => t.name), ['deploys', 'new', 'planning']);
});

test('API failure is silent and cold hints stay absent; retries are throttled then recover', async () => {
  const f = fixture();
  f.setRetrieve(async () => { throw new Error('offline'); });
  const m = message();
  assert.equal(f.cache.decorate(m, 7), m); await turn();
  assert.equal(f.cache.decorate(m, 7), m); await turn();
  assert.equal(f.calls(), 1);
  f.advance(); f.setRetrieve(async () => ({ result: 'success', topics: [] }));
  f.cache.decorate(m, 7); await turn();
  assert.ok(hints(f.cache.decorate(m, 7)));
});

test('DMs and disabled hints are byte-identical and never fetch', async () => {
  for (const [limit, m] of [[0, message()], [6, message(30, '', true)]] as const) {
    const f = fixture(limit);
    const before = JSON.stringify(m);
    assert.equal(f.cache.decorate(m, 7), m); await turn();
    assert.equal(JSON.stringify(m), before);
    assert.equal(f.calls(), 0);
  }
});

test('LRU stream entries and simultaneous fetches are bounded, even with a stuck API', async () => {
  const f = fixture(1);
  f.setRetrieve(() => new Promise(() => {}));
  for (let stream = 1; stream <= 300; stream++) f.cache.decorate(message(), stream);
  await turn();
  assert.ok(f.calls() <= 4);
  assert.ok((f.cache as any).streams.size <= 128);
});

test('API rows are ranked by max_id, not response order; late fetch cannot resurrect an evicted stream', async () => {
  const f = fixture(2);
  f.setTopics([{ name: 'old', max_id: 1 }, { name: 'recent', max_id: 29 }, { name: 'middle', max_id: 25 }]);
  f.cache.decorate(message(), 7); await turn();
  assert.deepEqual(hints(f.cache.decorate(message(31), 7))?.map(t => t.name), ['deploys', 'recent']);
  for (let id = 100; id < 230; id++) f.cache.observe(id, 't', 1, message().timestamp);
  assert.equal(f.cache.decorate(message(), 7).metadata?.activeTopics, undefined);
});

test('Zulip API error results never produce a cold hint', async () => {
  const f = fixture();
  f.setRetrieve(async () => ({ result: 'error', topics: [] }));
  f.cache.decorate(message(), 7); await turn();
  assert.equal(hints(f.cache.decorate(message(), 7)), undefined);
  assert.equal(f.calls(), 1);
});

test('a failed topic request frees only its own API slot while sibling requests are stuck', async () => {
  const f = fixture();
  let first = true;
  f.setRetrieve(async () => {
    if (first) { first = false; throw new Error('offline'); }
    return new Promise(() => {});
  });
  for (let stream = 1; stream < 5; stream++) f.cache.decorate(message(), stream);
  await turn(); f.advance();
  for (let stream = 10; stream < 20; stream++) f.cache.decorate(message(), stream);
  await turn();
  assert.equal(f.calls(), 5);
});

test('LRU touches retain the accessed stream, and late fetch completions cannot restore evictions', async () => {
  const f = fixture();
  let release!: (value: any) => void;
  f.setRetrieve(() => new Promise(resolve => { release = resolve; }));
  f.cache.decorate(message(), 7); await turn();
  for (let stream = 100; stream < 227; stream++) f.cache.observe(stream, 'topic', 1, message().timestamp);
  f.cache.observe(7, 'deploys', 31, message().timestamp); // touch stream 7
  f.cache.observe(227, 'topic', 1, message().timestamp);
  assert.equal((f.cache as any).streams.has(100), false);
  assert.equal((f.cache as any).streams.has(7), true);
  for (let stream = 300; stream < 429; stream++) f.cache.observe(stream, 'topic', 1, message().timestamp);
  release({ result: 'success', topics: [] }); await turn();
  assert.equal((f.cache as any).streams.has(7), false);
});

test('topic counts are capped and Zulip case-insensitive topic names do not duplicate', async () => {
  const f = fixture(1000);
  f.setTopics(Array.from({ length: 80 }, (_, i) => ({ name: `topic-${i}`, max_id: i + 1 })));
  f.cache.decorate(message(100), 7); await turn();
  const warm = f.cache.decorate(message(101, 'DEPLOYS'), 7);
  assert.equal(hints(warm)?.length, 50);
  assert.equal(hints(warm)?.filter(t => t.name.toLowerCase() === 'deploys').length, 1);
  assert.equal(hints(warm)?.[0].current, true);
});

test('r1: retry_after and HTTP 429 share a cooldown across streams, returned or thrown', async () => {
  for (const failure of [
    { result: 'error', code: 'RATE_LIMIT_HIT', retry_after: 180 },
    { status: 429 },
    { response: { status: 429, data: { retry_after: 180 } } },
  ]) {
    const f = fixture();
    f.setRetrieve(async () => { if ('response' in failure) throw failure; return failure as any; });
    f.cache.decorate(message(), 7); await turn();
    f.cache.decorate(message(), 8); await turn();
    assert.equal(f.calls(), 1, 'a different stream must respect shared cooldown');
    if ('retry_after' in failure || 'response' in failure) {
      f.advance(); f.cache.decorate(message(), 9); await turn();
      assert.equal(f.calls(), 1, 'retry_after may exceed the TTL');
      f.advance(); f.advance();
    } else f.advance();
    f.setRetrieve(async () => ({ result: 'success', topics: [] }));
    f.cache.decorate(message(), 10); await turn();
    assert.equal(f.calls(), 2);
  }
});

test('r1: topics use max_id ordering without timestamp GETs; all in-flight API requests capped at four', async () => {
  const f = fixture();
  let inflight = 0, peak = 0, messageGets = 0;
  const releases: (() => void)[] = [];
  f.setRetrieve(() => new Promise(resolve => {
    peak = Math.max(peak, ++inflight);
    releases.push(() => { inflight--; resolve({ result: 'success', topics: [{ name: 'unknown', max_id: 10 }] }); });
  }));
  f.client.messages.getById = async () => { messageGets++; peak = Math.max(peak, ++inflight); return new Promise(() => {}); };
  for (let id = 1; id < 20; id++) f.cache.decorate(message(), id);
  await turn();
  assert.equal(peak, 4);
  releases.forEach(release => release()); await turn();
  assert.equal(messageGets, 0);
  const result = hints(f.cache.decorate(message(), 1));
  assert.ok(result?.some(t => t.name === 'unknown' && !('lastMessageTime' in t)));
});

test('r1: refresh drops pre-refresh observations absent from the new snapshot, merging only events during fetch', async () => {
  const f = fixture();
  f.cache.decorate(message(30, 'old-name'), 7); await turn();
  f.advance();
  let release!: (value: any) => void;
  f.setRetrieve(() => new Promise(resolve => { release = resolve; }));
  f.cache.decorate(message(40, 'old-name'), 7); await turn();
  f.cache.observe(7, 'during', 41, message(41).timestamp);
  release({ result: 'success', topics: [{ name: 'renamed', max_id: 40 }] }); await turn();
  assert.deepEqual(hints(f.cache.decorate(message(42, 'renamed'), 7))?.map(t => t.name), ['renamed', 'during']);
});

test('r1: display names are JSON-quoted, escaped and clipped to 60 code points', async () => {
  const f = fixture(2);
  const name = 'bad"; \\topic\n' + '😀'.repeat(100);
  f.setTopics([{ name, max_id: 20 }]);
  f.cache.decorate(message(30, name), 7); await turn();
  const warm = f.cache.decorate(message(31, name), 7);
  const text = (warm.content[1] as any).text;
  const expected = Array.from(name.replace(/\n/g, ' ')).slice(0, 59).join('') + '…';
  assert.equal(text, '[active topics] *' + JSON.stringify(expected) + ' (0s)');
  assert.equal(hints(warm)?.[0].name, name.replace(/\n/g, ' '));
});

test('r1: text repeats only after TTL or a changed ordered name list; metadata stays on every warm event', async () => {
  let now = 1_700_000_031_000;
  let calls = 0;
  const client = { streams: { topics: { retrieve: async () => { calls++; return { result: 'success', topics: [{ name: 'deploys', max_id: 30 }] }; } } } };
  const cache = new ActiveTopicsCache(client, 6, () => now, 1000);
  cache.decorate(message(), 7); await turn();
  assert.equal(cache.decorate(message(31), 7).content.length, 2);
  const quiet = cache.decorate(message(32), 7);
  assert.ok(hints(quiet)); assert.equal(quiet.content.length, 1);
  assert.equal(cache.decorate(message(33, 'new'), 7).content.length, 2);
  assert.equal(cache.decorate(message(34, 'new'), 7).content.length, 1);
  now += 999;
  assert.equal(cache.decorate(message(34, 'new'), 7).content.length, 1);
  await turn(); assert.equal(calls, 1);
  now++;
  assert.equal(cache.decorate(message(35, 'new'), 7).content.length, 2);
  await turn(); assert.equal(calls, 2);
  assert.equal(cache.decorate(message(36, 'new'), 8).content.length, 1, 'windows are per stream');
});

test('invalidation retains display history but cannot resurrect pending snapshots', async () => {
  const f = fixture();
  f.cache.decorate(message(), 7); await turn();
  assert.equal(f.cache.decorate(message(), 7).content.length, 2);
  f.cache.invalidate(7);
  assert.equal(hints(f.cache.decorate(message(), 7)), undefined); await turn();
  const quiet = f.cache.decorate(message(), 7);
  assert.ok(hints(quiet)); assert.equal(quiet.content.length, 1);
  f.cache.invalidate(null);
  assert.equal(hints(f.cache.decorate(message(), 7)), undefined);
});
