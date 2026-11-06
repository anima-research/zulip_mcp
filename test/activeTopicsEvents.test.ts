import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';

const turn = () => new Promise<void>(resolve => setImmediate(resolve));
test('production stream event callback delivers cold immediately, then carries warm and own-message topic updates', async t => {
  let receive!: (...args: any[]) => Promise<void>;
  t.mock.method(ZulipEventLoop.prototype, 'start', async (_client: unknown, callback: typeof receive) => { receive = callback; });
  let finish!: (value: unknown) => void;
  let calls = 0;
  const client = {
    streams: { topics: { retrieve: () => { calls++; return new Promise(resolve => { finish = resolve; }); } } },
    messages: { getById: async () => ({ result: 'success', message: { id: 10, timestamp: 1_700_000_010 } }) },
  };
  const adapter = new ZulipAdapter(client, 99, 'test');
  const seen: any[] = [];
  adapter.startEvents(m => seen.push(m));
  const send = (id: number, subject: string, sender_id = 7) => receive('general', {
    id, subject, sender_id, stream_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
    display_recipient: 'general', content: 'hello', timestamp: 1_700_000_000 + id, type: 'stream',
  }, []);
  await send(30, 'deploys');
  assert.equal(seen.length, 1, 'must not await the unresolved topics API');
  assert.equal(seen[0].metadata.activeTopics, undefined);
  await turn();
  assert.equal(calls, 1);
  finish({ result: 'success', topics: [{ name: 'planning', max_id: 10 }] }); await turn();
  await send(31, 'deploys');
  assert.deepEqual(seen[1].metadata.activeTopics.map((v: any) => v.name), ['deploys', 'planning']);
  await send(32, 'self-topic', 99);
  await send(33, 'deploys');
  assert.deepEqual(seen[2].metadata.activeTopics.map((v: any) => v.name), ['deploys', 'self-topic', 'planning']);
  assert.match(seen[2].content[1].text, /\[active topics\] \*"deploys"/);
  adapter.stopEvents();
});

test('production toggle-off stream events and default-on DMs are byte-identical to the original toIncoming shape', async t => {
  const { normalizeMessage, toIncoming } = await import('../src/history.ts');
  let receive!: (...args: any[]) => Promise<void>;
  t.mock.method(ZulipEventLoop.prototype, 'start', async (_client: unknown, callback: typeof receive) => { receive = callback; });
  let calls = 0;
  const client = { streams: { topics: { retrieve: () => { calls++; throw new Error('must not fetch'); } } } };
  for (const dm of [false, true]) {
    const adapter = new ZulipAdapter(client, 99, 'test', dm ? {} : { activeTopics: 0 });
    const seen: any[] = [];
    adapter.startEvents(m => seen.push(m));
    const raw = {
      id: 30, subject: dm ? '' : 'deploys', sender_id: 7, stream_id: 7, sender_full_name: 'Ann', sender_email: 'ann@example.com',
      display_recipient: dm ? [{ id: 99, full_name: 'Bot', email: 'bot@example.com' }, { id: 7, full_name: 'Ann', email: 'ann@example.com' }] : 'general',
      content: 'hello', timestamp: 1_700_000_030, type: dm ? 'private' : 'stream', flags: [],
    };
    await receive(dm ? null : 'general', raw, []);
    const expected = toIncoming(dm ? 'zulip:dm:7' : 'zulip:general', normalizeMessage(raw as any), { selfUserId: 99, sessionId: 'test' });
    assert.equal(JSON.stringify(seen[0]), JSON.stringify(expected));
    adapter.stopEvents();
  }
  await turn(); assert.equal(calls, 0);
});

test('r1: move, rename/resolve and delete events invalidate lists without a change subscriber, including pending fetches', async t => {
  let receive!: (...args: any[]) => Promise<void>;
  let change!: (value: any) => void;
  t.mock.method(ZulipEventLoop.prototype, 'start', async (_client, callback, _system, _reaction, onChange) => { receive = callback; change = onChange!; });
  const pending: ((value: any) => void)[] = [];
  const client = { streams: { topics: { retrieve: () => new Promise(resolve => pending.push(resolve)) } }, messages: { getById: async () => ({ result: 'success', message: { id: 10, timestamp: 1_700_000_010 } }) } };
  const adapter = new ZulipAdapter(client, 99, 'test');
  const seen: any[] = [];
  adapter.startEvents(m => seen.push(m));
  const send = async (stream: number, id = 30) => receive('general', {
    id, subject: 'current', sender_id: 7, stream_id: stream, sender_full_name: 'Ann', sender_email: 'ann@example.com',
    display_recipient: 'general', content: 'hello', timestamp: 1_700_000_000 + id, type: 'stream',
  }, []);
  for (const event of [
    { kind: 'edit', streamId: 7, newStreamId: 8, topic: 'moved', origTopic: 'old' },
    { kind: 'edit', streamId: 7, newStreamId: null, topic: '✔ old', origTopic: 'old' },
    { kind: 'delete', streamId: 7, messageType: 'stream', topic: 'old' },
  ]) {
    await send(7); await send(8); await turn();
    pending.splice(0).forEach(resolve => resolve({ result: 'success', topics: [{ name: 'old', max_id: 10 }] })); await turn();
    await send(7, 31); await send(8, 31);
    assert.ok(seen.at(-2).metadata.activeTopics);
    assert.equal(typeof change, 'function');
    change(event);
    await send(7, 32);
    assert.equal(seen.at(-1).metadata.activeTopics, undefined);
    if (event.newStreamId === 8) {
      await send(8, 32); assert.equal(seen.at(-1).metadata.activeTopics, undefined);
    }
    await turn();
    change(event); // invalidate again while requests are pending
    pending.splice(0).forEach(resolve => resolve({ result: 'success', topics: [{ name: 'old', max_id: 10 }] })); await turn();
    await send(7, 33); assert.equal(seen.at(-1).metadata.activeTopics, undefined, 'late snapshot must not resurrect invalidated names');
    await turn();
    pending.splice(0).forEach(resolve => resolve({ result: 'success', topics: [] })); await turn();
  }
  adapter.stopEvents();
});
