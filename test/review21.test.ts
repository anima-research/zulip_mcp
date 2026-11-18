import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { mkdtempSync, readFileSync, rmSync, statSync, utimesSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { McplConnection, method } from '@animalabs/mcpl-core';
import { DeliveryState } from '../src/delivery.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import { streamKey } from '../src/channel-key.ts';
import { ZulipMcplServer } from '../src/server.ts';
import { MessageIds } from '../src/message-ids.ts';

const key = streamKey(7);
function state(t: any) {
  const dir = mkdtempSync(join(tmpdir(), 'review21-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const path = join(dir, 'test.delivery.json');
  writeFileSync(path, JSON.stringify({ streams: { 7: 'general' }, watermarks: { 'zulip:general': 100 }, held: { 'zulip:general': { 21: 0 } } }));
  const directory = new StreamDirectory();
  const delivery = new DeliveryState(dir, 'test', directory);
  return { dir, directory, delivery, path, saved: () => JSON.parse(readFileSync(path, 'utf8')) };
}

for (const route of ['open', 'closed']) for (const outcome of ['written', 'closed', 'throwing', 'destroyed']) {
  test(`${route} by-ID removal follows a successful write only (${outcome})`, async t => {
    t.mock.method(console, 'error', () => {});
    const { delivery, saved } = state(t);
    const input = new PassThrough();
    const output = new PassThrough();
    const conn = McplConnection.fromStreams(input, output);
    t.after(() => conn.close());
    conn.requestTimeout = 20;
    let writes = 0;
    const write = output.write.bind(output);
    output.write = ((...args: any[]) => {
      writes++;
      assert.deepEqual(saved().held['zulip:general'], { 21: 0 }, 'not removed before the writer accepts it');
      if (outcome === 'throwing') throw new Error('writer failed before writing');
      return (write as any)(...args);
    }) as any;
    if (outcome === 'closed') conn.close();
    if (outcome === 'destroyed') output.destroy();
    const server = Object.assign(Object.create(ZulipMcplServer.prototype), {
      conn, messageIds: new MessageIds('alpha', 'legacy'), mcplActive: true,
      ids: { channelIdOf: () => 'zulip:general' },
      grant: { has: () => true, isFeatureSetActive: () => true },
      delivery, hostRequestsOut: new Set(), streamNameOf: () => 'general',
    });
    let commits = 0;
    const written = () => { commits++; delivery.uncarry(key, [21]); delivery.save(); };
    const answer = route === 'open'
      ? server.sendIncomingToHost([], written)
      : server.pushEvent({ key, messageId: '21', timestamp: 'now', author: { id: '1', name: 'a' }, content: [] }, 'event21', {}, written);
    assert.equal(commits, outcome === 'written' ? 1 : 0, 'commit is synchronous with accepted write, not answer');
    assert.equal(writes, outcome === 'closed' || outcome === 'destroyed' ? 0 : 1);
    if (outcome === 'written') assert.equal(saved().held, undefined);
    else assert.deepEqual(saved().held['zulip:general'], { 21: 0 });
    conn.close(); // an unanswered but written request is not restored
    await Promise.resolve(answer).catch(() => {});
    assert.equal(commits, outcome === 'written' ? 1 : 0);
  });
}

test('absent stream keeps carried IDs across directory replacement and two saves/reloads', t => {
  const { delivery, directory, dir, saved } = state(t);
  directory.registration([]);
  delivery.save();
  assert.deepEqual(saved().held, { 'stream-id:7': { 21: 0 } });
  assert.equal(saved().watermarks['stream-id:7'], 100);
  const nextDirectory = new StreamDirectory();
  const next = new DeliveryState(dir, 'test', nextDirectory);
  assert.deepEqual(next.carriedIds(key), [21]);
  next.save();
  assert.deepEqual(new DeliveryState(dir, 'test', new StreamDirectory()).carriedIds(key), [21]);
  nextDirectory.registration([{ stream_id: 7, name: 'restored' }]);
  next.save();
  assert.deepEqual(saved().held['zulip:restored'], { 21: 0 });
});

test('saving and accepting queued work never traverse the 50,000-hold backlog or rewrite unchanged state', t => {
  const { delivery, path } = state(t);
  for (let id = 101; id < 50101; id++) delivery.hold(key, id, 'queued');
  const held = (delivery as any).held.get(key);
  t.mock.method(held, Symbol.iterator, () => { throw new Error('save traversed queued backlog'); });
  delivery.save();
  utimesSync(path, 1, 1);
  assert.equal(delivery.release(key, 50100), false, 'a queued release with no watermark/saved-list change needs no save');
  delivery.save();
  assert.equal(statSync(path).mtimeMs, 1000, 'unchanged snapshot does not replace state');
  delivery.hold(key, 50101, 'rejected');
  assert.ok(statSync(path).mtimeMs > 1000);
});

test('muted held replay and unfound history name each saved withdrawal and its reason', async t => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => logs.push(args.join(' ')));
  for (const muted of [true, false]) {
    const { delivery } = state(t);
    delivery.hold(key, 101, 'failed');
    // The saved ID is both carried and a current-run hold above the watermark.
    (delivery as any).carried.get(key).set(101, 0);
    const server = Object.assign(Object.create(ZulipMcplServer.prototype), { delivery, isMuted: () => muted });
    const w = { delivery };
    if (muted) {
      const hand = await server.readHeld({ ended: false }, key);
      hand({ w, lane: { waitingMessages: () => [] } });
    } else server.letGoUnfound(w, key, [101], { messages: [], complete: true });
    assert.ok(logs.some(line => line.includes('ID 101:') && line.includes(muted ? 'muted' : 'no longer exists')));
    logs.length = 0;
  }
});

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  test(`${mode} incoming results reject malformed acceptance and normalized duplicates independently`, async t => {
    t.mock.method(console, 'error', () => {});
    const ids = new MessageIds('alpha', mode);
    const format = (n: number) => String(ids.format(n));
    const server = Object.assign(Object.create(ZulipMcplServer.prototype), {
      messageIds: ids, conn: { sendRequest: async () => ({ results: [
        { messageId: format(21), accepted: 'false' },
        { messageId: format(22), accepted: true },
        { messageId: format(22), accepted: false },
        { messageId: format(23), accepted: true },
      ] }) },
    });
    const result = await server.sendIncomingToHost([21, 22, 23].map(messageId => ({ messageId: String(messageId) })));
    assert.deepEqual(result.results, [{ messageId: '23', accepted: true }]);
  });
}

test('unplaceable loaded holds log their IDs, and early discovered streams save their identity', t => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => logs.push(args.join(' ')));
  const { path, dir } = state(t);
  for (const streams of [undefined, {}]) {
    writeFileSync(path, JSON.stringify({ streams, held: { 'zulip:absent': { 123: 0, 456: 0 } } }));
    new DeliveryState(dir, 'test', new StreamDirectory());
    for (const id of [123, 456]) assert.ok(logs.some(line => line.includes(`ID ${id}:`) && line.includes('no stream list')));
    logs.length = 0;
  }
  rmSync(path);
  const directory = new StreamDirectory();
  directory.applyEvent({ op: 'create', streams: [{ stream_id: 7, name: 'early' }] });
  assert.equal(directory.isFilled, false);
  const delivery = new DeliveryState(dir, 'test', directory);
  delivery.hold(key, 21, 'failed');
  assert.deepEqual(new DeliveryState(dir, 'test', new StreamDirectory()).carriedIds(key), [21]);
});

test('in-memory cap eviction names a save-eligible hold without scanning it on save', t => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => logs.push(args.join(' ')));
  const { delivery, saved } = state(t);
  for (let id = 201; id <= 50200; id++) delivery.hold(key, id, 'queued');
  delivery.hold(key, 50200, 'rejected');
  delivery.hold(key, 150, 'queued');
  delivery.save();
  assert.ok(logs.some(line => line.includes('ID 50200: in-memory hold cap eviction')));
  assert.equal(saved().held['zulip:general']['50200'], undefined);
});

test('invalid loaded failure counts are normalized to a bounded integer', t => {
  const { path, dir } = state(t);
  writeFileSync(path, JSON.stringify({ streams: { 7: 'general' }, held: { 'zulip:general': { 21: -100, 22: 2.5, 23: '1', 24: 99, 25: 2 } } }));
  const delivery = new DeliveryState(dir, 'test', new StreamDirectory());
  delivery.save();
  assert.deepEqual(JSON.parse(readFileSync(path, 'utf8')).held['zulip:general'], { 21: 0, 22: 0, 23: 0, 24: 0, 25: 2 });
});

test('an immediate loopback host answer settles only after the written-request save', async t => {
  const { delivery, saved } = state(t);
  const input = new PassThrough();
  const output = new PassThrough();
  const conn = McplConnection.fromStreams(input, output);
  t.after(() => conn.close());
  output.on('data', chunk => {
    const request = JSON.parse(String(chunk));
    input.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, result: {} }) + '\n');
  });
  const server = Object.assign(Object.create(ZulipMcplServer.prototype), { conn, messageIds: new MessageIds('alpha', 'legacy') });
  await server.sendIncomingToHost([], () => { delivery.uncarry(key, [21]); delivery.save(); });
  assert.equal(saved().held, undefined);
});

test('unreadable write authorization failures require repair, not waiting out Retry-After', async t => {
  t.mock.method(console, 'error', () => {});
  const { ZulipNoAnswer } = await import('../src/zulip-api.ts');
  const { messageIdError } = await import('../src/message-ids.ts');
  for (const mode of ['legacy', 'compat', 'strict'] as const) for (const status of [401, 403]) {
    const safe = messageIdError(new ZulipNoAnswer('not-json', 'POST', '/messages', true, 'private', status, 10), new MessageIds('alpha', mode));
    assert.match(safe.message, /host operator must repair authorization/);
    assert.match(safe.message, /Server Retry-After: 10 seconds; access must be corrected first/);
    assert.doesNotMatch(safe.message, / Retry after/);
  }
});
