/**
 * The lanes: one job at a time on a channel, the host's queue first, and
 * what all lanes share.
 *
 * Run: node --import tsx --test test/lanes.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { streamKey } from '../src/channel-key.ts';
import { Lanes, type Lane } from '../src/lanes.ts';

const A = streamKey(1);
const B = streamKey(2);
const C = streamKey(3);

function gate(): { open: () => void; opened: Promise<void> } {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => { open = resolve; });
  return { open, opened };
}

function lanes(overflow: (lane: Lane<string>) => void = () => {}, max = 5000, moments: string[] = []): Lanes<string> {
  return new Lanes('writer', {
    entry: (w, key, id, ended) => moments.push(`${w} entry ${key} ${id}${ended ? ' ended' : ''}`),
    overflow: (w, lane) => { moments.push(`${w} overflow ${lane.key}`); overflow(lane); },
    start: (w) => moments.push(`${w} start`),
    end: (w) => moments.push(`${w} end`),
  }, { maxWaitingMessages: max });
}

const settled = async () => {};

test('a lane runs one job at a time, in the order the jobs entered', async () => {
  const l = lanes();
  const log: string[] = [];
  const first = gate();
  l.lane(A).enter(async () => { log.push('1 start'); await first.opened; log.push('1 end'); });
  l.lane(A).enter(() => { log.push('2'); });
  l.lane(A).enter(() => { log.push('3'); });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['1 start']);
  first.open();
  await l.quiet();
  assert.deepEqual(log, ['1 start', '1 end', '2', '3']);
});

test("the host's queue goes first: a host job waits for the running job only", async () => {
  const l = lanes();
  const log: string[] = [];
  const running = gate();
  l.lane(A).enter(async () => { await running.opened; log.push('message 1'); });
  l.lane(A).enter(() => { log.push('message 2'); });
  await new Promise((r) => setImmediate(r));
  l.lane(A).host(() => { log.push('open'); });
  running.open();
  await l.quiet();
  assert.deepEqual(log, ['message 1', 'open', 'message 2']);
});

test("a job held on one lane doesn't hold another lane", async () => {
  const l = lanes();
  const log: string[] = [];
  const held = gate();
  l.lane(A).enter(async () => { await held.opened; log.push('A'); });
  l.lane(B).enter(() => { log.push('B'); });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['B']);
  held.open();
  await l.quiet();
  assert.deepEqual(log, ['B', 'A']);
});

test("a hold keeps the other queue back until it settles, and the host's queue runs meanwhile", async () => {
  for (const settle of ['resolve', 'reject'] as const) {
    const l = lanes();
    const log: string[] = [];
    let release!: () => void;
    const answer = new Promise<void>((resolve, reject) => { release = settle === 'resolve' ? resolve : () => reject(new Error('no answer')); });
    l.lane(A).holdOtherUntil(answer);
    l.lane(A).enter(() => { log.push('message'); });
    l.lane(A).host(() => { log.push('open'); });
    await new Promise((r) => setImmediate(r));
    assert.deepEqual(log, ['open'], settle);
    let quiet = false;
    void l.quiet().then(() => { quiet = true; });
    await new Promise((r) => setImmediate(r));
    assert.equal(quiet, false, `${settle}: a message kept back by a hold is waiting`);
    release();
    await l.quiet();
    assert.deepEqual(log, ['open', 'message'], settle);
  }
});

test("holding all lanes keeps each other queue back until it settles, a lane made meanwhile included, and the host's queues run", async () => {
  const l = lanes();
  const log: string[] = [];
  const start = gate();
  l.lane(A).enter(() => { log.push('A message'); });
  l.holdAll(start.opened);
  l.lane(B).enter(() => { log.push('B message'); });
  l.lane(B).host(() => { log.push('B open'); });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['B open']);
  assert.equal(l.allHeld, true);
  l.lane(A).enterFirst(() => { log.push('A read'); });
  start.open();
  await l.quiet();
  assert.deepEqual(log.filter((s) => s.startsWith('A')), ['A read', 'A message']);
  assert.deepEqual(log.filter((s) => s.startsWith('B')), ['B open', 'B message']);
  assert.equal(l.allHeld, false);
});

test('enterFirst goes ahead of what waits in the other queue', async () => {
  const l = lanes();
  const log: string[] = [];
  const running = gate();
  l.lane(A).enter(async () => { await running.opened; log.push('running'); });
  l.lane(A).enter(() => { log.push('waiting'); });
  await new Promise((r) => setImmediate(r));
  l.lane(A).enterFirst(() => { log.push('again'); });
  running.open();
  await l.quiet();
  assert.deepEqual(log, ['running', 'again', 'waiting']);
});

test("one job that reads runs at a time across lanes, and a lane waiting for the turn runs its host's queue", async () => {
  const l = lanes();
  const log: string[] = [];
  const readA = gate();
  l.lane(A).enterRead(async () => { log.push('A read start'); await readA.opened; log.push('A read end'); });
  l.lane(B).enterRead(() => { log.push('B read'); });
  await new Promise((r) => setImmediate(r));
  l.lane(B).host(() => { log.push('B open'); });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['A read start', 'B open']);
  readA.open();
  await l.quiet();
  assert.deepEqual(log, ['A read start', 'B open', 'A read end', 'B read']);
});

test('a job that reads gives the turn back before what it hands on runs: another lane reads meanwhile, and the first job\'s `reading` is false while it hands', async () => {
  const l = lanes();
  const log: string[] = [];
  const handing = gate();
  let readingWhileHanding: boolean | null = null;
  l.lane(A).enterRead((r) => {
    log.push('A read');
    return async (ctx) => {
      log.push(`A hands, with ${ctx.w}`);
      readingWhileHanding = r.reading;
      await handing.opened;
      log.push('A handed');
    };
  });
  l.lane(B).enterRead(() => { log.push('B read'); });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['A read', 'A hands, with writer', 'B read']);
  assert.equal(readingWhileHanding, false);
  handing.open();
  await l.quiet();
  assert.deepEqual(log, ['A read', 'A hands, with writer', 'B read', 'A handed']);
});

test('the turn goes to the lane that has waited longest for it', async () => {
  const l = lanes();
  const log: string[] = [];
  for (const [name, key] of [['A', A], ['B', B], ['C', C]] as const) {
    for (let i = 1; i <= 3; i++) {
      l.lane(key).enterRead(async () => {
        log.push(`${name}${i}`);
        await new Promise((r) => setImmediate(r));
      });
    }
  }
  await l.quiet();
  assert.deepEqual(log, ['A1', 'B1', 'C1', 'A2', 'B2', 'C2', 'A3', 'B3', 'C3']);
});

test("a lane that has waited longest but can't start its read when the turn comes back doesn't keep it from the next", async () => {
  const l = lanes();
  const log: string[] = [];
  const readC = gate();
  const holdA = gate();
  l.lane(C).enterRead(async () => { log.push('C'); await readC.opened; });
  await new Promise((r) => setImmediate(r));
  l.lane(A).enterRead(() => { log.push('A'); });
  await new Promise((r) => setImmediate(r));
  l.lane(B).enterRead(() => { log.push('B'); });
  await new Promise((r) => setImmediate(r));
  l.lane(A).holdOtherUntil(holdA.opened);
  readC.open();
  for (let i = 0; i < 10 && log.length < 2; i++) await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['C', 'B']);
  holdA.open();
  await l.quiet();
  assert.deepEqual(log, ['C', 'B', 'A']);
});

test("a lane that a host's job reaches as the turn comes back to it doesn't keep the turn from the next lane that can read", async () => {
  const l = lanes();
  const log: string[] = [];
  const readC = gate();
  const hostA = gate();
  l.lane(C).enterRead(async () => {
    log.push('C');
    await readC.opened;
    return () => { l.lane(A).host(async () => { log.push('A host'); await hostA.opened; }); };
  });
  await new Promise((r) => setImmediate(r));
  l.lane(A).enterRead(() => { log.push('A'); });
  await new Promise((r) => setImmediate(r));
  l.lane(B).enterRead(() => { log.push('B'); });
  await new Promise((r) => setImmediate(r));
  readC.open();
  for (let i = 0; i < 10 && log.length < 3; i++) await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['C', 'A host', 'B']);
  hostA.open();
  await l.quiet();
  assert.deepEqual(log, ['C', 'A host', 'B', 'A']);
});

test("a lane that a host's job reaches just as its hold ends doesn't keep the turn from the next lane that can read, with the host's job 0 to 6 microtasks after the holds end", async () => {
  for (let k = 0; k <= 6; k++) {
    const l = lanes();
    const log: string[] = [];
    const readC = gate();
    const hostA = gate();
    const holdA = gate();
    const holdB = gate();
    l.lane(C).enterRead(async () => { log.push('C'); await readC.opened; });
    await new Promise((r) => setImmediate(r));
    l.lane(A).enterRead(() => { log.push('A'); });
    await new Promise((r) => setImmediate(r));
    l.lane(B).enterRead(() => { log.push('B'); });
    await new Promise((r) => setImmediate(r));
    l.lane(A).holdOtherUntil(holdA.opened);
    l.lane(B).holdOtherUntil(holdB.opened);
    await new Promise((r) => setImmediate(r));
    readC.open();
    await new Promise((r) => setImmediate(r));
    const hostJobAfter = (hops: number): void => {
      if (hops === 0) l.lane(A).host(async () => { log.push('A host'); await hostA.opened; });
      else queueMicrotask(() => hostJobAfter(hops - 1));
    };
    holdB.open();
    queueMicrotask(() => hostJobAfter(k));
    holdA.open();
    for (let i = 0; i < 10; i++) await new Promise((r) => setImmediate(r));
    assert.ok(log.includes('B'), `${k} hops: B didn't read while A's host job ran: ${log.join(', ')}`);
    hostA.open();
    await l.quiet();
    assert.deepEqual([...log].sort(), ['A', 'A host', 'B', 'C'], `${k} hops`);
  }
});

test('ending the lanes drops what waits, tells a running job, waits for it within the grace, and takes nothing after', async () => {
  const l = lanes();
  const log: string[] = [];
  const fetch = gate();
  let endedAfterWait: boolean | null = null;
  l.lane(A).enter(async (ctx) => { await fetch.opened; endedAfterWait = ctx.ended; });
  l.lane(A).enter(() => { log.push('waiting'); }, { kind: 'message', id: 7 });
  await new Promise((r) => setImmediate(r));
  assert.equal(l.waitingMessages, 1);
  const ending = l.finish(60_000, settled);
  assert.equal(l.waitingMessages, 0, 'what waited is gone');
  l.lane(A).host(() => { log.push('answer after the end'); });
  l.lane(B).enter(() => { log.push('new lane after the end'); });
  fetch.open();
  assert.equal(await ending, 0, 'no job is left running');
  await new Promise((r) => setImmediate(r));
  assert.equal(endedAfterWait, true);
  assert.deepEqual(log, []);
});

test('ending the lanes gives up waiting for a running job at the grace', async () => {
  const l = lanes();
  l.lane(A).enter(() => new Promise<void>(() => {}));
  await new Promise((r) => setImmediate(r));
  assert.equal(await l.finish(0, settled), 1, 'one job still running');
});

test('quiet settles only when no lane runs or waits', async () => {
  const l = lanes();
  const running = gate();
  l.lane(A).enter(async () => { await running.opened; });
  let quiet = false;
  void l.quiet().then(() => { quiet = true; });
  await new Promise((r) => setImmediate(r));
  assert.equal(quiet, false);
  running.open();
  await l.quiet();
  assert.equal(quiet, true);
});

test('a job that throws is logged with its channel, and its lane goes on', async (t) => {
  const logged: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logged.push(args.map(String).join(' ')); });
  const l = lanes();
  const log: string[] = [];
  l.lane(A).enter(() => { throw new Error('boom'); });
  l.lane(A).enter(() => { log.push('next'); });
  await l.quiet();
  assert.deepEqual(log, ['next']);
  assert.deepEqual(logged, [`[zulip-mcp] a delivery job on ${A} failed: boom`]);
});

test('jobs get the writer and their lane, an answer after the end too', async () => {
  const l = lanes();
  const seen: string[] = [];
  l.lane(A).enter((ctx) => { seen.push(`${ctx.w} ${ctx.lane.key} ${ctx.ended}`); });
  l.lane(B).host((ctx) => { seen.push(`${ctx.w} ${ctx.lane.key} ${ctx.ended}`); });
  await l.quiet();
  await l.finish(0, settled);
  l.lane(A).answer((ctx) => { seen.push(`${ctx.w} ${ctx.lane.key} ${ctx.ended}`); });
  assert.deepEqual(seen, [`writer ${A} false`, `writer ${B} false`, `writer ${A} true`]);
});

test('the lanes write at four moments, with the writer: their making, a message with an id that enters, the cap, and the end after what settles it', async () => {
  const moments: string[] = [];
  const l = lanes((lane) => { lane.replaceMessages(() => {}); }, 1, moments);
  assert.deepEqual(moments, ['writer start']);
  const running = gate();
  l.lane(A).enter(async () => { await running.opened; });
  l.lane(A).enter(() => {}, { kind: 'message', id: 10 });
  l.lane(A).enter(() => {}, { kind: 'message' });
  l.lane(B).enterFirst(() => {}, { kind: 'message', id: 20 });
  l.lane(A).enter(() => {});
  assert.deepEqual(moments, ['writer start', `writer entry ${A} 10`, `writer overflow ${A}`, `writer entry ${B} 20`]);
  moments.length = 0;
  running.open();
  await l.quiet();
  l.end();
  assert.deepEqual(moments, [], 'ending the lanes writes nothing');
  let ran = false;
  l.lane(A).enter(() => { ran = true; }, { kind: 'message', id: 30 });
  l.lane(C).enterFirst(() => { ran = true; }, { kind: 'message', id: 40 });
  await l.finish(0, async () => { moments.push('settle'); });
  assert.equal(ran, false, 'nothing runs on an ended lane');
  assert.deepEqual(moments, [`writer entry ${A} 30 ended`, `writer entry ${C} 40 ended`, 'settle', 'writer end']);
});

test('past the cap, the lane with the most waiting messages is given up: its messages leave, and one job takes the place of the first', async () => {
  const log: string[] = [];
  const overflowed: string[] = [];
  const running = gate();
  let given: number[] = [];
  const l = lanes((lane) => {
    overflowed.push(String(lane.key));
    given = lane.replaceMessages(() => { log.push('read'); });
  }, 3);
  l.lane(B).enter(async () => { await running.opened; log.push('B running'); });
  l.lane(B).enter(() => { log.push('B 20'); }, { kind: 'message', id: 20 });
  l.lane(A).enter(async () => { await running.opened; log.push('A running'); });
  l.lane(A).enter(() => { log.push('A 10'); }, { kind: 'message', id: 10 });
  l.lane(A).enter(() => { log.push('A change'); });
  l.lane(A).enter(() => { log.push('A 11'); }, { kind: 'message', id: 11 });
  assert.deepEqual(overflowed, []);
  l.lane(A).enter(() => { log.push('A 12'); }, { kind: 'message', id: 12 });
  assert.deepEqual(overflowed, [String(A)]);
  assert.deepEqual(given, [10, 11, 12]);
  assert.equal(l.waitingMessages, 1, "only B's message still waits");
  running.open();
  await l.quiet();
  assert.deepEqual(log.filter((x) => x.startsWith('A') || x === 'read'), ['A running', 'read', 'A change']);
  assert.ok(log.includes('B 20'));
});

test('at a give-up, a job about given-up messages alone leaves with them, and one about a message that waits elsewhere, or about none, stays', async () => {
  const log: string[] = [];
  const running = gate();
  const l = lanes((lane) => { lane.replaceMessages(() => { log.push('read'); }); }, 2);
  l.lane(A).enter(async () => { await running.opened; log.push('running'); });
  l.lane(A).enter(() => { log.push('10'); }, { kind: 'message', id: 10 });
  l.lane(A).enter(() => { log.push('about 10'); }, { kind: 'other', about: [10] });
  l.lane(A).enter(() => { log.push('11'); }, { kind: 'message', id: 11 });
  l.lane(A).enter(() => { log.push('about 10 and 5'); }, { kind: 'other', about: [10, 5] });
  l.lane(A).enter(() => { log.push('about 10 and 11'); }, { kind: 'other', about: [10, 11] });
  l.lane(A).enter(() => { log.push('about none'); });
  l.lane(A).enter(() => { log.push('12'); }, { kind: 'message', id: 12 });
  running.open();
  await l.quiet();
  assert.deepEqual(log, ['running', 'read', 'about 10 and 5', 'about none']);
});

test('the place moves up to what a job decides, and never down', async () => {
  const lane = lanes().lane(A);
  assert.equal(lane.place, undefined);
  lane.enter((ctx) => { ctx.decide(5); ctx.decide(3); ctx.decide(NaN); });
  await new Promise((r) => setImmediate(r));
  assert.equal(lane.place, 5);
  lane.enter((ctx) => { ctx.decide(8); });
  await new Promise((r) => setImmediate(r));
  assert.equal(lane.place, 8);
});
