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

function gate(): { open: () => void; opened: Promise<void> } {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => { open = resolve; });
  return { open, opened };
}

function lanes(overflow: (lane: Lane<string>) => void = () => {}, max = 5000): Lanes<string> {
  return new Lanes('writer', { maxWaitingMessages: max, onOverflow: overflow });
}

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

test("one job with a turn runs at a time across lanes, and a lane waiting for the turn runs its host's queue", async () => {
  const l = lanes();
  const log: string[] = [];
  const readA = gate();
  l.lane(A).enter(async () => { log.push('A read start'); await readA.opened; log.push('A read end'); }, { kind: 'other', turn: true });
  l.lane(B).enter(() => { log.push('B read'); }, { kind: 'other', turn: true });
  await new Promise((r) => setImmediate(r));
  l.lane(B).host(() => { log.push('B open'); });
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(log, ['A read start', 'B open']);
  readA.open();
  await l.quiet();
  assert.deepEqual(log, ['A read start', 'B open', 'A read end', 'B read']);
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
  const ending = l.end(60_000);
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
  assert.equal(await l.end(0), 1, 'one job still running');
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

test('jobs get the writer', async () => {
  const l = lanes();
  let seen: string | null = null;
  l.lane(A).enter((ctx) => { seen = ctx.w; });
  await l.quiet();
  assert.equal(seen, 'writer');
});

test('past the cap, the lane with the most waiting messages is given up: its messages leave, and one job takes the place of the first', async () => {
  const log: string[] = [];
  const overflowed: string[] = [];
  const running = gate();
  let given: number[] = [];
  const l = lanes((lane) => {
    overflowed.push(String(lane.key));
    given = lane.replaceMessages(() => { log.push('read'); }, { kind: 'other', turn: true });
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

test('the place moves up to what a lane decides, and never down', () => {
  const lane = lanes().lane(A);
  assert.equal(lane.place, undefined);
  lane.decide(5);
  lane.decide(3);
  assert.equal(lane.place, 5);
  lane.decide(8);
  assert.equal(lane.place, 8);
});
