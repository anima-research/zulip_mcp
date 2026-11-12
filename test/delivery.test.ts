/**
 * Delivery bookkeeping — watermarks, the open mirror, missed tallies, and
 * the pure catch-up helpers (mention vicinity, <missed> rendering).
 *
 * Run: node --import tsx --test test/delivery.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import { DeliveryState, attributeMessage, renderMissedBlock, selectMissed, viewOf, type MissedView } from '../src/delivery.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import { dmKey, streamKey, type KeyedMessage } from '../src/channel-key.ts';

function directoryOf(streams: Record<number, string> = {}): StreamDirectory {
  const directory = new StreamDirectory();
  directory.registration(Object.entries(streams).map(([id, name]) => ({ stream_id: Number(id), name })));
  return directory;
}

const GENERAL = streamKey(1);
const DEV = streamKey(2);
const NEVER = streamKey(3);

function tmp(): string {
  return mkdtempSync(join(tmpdir(), 'zulip-delivery-'));
}

test('watermarks only advance, and survive a restart', () => {
  const dir = tmp();
  try {
    const a = new DeliveryState(dir, 's1', directoryOf({ 1: 'general' }));
    assert.equal(a.watermark(GENERAL), undefined);
    assert.equal(a.advance(GENERAL, 10), true);
    assert.equal(a.advance(GENERAL, 7), false, 'never retreats');
    assert.equal(a.advance(GENERAL, 12), true);
    a.save();

    const b = new DeliveryState(dir, 's1', new StreamDirectory());
    assert.equal(b.watermark(GENERAL), 12);
    assert.deepEqual(b.watermarkedChannels(), [GENERAL]);

    // Sessions are isolated.
    const other = new DeliveryState(dir, 's2', new StreamDirectory());
    assert.equal(other.watermark(GENERAL), undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the undelivered floor: the watermark never passes an offered-but-unaccepted id, and lifts when it is delivered', () => {
  const state = new DeliveryState(null, 'mem', directoryOf());
  // 1, 2, 3 offered; 1 and 3 accepted, 2 rejected.
  for (const id of [1, 2, 3]) state.hold(GENERAL, id);
  assert.equal(state.floor(GENERAL), 1);
  state.release(GENERAL, 1);
  state.release(GENERAL, 3);
  state.hold(GENERAL, 2, 'rejected');
  assert.equal(state.watermark(GENERAL), 1, 'a channel with no watermark gets one just below a hold that is no longer on its way');
  assert.equal(state.advance(GENERAL, 3), false);
  assert.equal(state.watermark(GENERAL), 1, 'stops below the rejected id');

  // A later batch is accepted in full: still capped by the floor.
  state.hold(GENERAL, 4);
  state.release(GENERAL, 4);
  assert.equal(state.advance(GENERAL, 4), false);
  assert.equal(state.watermark(GENERAL), 1);
  assert.deepEqual(state.heldIds(GENERAL), [2]);
  assert.deepEqual(state.heldIds(GENERAL, { replayable: true }), [2], 'rejected → a live replay may try once');
  state.markReplayed(GENERAL, [2]);
  assert.deepEqual(state.heldIds(GENERAL, { replayable: true }), [], 'and only once');

  // The replay is accepted: the floor lifts and the watermark catches up to everything accepted.
  assert.equal(state.release(GENERAL, 2), true);
  assert.equal(state.watermark(GENERAL), 4);
  assert.equal(state.floor(GENERAL), undefined);

  // A batch given up on holds too; a catch-up block that scanned past it releases it.
  state.hold(GENERAL, 5, 'failed');
  state.hold(GENERAL, 6);
  state.release(GENERAL, 6);
  state.advance(GENERAL, 6);
  assert.equal(state.watermark(GENERAL), 4);
  assert.equal(state.advanceThrough(GENERAL, 6), true);
  assert.equal(state.watermark(GENERAL), 6);
  assert.deepEqual(state.heldChannels(), []);

  // Already forwarded ids are never held; releasing everything drops a channel's floor.
  state.hold(GENERAL, 3);
  assert.equal(state.floor(GENERAL), undefined);
  state.hold(GENERAL, 9, 'failed');
  state.advance(GENERAL, 10);
  assert.equal(state.watermark(GENERAL), 8);
  assert.equal(state.releaseAll(GENERAL), true);
  assert.equal(state.watermark(GENERAL), 10);
});

test('closing a channel starts a tally anchored at the watermark; reopening clears it', () => {
  const state = new DeliveryState(null, 'mem', directoryOf());
  state.advance(DEV, 100);
  state.markOpen(DEV);
  assert.equal(state.tally(DEV), undefined);
  assert.equal(state.countMissed(DEV, { id: 101, text: 'x' }), false, 'open channels are not tallied');

  state.markClosed(DEV);
  assert.deepEqual(state.tally(DEV), { anchorId: 100, talliedThrough: 100, messages: 0, characters: 0 });
  assert.equal(state.countMissed(DEV, { id: 101, text: 'hello' }), true);
  assert.equal(state.countMissed(DEV, { id: 103, text: 'world!' }), true);
  assert.deepEqual(state.tally(DEV), { anchorId: 100, talliedThrough: 103, messages: 2, characters: 11 });

  // Closing a channel that was never open changes nothing.
  state.markClosed(NEVER);
  assert.equal(state.tally(NEVER), undefined);

  state.markOpen(DEV);
  assert.equal(state.tally(DEV), undefined);
  assert.equal(state.wasOpen(DEV), true);
});

test('backfill folds fetched ambient into the tally and moves the cursor', () => {
  const state = new DeliveryState(null, 'mem', directoryOf());
  state.markOpen(DEV);
  state.markClosed(DEV);
  state.backfillMissed(DEV, [{ id: 5, text: 'ab' }, { id: 6, text: 'cde' }], 9);
  assert.deepEqual(state.tally(DEV), { anchorId: 0, talliedThrough: 9, messages: 2, characters: 5 });
});

test('the state file is round-tripped, sorted, and tolerant of garbage', () => {
  const dir = tmp();
  try {
    const A = streamKey(1);
    const B = streamKey(2);
    const a = new DeliveryState(dir, 's', directoryOf({ 1: 'a', 2: 'b' }));
    a.advance(B, 2);
    a.advance(A, 1);
    a.markOpen(B);
    a.markOpen(A);
    a.markClosed(A);
    a.countMissed(A, { id: 3, text: 'xyz' });
    a.save();

    const raw = JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8'));
    assert.deepEqual(Object.keys(raw.watermarks), ['zulip:a', 'zulip:b']);
    assert.deepEqual(raw.lastOpen, ['zulip:b']);
    assert.deepEqual(raw.missed['zulip:a'], { anchorId: 1, talliedThrough: 3, messages: 1, characters: 3 });
    assert.deepEqual(raw.streams, { 1: 'a', 2: 'b' });

    const b = new DeliveryState(dir, 's', new StreamDirectory());
    assert.equal(b.watermark(B), 2);
    assert.equal(b.wasOpen(B), true);
    assert.equal(b.wasOpen(A), false);
    assert.deepEqual(b.tally(A), { anchorId: 1, talliedThrough: 3, messages: 1, characters: 3 });

    // A corrupt file is reported and ignored, not fatal.
    const dirty = tmp();
    try {
      const path = join(dirty, 'x.delivery.json');
      writeFileSync(path, '{not json');
      const c = new DeliveryState(dirty, 'x', new StreamDirectory());
      assert.deepEqual(c.watermarkedChannels(), []);
    } finally {
      rmSync(dirty, { recursive: true, force: true });
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

function msg(id: number, mentioned: boolean, text = `m${id}`): IncomingChannelMessage {
  return {
    channelId: 'zulip:general',
    messageId: String(id),
    threadId: 'topic-a',
    author: { id: '9', name: 'Ann' },
    timestamp: new Date(1_700_000_000_000 + id * 1000).toISOString(),
    content: [{ type: 'text', text }, ...(id % 2 ? [] : [{ type: 'text' as const, text: '[attachments: 1]\n- shot.png' }])],
    tags: [],
    metadata: { topic: 'topic-a', mentioned, attachments: id % 2 ? undefined : [{ name: 'shot.png' }] },
  };
}

test('viewOf reads the sweep fields off an incoming message', () => {
  const v = viewOf(msg(4, true, 'hi there'));
  assert.equal(v.id, 4);
  assert.equal(v.mentioned, true);
  assert.equal(v.topic, 'topic-a');
  assert.equal(v.authorName, 'Ann');
  assert.equal(v.text, 'hi there', 'the attachment note is not part of the text');
  assert.deepEqual(v.attachmentNames, ['shot.png']);
  // A DM is addressed even without a mention flag.
  assert.equal(viewOf({ ...msg(5, false), metadata: { isDM: true } }).isDM, true);
});

test('selectMissed keeps every message for a channel the host had open, else mentions with vicinity', () => {
  const views: MissedView[] = Array.from({ length: 30 }, (_, i) => viewOf(msg(i + 1, i + 1 === 10 || i + 1 === 25)));
  assert.equal(selectMissed(views, { keepAll: true, vicinity: 2 }).length, 30);

  const kept = selectMissed(views, { keepAll: false, vicinity: 2 });
  assert.deepEqual(kept.map((v) => v.id), [8, 9, 10, 11, 12, 23, 24, 25, 26, 27]);

  // Windows overlap and clamp at the edges without duplicating.
  const edge = selectMissed(views.slice(0, 3).map((v) => ({ ...v, mentioned: v.id === 1 })), { keepAll: false, vicinity: 5 });
  assert.deepEqual(edge.map((v) => v.id), [1, 2, 3]);

  assert.deepEqual(selectMissed(views.map((v) => ({ ...v, mentioned: false })), { keepAll: false, vicinity: 7 }), []);
});

test('renderMissedBlock leads with ids, flags mentions, and reports counts by reason', () => {
  const views = [viewOf(msg(1, false, 'context')), viewOf(msg(2, true, 'ping')), viewOf(msg(3, false, 'after'))];
  const block = renderMissedBlock(views, {
    streamName: 'general',
    channelId: 'zulip:general',
    reason: 'mention',
    count: 1,
    formatTime: () => '12:00',
  });
  const lines = block.split('\n');
  assert.equal(lines[0], '<missed stream="#general" channelId="zulip:general" count="1" lines="3" reason="mention">');
  assert.equal(lines[1], '[12:00 id=1] [#general > topic-a] Ann: context');
  assert.equal(lines[2], '[12:00 id=2] [#general > topic-a] Ann (mention): ping [attachments: shot.png]');
  assert.equal(lines[4], '</missed>');

  const backscroll = renderMissedBlock(views, {
    streamName: 'general',
    channelId: 'zulip:general',
    reason: 'backscroll',
    count: 3,
    formatTime: () => '',
  });
  assert.match(backscroll, /^<missed stream="#general" channelId="zulip:general" count="3" reason="backscroll">/);
  assert.match(backscroll, /\n\[id=1\] /, 'an empty timestamp leaves the id alone');
});

test('renderMissedBlock elides the oldest lines over budget and points at what lies beyond', () => {
  const views = Array.from({ length: 50 }, (_, i) => viewOf(msg(i + 1, false, 'x'.repeat(100))));
  const block = renderMissedBlock(views, {
    streamName: 'general', channelId: 'zulip:general', reason: 'backscroll', count: 50, formatTime: () => '',
    maxChars: 1500, moreBeyond: true,
  });
  assert.ok(block.length < 1500 + 400, `block is ${block.length} chars`);
  const lines = block.split('\n');
  assert.match(lines[0], /elided="\d+"/);
  assert.match(lines[0], /truncated="true"/);
  assert.match(lines[1], /^\[\d+ earlier line\(s\) elided \(ids 1–\d+\).*fetch_history\(channel="zulip:general", before=\d+\)/);
  assert.match(lines[2], /^\[id=\d+\] /);
  assert.match(lines[lines.length - 1], /^<\/missed>$/);
  assert.match(lines[lines.length - 2], /catch-up ceiling was reached.*after=50/);
  assert.ok(block.includes('[id=50]'), 'the newest line survives');
  assert.ok(!block.includes('[id=1]'), 'the oldest is gone');

  // Under budget: untouched, no annotations.
  const small = renderMissedBlock(views.slice(0, 2), {
    streamName: 'general', channelId: 'zulip:general', reason: 'backscroll', count: 2, formatTime: () => '', maxChars: 40_000,
  });
  assert.doesNotMatch(small, /elided|truncated/);
});

test('renderMissedBlock: the budget is a hard cap on the whole block, one long line included', () => {
  // Header, elision note and ceiling note all count; the block never exceeds the budget.
  const views = Array.from({ length: 30 }, (_, i) => viewOf(msg(i + 1, false, 'y'.repeat(120))));
  const tight = renderMissedBlock(views, {
    streamName: 'general', channelId: 'zulip:dm:42', reason: 'backscroll', count: 30, formatTime: () => '',
    maxChars: 1000, moreBeyond: true, newestScannedId: 30,
  });
  assert.ok(tight.length <= 1000, `block is ${tight.length} chars`);
  assert.match(tight, /elided="\d+"/);
  assert.match(tight, /fetch_history\(channel="zulip:dm:42", before=\d+\)/, 'a DM conversation is pointed at by its channel id');
  assert.match(tight, /fetch_history\(channel="zulip:dm:42", after=30\)/);
  assert.ok(tight.includes('[id=30]'), 'the newest line survives');

  // A single 5 000-character message under a 1 000-character budget: cut, and said so.
  const huge = renderMissedBlock([viewOf(msg(7, true, 'z'.repeat(5000)))], {
    streamName: 'general', channelId: 'zulip:general', reason: 'mention', count: 1, formatTime: () => '', maxChars: 1000,
  });
  assert.ok(huge.length <= 1000, `block is ${huge.length} chars`);
  assert.match(huge, /^<missed stream="#general" channelId="zulip:general" count="1" lines="1" reason="mention">\n\[id=7\] \[#general > topic-a\] Ann \(mention\): z+ … \[line cut to fit the catch-up budget — fetch_around\(7\) has the whole message\]\n<\/missed>$/);
});

// ── attributeMessage ──

function incoming(over: Partial<KeyedMessage> = {}): KeyedMessage {
  return {
    key: streamKey(5),
    messageId: '17206924',
    threadId: 'router',
    author: { id: '760', name: 'Mykhailo Buialo' },
    timestamp: '2026-09-14T08:42:52.000Z',
    content: [{ type: 'text', text: '@Knowledge Resident do you have the same issue' }],
    tags: ['chat:mention', 'chat:from-human'],
    metadata: { topic: 'router', mentioned: true, isDM: false },
    ...over,
  };
}

test('attributeMessage renders who/where/when into the body in the fetch_history line shape, and keeps the fields', () => {
  const out = attributeMessage(incoming(), 'qa', () => '2026-09-14T08:42:52Z');
  assert.equal((out.content[0] as { text: string }).text, '[2026-09-14T08:42:52Z id=17206924] [#qa > router] Mykhailo Buialo (mention): @Knowledge Resident do you have the same issue');
  assert.deepEqual(out.author, { id: '760', name: 'Mykhailo Buialo' });
  assert.equal(out.threadId, 'router');
  assert.equal((out.metadata as { topic: string }).topic, 'router');
  assert.equal((out.metadata as { attributed: boolean }).attributed, true);
  assert.equal((out.metadata as { attributionHeader: string }).attributionHeader, '[2026-09-14T08:42:52Z id=17206924] [#qa > router] Mykhailo Buialo (mention): ', 'the exact prefix, so a host can strip it');
  assert.equal((out.content[0] as { text: string }).text.startsWith((out.metadata as { attributionHeader: string }).attributionHeader), true);

  // Ambient (no mention) has no marker; a DM says so instead of stream > topic.
  assert.equal((attributeMessage(incoming({ metadata: { topic: 'router', mentioned: false } }), 'qa', () => 'T').content[0] as { text: string }).text.startsWith('[T id=17206924] [#qa > router] Mykhailo Buialo: @'), true);
  const dm = attributeMessage(incoming({ key: dmKey([760]), threadId: undefined, metadata: { isDM: true, mentioned: false }, content: [{ type: 'text', text: 'hi' }] }), null, () => 'T');
  assert.equal((dm.content[0] as { text: string }).text, '[T id=17206924] [DM] Mykhailo Buialo: hi');
});

test('attributeMessage: no timestamp style, attachments after the body, image-only content, and idempotence', () => {
  const none = attributeMessage(incoming(), 'qa', () => '');
  assert.equal((none.content[0] as { text: string }).text.startsWith('[id=17206924] [#qa > router]'), true, 'AGENT_TIMESTAMP_STYLE=none drops the time, not the id');

  const withNote = attributeMessage(incoming({ content: [{ type: 'text', text: 'see file' }, { type: 'text', text: '[attachments: 1]\n- a.pdf' }] }), 'qa', () => 'T');
  assert.equal((withNote.content[0] as { text: string }).text, '[T id=17206924] [#qa > router] Mykhailo Buialo (mention): see file');
  assert.equal((withNote.content[1] as { text: string }).text, '[attachments: 1]\n- a.pdf', 'the note is untouched');

  const imageOnly = attributeMessage(incoming({ content: [{ type: 'image', data: 'AAAA', mimeType: 'image/png' }] }), 'qa', () => 'T');
  assert.equal((imageOnly.content[0] as { text: string }).text, '[T id=17206924] [#qa > router] Mykhailo Buialo (mention):');
  assert.equal((imageOnly.metadata as { attributionHeader: string }).attributionHeader, '[T id=17206924] [#qa > router] Mykhailo Buialo (mention):', 'the recorded prefix is what was inserted');
  assert.equal(imageOnly.content[1].type, 'image');

  const original = incoming();
  const once = attributeMessage(original, 'qa', () => 'T');
  assert.deepEqual(attributeMessage(once, 'qa', () => 'T'), once, 'a replay does not double the header');
  assert.equal(viewOf(original).text, '@Knowledge Resident do you have the same issue', 'the input is not mutated');
  assert.equal((original.metadata as { attributed?: boolean }).attributed, undefined);

  const bad = attributeMessage(incoming({ timestamp: 'not-a-date' }), 'qa', () => { throw new Error('must not format an invalid date'); });
  assert.equal((bad.content[0] as { text: string }).text.startsWith('[id=17206924] '), true, 'an unparseable timestamp drops the time, not the message');
});

test('renderMissedBlock on a DM conversation renders [DM] lines, with no mention mark though every DM line is selected as addressed', () => {
  const dmMsg = (id: number, text: string): IncomingChannelMessage => ({
    channelId: 'zulip:dm:42',
    messageId: String(id),
    author: { id: '42', name: 'Bo' },
    timestamp: new Date(1_700_000_000_000).toISOString(),
    content: [{ type: 'text', text }],
    metadata: { isDM: true, mentioned: false },
  });
  const views = [viewOf(dmMsg(1, 'hey')), viewOf(dmMsg(2, 'you there?'))];
  assert.equal(views[0].isDM, true, 'selection still treats a DM as addressed');
  const block = renderMissedBlock(views, { streamName: 'DM: Bo', channelId: 'zulip:dm:42', reason: 'mention', count: 2, formatTime: () => 'T' });
  assert.deepEqual(block.split('\n').slice(1, 3), ['[T id=1] [DM] Bo: hey', '[T id=2] [DM] Bo: you there?']);
});

test('a state file as main writes it, saved before a stream list fills the directory, keeps its entries for the next start', async () => {
  const dir = tmp();
  try {
    const tally = { anchorId: 4, talliedThrough: 6, messages: 2, characters: 9 };
    writeFileSync(join(dir, 's.delivery.json'), JSON.stringify({ watermarks: { 'zulip:general': 10 }, missed: { 'zulip:dev': tally }, lastOpen: ['zulip:general'] }));
    new DeliveryState(dir, 's', new StreamDirectory()).save();

    const directory = new StreamDirectory();
    const later = new DeliveryState(dir, 's', directory);
    directory.registration([{ stream_id: 1, name: 'general' }, { stream_id: 2, name: 'dev' }]);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(later.watermark(GENERAL), 10);
    assert.equal(later.wasOpen(GENERAL), true);
    assert.deepEqual(later.tally(DEV), tally);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
