import { MessageIds } from '../src/message-ids.ts';
import test, { mock } from 'node:test';
import assert from 'node:assert/strict';
import fs, { mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync, type PathLike } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import { CARRIED_READ_TRIES, DeliveryState, HELD_SAVED_PER_CHANNEL, attributeMessage, renderMissedBlock, selectMissed, viewOf, type MissedView } from '../src/delivery.ts';
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

    const other = new DeliveryState(dir, 's2', new StreamDirectory());
    assert.equal(other.watermark(GENERAL), undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the undelivered floor: the watermark never passes an offered-but-unaccepted id, and lifts when it is delivered', () => {
  const state = new DeliveryState(null, 'mem', directoryOf());
  for (const id of [1, 2, 3]) state.hold(GENERAL, id);
  assert.equal(state.floor(GENERAL), 1);
  state.release(GENERAL, 1);
  state.release(GENERAL, 3);
  state.hold(GENERAL, 2, 'rejected');
  assert.equal(state.watermark(GENERAL), 1, 'a channel with no watermark gets one just below a hold that is no longer on its way');
  assert.equal(state.advance(GENERAL, 3), false);
  assert.equal(state.watermark(GENERAL), 1, 'stops below the rejected id');

  state.hold(GENERAL, 4);
  state.release(GENERAL, 4);
  assert.equal(state.advance(GENERAL, 4), false);
  assert.equal(state.watermark(GENERAL), 1);
  assert.deepEqual(state.heldIds(GENERAL), [2]);
  assert.deepEqual(state.heldIds(GENERAL, { replayable: true }), [2], 'rejected → a live replay may try once');
  state.markReplayed(GENERAL, [2]);
  assert.deepEqual(state.heldIds(GENERAL, { replayable: true }), [], 'and only once');

  assert.equal(state.release(GENERAL, 2), true);
  assert.equal(state.watermark(GENERAL), 4);
  assert.equal(state.floor(GENERAL), undefined);

  state.hold(GENERAL, 5, 'failed');
  state.hold(GENERAL, 6);
  state.release(GENERAL, 6);
  state.advance(GENERAL, 6);
  assert.equal(state.watermark(GENERAL), 4);
  assert.equal(state.advanceThrough(GENERAL, 6), true);
  assert.equal(state.watermark(GENERAL), 6);
  assert.deepEqual(state.heldChannels(), []);

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
  assert.equal(viewOf({ ...msg(5, false), metadata: { isDM: true } }).isDM, true);
});

test('selectMissed keeps every message for a channel the host had open, else mentions with vicinity', () => {
  const views: MissedView[] = Array.from({ length: 30 }, (_, i) => viewOf(msg(i + 1, i + 1 === 10 || i + 1 === 25)));
  assert.equal(selectMissed(views, { keepAll: true, vicinity: 2 }).length, 30);

  const kept = selectMissed(views, { keepAll: false, vicinity: 2 });
  assert.deepEqual(kept.map((v) => v.id), [8, 9, 10, 11, 12, 23, 24, 25, 26, 27]);

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
  }).text;
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
  }).text;
  assert.match(backscroll, /^<missed stream="#general" channelId="zulip:general" count="3" reason="backscroll">/);
  assert.match(backscroll, /\n\[id=1\] /, 'an empty timestamp leaves the id alone');
});

test('renderMissedBlock elides the oldest lines over budget and points at what lies beyond', () => {
  const views = Array.from({ length: 50 }, (_, i) => viewOf(msg(i + 1, false, 'x'.repeat(100))));
  const block = renderMissedBlock(views, {
    streamName: 'general', channelId: 'zulip:general', reason: 'backscroll', count: 50, formatTime: () => '',
    maxChars: 1500, moreBeyond: true,
  }).text;
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

  const small = renderMissedBlock(views.slice(0, 2), {
    streamName: 'general', channelId: 'zulip:general', reason: 'backscroll', count: 2, formatTime: () => '', maxChars: 40_000,
  }).text;
  assert.doesNotMatch(small, /elided|truncated/);
});

test('renderMissedBlock: the budget is a hard cap on the whole block, one long line included', () => {
  const views = Array.from({ length: 30 }, (_, i) => viewOf(msg(i + 1, false, 'y'.repeat(120))));
  const tight = renderMissedBlock(views, {
    streamName: 'general', channelId: 'zulip:dm:42', reason: 'backscroll', count: 30, formatTime: () => '',
    maxChars: 1000, moreBeyond: true, newestScannedId: 30,
  }).text;
  assert.ok(tight.length <= 1000, `block is ${tight.length} chars`);
  assert.match(tight, /elided="\d+"/);
  assert.match(tight, /fetch_history\(channel="zulip:dm:42", before=\d+\)/, 'a DM conversation is pointed at by its channel id');
  assert.match(tight, /fetch_history\(channel="zulip:dm:42", after=30\)/);
  assert.ok(tight.includes('[id=30]'), 'the newest line survives');

  const huge = renderMissedBlock([viewOf(msg(7, true, 'z'.repeat(5000)))], {
    streamName: 'general', channelId: 'zulip:general', reason: 'mention', count: 1, formatTime: () => '', maxChars: 1000,
  }).text;
  const tagged = renderMissedBlock([viewOf(msg(7, true, 'z'.repeat(5000)))], {
    streamName: 'general', channelId: 'zulip:general', reason: 'mention', count: 1, formatTime: () => '', maxChars: 1000,
    messageIds: new MessageIds('alpha', 'strict'),
  }).text;
  assert.ok(tagged.length <= 1000, `block is ${tagged.length} chars`);
  assert.match(tagged, /fetch_around\(message_id="alpha:7"\)/);
  assert.ok(huge.length <= 1000, `block is ${huge.length} chars`);
  assert.match(huge, /^<missed stream="#general" channelId="zulip:general" count="1" lines="1" reason="mention">\n\[id=7\] \[#general > topic-a\] Ann \(mention\): z+ … \[line cut to fit the catch-up budget — fetch_around\(7\) has the whole message\]\n<\/missed>$/);
});

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
  const block = renderMissedBlock(views, { streamName: 'DM: Bo', channelId: 'zulip:dm:42', reason: 'mention', count: 2, formatTime: () => 'T' }).text;
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

function withLogs(run: (logs: string[], dir: string, path: string) => void): void {
  const dir = mkdtempSync(join(tmpdir(), 'zulip-delivery-unreadable-'));
  const original = console.error;
  const logs: string[] = [];
  console.error = (...a: unknown[]) => { logs.push(a.map(String).join(' ')); };
  try {
    run(logs, dir, join(dir, 's1.delivery.json'));
  } finally {
    console.error = original;
    rmSync(dir, { recursive: true, force: true });
  }
}

const BROKEN = '{"watermarks": {"zulip:general": 42}, ';

test('a delivery state file that fails to load stays in place until a save, which first moves it aside under the name its log line gives', () => {
  withLogs((logs, dir, path) => {
    writeFileSync(path, BROKEN);
    const state = new DeliveryState(dir, 's1', directoryOf({ 1: 'general' }));
    assert.ok(logs.some((l) => l.includes('Failed to load delivery state')), 'the failure is logged');
    assert.deepEqual(readdirSync(dir), ['s1.delivery.json'], 'a load alone moves nothing');
    assert.equal(readFileSync(path, 'utf-8'), BROKEN);
    state.advance(GENERAL, 7);
    state.save();
    const aside = readdirSync(dir).filter((f) => f !== 's1.delivery.json');
    assert.equal(aside.length, 1, 'one file set aside');
    assert.ok(logs.some((l) => l.includes(join(dir, aside[0]))), 'a log line names where it went');
    assert.equal(readFileSync(join(dir, aside[0]), 'utf-8'), BROKEN, 'the unreadable bytes are kept as they were');
    assert.equal(JSON.parse(readFileSync(path, 'utf-8')).watermarks['zulip:general'], 7, 'the new state is under the old name');
  });
});

test('a save that cannot write the new state moves nothing, and the next save moves the file aside first', () => {
  withLogs((logs, dir, path) => {
    writeFileSync(path, BROKEN);
    const state = new DeliveryState(dir, 's1', directoryOf({ 1: 'general' }));
    const tmp = `${path}.${process.pid}.tmp`;
    mkdirSync(tmp);
    state.advance(GENERAL, 7);
    state.save();
    assert.ok(logs.some((l) => l.includes('Failed to save delivery state')), 'the save tried to write, and its failure is logged');
    assert.equal(readFileSync(path, 'utf-8'), BROKEN, 'the unreadable file is where it was');
    assert.deepEqual(readdirSync(dir).filter((f) => f.includes('.unreadable-')), [], 'nothing moved');
    rmSync(tmp, { recursive: true });
    state.save();
    const aside = readdirSync(dir).filter((f) => f.includes('.unreadable-'));
    assert.equal(aside.length, 1, 'the next save sets the file aside');
    assert.equal(readFileSync(join(dir, aside[0]), 'utf-8'), BROKEN);
    assert.equal(JSON.parse(readFileSync(path, 'utf-8')).watermarks['zulip:general'], 7);
  });
});

test('a save that cannot move the unreadable file aside writes nothing and leaves no temporary file, and the next save moves it first', () => {
  withLogs((logs, dir, path) => {
    writeFileSync(path, BROKEN);
    const state = new DeliveryState(dir, 's1', directoryOf({ 1: 'general' }));
    const rename = fs.renameSync;
    mock.method(fs, 'renameSync', (from: PathLike, to: PathLike) => {
      if (String(to).includes('.unreadable-')) throw Object.assign(new Error('EBUSY: resource busy'), { code: 'EBUSY' });
      return rename(from, to);
    });
    syncBuiltinESMExports();
    try {
      state.advance(GENERAL, 7);
      state.save();
    } finally {
      mock.restoreAll();
      syncBuiltinESMExports();
    }
    assert.ok(logs.some((l) => l.includes('could not be moved aside')), 'the save tried the move, and its failure is logged');
    assert.deepEqual(readdirSync(dir), ['s1.delivery.json'], 'nothing moved, nothing written, no temporary file left');
    assert.equal(readFileSync(path, 'utf-8'), BROKEN);
    state.save();
    const aside = readdirSync(dir).filter((f) => f.includes('.unreadable-'));
    assert.equal(aside.length, 1, 'the next save sets the file aside');
    assert.equal(readFileSync(join(dir, aside[0]), 'utf-8'), BROKEN);
    assert.equal(JSON.parse(readFileSync(path, 'utf-8')).watermarks['zulip:general'], 7);
  });
});

test('a save after a failed load whose file is gone by then writes the state, with nothing to move', () => {
  withLogs((_logs, dir, path) => {
    writeFileSync(path, BROKEN);
    const state = new DeliveryState(dir, 's1', directoryOf({ 1: 'general' }));
    rmSync(path);
    state.advance(GENERAL, 7);
    state.save();
    assert.deepEqual(readdirSync(dir), ['s1.delivery.json']);
    assert.equal(JSON.parse(readFileSync(path, 'utf-8')).watermarks['zulip:general'], 7);
  });
});

test('recovery advice JSON-escapes channel IDs in every mode', () => {
  const channelId = 'zulip:team \\ "alpha"';
  for (const mode of ['legacy', 'compat', 'strict'] as const) {
    const text = renderMissedBlock([viewOf(msg(7, true, 'hello'))], {
      streamName: 'team "alpha"', channelId, reason: 'backscroll', count: 1,
      formatTime: () => '', moreBeyond: true, messageIds: new MessageIds('alpha', mode),
    }).text;
    const channel = JSON.stringify(channelId);
    assert.ok(text.includes(`fetch_history(channel=${channel}, after=${mode === 'legacy' ? '7' : '"alpha:7"'})`));
  }
});


test('enabled catch-up bounds long channel identifiers and realm tags', () => {
  for (const mode of ['compat', 'strict'] as const) {
    const ids = new MessageIds('a'.repeat(64), mode);
    const block = renderMissedBlock([viewOf(msg(7, false, 'x'.repeat(2000))), viewOf(msg(8, false, 'y'.repeat(2000)))], {
      streamName: 'group', channelId: 'zulip:dm:' + Array.from({ length: 400 }, (_, i) => 10000 + i).join('+'), reason: 'backscroll', count: 2,
      formatTime: () => '', maxChars: 1000, moreBeyond: true, newestScannedId: 8, messageIds: ids,
    }).text;
    assert.ok(block.length <= 1000);
    assert.ok(block.includes(`fetch_around(message_id="${'a'.repeat(64)}:7")`));
    assert.match(block, /Newer messages also exist/);
  }
});

test('what the host accepted above a hold is not durable: an accepted later scan can recur after restart', () => {
  const dir = tmp();
  try {
    const state = new DeliveryState(dir, 'pinned', directoryOf({ 1: 'general' }));
    state.advance(GENERAL, 9);
    state.hold(GENERAL, 10, 'rejected');
    state.hold(GENERAL, 21);
    state.hold(GENERAL, 30);
    state.advanceThrough(GENERAL, 30, 20);
    state.save();
    assert.equal(state.watermark(GENERAL), 9);
    assert.equal(state.acceptedThrough(GENERAL), 30);
    const restarted = new DeliveryState(dir, 'pinned', new StreamDirectory());
    assert.equal(restarted.watermark(GENERAL), 9);
    assert.equal(restarted.acceptedThrough(GENERAL), 9, 'a restarted scan begins at 9, not at the accepted 30');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('held ids are saved beside the watermark and carried into the next run; a file with nothing held has no list', () => {
  const dir = tmp();
  const saved = () => JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8'));
  try {
    const a = new DeliveryState(dir, 's', directoryOf({ 1: 'general', 2: 'dev' }));
    a.advance(GENERAL, 9);
    a.advance(DEV, 5);
    a.save();
    assert.deepEqual(Object.keys(saved()), ['watermarks', 'missed', 'lastOpen', 'streams', 'formerNames'], 'the keys main writes, no more');

    a.hold(GENERAL, 12, 'rejected');
    a.hold(GENERAL, 10, 'failed');
    a.hold(GENERAL, 14);
    a.advance(GENERAL, 15);
    a.save();
    assert.equal(saved().watermarks['zulip:general'], 9, 'a hold keeps the watermark below it, as before');
    assert.deepEqual(saved().held, { 'zulip:general': { 10: 0, 12: 0 } }, 'not 14, which is on its way to the host');
    a.failQueued();
    a.save();
    assert.deepEqual(saved().held, { 'zulip:general': { 10: 0, 12: 0, 14: 0 } }, 'as the connection ends, what was on its way is held too');
    assert.deepEqual(a.carriedIds(GENERAL), [], 'what this run holds is not carried by it');

    const b = new DeliveryState(dir, 's', new StreamDirectory());
    assert.deepEqual(b.carriedChannels(), [GENERAL]);
    assert.deepEqual(b.carriedIds(GENERAL), [10, 12, 14]);
    assert.equal(b.floor(GENERAL), undefined, 'a carried id holds no watermark back');
    b.save();
    assert.deepEqual(saved().held, { 'zulip:general': { 10: 0, 12: 0, 14: 0 } }, 'kept until offered');

    b.uncarry(GENERAL, [10]);
    b.hold(GENERAL, 14, 'failed');
    b.release(GENERAL, 14);
    b.hold(GENERAL, 16, 'failed');
    b.save();
    assert.deepEqual(saved().held, { 'zulip:general': { 12: 0, 16: 0 } }, "the last run's and this run's together, less what the host accepted or was let go");
    b.advanceThrough(GENERAL, 16, 9);
    b.save();
    assert.equal(saved().held, undefined, 'a scan the host accepted takes the ids it covers off the list');
    assert.equal(saved().watermarks['zulip:general'], 16);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("past the cap the oldest held ids leave the saved list with a warning, on save and on load, the last run's before this run's newer ones", (t) => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args.join(' ')); });
  const dir = tmp();
  const saved = () => JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8'));
  try {
    const a = new DeliveryState(dir, 's', directoryOf({ 1: 'general' }));
    a.advance(GENERAL, 100);
    for (let id = 101; id <= 100 + HELD_SAVED_PER_CHANNEL + 3; id++) a.hold(GENERAL, id, 'failed');
    for (let id = 300; id < 400; id++) a.hold(GENERAL, id);
    a.save();
    const kept = Object.keys(saved().held['zulip:general']).map(Number);
    assert.equal(kept.length, HELD_SAVED_PER_CHANNEL);
    assert.deepEqual([kept[0], kept[kept.length - 1]], [104, 153], 'the newest are kept; what is on its way to the host takes no place');
    assert.equal(saved().watermarks['zulip:general'], 100, 'the watermark stays below every hold, so the next catch-up starts at the dropped ones');
    assert.equal(logs.length, 3, 'each non-queued hold is saved immediately');
    for (let i = 0; i < 3; i++) assert.ok(logs[i].includes(`the oldest 1 (${101 + i}–${101 + i})`));
    a.save();
    assert.equal(logs.length, 3, 'the same ids are not reported at every save');
    a.releaseAll(GENERAL);
    for (let id = 105; id <= 155; id++) a.hold(GENERAL, id, 'failed');
    a.save();
    assert.match(logs[3], /the oldest 1 \(105–105\)/, 'a further id is');

    const many = Object.fromEntries(Array.from({ length: HELD_SAVED_PER_CHANNEL + 2 }, (_, i) => [String(i + 1), 0]));
    writeFileSync(join(dir, 's.delivery.json'), JSON.stringify({ watermarks: { 'zulip:general': 100 }, streams: { 1: 'general' }, held: { 'zulip:general': { ...many, nonsense: 1, '-4': 0, '7.5': 0 }, 'zulip:gone': { 3: 0 } } }));
    logs.length = 0;
    const b = new DeliveryState(dir, 's', new StreamDirectory());
    assert.equal(b.carriedIds(GENERAL).length, HELD_SAVED_PER_CHANNEL, 'a longer list in the file is cut on load, so a start reads at most that many by id');
    assert.equal(b.carriedIds(GENERAL)[0], 3);
    assert.ok(logs.some((line) => /the oldest 2 \(1–2\) are dropped/.test(line)));
    assert.ok(logs.some((line) => line.includes('zulip:gone: dropping saved held ID 3:')), "a channel the file's stream list doesn't hold is dropped aloud, as its watermark is");

    logs.length = 0;
    for (let id = 201; id <= 260; id++) b.hold(GENERAL, id, 'rejected');
    b.save();
    assert.deepEqual(Object.keys(saved().held['zulip:general']).map(Number), Array.from({ length: HELD_SAVED_PER_CHANNEL }, (_, i) => 211 + i), "the newest of the two runs' ids together are kept");
    assert.deepEqual(b.carriedIds(GENERAL), [], "the last run's ids, the oldest of all, gave way to this run's");
    assert.equal(logs.filter((line) => line.includes('are dropped')).length, 60);
    assert.match(logs[0], /the oldest 1 \(3–3\)/);
    assert.match(logs.at(-1)!, /the oldest 1 \(210–210\)/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("a full saved list and one newer hold after a restart: the oldest saved id gives way, not the new one", (t) => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args.join(' ')); });
  const dir = tmp();
  const saved = () => JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8'));
  const savedIds = () => Object.keys(saved().held['zulip:general']).map(Number);
  const range = (from: number, to: number) => Array.from({ length: to - from + 1 }, (_, i) => from + i);
  try {
    const a = new DeliveryState(dir, 's', directoryOf({ 1: 'general' }));
    a.advance(GENERAL, 100);
    for (let id = 101; id <= 100 + HELD_SAVED_PER_CHANNEL; id++) a.hold(GENERAL, id, 'failed');
    a.save();
    assert.deepEqual(savedIds(), range(101, 150));
    assert.deepEqual(logs, [], 'exactly the cap fits');

    const b = new DeliveryState(dir, 's', new StreamDirectory());
    assert.deepEqual(b.carriedIds(GENERAL), range(101, 150));
    b.carriedReadFailed(GENERAL, 102);
    b.hold(GENERAL, 150, 'rejected');
    b.save();
    assert.deepEqual(savedIds(), range(101, 150), 'an id both runs held takes one place');
    assert.deepEqual(logs, []);

    b.hold(GENERAL, 151, 'failed');
    b.save();
    assert.deepEqual(savedIds(), range(102, 151), 'the new hold is saved and the oldest id of the whole list is the one dropped');
    assert.deepEqual(logs, ['[zulip-mcp] zulip:general: more than 50 undelivered message ids to save for a restart; the oldest 1 (101–101) are dropped from that list and will not be offered by id']);
    assert.equal(saved().held['zulip:general'][102], 1, 'a kept id keeps its count of failed reads');
    assert.deepEqual(b.carriedIds(GENERAL), range(102, 150), 'the dropped id is not read by id in this run either');
    assert.equal(saved().watermarks['zulip:general'], 100, 'the watermark is where it was');
    b.save();
    assert.equal(logs.length, 1, 'said once');

    b.hold(GENERAL, 153, 'rejected');
    b.hold(GENERAL, 152);
    b.save();
    assert.deepEqual(savedIds(), [...range(103, 151), 153], 'each further hold pushes out the next oldest; one still on its way to the host pushes out none');
    assert.match(logs[1], /the oldest 1 \(102–102\) are dropped/);

    logs.length = 0;
    const c = new DeliveryState(dir, 's', new StreamDirectory());
    assert.deepEqual(c.carriedIds(GENERAL), [...range(103, 151), 153], 'the next run carries the list as saved');
    assert.deepEqual(logs, []);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a carried id whose read fails is kept for a bounded number of starts, then dropped with a warning', (t) => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args.join(' ')); });
  const dir = tmp();
  try {
    writeFileSync(join(dir, 's.delivery.json'), JSON.stringify({ watermarks: { 'zulip:general': 100 }, streams: { 1: 'general' }, held: { 'zulip:general': { 40: 0, 41: CARRIED_READ_TRIES - 1 } } }));
    const state = new DeliveryState(dir, 's', new StreamDirectory());
    for (const id of [40, 41, 99]) state.carriedReadFailed(GENERAL, id);
    assert.deepEqual(state.carriedIds(GENERAL), [40]);
    assert.deepEqual(logs, ['[zulip-mcp] zulip:general: undelivered message 41 could not be read by id in 3 starts and is dropped; it will not be offered']);
    state.save();
    assert.deepEqual(JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8')).held, { 'zulip:general': { 40: 1 } });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('refusing a queued scan saves held IDs even when its watermark cannot move', () => {
  const dir = tmp();
  try {
    const state = new DeliveryState(dir, 's', directoryOf({ 1: 'general' }));
    state.advance(GENERAL, 20);
    state.save();
    state.hold(GENERAL, 21);
    state.hold(GENERAL, 22);
    state.refuse(GENERAL, 20, 22);
    const saved = JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf8'));
    assert.deepEqual(saved.held['zulip:general'], { 21: 0, 22: 0 });
    assert.equal(saved.watermarks['zulip:general'], 20);
  } finally { rmSync(dir, { recursive: true, force: true }); }
});

test('overflow drops never resurrect and newly dropped lower IDs are reported', t => {
  t.mock.method(console, 'error', () => {});
  const dir = tmp();
  const state = new DeliveryState(dir, 's', directoryOf({ 1: 'general' }));
  const savedIds = () => Object.keys(JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf8')).held?.['zulip:general'] ?? {});
  try {
    state.advance(GENERAL, 100);
    for (let id = 1001; id <= 1051; id++) state.hold(GENERAL, id, 'failed');
    assert.ok(!savedIds().includes('1001'));
    for (let id = 1002; id <= 1051; id++) state.release(GENERAL, id);
    state.save();
    assert.deepEqual(savedIds(), [], 'the still-held overflow ID cannot return to disk');
    assert.deepEqual(state.heldIds(GENERAL), [1001], 'its watermark hold is unchanged');
    state.releaseAll(GENERAL);
    const logs: string[] = [];
    t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args.join(' ')); });
    for (let id = 101; id <= 151; id++) state.hold(GENERAL, id, 'failed');
    assert.match(logs.join('\n'), /the oldest 1 \(101–101\)/, 'a lower range is not silenced by an earlier high-range warning');
  } finally { rmSync(dir, { recursive: true, force: true }); }
});
