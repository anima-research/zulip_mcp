import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import { DeliveryState, attributeMessage, renderMissedBlock, selectMissed, viewOf, type MissedView } from '../src/delivery.ts';

function tmp(): string {
  return mkdtempSync(join(tmpdir(), 'zulip-delivery-'));
}

test('watermarks only advance, and survive a restart', () => {
  const dir = tmp();
  try {
    const a = new DeliveryState(dir, 's1');
    assert.equal(a.watermark('zulip:general'), undefined);
    assert.equal(a.advance('zulip:general', 10), true);
    assert.equal(a.advance('zulip:general', 7), false, 'never retreats');
    assert.equal(a.advance('zulip:general', 12), true);
    a.save();

    const b = new DeliveryState(dir, 's1');
    assert.equal(b.watermark('zulip:general'), 12);
    assert.deepEqual(b.watermarkedChannels(), ['zulip:general']);

    const other = new DeliveryState(dir, 's2');
    assert.equal(other.watermark('zulip:general'), undefined);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the undelivered floor: the watermark never passes an offered-but-unaccepted id, and lifts when it is delivered', () => {
  const state = new DeliveryState(null, 'mem');
  for (const id of [1, 2, 3]) state.hold('zulip:general', id);
  assert.equal(state.floor('zulip:general'), 1);
  state.release('zulip:general', 1);
  state.release('zulip:general', 3);
  state.hold('zulip:general', 2, 'rejected');
  assert.equal(state.advance('zulip:general', 3), true);
  assert.equal(state.watermark('zulip:general'), 1, 'stops below the rejected id');

  state.hold('zulip:general', 4);
  state.release('zulip:general', 4);
  assert.equal(state.advance('zulip:general', 4), false);
  assert.equal(state.watermark('zulip:general'), 1);
  assert.deepEqual(state.heldIds('zulip:general'), [2]);
  assert.deepEqual(state.heldIds('zulip:general', { replayable: true }), [2], 'rejected → a live replay may try once');
  state.markReplayed('zulip:general', [2]);
  assert.deepEqual(state.heldIds('zulip:general', { replayable: true }), [], 'and only once');

  assert.equal(state.release('zulip:general', 2), true);
  assert.equal(state.watermark('zulip:general'), 4);
  assert.equal(state.floor('zulip:general'), undefined);

  state.hold('zulip:general', 5, 'failed');
  state.hold('zulip:general', 6);
  state.release('zulip:general', 6);
  state.advance('zulip:general', 6);
  assert.equal(state.watermark('zulip:general'), 4);
  assert.equal(state.advanceThrough('zulip:general', 6), true);
  assert.equal(state.watermark('zulip:general'), 6);
  assert.deepEqual(state.heldChannels(), []);

  state.hold('zulip:general', 3);
  assert.equal(state.floor('zulip:general'), undefined);
  state.hold('zulip:general', 9, 'failed');
  state.advance('zulip:general', 10);
  assert.equal(state.watermark('zulip:general'), 8);
  assert.equal(state.releaseAll('zulip:general'), true);
  assert.equal(state.watermark('zulip:general'), 10);
});

test('closing a channel starts a tally anchored at the watermark; reopening clears it', () => {
  const state = new DeliveryState(null, 'mem');
  state.advance('zulip:dev', 100);
  state.markOpen('zulip:dev');
  assert.equal(state.tally('zulip:dev'), undefined);
  assert.equal(state.countMissed('zulip:dev', { id: 101, text: 'x' }), false, 'open channels are not tallied');

  state.markClosed('zulip:dev');
  assert.deepEqual(state.tally('zulip:dev'), { anchorId: 100, talliedThrough: 100, messages: 0, characters: 0 });
  assert.equal(state.countMissed('zulip:dev', { id: 101, text: 'hello' }), true);
  assert.equal(state.countMissed('zulip:dev', { id: 103, text: 'world!' }), true);
  assert.deepEqual(state.tally('zulip:dev'), { anchorId: 100, talliedThrough: 103, messages: 2, characters: 11 });

  state.markClosed('zulip:never');
  assert.equal(state.tally('zulip:never'), undefined);

  state.markOpen('zulip:dev');
  assert.equal(state.tally('zulip:dev'), undefined);
  assert.equal(state.wasOpen('zulip:dev'), true);
});

test('backfill folds fetched ambient into the tally and moves the cursor', () => {
  const state = new DeliveryState(null, 'mem');
  state.markOpen('zulip:dev');
  state.markClosed('zulip:dev');
  state.backfillMissed('zulip:dev', [{ id: 5, text: 'ab' }, { id: 6, text: 'cde' }], 9);
  assert.deepEqual(state.tally('zulip:dev'), { anchorId: 0, talliedThrough: 9, messages: 2, characters: 5 });
});

test('the state file is round-tripped, sorted, and tolerant of garbage', () => {
  const dir = tmp();
  try {
    const a = new DeliveryState(dir, 's');
    a.advance('zulip:b', 2);
    a.advance('zulip:a', 1);
    a.markOpen('zulip:b');
    a.markOpen('zulip:a');
    a.markClosed('zulip:a');
    a.countMissed('zulip:a', { id: 3, text: 'xyz' });
    a.save();

    const raw = JSON.parse(readFileSync(join(dir, 's.delivery.json'), 'utf-8'));
    assert.deepEqual(Object.keys(raw.watermarks), ['zulip:a', 'zulip:b']);
    assert.deepEqual(raw.lastOpen, ['zulip:b']);
    assert.deepEqual(raw.missed['zulip:a'], { anchorId: 1, talliedThrough: 3, messages: 1, characters: 3 });

    const b = new DeliveryState(dir, 's');
    assert.equal(b.watermark('zulip:b'), 2);
    assert.equal(b.wasOpen('zulip:b'), true);
    assert.equal(b.wasOpen('zulip:a'), false);
    assert.deepEqual(b.tally('zulip:a'), { anchorId: 1, talliedThrough: 3, messages: 1, characters: 3 });

    const dirty = tmp();
    try {
      const path = join(dirty, 'x.delivery.json');
      writeFileSync(path, '{not json');
      const c = new DeliveryState(dirty, 'x');
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
  assert.equal(viewOf({ ...msg(5, false), metadata: { isDM: true } }).mentioned, true);
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

  const small = renderMissedBlock(views.slice(0, 2), {
    streamName: 'general', channelId: 'zulip:general', reason: 'backscroll', count: 2, formatTime: () => '', maxChars: 40_000,
  });
  assert.doesNotMatch(small, /elided|truncated/);
});

test('renderMissedBlock: the budget is a hard cap on the whole block, one long line included', () => {
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

  const huge = renderMissedBlock([viewOf(msg(7, true, 'z'.repeat(5000)))], {
    streamName: 'general', channelId: 'zulip:general', reason: 'mention', count: 1, formatTime: () => '', maxChars: 1000,
  });
  assert.ok(huge.length <= 1000, `block is ${huge.length} chars`);
  assert.match(huge, /^<missed stream="#general" channelId="zulip:general" count="1" lines="1" reason="mention">\n\[id=7\] \[#general > topic-a\] Ann \(mention\): z+ … \[line cut to fit the catch-up budget — fetch_around\(7\) has the whole message\]\n<\/missed>$/);
});

function incoming(over: Partial<IncomingChannelMessage> = {}): IncomingChannelMessage {
  return {
    channelId: 'zulip:qa',
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
  const out = attributeMessage(incoming(), () => '2026-09-14T08:42:52Z');
  assert.equal((out.content[0] as { text: string }).text, '[2026-09-14T08:42:52Z id=17206924] [#qa > router] Mykhailo Buialo (mention): @Knowledge Resident do you have the same issue');
  assert.deepEqual(out.author, { id: '760', name: 'Mykhailo Buialo' });
  assert.equal(out.threadId, 'router');
  assert.equal((out.metadata as { topic: string }).topic, 'router');
  assert.equal((out.metadata as { attributed: boolean }).attributed, true);
  assert.equal((out.metadata as { attributionHeader: string }).attributionHeader, '[2026-09-14T08:42:52Z id=17206924] [#qa > router] Mykhailo Buialo (mention): ', 'the exact prefix, so a host can strip it');
  assert.equal((out.content[0] as { text: string }).text.startsWith((out.metadata as { attributionHeader: string }).attributionHeader), true);

  assert.equal((attributeMessage(incoming({ metadata: { topic: 'router', mentioned: false } }), () => 'T').content[0] as { text: string }).text.startsWith('[T id=17206924] [#qa > router] Mykhailo Buialo: @'), true);
  const dm = attributeMessage(incoming({ channelId: 'zulip:dm:760', threadId: undefined, metadata: { isDM: true, mentioned: false }, content: [{ type: 'text', text: 'hi' }] }), () => 'T');
  assert.equal((dm.content[0] as { text: string }).text, '[T id=17206924] [DM] Mykhailo Buialo: hi');
});

test('attributeMessage: no timestamp style, attachments after the body, image-only content, and idempotence', () => {
  const none = attributeMessage(incoming(), () => '');
  assert.equal((none.content[0] as { text: string }).text.startsWith('[id=17206924] [#qa > router]'), true, 'AGENT_TIMESTAMP_STYLE=none drops the time, not the id');

  const withNote = attributeMessage(incoming({ content: [{ type: 'text', text: 'see file' }, { type: 'text', text: '[attachments: 1]\n- a.pdf' }] }), () => 'T');
  assert.equal((withNote.content[0] as { text: string }).text, '[T id=17206924] [#qa > router] Mykhailo Buialo (mention): see file');
  assert.equal((withNote.content[1] as { text: string }).text, '[attachments: 1]\n- a.pdf', 'the note is untouched');

  const imageOnly = attributeMessage(incoming({ content: [{ type: 'image', data: 'AAAA', mimeType: 'image/png' }] }), () => 'T');
  assert.equal((imageOnly.content[0] as { text: string }).text, '[T id=17206924] [#qa > router] Mykhailo Buialo (mention):');
  assert.equal((imageOnly.metadata as { attributionHeader: string }).attributionHeader, '[T id=17206924] [#qa > router] Mykhailo Buialo (mention):', 'the recorded prefix is what was inserted');
  assert.equal(imageOnly.content[1].type, 'image');

  const original = incoming();
  const once = attributeMessage(original, () => 'T');
  assert.deepEqual(attributeMessage(once, () => 'T'), once, 'a replay does not double the header');
  assert.equal(viewOf(original).text, '@Knowledge Resident do you have the same issue', 'the input is not mutated');
  assert.equal((original.metadata as { attributed?: boolean }).attributed, undefined);

  const bad = attributeMessage(incoming({ timestamp: 'not-a-date' }), () => { throw new Error('must not format an invalid date'); });
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
  assert.equal(views[0].mentioned, true, 'selection still treats a DM as addressed');
  const block = renderMissedBlock(views, { streamName: 'DM: Bo', channelId: 'zulip:dm:42', reason: 'mention', count: 2, formatTime: () => 'T' });
  assert.deepEqual(block.split('\n').slice(1, 3), ['[T id=1] [DM] Bo: hey', '[T id=2] [DM] Bo: you there?']);
});
