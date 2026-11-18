import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { DeliveryState, renderMissedBlock } from '../src/delivery.ts';
import { StreamDirectory } from '../src/stream-directory.ts';
import { streamKey } from '../src/channel-key.ts';
import { MessageIds, messageIdError } from '../src/message-ids.ts';
import { createZulipUploader } from '../src/uploads.ts';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';
import { ZulipNoAnswer, zulipApi } from '../src/zulip-api.ts';
import type { PlatformSystemEvent } from '../src/platforms/adapter.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { standInApi } from './zulip-stand-in.ts';

const key = streamKey(7);
function state(t: any) {
  const dir = mkdtempSync(join(tmpdir(), 'review20-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const directory = new StreamDirectory();
  directory.registration([{ stream_id: 7, name: 'general' }]);
  return { dir, directory, delivery: new DeliveryState(dir, 'test', directory),
    saved: () => JSON.parse(readFileSync(join(dir, 'test.delivery.json'), 'utf8')) };
}

test('bulk holds replace state once, and retry reservation preserves saved eligibility', t => {
  t.mock.method(console, 'error', () => {});
  const { delivery, saved } = state(t);
  delivery.advance(key, 100);
  const saves = t.mock.method(delivery, 'save');
  for (let id = 101; id <= 1100; id++) delivery.hold(key, id, 'failed', false);
  assert.equal(saves.mock.callCount(), 0, 'no write per held ID; candidate trimming is bounded to 51 IDs');
  delivery.save();
  assert.equal(saves.mock.callCount(), 1);
  assert.deepEqual(Object.keys(saved().held['zulip:general']), Array.from({ length: 50 }, (_, i) => String(1051 + i)));
  delivery.hold(key, 1100, 'queued');
  delivery.save();
  assert.equal(saved().held['zulip:general']['1100'], 0, 'a pending retry does not erase an earlier saved refusal');
});

test('withdrawing only a carried ID signals a save, even below the watermark', t => {
  const { delivery, saved, dir, directory } = state(t);
  delivery.advance(key, 100);
  delivery.hold(key, 101, 'rejected');
  const next = new DeliveryState(dir, 'test', directory);
  next.advance(key, 200);
  assert.equal(next.release(key, 101), true);
  next.save();
  assert.equal(saved().held, undefined);
  assert.equal(next.watermark(key), 200);
});

for (const status of [401, 403, 429]) {
  test(`JSON poll refusal preserves HTTP ${status}, repair advice and numeric backoff`, async t => {
    t.mock.method(console, 'error', () => {});
    const events: PlatformSystemEvent[] = [];
    const delays: number[] = [];
    const loop = new ZulipEventLoop({ degradedThreshold: 1, sleep: async ms => { delays.push(ms); loop.stop(); } });
    const api = zulipApi(async (_method, path) => path === '/register'
      ? { status: 200, body: { result: 'success', queue_id: 'q', last_event_id: -1 } }
      : { status, body: { result: 'error', code: status === 429 ? 'RATE_LIMIT_HIT' : 'UNAUTHORIZED', 'retry-after': 120 } });
    await loop.start(api, () => {}, event => events.push(event));
    assert.deepEqual(delays, [120_000]);
    assert.match(events[0].text, new RegExp(`HTTP ${status}`));
    assert.match(events[0].text, /polling retries automatically/);
    if (status !== 429) assert.match(events[0].text, /host operator must repair authorization; retries alone cannot fix access/);
  });
}

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  const ids = new MessageIds('alpha', mode);
  test(`${mode} compact recovery distinguishes by-ID selection and retains ceiling continuation`, () => {
    const rows = [10, 100].map(id => ({ id, authorName: 'a', timestamp: new Date(), topic: '', text: 'hello', attachmentNames: [], mentioned: true }));
    const opts = { streamName: 'x'.repeat(2000), channelId: 'zulip:' + 'x'.repeat(2000), reason: 'backscroll' as const, count: 2, formatTime: () => '', maxChars: 1000, messageIds: ids, newestScannedId: 100 };
    const byId = renderMissedBlock(rows, { ...opts, byId: true }).text;
    assert.match(byId, /by-ID offer of the listed messages only/);
    assert.doesNotMatch(byId, /acknowledges the entire scanned interval/);
    assert.ok(byId.length <= 1000);
    const scanned = renderMissedBlock(rows, { ...opts, moreBeyond: true }).text;
    assert.ok(scanned.length <= 1000);
    assert.ok(scanned.includes(`fetch_history(channel=channelId, after=${JSON.stringify(ids.format(100))})`));
    const maximum = new MessageIds('x'.repeat(64), mode);
    const largeIds = rows.map((row, i) => ({ ...row, id: Number.MAX_SAFE_INTEGER - 1 + i }));
    const bounded = renderMissedBlock(largeIds, { ...opts, messageIds: maximum, moreBeyond: true, newestScannedId: Number.MAX_SAFE_INTEGER }).text;
    assert.ok(bounded.length <= 1000, 'continuation fits even with maximum-width tagged IDs');
    assert.ok(bounded.includes(`after=${JSON.stringify(maximum.format(Number.MAX_SAFE_INTEGER))}`));
  });

  test(`${mode} self-ID and native authorization advice requires operator repair before retry`, t => {
    t.mock.method(console, 'error', () => {});
    for (const method of ['GET', 'POST'] as const) {
      const safe = messageIdError(new ZulipNoAnswer('no-self-id', method, '/messages', false, 'private', 401), ids);
      assert.match(safe.message, /host operator must repair authorization/);
      assert.match(safe.message, /not sent/);
    }
  });

  test(`${mode} native upload failures retain the failing name and JSON refusals retain their code`, async t => {
    t.mock.method(console, 'error', () => {});
    for (const [status, body] of [[200, { result: 'success' }], [400, { result: 'error', code: 'BAD_REQUEST' }], [403, { result: 'error', code: 'DENIED' }]] as const) {
      const uploader = createZulipUploader({ realm: 'https://alpha.example', authHeader: 'test' }, async () => ({ ok: status === 200, status, headers: new Headers({ 'Retry-After': '12' }), text: async () => JSON.stringify(body), json: async () => body }));
      await assert.rejects(uploader.upload({ name: 'second.txt', data: Buffer.from('a') }), error => {
        const safe = messageIdError(error, ids);
        assert.match(safe.message, /second.txt/);
        if (status === 400) assert.match(safe.message, /BAD_REQUEST/);
        if (status === 403) {
          assert.match(safe.message, /host operator must repair authorization/);
          assert.match(safe.message, /access must be corrected first/);
          assert.doesNotMatch(safe.message, /\. Retry after/);
        }
        return true;
      });
    }
  });

  test(`${mode} text-only sends keep refusal-code fallback`, async t => {
    t.mock.method(console, 'error', () => {});
    const { dir } = state(t);
    const { api } = standInApi({ 'POST /messages': { result: 'error', code: 'SOME_REASON' } });
    const runtime = new ZulipToolRuntime({ api, selfUserId: 1, realm: 'https://alpha.example', authHeader: '', sessionId: 'test' }, dir, { messageIds: ids });
    await assert.rejects(runtime.handleToolCall('send_message', { type: 'stream', to: 'general', topic: 't', content: 'hello' }), /SOME_REASON/);
  });
}

test('saved-ID withdrawals name the dropped ID and reason, including invalid loaded IDs', t => {
  const logs: string[] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args.join(' ')); });
  const { delivery, dir, directory } = state(t);
  delivery.advance(key, 100);
  delivery.hold(key, 101, 'rejected');
  const next = new DeliveryState(dir, 'test', directory);
  next.uncarry(key, [101], 'message no longer exists');
  assert.ok(logs.some(line => /ID 101: message no longer exists/.test(line)));
  writeFileSync(join(dir, 'test.delivery.json'), JSON.stringify({ streams: { 7: 'general' }, held: { 'zulip:general': { bad: 0 } } }));
  new DeliveryState(dir, 'test', directory);
  assert.ok(logs.some(line => /ID \"bad\": not a positive safe integer/.test(line)));
});
