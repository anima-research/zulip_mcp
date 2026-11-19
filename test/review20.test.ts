import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { renderMissedBlock } from '../src/delivery.ts';
import { MessageIds, messageIdError } from '../src/message-ids.ts';
import { createZulipUploader } from '../src/uploads.ts';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';
import { ZulipNoAnswer, zulipApi } from '../src/zulip-api.ts';
import type { PlatformSystemEvent } from '../src/platforms/adapter.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { standInApi } from './zulip-stand-in.ts';

function state(t: any) {
  const dir = mkdtempSync(join(tmpdir(), 'review20-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return { dir };
}

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
  test(`${mode} compact recovery keeps the count, range, anchor and ceiling continuation without delivery mechanics`, () => {
    const rows = [10, 100].map(id => ({ id, authorName: 'a', timestamp: new Date(), topic: '', text: 'hello', attachmentNames: [], mentioned: true }));
    const opts = { streamName: 'x'.repeat(2000), channelId: 'zulip:' + 'x'.repeat(2000), reason: 'backscroll' as const, count: 2, formatTime: () => '', maxChars: 1000, messageIds: ids, newestScannedId: 100 };
    const plain = renderMissedBlock(rows, opts).text;
    assert.ok(plain.includes(`[2 messages omitted (ids ${ids.format(10)}–${ids.format(100)})`));
    assert.ok(plain.includes(`fetch_around(message_id=${JSON.stringify(ids.format(10))})`));
    assert.doesNotMatch(plain, /accept|watermark|hold|restart/i);
    assert.ok(plain.length <= 1000);
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
