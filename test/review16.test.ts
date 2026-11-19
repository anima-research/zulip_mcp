import test from 'node:test';
import assert from 'node:assert/strict';
import { MessageIds, messageIdError, hasLoggedDiagnostic, logDiagnostic, markTransportError } from '../src/message-ids.ts';
import { ZulipNoAnswer } from '../src/zulip-api.ts';
import { fetchCapped, buildAttachmentBlocks } from '../src/attachments.ts';
import { fetchAttachmentBytes } from '../src/content.ts';
import { createZulipUploader, uploadBlocks } from '../src/uploads.ts';
import { sendMessage, SendRefused } from '../src/outgoing.ts';
import { standInApi } from './zulip-stand-in.ts';

for (const mode of ['legacy', 'compat', 'strict'] as const) {
  const ids = new MessageIds('alpha', mode);
  test(`${mode} authorization read failures require correcting access rather than blind retry`, () => {
    for (const status of [401, 403]) {
      const safe = messageIdError(new ZulipNoAnswer('not-json', 'GET', '/private', false, 'secret', status), ids, false);
      assert.match(safe.message, /Correct the access configuration before retrying/);
      assert.doesNotMatch(safe.message, /Retry the read/);
    }
  });

  test(`${mode} native downloads retain Retry-After on refusal and body failure`, async t => {
    t.mock.method(console, 'error', () => {});
    for (const refused of [true, false]) {
      t.mock.method(globalThis, 'fetch', async () => refused
        ? new Response('', { status: 429, headers: { 'Retry-After': '120' } })
        : new Response(new ReadableStream({ start(controller) { controller.error(new Error('private body error')); } }), { status: 200, headers: { 'Retry-After': '120' } }));
      for (const download of [
        () => fetchCapped('https://example.invalid/user_uploads/a', {}, 100, new AbortController().signal),
        () => fetchAttachmentBytes('https://example.invalid/user_uploads/a', 'a'),
      ]) await assert.rejects(download(), error => {
        const safe = messageIdError(error, ids);
        assert.match(safe.message, /Retry after 120 seconds/);
        assert.match(safe.message, new RegExp(`HTTP ${refused ? 429 : 200}`));
        assert.doesNotMatch(safe.message, /private body error/);
        return true;
      });
    }
  });

  test(`${mode} inline refusals do not direct a repeat of the same fetch`, async t => {
    t.mock.method(console, 'error', () => {});
    t.mock.method(globalThis, 'fetch', async () => new Response('', { status: 403 }));
    const blocks = await buildAttachmentBlocks([{ path: '/user_uploads/a', name: 'a.txt', mimeType: 'text/plain', isImage: false }], {
      fetch: (path, max, signal) => fetchCapped('https://example.invalid' + path, {}, max, signal),
    }, { inlineImages: false, maxImages: 0, inlineTextMaxBytes: 100 }, new AbortController().signal, ids);
    const text = (blocks[0] as { text: string }).text;
    assert.equal(text, '[attachment: a.txt — could not fetch (refused, HTTP 403, /user_uploads/a); check access and the path before retrying]');
    assert.doesNotMatch(text, /fetch_attachment/);
  });

  test(`${mode} ordinary JSON upload refusals preserve Retry-After`, async () => {
    const uploader = createZulipUploader({ realm: 'https://example.invalid', authHeader: 'test' }, async () => new Response(JSON.stringify({ result: 'error', msg: 'Rate limited' }), { status: 429, headers: { 'Retry-After': '60' } }));
    await assert.rejects(uploader.upload({ name: 'a', data: Buffer.from('a') }), error => {
      assert.match(messageIdError(error, ids, false).message, /Rate limited.*Retry after 60 seconds/);
      return true;
    });
  });

  test(`${mode} failed first upload says message posting was not reached`, async t => {
    t.mock.method(console, 'error', () => {});
    await assert.rejects(uploadBlocks({ upload: async () => { throw markTransportError(new Error('secret'), { operation: 'Upload' }); } }, [{ type: 'image', mimeType: 'image/png', data: 'YQ==' }], {
      roots: new Map(), maxBytes: 100, maxTotalBytes: 100, maxCount: 1,
    }), /No message was sent: attachment upload failed before message posting began/);
  });

  test(`${mode} post refusal and uncertainty retain every confirmed upload path`, async t => {
    t.mock.method(console, 'error', () => {});
    for (const refused of [true, false]) {
      const { api } = standInApi({ 'POST /messages': () => {
        if (refused) return { result: 'error', msg: 'No permission' };
        throw new ZulipNoAnswer('timeout', 'POST', '/messages', true, 'private');
      } });
      const files = ['a', 'b'].map(name => ({ name, path: `/user_uploads/${name}`, url: `https://example.invalid/user_uploads/${name}` }));
      await assert.rejects(sendMessage(api, { type: 'stream', to: 'general', topic: 't' }, 'hello', {
        messageIds: ids, limit: 10000, unlinkable: 'refuse', upload: async () => files,
      }), error => {
        const text = error instanceof SendRefused ? error.answer.msg : (error as Error).message;
        assert.match(text, /Attachment uploads confirmed:.*\/user_uploads\/a.*\/user_uploads\/b/);
        assert.match(text, /reuse the confirmed paths after checking the message-post outcome/);
        if (!refused) assert.match(text, /may have been applied/);
        assert.doesNotMatch(text, /No message was sent|private/);
        return true;
      });
    }
  });
}

test('contextual delivery diagnostics retain the original error once and track ownership', t => {
  const logs: unknown[][] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { logs.push(args); });
  const native = new Error('private socket cause');
  const failure = new ZulipNoAnswer('network', 'GET', '/messages', false, 'outer', undefined, undefined, undefined, native);
  logDiagnostic(failure, 'catch-up history failed:');
  assert.equal(logs[0][1], failure);
  assert.equal(failure.cause, native);
  assert.ok(failure.stack);
  assert.equal(hasLoggedDiagnostic(failure), true);
  messageIdError(failure, new MessageIds('alpha', 'strict'));
  logDiagnostic(failure, 'outer boundary:');
  assert.equal(logs.length, 1);
});
