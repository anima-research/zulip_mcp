import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer, type Server } from 'node:http';
import { once } from 'node:events';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import sharp from 'sharp';
import {
  IMAGE_LONG_EDGE_MAX,
  MAX_TEXT_BYTES,
  buildAttachmentBlocks,
  fetchCapped,
  normalizeImageForInference,
  resolveInlineOptions,
  type AttachmentSource,
} from '../src/attachments.ts';
import type { AttachmentRef } from '../src/content.ts';

async function png(width: number, height: number, alpha = false): Promise<Buffer> {
  return sharp({ create: { width, height, channels: alpha ? 4 : 3, background: alpha ? { r: 200, g: 20, b: 20, alpha: 0.5 } : '#3366cc' } })
    .png()
    .toBuffer();
}

test('small API-safe images pass through unchanged', async () => {
  const src = await png(200, 100);
  const norm = await normalizeImageForInference(src);
  assert.ok(norm);
  assert.equal(norm.mimeType, 'image/png');
  assert.equal(Buffer.from(norm.data, 'base64').equals(src), true);
});

test('oversized images are downsampled to the model ceiling; opaque → jpeg, alpha → png', async () => {
  const opaque = await normalizeImageForInference(await png(4000, 1000));
  assert.ok(opaque);
  assert.equal(opaque.mimeType, 'image/jpeg');
  const meta = await sharp(Buffer.from(opaque.data, 'base64')).metadata();
  assert.equal(meta.width, IMAGE_LONG_EDGE_MAX);
  assert.equal(meta.height, Math.round((1000 * IMAGE_LONG_EDGE_MAX) / 4000));

  const withAlpha = await normalizeImageForInference(await png(2000, 2000, true));
  assert.ok(withAlpha);
  assert.equal(withAlpha.mimeType, 'image/png');
  assert.equal((await sharp(Buffer.from(withAlpha.data, 'base64')).metadata()).hasAlpha, true);
});

test('non-API formats are rasterized rather than emitted as-is, and garbage yields null', async () => {
  const svg = Buffer.from('<svg xmlns="http://www.w3.org/2000/svg" width="40" height="20"><rect width="40" height="20" fill="red"/></svg>');
  const norm = await normalizeImageForInference(svg);
  assert.ok(norm);
  assert.notEqual(norm.mimeType, 'image/svg');
  assert.ok(['image/jpeg', 'image/png'].includes(norm.mimeType));
  assert.equal(await normalizeImageForInference(Buffer.from('not an image')), null);
});

test('an animated GIF under the cap is inlined as it is, frames unresized, even past the long-edge ceiling', async () => {
  const frame = (background: string) =>
    sharp({ create: { width: IMAGE_LONG_EDGE_MAX + 400, height: 20, channels: 3, background } }).png().toBuffer();
  const gif = await sharp([await frame('#ff0000'), await frame('#0000ff')], { join: { animated: true } })
    .gif()
    .toBuffer();
  assert.equal((await sharp(gif, { animated: true }).metadata()).pages, 2);
  const norm = await normalizeImageForInference(gif);
  assert.ok(norm);
  assert.equal(norm.mimeType, 'image/gif');
  assert.equal(Buffer.from(norm.data, 'base64').equals(gif), true);
});

const REFUSE_SHARP = `data:text/javascript,${encodeURIComponent(
  `import { register } from 'node:module';
register('data:text/javascript,' + encodeURIComponent('export async function resolve(s, c, n) { if (s === "sharp") throw new Error("sharp refused by this test"); return n(s, c); }'));`,
)}`;

function startWithoutSharp(env: Record<string, string>) {
  const base = Object.fromEntries(Object.entries(process.env).filter(([k]) => !k.startsWith('ZULIP_')));
  return spawnSync(process.execPath, ['--import', 'tsx', '--import', REFUSE_SHARP, fileURLToPath(new URL('../src/index.ts', import.meta.url))], {
    env: { ...base, ...env },
    encoding: 'utf8',
    timeout: 60_000,
  });
}

test('with image inlining on, a server whose sharp cannot load exits at start, naming both ways through', () => {
  const run = startWithoutSharp({});
  assert.equal(run.status, 1);
  assert.match(run.stderr, /sharp refused by this test/);
  const root = resolve(fileURLToPath(new URL('..', import.meta.url)));
  const { version } = JSON.parse(readFileSync(new URL('../node_modules/sharp/package.json', import.meta.url), 'utf8')) as { version: string };
  assert.ok(run.stderr.includes(`by running npm install --no-save @img/sharp-wasm32@${version} in this directory:\n${root}\n`), run.stderr);
  assert.match(run.stderr, /ZULIP_INLINE_IMAGES=false/);
  assert.doesNotMatch(run.stderr, /ZULIP_REALM must be set/);
});

test('with image inlining off, or its per-message limit at 0, the server starts without loading sharp', () => {
  for (const env of [{ ZULIP_INLINE_IMAGES: 'false' }, { ZULIP_INLINE_IMAGES_MAX: '0' }]) {
    const run = startWithoutSharp(env);
    assert.equal(run.status, 1, JSON.stringify(env));
    assert.match(run.stderr, /ZULIP_REALM must be set/, JSON.stringify(env));
    assert.doesNotMatch(run.stderr, /sharp refused by this test/, JSON.stringify(env));
  }
});

test('resolveInlineOptions clamps the text cap and reads the knobs', () => {
  assert.deepEqual(resolveInlineOptions({}), { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 });
  const opts = resolveInlineOptions({ ZULIP_INLINE_IMAGES: 'false', ZULIP_ATTACHMENT_INLINE_MAX_BYTES: '99999999', ZULIP_INLINE_IMAGES_MAX: '1' });
  assert.equal(opts.inlineImages, false);
  assert.equal(opts.inlineTextMaxBytes, MAX_TEXT_BYTES);
  assert.equal(opts.maxImages, 1);
  assert.equal(resolveInlineOptions({ ZULIP_ATTACHMENT_INLINE_MAX_BYTES: 'junk' }).inlineTextMaxBytes, 5120);
});

async function uploadHost(files: Record<string, { body: Buffer; type: string; chunked?: boolean }>): Promise<{ server: Server; origin: string; hits: string[] }> {
  const hits: string[] = [];
  const server = createServer((req, res) => {
    hits.push(`${req.headers.authorization ?? '-'} ${req.url}`);
    const f = files[req.url ?? ''];
    if (!f || req.headers.authorization !== 'Basic dGVzdA==') {
      res.writeHead(f ? 401 : 404).end();
      return;
    }
    if (f.chunked) {
      res.writeHead(200, { 'content-type': f.type, 'transfer-encoding': 'chunked' });
      for (let i = 0; i < f.body.length; i += 4096) res.write(f.body.subarray(i, i + 4096));
      res.end();
      return;
    }
    res.writeHead(200, { 'content-type': f.type });
    res.end(f.body);
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address() as { port: number };
  return { server, origin: `http://127.0.0.1:${address.port}`, hits };
}

test('fetchCapped sends the credentials, honours the ceiling, and reports overflow without buffering', async () => {
  const big = Buffer.alloc(50_000, 0x41);
  const { server, origin, hits } = await uploadHost({
    '/user_uploads/1/a/small.txt': { body: Buffer.from('hello'), type: 'text/plain' },
    '/user_uploads/1/a/big.txt': { body: big, type: 'text/plain' },
    '/user_uploads/1/a/nolength.txt': { body: big, type: 'text/plain', chunked: true },
  });
  try {
    const headers = { Authorization: 'Basic dGVzdA==' };
    const open = new AbortController().signal;
    const small = await fetchCapped(`${origin}/user_uploads/1/a/small.txt`, headers, 100, open);
    assert.equal(small.overflow, false);
    assert.equal(small.buf.toString(), 'hello');
    assert.equal(small.mimeType, 'text/plain');

    const over = await fetchCapped(`${origin}/user_uploads/1/a/big.txt`, headers, 1000, open);
    assert.equal(over.overflow, true);
    assert.equal(over.buf.length, 0);

    const nolength = await fetchCapped(`${origin}/user_uploads/1/a/nolength.txt`, headers, 1000, open);
    assert.equal(nolength.overflow, true);
    assert.equal(nolength.buf.length, 0);

    await assert.rejects(fetchCapped(`${origin}/user_uploads/1/a/small.txt`, {}, 100, open), /HTTP 401/);
    await assert.rejects(fetchCapped(`${origin}/nope`, headers, 100, open), /HTTP 404/);
    assert.ok(hits.every((h) => h.startsWith('Basic dGVzdA== ') || h.startsWith('- ')));
  } finally {
    server.close();
  }
});

function ref(path: string, mimeType: string, isImage: boolean): AttachmentRef {
  return { path, name: path.split('/').pop()!, mimeType, isImage };
}

test('fetchCapped ends when its signal aborts', { timeout: 5000 }, async () => {
  let arrived!: () => void;
  const request = new Promise<void>((resolve) => { arrived = resolve; });
  const server = createServer(() => arrived());
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  try {
    const deadline = new AbortController();
    const silent = fetchCapped(`${origin}/user_uploads/1/a/silent.txt`, {}, 100, deadline.signal);
    await request;
    deadline.abort();
    await assert.rejects(silent);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test('fetchCapped ends when its signal aborts while it waits for more of a body', { timeout: 5000 }, async () => {
  let wrote!: () => void;
  const partWritten = new Promise<void>((resolve) => { wrote = resolve; });
  const server = createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.write('the first part of a body', () => wrote());
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const origin = `http://127.0.0.1:${(server.address() as { port: number }).port}`;
  try {
    const deadline = new AbortController();
    const stalled = fetchCapped(`${origin}/user_uploads/1/a/stalled.txt`, {}, 1_000_000, deadline.signal);
    let settled = false;
    void stalled.then(() => { settled = true; }, () => { settled = true; });
    await partWritten;
    for (let i = 0; i < 20; i++) await new Promise((r) => setImmediate(r));
    assert.equal(settled, false, 'the fetch waits for the rest of the body');
    deadline.abort();
    await assert.rejects(stalled);
  } finally {
    server.closeAllConnections();
    server.close();
  }
});

test("a message's attachments share one deadline: the fetch it cuts, and every one not yet started, become notes", { timeout: 5000 }, async () => {
  const calls: { path: string; signal: AbortSignal }[] = [];
  let started!: () => void;
  const firstStarted = new Promise<void>((resolve) => { started = resolve; });
  const source: AttachmentSource = {
    fetch(path, _maxBytes, signal) {
      calls.push({ path, signal });
      started();
      return new Promise(() => {});
    },
  };
  const deadline = new AbortController();
  const refs = [
    ref('/user_uploads/1/a/shot.png', 'image/png', true),
    ref('/user_uploads/1/a/run.log', 'text/plain', false),
  ];
  const built = buildAttachmentBlocks(refs, source, { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 4 }, deadline.signal);
  await firstStarted;
  assert.equal(calls[0].signal.aborted, false, 'the fetch starts with a live signal');
  deadline.abort();
  const blocks = await built;
  assert.deepEqual(blocks.map((b) => (b as { text: string }).text), [
    "[attachment: shot.png — could not fetch (out of time for this message's attachments); fetch_attachment /user_uploads/1/a/shot.png]",
    "[attachment: run.log — could not fetch (out of time for this message's attachments); fetch_attachment /user_uploads/1/a/run.log]",
  ]);
  assert.deepEqual(calls.map((c) => c.path), ['/user_uploads/1/a/shot.png'], 'nothing is fetched once the deadline has passed');
  assert.equal(calls[0].signal.aborted, true, 'the fetch the deadline cut is told to stop');
});

test('buildAttachmentBlocks inlines images and small text, and degrades everything else to notes', async () => {
  const image = await png(3000, 1500);
  const { server, origin } = await uploadHost({
    '/user_uploads/1/a/shot.png': { body: image, type: 'image/png' },
    '/user_uploads/1/a/notes.md': { body: Buffer.from('# notes\nhi'), type: 'text/markdown' },
    '/user_uploads/1/a/huge.log': { body: Buffer.alloc(20_000, 0x42), type: 'text/plain' },
    '/user_uploads/1/a/second.png': { body: image, type: 'image/png' },
  });
  const source: AttachmentSource = {
    fetch: (path, maxBytes, signal) => fetchCapped(`${origin}${path}`, { Authorization: 'Basic dGVzdA==' }, maxBytes, signal),
  };
  try {
    const refs = [
      ref('/user_uploads/1/a/missing.png', 'image/png', true),
      ref('/user_uploads/1/a/shot.png', 'image/png', true),
      ref('/user_uploads/1/a/notes.md', 'text/markdown', false),
      ref('/user_uploads/1/a/huge.log', 'text/plain', false),
      ref('/user_uploads/1/a/deck.pdf', 'application/pdf', false),
      ref('/user_uploads/1/a/second.png', 'image/png', true),
    ];
    const open = new AbortController().signal;
    const blocks = await buildAttachmentBlocks(refs, source, { inlineImages: true, inlineTextMaxBytes: 5120, maxImages: 1 }, open);
    assert.match((blocks[0] as { text: string }).text, /missing\.png — could not fetch \(HTTP 404\)/);
    assert.equal(blocks[1].type, 'image');
    assert.equal((blocks[1] as { mimeType: string }).mimeType, 'image/jpeg');
    assert.equal((blocks[2] as { text: string }).text, '[image attachment: shot.png]');
    assert.equal((blocks[3] as { text: string }).text, '[attachment: notes.md (10B)]\n# notes\nhi');
    assert.match((blocks[4] as { text: string }).text, /^\[attachment: huge\.log over the 5KB inline cap — not inlined: fetch_attachment \/user_uploads\/1\/a\/huge\.log\]$/);
    assert.match((blocks[5] as { text: string }).text, /second\.png" not inlined \(per-message image limit reached\)/);
    assert.equal(blocks.length, 6);

    const off = await buildAttachmentBlocks(refs.slice(0, 1), source, { inlineImages: false, inlineTextMaxBytes: 0, maxImages: 4 }, open);
    assert.match((off[0] as { text: string }).text, /image inlining is off/);
  } finally {
    server.close();
  }
});
