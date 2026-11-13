import test from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { once } from 'node:events';
import { mkdtempSync, mkdirSync, readdirSync, realpathSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { open } from 'node:fs/promises';

/** Local-file attachments are Linux-only (descriptor-bound containment); their tests run there. */
const linuxOnly = { skip: LOCAL_FILES_SUPPORTED ? false : 'local-file attachments are Linux-only' };
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DEFAULT_UPLOAD_MAX_BYTES,
  MAX_ATTACHMENTS_PER_MESSAGE,
  bareMimeType,
  base64DecodedLength,
  closeAll,
  createZulipUploader,
  guessMimeType,
  isValidMimeType,
  LOCAL_FILES_SUPPORTED,
  multipartBody,
  parseUploadRoots,
  prepareAttachmentArg,
  prepareAttachments,
  prepareBlocks,
  resolveUploadPolicy,
  sniffImageType,
  uploadAttachmentArgs,
  uploadBlocks,
  uploadPrepared,
  verifyOpenedInsideRoot,
  withAttachmentLinks,
  type UploadPolicy,
  type Uploader,
} from '../src/uploads.ts';

function tmp(): string {
  return realpathSync(mkdtempSync(join(tmpdir(), 'zulip-uploads-')));
}

function policy(over: Partial<UploadPolicy> = {}): UploadPolicy {
  return { roots: new Map(), maxBytes: 1024, maxTotalBytes: 4096, maxCount: 10, ...over };
}

function fakeUploader() {
  const uploaded: { name: string; bytes: string; mimeType?: string }[] = [];
  const uploader: Uploader = {
    async upload(i) {
      uploaded.push({ name: i.name, bytes: i.data.toString(), mimeType: i.mimeType });
      return { name: i.name, path: `/user_uploads/1/${i.name}`, url: `https://z/user_uploads/1/${i.name}` };
    },
  };
  return { uploader, uploaded };
}

const b64 = (s: string) => Buffer.from(s).toString('base64');

test('the uploader POSTs multipart to /api/v1/user_uploads with the bot auth and returns the upload path', async () => {
  let seen: { auth: string | undefined; contentType: string | undefined; body: string; url: string | undefined } | null = null;
  const server = createServer((req, res) => {
    const chunks: Buffer[] = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      seen = { auth: req.headers.authorization, contentType: req.headers['content-type'], body: Buffer.concat(chunks).toString('latin1'), url: req.url };
      res.setHeader('content-type', 'application/json');
      res.end(JSON.stringify({ result: 'success', msg: '', uri: '/user_uploads/2/Ab/cd/report.txt' }));
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  try {
    const up = createZulipUploader({ realm: `http://127.0.0.1:${port}/`, authHeader: 'Basic abc' });
    const out = await up.upload({ name: 'report.txt', data: Buffer.from('hello there'), mimeType: 'text/plain' });
    assert.deepEqual(out, { name: 'report.txt', path: '/user_uploads/2/Ab/cd/report.txt', url: `http://127.0.0.1:${port}/user_uploads/2/Ab/cd/report.txt` });
    assert.ok(seen);
    assert.equal(seen!.url, '/api/v1/user_uploads');
    assert.equal(seen!.auth, 'Basic abc');
    assert.match(seen!.contentType ?? '', /^multipart\/form-data; boundary=/);
    assert.match(seen!.body, /name="file"; filename="report.txt"/);
    assert.match(seen!.body, /Content-Type: text\/plain/);
    assert.match(seen!.body, /hello there/);
  } finally {
    server.close();
  }
});

test('the uploader surfaces Zulip errors, and accepts both the `uri` and the newer `url` field', async () => {
  let mode: 'error' | 'url' = 'error';
  const server = createServer((req, res) => {
    req.resume();
    req.on('end', () => {
      res.setHeader('content-type', 'application/json');
      if (mode === 'error') {
        res.statusCode = 413;
        res.end(JSON.stringify({ result: 'error', msg: 'Uploaded file is larger than the allowed limit of 25 MiB' }));
      } else {
        res.end(JSON.stringify({ result: 'success', msg: '', url: '/user_uploads/2/Ab/cd/x.png' }));
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  try {
    const up = createZulipUploader({ realm: `http://127.0.0.1:${port}`, authHeader: 'Basic abc' });
    await assert.rejects(up.upload({ name: 'big.bin', data: Buffer.alloc(3) }), /upload of "big.bin" failed: Uploaded file is larger/);
    mode = 'url';
    const out = await up.upload({ name: 'x.png', data: Buffer.alloc(3) });
    assert.equal(out.path, '/user_uploads/2/Ab/cd/x.png');
  } finally {
    server.close();
  }
});

test('an uploader without credentials refuses rather than sending an anonymous upload', async () => {
  const up = createZulipUploader({ realm: 'https://z.example.com', authHeader: '' }, async () => { throw new Error('must not be called'); });
  await assert.rejects(up.upload({ name: 'a', data: Buffer.alloc(1) }), /credentials are unknown/);
  assert.throws(() => createZulipUploader({ realm: '', authHeader: 'Basic x' }), /realm is unknown/);
});

test('the part header cannot be injected through the name or the type', () => {
  const body = multipartBody({ name: 'a"\r\nX-Evil: 1\r\n\r\n.txt', data: Buffer.from('x'), mimeType: 'text/plain\r\nX-Evil: 1' }, 'B').toString();
  assert.doesNotMatch(body, /^X-Evil/m, 'no injected header line');
  assert.match(body, /filename="a___X-Evil: 1____.txt"/, 'quotes and line breaks in the name are neutralised');
  assert.match(body, /Content-Type: application\/octet-stream/, 'an invalid type is not written into the header');
  assert.equal(body.split('\r\n').length, 7, 'exactly the structural lines');
  assert.equal(isValidMimeType('image/svg+xml'), true);
  assert.equal(isValidMimeType('text/plain; charset=utf-8'), false);
  assert.equal(isValidMimeType('text/plain\r\nX: 1'), false);
  assert.equal(isValidMimeType('noslash'), false);
});

test('parseUploadRoots takes name=path entries, canonicalises them, and refuses what does not exist', () => {
  const dir = tmp();
  try {
    mkdirSync(join(dir, 'notes'));
    mkdirSync(join(dir, 'out'));
    const roots = parseUploadRoots(' notes=./notes , out=' + join(dir, 'out') + ' ,', dir);
    assert.deepEqual([...roots], [['notes', join(dir, 'notes')], ['out', join(dir, 'out')]]);
    assert.equal(parseUploadRoots(undefined, dir).size, 0);
    assert.equal(parseUploadRoots('', dir).size, 0);
    assert.throws(() => parseUploadRoots('notes=./missing', dir), /root "notes" \(\.\/missing\): ENOENT/);
    assert.throws(() => parseUploadRoots('./notes', dir), /is not name=path/);
    assert.throws(() => parseUploadRoots('bad name=./notes', dir), /is not name=path/);
    assert.throws(() => parseUploadRoots('notes=./notes,notes=./out', dir), /listed twice/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('with no roots, local files are refused outright; inline data still works', async () => {
  const refused = LOCAL_FILES_SUPPORTED ? /local-file attachments are disabled: no upload roots configured/ : /supported on Linux only/;
  await assert.rejects(prepareAttachmentArg({ file: 'notes/a.txt' }, policy()), refused);
  await assert.rejects(prepareAttachmentArg({ file: '/etc/passwd' }, policy()), refused);
  const inline = await prepareAttachmentArg({ data: b64('hi'), name: 'a.txt' }, policy());
  assert.equal((await inline.read()).toString(), 'hi');
  assert.deepEqual(await prepareAttachments([], policy()), []);
});

test('a local file is <root>/<path>, confined to the named root after symlink resolution', linuxOnly, async () => {
  const dir = tmp();
  try {
    const notes = join(dir, 'notes');
    mkdirSync(join(notes, 'sub'), { recursive: true });
    writeFileSync(join(notes, 'sub', 'report.md'), '# report');
    writeFileSync(join(dir, 'secret.txt'), 'SECRET');
    symlinkSync(join(dir, 'secret.txt'), join(notes, 'escape.txt'));
    symlinkSync(join(notes, 'sub', 'report.md'), join(notes, 'alias.md'));
    const p = policy({ roots: new Map([['notes', notes]]) });

    const ok = await prepareAttachmentArg({ file: 'notes/sub/report.md' }, p);
    assert.equal(ok.name, 'report.md');
    assert.equal(ok.mimeType, 'text/markdown', 'guessed from the extension');
    assert.equal(ok.size, 8);
    assert.equal((await ok.read()).toString(), '# report');
    await assert.rejects(ok.read(), /already read/);

    const alias = await prepareAttachmentArg({ file: 'notes/alias.md', name: 'renamed.md', mime_type: 'text/x-markdown' }, p);
    assert.equal(alias.name, 'renamed.md');
    assert.equal(alias.mimeType, 'text/x-markdown');
    assert.equal((await alias.read()).toString(), '# report', 'a symlink that stays inside the root is fine');

    await assert.rejects(prepareAttachmentArg({ file: 'notes/escape.txt' }, p), /resolves outside upload root "notes"/);
    await assert.rejects(prepareAttachmentArg({ file: 'notes/../secret.txt' }, p), /resolves outside upload root "notes"/);
    await assert.rejects(prepareAttachmentArg({ file: join(notes, 'sub', 'report.md') }, p), /paths are root-relative/);
    await assert.rejects(prepareAttachmentArg({ file: 'other/report.md' }, p), /unknown upload root "other"; available roots: notes/);
    await assert.rejects(prepareAttachmentArg({ file: 'notes' }, p), /names a root, not a file/);
    await assert.rejects(prepareAttachmentArg({ file: 'notes/sub' }, p), /not a regular file/);
    await assert.rejects(prepareAttachmentArg({ file: 'notes/missing.md' }, p), (err: Error) => {
      assert.equal(err.message, 'attachment "notes/missing.md": not found', 'no host path in the message');
      return true;
    });
    await assert.rejects(prepareAttachmentArg({ file: 'notes/sub/report.md/x' }, p), /attachment "notes\/sub\/report.md\/x": not found/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the per-file ceiling is enforced on the bytes read, not only on stat', linuxOnly, async () => {
  const dir = tmp();
  try {
    const f = join(dir, 'grow.txt');
    writeFileSync(f, 'x'.repeat(100));
    const p = policy({ roots: new Map([['d', dir]]), maxBytes: 150 });
    const prepared = await prepareAttachmentArg({ file: 'd/grow.txt' }, p);
    writeFileSync(f, 'x'.repeat(400));
    await assert.rejects(prepared.read(), /grew past the 150B upload ceiling/);

    writeFileSync(f, 'x'.repeat(151));
    await assert.rejects(prepareAttachmentArg({ file: 'd/grow.txt' }, p), /is 151B, over the 150B upload ceiling/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('inline data is strict base64, measured before it is decoded', async () => {
  const p = policy({ maxBytes: 8 });
  assert.equal(base64DecodedLength('aGk='), 2);
  assert.equal(base64DecodedLength('aGVsbG8='), 5);
  assert.equal(base64DecodedLength(''), 0);
  assert.throws(() => base64DecodedLength('data:image/png;base64,iVBOR'), /not valid base64/);
  assert.throws(() => base64DecodedLength('!!!not base64!!!'), /not valid base64/);
  assert.throws(() => base64DecodedLength('aGk'), /not valid base64/);

  await assert.rejects(prepareAttachmentArg({ data: b64('x'.repeat(9)), name: 'a.txt' }, p), /is 9B, over the 8B upload ceiling/);
  await assert.rejects(prepareAttachmentArg({ data: 'aGk', name: 'a.txt' }, p), /attachment "a.txt": data is not valid base64/);
  await assert.rejects(prepareAttachmentArg({ data: 'data:text/plain;base64,aGk=', name: 'a.txt' }, p), /not valid base64/);
  await assert.rejects(prepareAttachmentArg({ data: '====', name: 'a.txt' }, p), /not valid base64/);
  await assert.rejects(prepareAttachmentArg({ data: 'aGk=' }, p), /need a `name`/);
  await assert.rejects(prepareAttachmentArg({}, p), /exactly one of `file`/);
  await assert.rejects(prepareAttachmentArg({ file: 'd/a', data: 'aGk=' }, p), /exactly one of `file`/);
  await assert.rejects(prepareAttachmentArg('x' as never, p), /must be an object/);
  await assert.rejects(prepareAttachmentArg({ data: 'aGk=', name: 'a.txt', mime_type: 'text/plain\r\nX: 1' }, p), /mime_type must be type\/subtype/);
  assert.equal((await prepareAttachmentArg({ data: 'aGk=', name: 'a.bin', mime_type: 'text/plain; charset=utf-8' }, p)).mimeType, 'text/plain', 'parameters are dropped');
  assert.equal(bareMimeType('Image/PNG ; q=1'), 'Image/PNG');
  assert.equal(bareMimeType('; charset=utf-8'), undefined);

  const wrapped = await prepareAttachmentArg({ data: 'aGVs\nbG8=\n', name: 'a.txt' }, p);
  assert.equal(wrapped.size, 5);
  assert.equal((await wrapped.read()).toString(), 'hello');
  assert.equal((await prepareAttachmentArg({ data: 'aGk=', name: 'blob' }, p)).mimeType, undefined, 'no extension, no guess');
  assert.equal((await prepareAttachmentArg({ data: 'aGk=', name: 'shot.PNG' }, p)).mimeType, 'image/png');
});

test('count and aggregate budgets are checked before anything is uploaded', async () => {
  const { uploader, uploaded } = fakeUploader();
  const p = policy({ maxBytes: 100, maxTotalBytes: 250, maxCount: 3 });
  const entry = (n: number) => ({ data: b64('x'.repeat(n)), name: `f${n}.txt` });
  await assert.rejects(uploadAttachmentArgs(uploader, [entry(1), entry(2), entry(3), entry(4)], p), /at most 3 attachments per message \(got 4\)/);
  await assert.rejects(uploadAttachmentArgs(uploader, [entry(100), entry(100), entry(51)], p), /total 251B, over the 250B per-message budget/);
  await assert.rejects(uploadAttachmentArgs(uploader, [entry(1), { data: 'aGk=' }], p), /need a `name`/);
  assert.deepEqual(uploaded, [], 'nothing reached the uploader');
  const out = await uploadAttachmentArgs(uploader, [entry(100), entry(100), entry(50)], p);
  assert.deepEqual(uploaded.map((u) => u.name), ['f100.txt', 'f100.txt', 'f50.txt']);
  assert.deepEqual(out.map((f) => f.path), ['/user_uploads/1/f100.txt', '/user_uploads/1/f100.txt', '/user_uploads/1/f50.txt']);
  assert.deepEqual(await uploadAttachmentArgs(uploader, undefined, p), []);
  await assert.rejects(uploadAttachmentArgs(uploader, 'nope', p), /must be an array/);
});

test('withAttachmentLinks appends Zulip-style links, or is just the links when there is no text', () => {
  const files = [
    { name: 'a.png', path: '/user_uploads/1/a.png', url: 'https://z/user_uploads/1/a.png' },
    { name: 'b.pdf', path: '/user_uploads/1/b.pdf', url: 'https://z/user_uploads/1/b.pdf' },
  ];
  assert.equal(withAttachmentLinks('see attached', files), 'see attached\n\n[a.png](/user_uploads/1/a.png)\n[b.pdf](/user_uploads/1/b.pdf)');
  assert.equal(withAttachmentLinks('  ', files), '[a.png](/user_uploads/1/a.png)\n[b.pdf](/user_uploads/1/b.pdf)');
  assert.equal(withAttachmentLinks('plain', []), 'plain');
});

/** Open descriptors of this process (macOS and Linux both list them under /dev/fd). */
function openFds(): number {
  return readdirSync('/dev/fd').length;
}

test('a refused batch releases every handle it opened without reading anything', linuxOnly, async () => {
  const dir = tmp();
  try {
    for (const n of ['a', 'b', 'c']) writeFileSync(join(dir, `${n}.txt`), 'x'.repeat(100));
    const p = policy({ roots: new Map([['d', dir]]), maxBytes: 100, maxTotalBytes: 250 });
    const before = openFds();
    await assert.rejects(prepareAttachments([{ file: 'd/a.txt' }, { file: 'd/b.txt' }, { file: 'd/c.txt' }], p), /per-message budget/);
    assert.equal(openFds(), before, 'handles released');
    await assert.rejects(prepareAttachments([{ file: 'd/a.txt' }, { data: 'aGk=' }], p), /need a `name`/);
    assert.equal(openFds(), before);
    const one = await prepareAttachmentArg({ file: 'd/a.txt' }, p);
    await one.close();
    await one.close();
    await assert.rejects(one.read(), /already read or released/);
    assert.equal(openFds(), before);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('uploadPrepared releases the remaining handles when an upload throws, and re-applies the budget to bytes actually read', linuxOnly, async () => {
  const dir = tmp();
  try {
    for (const n of ['a', 'b', 'c']) writeFileSync(join(dir, `${n}.txt`), 'x'.repeat(100));
    const p = policy({ roots: new Map([['d', dir]]), maxBytes: 150, maxTotalBytes: 250 });
    const before = openFds();
    const prepared = await prepareAttachments([{ file: 'd/a.txt' }, { file: 'd/b.txt' }, { file: 'd/c.txt' }], p.roots.size ? { ...p, maxTotalBytes: 300 } : p);
    assert.equal(openFds(), before + 3);
    await assert.rejects(uploadPrepared({ async upload() { throw new Error('quota'); } }, prepared, { ...p, maxTotalBytes: 300 }), /quota/);
    assert.equal(openFds(), before, 'the two unread handles are released too');

    const { uploader, uploaded } = fakeUploader();
    const again = await prepareAttachments([{ file: 'd/a.txt' }, { file: 'd/b.txt' }, { file: 'd/c.txt' }], { ...p, maxTotalBytes: 300 });
    writeFileSync(join(dir, 'b.txt'), 'x'.repeat(150));
    await assert.rejects(uploadPrepared(uploader, again, { ...p, maxTotalBytes: 260 }), /attachments total over the 260B per-message budget/);
    assert.deepEqual(uploaded.map((u) => u.name), ['a.txt', 'b.txt'], 'stopped at the file that broke the budget (100 + 150 + 100 > 260)');
    assert.equal(openFds(), before);
    await closeAll(again);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the containment check is bound to the opened object, not to the pathname', linuxOnly, async () => {
  const dir = tmp();
  try {
    const root = join(dir, 'root');
    mkdirSync(root);
    writeFileSync(join(root, 'in.txt'), 'in');
    writeFileSync(join(dir, 'out.txt'), 'out');
    writeFileSync(`${root} (deleted)`, 'sibling');
    const insideHandle = await open(join(root, 'in.txt'), 'r');
    const outsideHandle = await open(join(dir, 'out.txt'), 'r');
    const siblingHandle = await open(`${root} (deleted)`, 'r');
    const unlinked = await open(join(root, 'in.txt'), 'r');
    try {
      assert.equal(await verifyOpenedInsideRoot(insideHandle, root), true);
      assert.equal(await verifyOpenedInsideRoot(outsideHandle, root), false);
      assert.equal(await verifyOpenedInsideRoot(siblingHandle, root), false);
      // A file unlinked after open is still the file that was inside.
      rmSync(join(root, 'in.txt'));
      assert.equal(await verifyOpenedInsideRoot(unlinked, root), true);
    } finally {
      await Promise.all([insideHandle, outsideHandle, siblingHandle, unlinked].map((h) => h.close()));
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('off Linux, local files are refused with the base64 hint and nothing is opened', { skip: LOCAL_FILES_SUPPORTED ? 'Linux' : false }, async () => {
  const dir = tmp();
  try {
    writeFileSync(join(dir, 'a.txt'), 'a');
    const p = policy({ roots: new Map([['d', dir]]) });
    const before = readdirSync('/dev/fd').length;
    await assert.rejects(prepareAttachmentArg({ file: 'd/a.txt' }, p), /supported on Linux only .* pass base64 `data` instead/);
    assert.equal(readdirSync('/dev/fd').length, before);
    const handle = await open(join(dir, 'a.txt'), 'r');
    try {
      assert.equal(await verifyOpenedInsideRoot(handle, dir), false, 'the guard itself fails closed');
    } finally {
      await handle.close();
    }
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

const PNG_HEAD = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0, 0, 0, 0]);

test('media blocks with inline data become uploads; URIs of any scheme and text are left alone', async () => {
  const { uploader, uploaded } = fakeUploader();
  const p = policy();
  const out = await uploadBlocks(uploader, [
    { type: 'text', text: 'hello' },
    { type: 'image', data: b64('png!'), mimeType: 'image/png' },
    { type: 'resource', uri: 'file:///etc/passwd' },
    { type: 'resource', uri: 'https://example.com/x.pdf' },
    { type: 'audio', data: b64('ogg!'), mimeType: 'audio/ogg' },
    { type: 'image', uri: 'https://example.com/remote.png' },
    { type: 'image', data: b64('svg'), mimeType: 'image/svg+xml' },
    { type: 'image', data: b64('jpg'), mimeType: 'image/jpeg; q=0.9' },
  ], p);
  assert.deepEqual(uploaded.map((u) => [u.name, u.bytes, u.mimeType]), [
    ['image-2.png', 'png!', 'image/png'],
    ['audio-5.ogg', 'ogg!', 'audio/ogg'],
    ['image-7.svg', 'svg', 'image/svg+xml'],
    ['image-8.jpg', 'jpg', 'image/jpeg'],
  ]);
  assert.deepEqual(out.map((f) => f.path), ['/user_uploads/1/image-2.png', '/user_uploads/1/audio-5.ogg', '/user_uploads/1/image-7.svg', '/user_uploads/1/image-8.jpg']);
});

test('an image block without a type is sniffed so it still previews; an invalid type is dropped, not injected', async () => {
  const { uploader, uploaded } = fakeUploader();
  await uploadBlocks(uploader, [
    { type: 'image', data: PNG_HEAD.toString('base64') },
    { type: 'image', data: b64('????'), mimeType: 'image/png\r\nX: 1' },
    { type: 'audio', data: b64('????') },
  ], policy());
  assert.deepEqual(uploaded.map((u) => [u.name, u.mimeType]), [
    ['image-1.png', 'image/png'],
    ['image-2.bin', undefined],
    ['audio-3.bin', undefined],
  ]);
  assert.equal(sniffImageType(Buffer.from([0xff, 0xd8, 0xff, 0xe0])), 'image/jpeg');
  assert.equal(sniffImageType(Buffer.from('GIF89a......')), 'image/gif');
  assert.equal(sniffImageType(Buffer.from('RIFF....WEBPVP8 ')), 'image/webp');
  assert.equal(sniffImageType(Buffer.from('nope')), undefined);
});

test('block uploads honour the same ceilings and budgets', async () => {
  const p = policy({ maxBytes: 4, maxTotalBytes: 6, maxCount: 2 });
  await assert.rejects(prepareBlocks([{ type: 'image', data: b64('12345') }], p), /image block 1 is 5B, over the 4B upload ceiling/);
  await assert.rejects(prepareBlocks([{ type: 'image', data: b64('1234') }, { type: 'image', data: b64('123') }], p), /total 7B, over the 6B per-message budget/);
  await assert.rejects(prepareBlocks([{ type: 'image', data: b64('1') }, { type: 'image', data: b64('1') }, { type: 'image', data: b64('1') }], p), /at most 2 attachments/);
  await assert.rejects(prepareBlocks([{ type: 'image', data: 'not base64!' }], p), /not valid base64/);
  assert.deepEqual(await prepareBlocks([{ type: 'text', text: 'x' }], p), []);
});

test('the policy takes the per-file ceiling from the env, else the realm, else the Cloud default, and derives the budgets', () => {
  const none = resolveUploadPolicy({}, null);
  assert.equal(none.maxBytes, DEFAULT_UPLOAD_MAX_BYTES);
  assert.equal(none.maxTotalBytes, DEFAULT_UPLOAD_MAX_BYTES * 4);
  assert.equal(none.maxCount, MAX_ATTACHMENTS_PER_MESSAGE);
  assert.equal(none.roots.size, 0);
  assert.equal(resolveUploadPolicy({}, 80 * 1024 * 1024).maxBytes, 80 * 1024 * 1024, 'the realm advertises its cap');
  assert.equal(resolveUploadPolicy({ ZULIP_UPLOAD_MAX_BYTES: '1000' }, 80 * 1024 * 1024).maxBytes, 1000, 'the env overrides it');
  assert.equal(resolveUploadPolicy({ ZULIP_UPLOAD_MAX_BYTES: 'junk' }).maxBytes, DEFAULT_UPLOAD_MAX_BYTES);
  assert.equal(resolveUploadPolicy({ ZULIP_UPLOAD_MAX_BYTES: '0' }).maxBytes, DEFAULT_UPLOAD_MAX_BYTES);
  assert.throws(() => resolveUploadPolicy({ ZULIP_UPLOAD_ROOTS: 'x=/definitely/not/here' }), /ZULIP_UPLOAD_ROOTS: root "x"/);
});

test('guessMimeType covers the image types Zulip thumbnails, and nothing it does not know', () => {
  assert.equal(guessMimeType('a.png'), 'image/png');
  assert.equal(guessMimeType('a.JPG'), 'image/jpeg');
  assert.equal(guessMimeType('report.pdf'), 'application/pdf');
  assert.equal(guessMimeType('Makefile'), undefined);
  assert.equal(guessMimeType('a.xyz'), undefined);
});
