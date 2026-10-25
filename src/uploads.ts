/**
 * zulip-js has no upload surface, so this goes straight at
 * `POST /api/v1/user_uploads` with the bot's credentials. Zulip attaches a
 * file to a message by link: the upload returns a `/user_uploads/...` path
 * and the message body links to it.
 */

import { randomBytes } from 'node:crypto';
import { constants as fsConstants, type FileHandle, open, readlink, realpath } from 'node:fs/promises';
import { realpathSync } from 'node:fs';
import { basename, isAbsolute, resolve, sep } from 'node:path';
import type { ContentBlock } from '@animalabs/mcpl-core';

/** Zulip Cloud's default `MAX_FILE_UPLOAD_SIZE`, used when the realm does not advertise its own. */
export const DEFAULT_UPLOAD_MAX_BYTES = 25 * 1024 * 1024;
export const MAX_ATTACHMENTS_PER_MESSAGE = 10;
export const TOTAL_BUDGET_MULTIPLIER = 4;

export interface UploadPolicy {
  roots: ReadonlyMap<string, string>;
  maxBytes: number;
  maxTotalBytes: number;
  maxCount: number;
}

export function parseUploadRoots(spec: string | undefined, cwd: string = process.cwd()): Map<string, string> {
  const roots = new Map<string, string>();
  for (const raw of (spec ?? '').split(',')) {
    const entry = raw.trim();
    if (!entry) continue;
    const eq = entry.indexOf('=');
    const name = eq > 0 ? entry.slice(0, eq).trim() : '';
    const dir = eq > 0 ? entry.slice(eq + 1).trim() : '';
    if (!name || !dir || !/^[A-Za-z0-9_-]+$/.test(name)) {
      throw new Error(`ZULIP_UPLOAD_ROOTS: entry "${entry}" is not name=path (name: letters, digits, _ or -)`);
    }
    if (roots.has(name)) throw new Error(`ZULIP_UPLOAD_ROOTS: root "${name}" is listed twice`);
    let canonical: string;
    try {
      canonical = realpathSync(resolve(cwd, dir));
    } catch (err) {
      throw new Error(`ZULIP_UPLOAD_ROOTS: root "${name}" (${dir}): ${(err as Error).message}`);
    }
    roots.set(name, canonical);
  }
  return roots;
}

export function resolveUploadPolicy(env: NodeJS.ProcessEnv = process.env, realmMaxBytes?: number | null): UploadPolicy {
  const knob = Number(env.ZULIP_UPLOAD_MAX_BYTES ?? '');
  const maxBytes =
    env.ZULIP_UPLOAD_MAX_BYTES !== undefined && Number.isFinite(knob) && knob > 0
      ? Math.floor(knob)
      : realmMaxBytes && Number.isFinite(realmMaxBytes) && realmMaxBytes > 0
        ? Math.floor(realmMaxBytes)
        : DEFAULT_UPLOAD_MAX_BYTES;
  return {
    roots: parseUploadRoots(env.ZULIP_UPLOAD_ROOTS),
    maxBytes,
    maxTotalBytes: maxBytes * TOTAL_BUDGET_MULTIPLIER,
    maxCount: MAX_ATTACHMENTS_PER_MESSAGE,
  };
}

export interface UploadInput {
  name: string;
  data: Buffer;
  mimeType?: string;
}

export interface UploadedFile {
  name: string;
  path: string;
  url: string;
}

export interface Uploader {
  upload(input: UploadInput): Promise<UploadedFile>;
}

export interface UploadTarget {
  realm: string;
  authHeader: string;
}

type FetchLike = (url: string, init: { method: string; headers: Record<string, string>; body: Buffer }) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>;

/** RFC 2045 `type/subtype`, no parameters. Anything else cannot go into a part header. */
const MIME_RE = /^[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}\/[A-Za-z0-9][A-Za-z0-9!#$&^_.+-]{0,126}$/;

export function isValidMimeType(value: string): boolean {
  return MIME_RE.test(value);
}

/**
 * zulip-js drags in isomorphic-form-data, which replaces the global `FormData`
 * with a stream-based one that cannot take a Blob, so neither the WHATWG form
 * nor that one is safe to rely on. A Buffer body works under every fetch.
 */
export function multipartBody(input: UploadInput, boundary: string): Buffer {
  const name = input.name.replace(/["\r\n]/g, '_');
  const type = bareMimeType(input.mimeType) ?? 'application/octet-stream';
  const head =
    `--${boundary}\r\n` +
    `Content-Disposition: form-data; name="file"; filename="${name}"\r\n` +
    `Content-Type: ${type}\r\n\r\n`;
  return Buffer.concat([Buffer.from(head, 'utf8'), input.data, Buffer.from(`\r\n--${boundary}--\r\n`, 'utf8')]);
}

export function createZulipUploader(target: UploadTarget, fetchImpl: FetchLike = fetch as unknown as FetchLike): Uploader {
  if (!target.realm) throw new Error('Cannot upload files: realm is unknown');
  const realm = target.realm.replace(/\/+$/, '');
  return {
    async upload(input: UploadInput): Promise<UploadedFile> {
      if (!target.authHeader) throw new Error('Cannot upload files: bot credentials are unknown (no email/api key)');
      const boundary = `----zulip-mcp-${randomBytes(12).toString('hex')}`;
      // Single-request upload. Zulip's docs warn this endpoint may time out
      // on bodies past ~25 MB; the resumable tus endpoint (feature level
      // 296+) is the road there if a realm ever raises its cap that far.
      const res = await fetchImpl(`${realm}/api/v1/user_uploads`, {
        method: 'POST',
        headers: { Authorization: target.authHeader, 'Content-Type': `multipart/form-data; boundary=${boundary}` },
        body: multipartBody(input, boundary),
      });
      let body: { result?: string; msg?: string; uri?: string; url?: string } = {};
      try {
        body = (await res.json()) as typeof body;
      } catch {
      }
      if (!res.ok || body.result !== 'success') {
        throw new Error(`upload of "${input.name}" failed: ${body.msg ?? `HTTP ${res.status}`}`);
      }
      // Zulip 9.0 (feature level 272) renamed `uri` to `url`; older servers
      // send only `uri`. Either is the `/user_uploads/...` path.
      const path = body.url ?? body.uri;
      if (typeof path !== 'string' || !path.startsWith('/user_uploads/')) {
        throw new Error(`upload of "${input.name}" returned no usable path`);
      }
      return { name: input.name, path, url: `${realm}${path}` };
    },
  };
}

/** Zulip only thumbnails (inline-previews) an upload whose declared type is
 *  `image/*`, and it does not sniff bytes or the filename: an image sent as
 *  octet-stream renders as a bare link forever. So a missing type is guessed
 *  from the extension for the common cases. */
const MIME_BY_EXT: Record<string, string> = {
  png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif', webp: 'image/webp',
  svg: 'image/svg+xml', bmp: 'image/bmp', tif: 'image/tiff', tiff: 'image/tiff', heic: 'image/heic', avif: 'image/avif',
  mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', m4a: 'audio/mp4', flac: 'audio/flac',
  mp4: 'video/mp4', webm: 'video/webm', mov: 'video/quicktime',
  pdf: 'application/pdf', zip: 'application/zip', gz: 'application/gzip', tar: 'application/x-tar',
  json: 'application/json', xml: 'application/xml', yaml: 'application/yaml', yml: 'application/yaml',
  txt: 'text/plain', md: 'text/markdown', csv: 'text/csv', tsv: 'text/tab-separated-values', html: 'text/html', htm: 'text/html', log: 'text/plain',
  docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
};

const EXT_BY_MIME: Record<string, string> = {};
for (const [ext, mime] of Object.entries(MIME_BY_EXT)) if (!(mime in EXT_BY_MIME)) EXT_BY_MIME[mime] = ext;

export function guessMimeType(name: string): string | undefined {
  const ext = name.split('.').pop()?.toLowerCase() ?? '';
  return ext && ext !== name.toLowerCase() ? MIME_BY_EXT[ext] : undefined;
}

export function sniffImageType(data: Buffer): string | undefined {
  if (data.length >= 8 && data.subarray(0, 8).equals(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]))) return 'image/png';
  if (data.length >= 3 && data[0] === 0xff && data[1] === 0xd8 && data[2] === 0xff) return 'image/jpeg';
  if (data.length >= 6 && (data.subarray(0, 6).toString('latin1') === 'GIF87a' || data.subarray(0, 6).toString('latin1') === 'GIF89a')) return 'image/gif';
  if (data.length >= 12 && data.subarray(0, 4).toString('latin1') === 'RIFF' && data.subarray(8, 12).toString('latin1') === 'WEBP') return 'image/webp';
  return undefined;
}

function extensionFor(mimeType: string | undefined): string {
  if (!mimeType) return 'bin';
  const lower = mimeType.toLowerCase();
  return EXT_BY_MIME[lower] ?? lower.split('/')[1]?.replace(/[^a-z0-9]/g, '').slice(0, 8) ?? 'bin';
}

export interface AttachmentArg {
  file?: string;
  data?: string;
  name?: string;
  mime_type?: string;
}

export interface PreparedUpload {
  name: string;
  mimeType?: string;
  size: number;
  read(): Promise<Buffer>;
  close(): Promise<void>;
}

function fmt(n: number): string {
  return n >= 1048576 ? `${(n / 1048576).toFixed(1)}MB` : n >= 1024 ? `${Math.round(n / 1024)}KB` : `${n}B`;
}

export function bareMimeType(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined;
  const bare = value.split(';')[0].trim();
  return isValidMimeType(bare) ? bare : undefined;
}

function mimeFromArg(arg: AttachmentArg, what: string): string | undefined {
  if (arg.mime_type === undefined || arg.mime_type === null || arg.mime_type === '') return undefined;
  const bare = bareMimeType(arg.mime_type);
  if (!bare) throw new Error(`attachment "${what}": mime_type must be type/subtype (got ${JSON.stringify(arg.mime_type)})`);
  return bare;
}

function inside(path: string, rootDir: string): boolean {
  return path === rootDir || path.startsWith(rootDir + sep);
}

function fsFailure(file: string, err: unknown): Error {
  const code = (err as NodeJS.ErrnoException)?.code;
  const why =
    code === 'ENOENT' || code === 'ENOTDIR' ? 'not found'
      : code === 'EACCES' || code === 'EPERM' ? 'permission denied'
        : code === 'ELOOP' ? 'too many symbolic links'
          : code === 'EISDIR' ? 'is a directory'
            : `cannot be read${code ? ` (${code})` : ''}`;
  return new Error(`attachment "${file}": ${why}`);
}

async function resolveUnderRoot(file: string, policy: UploadPolicy): Promise<{ canonical: string; rootDir: string; rootName: string }> {
  if (!LOCAL_FILES_SUPPORTED) {
    throw new Error(`local-file attachments are supported on Linux only (containment is bound to the open descriptor via /proc/self/fd); pass base64 \`data\` instead`);
  }
  if (policy.roots.size === 0) {
    throw new Error(`local-file attachments are disabled: no upload roots configured (set ZULIP_UPLOAD_ROOTS=name=/dir,...); pass base64 \`data\` instead`);
  }
  const names = [...policy.roots.keys()].join(', ');
  if (isAbsolute(file) || file.startsWith('\\') || /^[A-Za-z]:/.test(file)) {
    throw new Error(`attachment "${file}": paths are root-relative (<root>/<path>); available roots: ${names}`);
  }
  const slash = file.indexOf('/');
  const rootName = slash < 0 ? file : file.slice(0, slash);
  const rest = slash < 0 ? '' : file.slice(slash + 1);
  const rootDir = policy.roots.get(rootName);
  if (!rootDir) throw new Error(`attachment "${file}": unknown upload root "${rootName}"; available roots: ${names}`);
  if (!rest) throw new Error(`attachment "${file}": names a root, not a file in it`);
  const outside = new Error(`attachment "${file}": resolves outside upload root "${rootName}"`);
  const candidate = resolve(rootDir, rest);
  if (!inside(candidate, rootDir)) throw outside;
  let canonical: string;
  try {
    canonical = await realpath(candidate);
  } catch (err) {
    throw fsFailure(file, err);
  }
  if (!inside(canonical, rootDir)) throw outside;
  return { canonical, rootDir, rootName };
}

/** Local-file attachments need descriptor-bound containment, which only procfs provides. */
export const LOCAL_FILES_SUPPORTED = process.platform === 'linux';

/**
 * O_NOFOLLOW guards only the final component, so a parent directory swapped
 * for a symlink between `realpath` and `open` would open a file outside the
 * root; `/proc/self/fd/N` says what the descriptor actually refers to. A file
 * unlinked after open reads back as `<root>/x (deleted)`, which still starts
 * with `<root>/`, and a sibling literally named `<root> (deleted)` must not
 * pass as the root. Without procfs, Node has no descriptor-relative
 * resolution.
 */
export async function verifyOpenedInsideRoot(handle: FileHandle, rootDir: string): Promise<boolean> {
  if (!LOCAL_FILES_SUPPORTED) return false;
  const actual = await readlink(`/proc/self/fd/${handle.fd}`);
  return actual.startsWith(rootDir + sep);
}

async function readCapped(handle: FileHandle, maxBytes: number): Promise<Buffer> {
  const chunks: Buffer[] = [];
  let total = 0;
  const chunk = Buffer.alloc(Math.min(64 * 1024, maxBytes + 1));
  while (total <= maxBytes) {
    const { bytesRead } = await handle.read(chunk, 0, Math.min(chunk.length, maxBytes + 1 - total), null);
    if (bytesRead === 0) break;
    chunks.push(Buffer.from(chunk.subarray(0, bytesRead)));
    total += bytesRead;
  }
  return Buffer.concat(chunks);
}

async function prepareFile(arg: AttachmentArg, policy: UploadPolicy): Promise<PreparedUpload> {
  const file = arg.file as string;
  const { canonical, rootDir, rootName } = await resolveUnderRoot(file, policy);
  const name = (typeof arg.name === 'string' && arg.name.trim()) || basename(canonical);
  const mimeType = mimeFromArg(arg, file) ?? guessMimeType(name);

  let handle: FileHandle;
  try {
    handle = await open(canonical, fsConstants.O_RDONLY | (fsConstants.O_NOFOLLOW ?? 0));
  } catch (err) {
    throw fsFailure(file, err);
  }
  let size: number;
  try {
    let contained: boolean;
    try {
      contained = await verifyOpenedInsideRoot(handle, rootDir);
    } catch (err) {
      throw fsFailure(file, err);
    }
    if (!contained) throw new Error(`attachment "${file}": resolves outside upload root "${rootName}"`);
    const info = await handle.stat();
    if (!info.isFile()) throw new Error(`attachment "${file}" is not a regular file`);
    if (info.size > policy.maxBytes) throw new Error(`attachment "${file}" is ${fmt(info.size)}, over the ${fmt(policy.maxBytes)} upload ceiling`);
    size = info.size;
  } catch (err) {
    await handle.close();
    throw err;
  }
  let done = false;
  const close = async () => {
    if (done) return;
    done = true;
    await handle.close();
  };
  return {
    name,
    mimeType,
    size,
    async read() {
      if (done) throw new Error(`attachment "${file}" already read or released`);
      try {
        const data = await readCapped(handle, policy.maxBytes);
        if (data.length > policy.maxBytes) throw new Error(`attachment "${file}" grew past the ${fmt(policy.maxBytes)} upload ceiling`);
        return data;
      } finally {
        await close();
      }
    },
    close,
  };
}

const BASE64_RE = /^[A-Za-z0-9+/]*={0,2}$/;

export function base64DecodedLength(text: string): number {
  if (!BASE64_RE.test(text) || text.length % 4 !== 0) throw new Error('data is not valid base64');
  const padding = text.endsWith('==') ? 2 : text.endsWith('=') ? 1 : 0;
  return (text.length / 4) * 3 - padding;
}

function prepareInline(arg: AttachmentArg, policy: UploadPolicy): PreparedUpload {
  const name = typeof arg.name === 'string' ? arg.name.trim() : '';
  if (!name) throw new Error('inline attachments (`data`) need a `name`');
  const text = (arg.data as string).replace(/\s+/g, '');
  let size: number;
  try {
    size = base64DecodedLength(text);
  } catch (err) {
    throw new Error(`attachment "${name}": ${(err as Error).message}`);
  }
  if (size === 0) throw new Error(`attachment "${name}": data is empty`);
  if (size > policy.maxBytes) throw new Error(`attachment "${name}" is ${fmt(size)}, over the ${fmt(policy.maxBytes)} upload ceiling`);
  const mimeType = mimeFromArg(arg, name) ?? guessMimeType(name);
  return {
    name,
    mimeType,
    size,
    async read() {
      const data = Buffer.from(text, 'base64');
      if (data.length !== size) throw new Error(`attachment "${name}": base64 decoded to an unexpected length`);
      return data;
    },
    async close() {},
  };
}

export async function prepareAttachmentArg(arg: AttachmentArg, policy: UploadPolicy): Promise<PreparedUpload> {
  if (typeof arg !== 'object' || arg === null) throw new Error('each attachment must be an object with `file` or `data`');
  const hasFile = typeof arg.file === 'string' && arg.file.trim() !== '';
  const hasData = typeof arg.data === 'string' && arg.data !== '';
  if (hasFile === hasData) throw new Error('each attachment needs exactly one of `file` (root-relative path) or `data` (base64)');
  return hasFile ? prepareFile(arg, policy) : prepareInline(arg, policy);
}

function checkBudget(prepared: PreparedUpload[], policy: UploadPolicy): void {
  if (prepared.length > policy.maxCount) throw new Error(`at most ${policy.maxCount} attachments per message (got ${prepared.length})`);
  const total = prepared.reduce((n, p) => n + p.size, 0);
  if (total > policy.maxTotalBytes) throw new Error(`attachments total ${fmt(total)}, over the ${fmt(policy.maxTotalBytes)} per-message budget`);
}

export async function prepareAttachments(args: unknown, policy: UploadPolicy): Promise<PreparedUpload[]> {
  if (args === undefined || args === null) return [];
  if (!Array.isArray(args)) throw new Error('attachments must be an array');
  if (args.length > policy.maxCount) throw new Error(`at most ${policy.maxCount} attachments per message (got ${args.length})`);
  const prepared: PreparedUpload[] = [];
  try {
    for (const arg of args) prepared.push(await prepareAttachmentArg(arg as AttachmentArg, policy));
    checkBudget(prepared, policy);
  } catch (err) {
    await closeAll(prepared);
    throw err;
  }
  return prepared;
}

export async function closeAll(prepared: PreparedUpload[]): Promise<void> {
  await Promise.all(prepared.map((p) => p.close().catch(() => undefined)));
}

/**
 * The aggregate budget is re-applied to the bytes actually read: a file may
 * have grown since its `fstat`, and most procfs files report a size of 0.
 */
export async function uploadPrepared(uploader: Uploader, prepared: PreparedUpload[], policy: UploadPolicy): Promise<UploadedFile[]> {
  const out: UploadedFile[] = [];
  let total = 0;
  try {
    for (const p of prepared) {
      const data = await p.read();
      total += data.length;
      if (total > policy.maxTotalBytes) throw new Error(`attachments total over the ${fmt(policy.maxTotalBytes)} per-message budget`);
      out.push(await uploader.upload({ name: p.name, mimeType: p.mimeType, data }));
    }
  } finally {
    await closeAll(prepared);
  }
  return out;
}

/** Uploads happen before the send; a send that then fails leaves them
 *  unreferenced, and by default Zulip deletes unclaimed uploads after five
 *  weeks. */
export async function uploadAttachmentArgs(uploader: Uploader, args: unknown, policy: UploadPolicy): Promise<UploadedFile[]> {
  return uploadPrepared(uploader, await prepareAttachments(args, policy), policy);
}

/** The markdown Zulip uses to attach an upload to a message. */
export function attachmentMarkdown(file: UploadedFile): string {
  return `[${file.name}](${file.path})`;
}

export function withAttachmentLinks(content: string, files: UploadedFile[]): string {
  if (files.length === 0) return content;
  const links = files.map(attachmentMarkdown).join('\n');
  const text = content.trim();
  return text ? `${text}\n\n${links}` : links;
}

/**
 * URI-bearing blocks are left alone: Zulip can only attach bytes this server
 * holds, and a host→server `file://` reference has no meaning in the spec.
 */
export async function prepareBlocks(blocks: ContentBlock[], policy: UploadPolicy): Promise<PreparedUpload[]> {
  const prepared: PreparedUpload[] = [];
  for (const [i, block] of blocks.entries()) {
    if ((block.type !== 'image' && block.type !== 'audio') || typeof block.data !== 'string') continue;
    const text = block.data.replace(/\s+/g, '');
    const size = base64DecodedLength(text);
    if (size === 0) continue;
    if (size > policy.maxBytes) throw new Error(`${block.type} block ${i + 1} is ${fmt(size)}, over the ${fmt(policy.maxBytes)} upload ceiling`);
    const declared = bareMimeType(block.mimeType);
    const kind = block.type;
    prepared.push({
      name: `${kind}-${i + 1}.${extensionFor(declared)}`,
      mimeType: declared,
      size,
      async read() {
        return Buffer.from(text, 'base64');
      },
      async close() {},
    });
  }
  checkBudget(prepared, policy);
  return prepared;
}

export async function uploadBlocks(uploader: Uploader, blocks: ContentBlock[], policy: UploadPolicy): Promise<UploadedFile[]> {
  const out: UploadedFile[] = [];
  for (const p of await prepareBlocks(blocks, policy)) {
    const data = await p.read();
    let mimeType = p.mimeType;
    let name = p.name;
    if (!mimeType && name.startsWith('image-')) {
      mimeType = sniffImageType(data);
      if (mimeType) name = name.replace(/\.bin$/, `.${extensionFor(mimeType)}`);
    }
    out.push(await uploader.upload({ name, mimeType, data }));
  }
  return out;
}
