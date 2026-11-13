import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import type sharpModule from 'sharp';
import type { ContentBlock, TextContent } from '@animalabs/mcpl-core';
import type { AttachmentRef } from './content.js';

/** Anthropic downscales an image whose long edge is over 1568 px or whose cost
 *  would be over 1568 visual tokens (2576 px and 4784 tokens on Claude 4.7 and
 *  later models): https://platform.claude.com/docs/en/build-with-claude/vision */
export const IMAGE_LONG_EDGE_MAX = 1568;
const IMAGE_JPEG_QUALITY = 85;
/** Anthropic accepts up to 10 MB of base64 per image on the Claude API, and
 *  5 MB on Amazon Bedrock, by the same page; base64 adds a third, so 3.5 MB of
 *  image bytes stays under the smaller limit. */
export const IMAGE_OUTPUT_RAW_CAP = 3.5 * 1024 * 1024;
export const IMAGE_FETCH_CEILING = 25 * 1024 * 1024;
export const MAX_TEXT_BYTES = 256 * 1024;
export const DEFAULT_ATTACHMENT_INLINE_MAX_BYTES = 5120;
export const DEFAULT_MAX_INLINE_IMAGES = 4;

/** Formats the model API accepts. sharp also reads svg/tiff/avif/heif; an SVG
 *  passed through would be a block the API rejects on every request. */
const API_SAFE_FORMATS = new Set(['jpeg', 'png', 'gif', 'webp']);

const IMAGE_MAX_INPUT_PIXELS = 40_000_000;
const sharpOpts = { limitInputPixels: IMAGE_MAX_INPUT_PIXELS };

const IMAGE_DECODE_CONCURRENCY = 2;
let decodesInFlight = 0;
const decodeWaiters: (() => void)[] = [];
async function withDecodeSlot<T>(fn: () => Promise<T>): Promise<T> {
  if (decodesInFlight >= IMAGE_DECODE_CONCURRENCY) {
    await new Promise<void>((resolve) => decodeWaiters.push(resolve));
  }
  decodesInFlight++;
  try {
    return await fn();
  } finally {
    decodesInFlight--;
    decodeWaiters.shift()?.();
  }
}

export interface NormalizedImage {
  data: string;
  mimeType: string;
}

export function normalizeImageForInference(buf: Buffer): Promise<NormalizedImage | null> {
  return withDecodeSlot(() => normalizeImageUnbounded(buf));
}

let sharpLoad: Promise<typeof sharpModule> | undefined;
function loadSharp(): Promise<typeof sharpModule> {
  sharpLoad ??= import('sharp').then((m) => m.default);
  return sharpLoad;
}

function wasmInstall(): { command: string; dir?: string } {
  const command = 'npm install --no-save @img/sharp-wasm32';
  try {
    const sharpDir = dirname(dirname(createRequire(import.meta.url).resolve('sharp')));
    const { version } = JSON.parse(readFileSync(join(sharpDir, 'package.json'), 'utf8')) as { version: string };
    return { command: `${command}@${version}`, dir: dirname(dirname(sharpDir)) };
  } catch {
    return { command };
  }
}

export async function checkImageDecoder(options: InlineOptions): Promise<string | null> {
  if (!options.inlineImages || options.maxImages <= 0) return null;
  try {
    await loadSharp();
    return null;
  } catch (err) {
    const why = err instanceof Error ? err.message : String(err);
    const { command, dir } = wasmInstall();
    const where = dir ? `in this directory:\n${dir}` : 'in the directory the server is installed in.';
    return `${why}\n[zulip-mcp] Image inlining needs sharp, which couldn't load here. Loading it needs Node 20.10 or later. With that, either set ZULIP_INLINE_IMAGES=false to run without inlining images, or install sharp's WebAssembly build by running ${command} ${where}`;
  }
}

async function normalizeImageUnbounded(buf: Buffer): Promise<NormalizedImage | null> {
  try {
    const sharp = await loadSharp();
    const meta = await sharp(buf, { ...sharpOpts, animated: true }).metadata();
    const longest = Math.max(meta.width ?? 0, meta.height ?? 0);
    const isAnimated = (meta.pages ?? 1) > 1;
    const apiSafe = API_SAFE_FORMATS.has(meta.format ?? '');

    if (isAnimated) {
      return apiSafe && buf.length <= IMAGE_OUTPUT_RAW_CAP
        ? { data: buf.toString('base64'), mimeType: `image/${meta.format}` }
        : null;
    }

    if (apiSafe && longest > 0 && longest <= IMAGE_LONG_EDGE_MAX && buf.length <= IMAGE_OUTPUT_RAW_CAP) {
      return { data: buf.toString('base64'), mimeType: `image/${meta.format}` };
    }

    // Fresh pipeline per encode (sharp instances aren't safely reusable across
    // multiple toBuffer() calls). resize() with withoutEnlargement is a no-op
    // when the image is already within bounds.
    const resizeOpts = { width: IMAGE_LONG_EDGE_MAX, height: IMAGE_LONG_EDGE_MAX, fit: 'inside' as const, withoutEnlargement: true };
    const base = () => sharp(buf, sharpOpts).resize(resizeOpts);

    let out: Buffer;
    let mimeType: string;
    if (meta.hasAlpha) {
      out = await base().png({ compressionLevel: 9 }).toBuffer();
      mimeType = 'image/png';
    } else {
      out = await base().jpeg({ quality: IMAGE_JPEG_QUALITY }).toBuffer();
      mimeType = 'image/jpeg';
    }

    if (out.length > IMAGE_OUTPUT_RAW_CAP) {
      out = await sharp(buf, sharpOpts)
        .resize({ width: 1024, height: 1024, fit: 'inside', withoutEnlargement: true })
        .flatten({ background: '#ffffff' })
        .jpeg({ quality: 70 })
        .toBuffer();
      mimeType = 'image/jpeg';
      if (out.length > IMAGE_OUTPUT_RAW_CAP) return null;
    }

    return { data: out.toString('base64'), mimeType };
  } catch {
    return null;
  }
}

export interface FetchedBytes {
  buf: Buffer;
  mimeType: string | null;
  overflow: boolean;
}

export async function fetchCapped(
  url: string,
  headers: Record<string, string>,
  maxBytes: number,
  signal: AbortSignal,
): Promise<FetchedBytes> {
  const res = await fetch(url, { headers, redirect: 'follow', signal });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const mimeType = res.headers.get('content-type')?.split(';')[0].trim() ?? null;
  const declared = Number(res.headers.get('content-length') ?? '');
  if (Number.isFinite(declared) && declared > maxBytes) {
    return { buf: Buffer.alloc(0), mimeType, overflow: true };
  }
  if (!res.body) return { buf: Buffer.alloc(0), mimeType, overflow: true };
  const reader = res.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    total += value.length;
    if (total > maxBytes) {
      void reader.cancel().catch(() => {});
      return { buf: Buffer.alloc(0), mimeType, overflow: true };
    }
    chunks.push(value);
  }
  return { buf: Buffer.concat(chunks), mimeType, overflow: false };
}

const TEXT_EXT =
  /\.(txt|md|markdown|json|jsonl|csv|tsv|log|ya?ml|xml|html?|css|js|mjs|cjs|ts|tsx|jsx|py|rb|go|rs|java|kt|c|h|cpp|hpp|sh|bash|zsh|toml|ini|cfg|conf|sql|diff|patch|env)$/i;

export function isTextAttachment(ref: AttachmentRef): boolean {
  return ref.mimeType.startsWith('text/') || TEXT_EXT.test(ref.name);
}

export interface AttachmentSource {
  fetch(path: string, maxBytes: number, signal: AbortSignal): Promise<FetchedBytes>;
}

export interface InlineOptions {
  inlineImages: boolean;
  inlineTextMaxBytes: number;
  maxImages: number;
}

export function resolveInlineOptions(env: NodeJS.ProcessEnv = process.env): InlineOptions {
  const knob = Number(env.ZULIP_ATTACHMENT_INLINE_MAX_BYTES ?? '');
  const maxImages = Number(env.ZULIP_INLINE_IMAGES_MAX ?? '');
  return {
    inlineImages: env.ZULIP_INLINE_IMAGES !== 'false',
    inlineTextMaxBytes: Math.min(
      MAX_TEXT_BYTES,
      Number.isFinite(knob) && knob >= 0 && env.ZULIP_ATTACHMENT_INLINE_MAX_BYTES !== undefined ? Math.floor(knob) : DEFAULT_ATTACHMENT_INLINE_MAX_BYTES,
    ),
    maxImages: Number.isFinite(maxImages) && maxImages >= 0 && env.ZULIP_INLINE_IMAGES_MAX !== undefined ? Math.floor(maxImages) : DEFAULT_MAX_INLINE_IMAGES,
  };
}

function fmt(n: number): string {
  return n >= 1048576 ? `${(n / 1048576).toFixed(1)}MB` : n >= 1024 ? `${Math.round(n / 1024)}KB` : `${n}B`;
}

function text(t: string): TextContent {
  return { type: 'text', text: t };
}

function outOfTime(): Error {
  return new Error("out of time for this message's attachments");
}

async function fetchBefore(source: AttachmentSource, path: string, maxBytes: number, deadline: AbortSignal): Promise<FetchedBytes> {
  if (deadline.aborted) throw outOfTime();
  const fetching = source.fetch(path, maxBytes, deadline);
  let expire!: () => void;
  const expired = new Promise<never>((_, reject) => {
    expire = () => reject(outOfTime());
    deadline.addEventListener('abort', expire, { once: true });
  });
  try {
    return await Promise.race([fetching, expired]);
  } finally {
    deadline.removeEventListener('abort', expire);
    fetching.catch(() => {});
  }
}

export async function buildAttachmentBlocks(
  refs: AttachmentRef[],
  source: AttachmentSource,
  options: InlineOptions,
  deadline: AbortSignal,
): Promise<ContentBlock[]> {
  const blocks: ContentBlock[] = [];
  let imagesLeft = options.inlineImages ? options.maxImages : 0;
  for (const ref of refs) {
    try {
      if (ref.isImage) {
        if (imagesLeft <= 0) {
          blocks.push(text(`[image attachment "${ref.name}" not inlined (${options.inlineImages ? 'per-message image limit reached' : 'image inlining is off'}) — fetch_attachment ${ref.path}]`));
          continue;
        }
        const got = await fetchBefore(source, ref.path, IMAGE_FETCH_CEILING, deadline);
        if (got.overflow) {
          blocks.push(text(`[image attachment "${ref.name}" too large to fetch (over ${fmt(IMAGE_FETCH_CEILING)}) — ${ref.path}]`));
          continue;
        }
        const norm = await normalizeImageForInference(got.buf);
        if (!norm) {
          blocks.push(text(`[image attachment "${ref.name}" (${fmt(got.buf.length)}) could not be inlined — fetch_attachment ${ref.path}]`));
          continue;
        }
        imagesLeft--;
        blocks.push({ type: 'image', data: norm.data, mimeType: norm.mimeType });
        blocks.push(text(`[image attachment: ${ref.name}]`));
      } else if (isTextAttachment(ref) && options.inlineTextMaxBytes > 0) {
        const got = await fetchBefore(source, ref.path, options.inlineTextMaxBytes, deadline);
        if (got.overflow) {
          blocks.push(text(`[attachment: ${ref.name} over the ${fmt(options.inlineTextMaxBytes)} inline cap — not inlined: fetch_attachment ${ref.path}]`));
          continue;
        }
        blocks.push(text(`[attachment: ${ref.name} (${fmt(got.buf.length)})]\n${got.buf.toString('utf8')}`));
      }
    } catch (err) {
      blocks.push(text(`[attachment: ${ref.name} — could not fetch (${(err as Error).message}); fetch_attachment ${ref.path}]`));
    }
  }
  return blocks;
}
