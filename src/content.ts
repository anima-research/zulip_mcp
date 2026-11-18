import { markTransportError } from "./message-ids.js";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { DEFAULT_TIMEOUT_MS, parseRetryAfter } from "./zulip-api.js";

/**
 * Node resolves the entry module to its realpath while process.argv[1] keeps
 * the symlink path, so a naive `import.meta.url === pathToFileURL(argv[1]).href`
 * is false when launched via `npx`/a bin shim. Both sides are canonicalised:
 * argv[1] for the bin shim, and the entry path because it can itself sit
 * behind a symlinked directory (macOS's /var → /private/var, --preserve-symlinks).
 */
export function isMainModule(importMetaUrl: string, argv1: string | undefined): boolean {
  if (!argv1) return false;
  try {
    return realpathSync(fileURLToPath(importMetaUrl)) === realpathSync(argv1);
  } catch {
    return false;
  }
}

// Anthropic refuses an image over 10 MB of base64 on the Claude API, and over
// 5 MB on Amazon Bedrock (https://platform.claude.com/docs/en/build-with-claude/vision).
// A fetched image goes out as fetched, and base64 adds a third, so one at this
// cap is within the first limit and over the second.
export const MAX_ATTACHMENT_BYTES = 5 * 1024 * 1024;

const TEXT_MIME_RE = /^(text\/|application\/(json|xml|x-yaml|yaml|csv|x-www-form-urlencoded)\b)/i;

export interface FetchedAttachment {
  buf: Buffer;
  mimeType: string;
  name: string;
}

export async function fetchAttachmentBytes(
  url: string,
  fallbackName: string,
  opts: { headers?: Record<string, string> } = {},
): Promise<FetchedAttachment> {
  const controller = new AbortController();
  let timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  let status: number | undefined;
  let retryAfterSeconds: number | undefined;
  const progressed = (receivedStatus?: number, receivedRetryAfter?: number) => {
    if (receivedStatus !== undefined) status = receivedStatus;
    if (receivedRetryAfter !== undefined) retryAfterSeconds = receivedRetryAfter;
    clearTimeout(timer);
    timer = setTimeout(() => controller.abort(), DEFAULT_TIMEOUT_MS);
  };
  try {
    return await readAttachment(url, fallbackName, opts.headers ?? {}, controller.signal, progressed);
  } catch (err) {
    if (controller.signal.aborted) throw markTransportError(new Error(`The download of ${fallbackName} got no data for ${DEFAULT_TIMEOUT_MS / 1000} s.`), { operation: 'Attachment download', status, retryAfterSeconds, timeoutSeconds: DEFAULT_TIMEOUT_MS / 1000, original: err });
    throw err;
  } finally {
    clearTimeout(timer);
  }
}

async function readAttachment(
  url: string,
  fallbackName: string,
  headers: Record<string, string>,
  signal: AbortSignal,
  progressed: (status?: number, retryAfterSeconds?: number) => void,
): Promise<FetchedAttachment> {
  const resp = await fetch(url, { headers, redirect: "follow", signal }).catch(error => { throw markTransportError(error, { operation: 'Attachment download' }); });
  const retryAfterSeconds = parseRetryAfter(resp.headers.get('retry-after'));
  progressed(resp.status, retryAfterSeconds);
  if (!resp.ok) throw markTransportError(new Error(`fetch failed: ${resp.status} ${resp.statusText}`), { operation: 'Attachment download', status: resp.status, completed: true, retryAfterSeconds });

  const declared = resp.headers.get("content-length");
  if (declared !== null) {
    const n = Number(declared);
    if (Number.isFinite(n) && n > MAX_ATTACHMENT_BYTES) {
      throw new Error(`attachment too large: ${n} bytes (max ${MAX_ATTACHMENT_BYTES})`);
    }
  }

  const reader = resp.body?.getReader();
  let buf: Buffer;
  if (!reader) {
    buf = Buffer.from(await resp.arrayBuffer().catch(error => { throw markTransportError(error, { operation: 'Attachment download', status: resp.status, retryAfterSeconds }); }));
    if (buf.byteLength > MAX_ATTACHMENT_BYTES) {
      throw new Error(`attachment too large: ${buf.byteLength} bytes (max ${MAX_ATTACHMENT_BYTES})`);
    }
  } else {
    const chunks: Buffer[] = [];
    let total = 0;
    while (true) {
      const { value, done } = await reader.read().catch(error => { throw markTransportError(error, { operation: 'Attachment download', status: resp.status, retryAfterSeconds }); });
      if (done) break;
      if (!value) continue;
      progressed();
      total += value.byteLength;
      if (total > MAX_ATTACHMENT_BYTES) {
        reader.cancel().catch(() => {});
        throw new Error(`attachment too large: streamed past ${MAX_ATTACHMENT_BYTES} bytes`);
      }
      chunks.push(Buffer.from(value.buffer, value.byteOffset, value.byteLength));
    }
    buf = Buffer.concat(chunks, total);
  }

  const headerMime = resp.headers.get("content-type")?.split(";")[0].trim();
  const classified = classifyExtension(fallbackName);
  const mimeType = headerMime || classified.mimeType;
  return { buf, mimeType, name: fallbackName };
}

export function toFetchResult({ buf, mimeType, name }: FetchedAttachment): Record<string, unknown> {
  if (mimeType.startsWith("image/")) {
    return {
      _content: [
        { type: "text", text: `Fetched ${name} (${mimeType}, ${buf.byteLength} bytes):` },
        { type: "image", data: buf.toString("base64"), mimeType },
      ],
    };
  }
  if (TEXT_MIME_RE.test(mimeType)) {
    return {
      name,
      mimeType,
      size: buf.byteLength,
      content_text: buf.toString("utf-8"),
    };
  }
  return {
    name,
    mimeType,
    size: buf.byteLength,
    base64: buf.toString("base64"),
    note: "Non-image, non-text attachment returned as base64. Decode externally as needed.",
  };
}

/**
 * The prefix check relies on `new URL()` normalizing dot segments before
 * `pathname` is read: '/user_uploads/../api/v1/users/me' (and its
 * percent-encoded variants; the WHATWG parser treats '%2e%2e' as '..')
 * normalizes to '/api/v1/users/me' and is rejected. Never replace this with a
 * check on the raw input string.
 */
export function parseZulipAttachmentUrl(rawPath: string, zulipRealm: string): URL {
  if (!rawPath) throw new Error("path is required");
  if (!zulipRealm) throw new Error("Zulip realm not configured");

  let realm: URL;
  try {
    realm = new URL(zulipRealm);
    if (!['https:', 'http:'].includes(realm.protocol) || !realm.hostname ||
        realm.username || realm.password || realm.search || realm.hash || !['/', '/api', '/api/'].includes(realm.pathname)) throw new Error();
  } catch {
    throw new Error("Invalid configured realm URL; use an HTTP(S) origin, optionally suffixed /api, without credentials, query or fragment; ask the host operator to repair the configuration.");
  }
  // Preserve main's input grammar: only lowercase http(s) prefixes denote
  // absolute URLs; other spellings must parse as /user_uploads/ paths when
  // appended to the origin. In particular, //host and HTTPS:// are not aliases.
  if (!rawPath.startsWith('http://') && !rawPath.startsWith('https://')) {
    const path = rawPath.startsWith('/') ? rawPath : `/${rawPath}`;
    const legacyUrl = new URL(realm.origin + path);
    if (!legacyUrl.pathname.startsWith('/user_uploads/')) {
      throw new Error(`fetch_attachment only serves /user_uploads/ paths (got ${legacyUrl.pathname})`);
    }
  }
  // Check the resolved URL, not its spelling: protocol-relative paths and
  // WHATWG backslash/host normalization can change the origin during parsing.
  const url = new URL(rawPath, realm.origin + '/');
  if (url.username || url.password) throw new Error('fetch_attachment URLs must not contain credentials');
  if (url.origin !== realm.origin) {
    if (url.host !== realm.host) {
      throw new Error(`refusing to fetch from foreign host ${url.host}; expected ${realm.host}`);
    }
    throw new Error(`refusing to fetch over ${url.protocol.replace(/:$/, "")}; the realm is ${realm.protocol.replace(/:$/, "")}`);
  }
  if (!url.pathname.startsWith("/user_uploads/") || /%2f|%5c/i.test(url.pathname)) {
    throw new Error(`fetch_attachment only serves /user_uploads/ paths (got ${url.pathname})`);
  }
  return url;
}

export interface AttachmentRef {
  path: string;
  name: string;
  mimeType: string;
  isImage: boolean;
}

const IMAGE_EXT_MIME: Record<string, string> = {
  png: 'image/png',
  jpg: 'image/jpeg',
  jpeg: 'image/jpeg',
  gif: 'image/gif',
  webp: 'image/webp',
};

const NONIMAGE_EXT_MIME: Record<string, string> = {
  pdf: 'application/pdf',
  txt: 'text/plain',
  md: 'text/markdown',
  json: 'application/json',
  csv: 'text/csv',
};

export function classifyExtension(name: string): { mimeType: string; isImage: boolean } {
  const ext = name.split('.').pop()?.toLowerCase() ?? '';
  if (IMAGE_EXT_MIME[ext]) return { mimeType: IMAGE_EXT_MIME[ext], isImage: true };
  if (NONIMAGE_EXT_MIME[ext]) return { mimeType: NONIMAGE_EXT_MIME[ext], isImage: false };
  return { mimeType: 'application/octet-stream', isImage: false };
}

// Zulip uploads appear in markdown as `[name](/user_uploads/X/Yy/Zz/name.ext)`
// or inline-image syntax `![name](/user_uploads/...)`.
export function extractZulipAttachments(rawContent: string): AttachmentRef[] {
  const refs: AttachmentRef[] = [];
  const seen = new Set<string>();
  const re = /\/user_uploads\/[^\s)>\]"']+/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(rawContent)) !== null) {
    const path = m[0];
    if (seen.has(path)) continue;
    seen.add(path);
    const name = decodeURIComponent(path.split('/').pop() ?? 'attachment');
    const { mimeType, isImage } = classifyExtension(name);
    refs.push({ path, name, mimeType, isImage });
  }
  return refs;
}

// Only for paths where Zulip returns rendered HTML (fetched without
// `apply_markdown:false`): on raw markdown this strip deletes author-typed
// XML/HTML (#24).
export function cleanContent(html: string): string {
  let content = html;

  content = content.replace(
    /<span class="user-mention"[^>]*data-user-id="(\d+)"[^>]*>@([^<]+)<\/span>/g,
    '@$2 (uid:$1)'
  );

  content = content.replace(
    /<span class="user-mention silent"[^>]*data-user-id="(\d+)"[^>]*>([^<]+)<\/span>/g,
    '$2 (uid:$1)'
  );

  // Zulip renders uploads as `<a href="/user_uploads/...">name</a>` and inline
  // images as the same anchor wrapping an `<img>`.
  content = content.replace(
    /<a [^>]*href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/g,
    (_match, href: string, inner: string) => {
      const hasImg = /<img\b/i.test(inner);
      const text = inner.replace(/<[^>]*>/g, '').trim();
      if (href.startsWith('/user_uploads/')) {
        if (hasImg) return `[image: ${href}]`;
        return text ? `[attachment: ${text} — ${href}]` : `[attachment: ${href}]`;
      }
      if (hasImg) return `[image: ${href}]`;
      return text || href;
    }
  );

  // Bare `<img>` tags (rare in Zulip, but possible via external image previews).
  content = content.replace(
    /<img [^>]*src="([^"]+)"[^>]*>/g,
    '[image: $1]'
  );

  content = content
    .replace(/<p>/gi, '\n')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<[^>]*>/g, '')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .trim();

  return content;
}

/**
 * Not an HTML stripper: raw markdown (`apply_markdown: false`) contains no
 * HTML of Zulip's making, so any `<...>` in it is the author's own text (XML
 * in a fenced block, generics such as `Map<string, T>`, comparisons) (#24).
 */
export function cleanMarkdown(raw: string): string {
  return raw.replace(/\r\n?/g, '\n').trim();
}
