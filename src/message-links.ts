/** Exact opt-in message references; never infer links from other text. */
import type { ZulipSession } from './zulip-client.js';

const MAX_TOKENS = 20;
interface Token { start: number; end: number; text: string; id?: number }

/** Zulip web/src/internal_url.ts encodeHashComponent (not ordinary URI encoding). */
function hashEncode(value: string): string {
  return encodeURIComponent(value).replace(/[%!'()*.]/g, (char) =>
    char === '%' ? '.' : `.${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

/** Scan every token, independent of Markdown; only an immediate backslash escapes it. */
function tokensIn(content: string): Token[] {
  const tokens: Token[] = [];
  const syntax = /\\?\[\[msg:([0-9]+)\]\]/g;
  let count = 0;
  for (let match; (match = syntax.exec(content));) {
    const escaped = match[0].startsWith('\\');
    const text = escaped ? match[0].slice(1) : match[0];
    let id: number | undefined;
    if (!escaped) {
      if (++count > MAX_TOKENS) throw new Error(`At most ${MAX_TOKENS} message-link tokens are allowed per call (at ${text})`);
      id = Number(match[1]);
      if (!Number.isSafeInteger(id) || id <= 0) throw new Error(`Cannot resolve ${text}: message id must be a positive safe integer`);
    }
    tokens.push({ start: match.index, end: syntax.lastIndex, text, id });
  }
  return tokens;
}

function positiveId(id: unknown): id is number {
  return typeof id === 'number' && Number.isSafeInteger(id) && id > 0;
}

function messageUrl(realm: string, id: number, message: any): string {
  if (!message || message.id !== id) throw new Error('Zulip returned no matching readable message');
  const base = new URL(realm);
  if (!['https:', 'http:'].includes(base.protocol) || base.search || base.hash) throw new Error('Invalid Zulip realm URL');
  let narrow: string;
  if (message.type === 'stream') {
    if (!positiveId(message.stream_id) || typeof message.display_recipient !== 'string' || !message.display_recipient || typeof message.subject !== 'string') {
      throw new Error('Zulip returned no readable stream/topic');
    }
    const slug = `${message.stream_id}-${message.display_recipient.replace(/ /g, '-')}`;
    const encoded = hashEncode(slug);
    // Zulip internal_url.ts encode_slug measures the entire id-name operand.
    const operand = encoded.length - slug.length > 10 || encoded.length > slug.length * 1.5
      ? String(message.stream_id) : encoded;
    narrow = `channel/${operand}/topic/${hashEncode(message.subject)}`;
  } else if (message.type === 'private') {
    const recipients = message.display_recipient;
    if (!Array.isArray(recipients) || !recipients.length || recipients.some((r) => !positiveId(r?.id))) {
      throw new Error('Zulip returned no readable DM participants');
    }
    // Copy-message permalinks include everyone, including the bot, so the
    // link addresses the same conversation for other readers too.
    const ids = [...new Set<number>(recipients.map((r) => r.id))].sort((a, b) => a - b);
    narrow = `dm/${ids.join(',')}-${ids.length >= 3 ? 'group' : 'dm'}`;
  } else throw new Error('Zulip returned an unknown message type');
  return `${realm.replace(/\/+$/, '')}/#narrow/${narrow}/near/${id}`;
}

export interface MessageLink { token: string; url: string }
export type MessageMemo = Map<number, Promise<any>>;
export interface LinkOptions {
  destination?: Record<string, any> | (() => Promise<Record<string, any>>);
  allowPrivateReferences?: boolean;
  memo?: MessageMemo;
  links?: MessageLink[];
  /** Exact rendered replacements, protected from send chunk boundaries. */
  replacements?: string[];
  selfUserId?: number | null;
}

/** Lookup diagnostics never include API bodies, request URLs or client exception text. */
export function lookupError(code?: unknown): string {
  if (code === 'RATE_LIMIT_HIT' || code === 'INTERNAL_SERVER_ERROR' || code === 'TRANSPORT_ERROR') {
    return `temporarily unavailable (${code}); retry`;
  }
  return 'message not found or not accessible';
}

export function readMessage(client: any, id: number, memo: MessageMemo): Promise<any> {
  if (!memo.has(id)) memo.set(id, (async () => {
    let result;
    try {
      result = await client.messages.getById({ message_id: id, allow_empty_topic_name: true });
    } catch {
      // Client exceptions can include response-body bytes and request URLs.
      throw new Error(lookupError('TRANSPORT_ERROR'));
    }
    if (result?.result !== 'success') throw new Error(lookupError(result?.code));
    if (!result.message) throw new Error(lookupError());
    return result.message;
  })());
  return memo.get(id)!;
}

function sameConversation(message: any, destination: Record<string, any>, self?: number | null): boolean {
  if (message.type === 'stream') {
    return ['stream', 'channel'].includes(destination.type) &&
      (destination.to === message.stream_id || destination.to === message.display_recipient) &&
      destination.topic === message.subject;
  }
  if (message.type !== 'private' || !['private', 'direct'].includes(destination.type) || !positiveId(self)) return false;
  const to = Array.isArray(destination.to) ? destination.to : [destination.to];
  const ids = to.map((id: unknown) => typeof id === 'string' && /^\d+$/.test(id) ? Number(id) : id);
  if (ids.some((id: unknown) => !positiveId(id)) || !Array.isArray(message.display_recipient)) return false;
  const canonical = (values: number[]) => [...new Set(values)].sort((a,b) => a-b).join(',');
  return canonical([...(ids as number[]), self]) === canonical(message.display_recipient.map((r: any) => r.id));
}

/** Inline backticks and ordinary code fences use bare URLs, not literal Markdown labels.
 * Quote/spoiler fences are rendered prose. Token scanning itself is never disabled. */
function inCode(content: string, offset: number): boolean {
  let fence: string | undefined, inline = 0;
  let start = 0, table = false;
  const lines = content.split('\n');
  for (const [index, line] of lines.entries()) {
    // Only a header + separator at a block boundary establishes table cells.
    // An ordinary pipe (notably inside `a|b`) must not close inline code.
    if (!line.trim()) table = false;
    if (!fence && (index === 0 || !lines[index - 1].trim()) && line.includes('|')) {
      const cells = (row: string) => row.trim().replace(/^\||\|$/g, '').split('|');
      const separator = cells(lines[index + 1] ?? '');
      table = separator.length === cells(line).length &&
        separator.every(cell => /^ *:?-+:? *$/.test(cell));
    }
    if (table) inline = 0; // Table rows render independently.
    const end = start + line.length;
    const marker = /^[ >\t-]*(`{3,}|~{3,})(.*)$/.exec(line);
    if (marker) {
      if (fence && marker[1][0] === fence[0] && marker[1].length >= fence.length) {
        if (offset <= end) return true;
        fence = undefined;
      } else if (!fence && !/^(quote|spoiler)(?:\s|$)/.test(marker[2].trim())) {
        fence = marker[1];
      }
    }
    if (offset <= end) {
      if (fence) return true;
      if (marker && /^(quote|spoiler)(?:\s|$)/.test(marker[2].trim())) return false;
      for (const match of line.slice(0, offset-start).matchAll(/`+|\|/g)) {
        if (match[0] === '|') {
          // A paired code span owns its internal pipes, even in a table.
          if (table && ![...line.slice(match.index! + 1).matchAll(/`+/g)].some(ticks => ticks[0].length === inline)) inline = 0;
        } else if (!inline) inline = match[0].length;
        else if (inline === match[0].length) inline = 0;
      }
      return inline > 0;
    }
    // Inline code may span lines.
    if (!marker && !fence) for (const match of line.matchAll(/`+|\|/g)) {
      if (match[0] === '|') {
        // A paired code span owns its internal pipes, even in a table.
        if (table && ![...line.slice(match.index! + 1).matchAll(/`+/g)].some(ticks => ticks[0].length === inline)) inline = 0;
      } else if (!inline) inline = match[0].length;
      else if (inline === match[0].length) inline = 0;
    }
    start = end + 1;
  }
  return false;
}

function labelled(message: any, url: string): string {
  // Port of Zulip 12.3 topic_link_util.py's get_fallback_markdown_link and
  // escape_invalid_stream_topic_characters. Zulip disables Markdown escapes;
  // replace each match once so generated entities are not escaped again.
  const mapping: Record<string, string> = {
    '`': '&#96;', '>': '&gt;', '*': '&#42;', '&': '&amp;',
    '$$': '&#36;&#36;', '[': '&#91;', ']': '&#93;',
  };
  // Legacy metadata may contain line breaks, even though current Zulip rejects
  // them. Keep generated labels on one line (the URL still encodes the name).
  const escape = (s: string) => s.replace(/[\r\n]+/g, ' ')
    .replace(/[`>*&[\]]|(\$\$)/g, match => mapping[match]);
  const label = message.type === 'stream'
    ? `#${escape(message.display_recipient)} > ${escape(message.subject || 'general chat')} @ 💬` : 'DM @ 💬';
  return `[${label}](${url})`;
}

/** Resolve and authorize every reference before uploads, chunks, sends or edits. */
export async function resolveMessageLinks(content: string, session: Pick<ZulipSession, 'client' | 'realm'>, options: LinkOptions = {}): Promise<string> {
  const tokens = tokensIn(content); // All synchronous checks precede all reads.
  if (!tokens.some(t => t.id !== undefined)) {
    return content.replace(/\\(\[\[msg:[0-9]+\]\])/g, '$1');
  }
  const memo = options.memo ?? new Map();
  const messages = new Map<number, any>();
  for (const token of tokens) {
    if (token.id === undefined || messages.has(token.id)) continue;
    try { messages.set(token.id, await readMessage(session.client, token.id, memo)); }
    catch (error) { throw new Error(`Cannot resolve ${token.text}: ${(error as Error).message}`); }
  }
  const destination = options.allowPrivateReferences === true ? undefined : typeof options.destination === 'function' ? await options.destination() : options.destination;
  const publicStreams = new Map<number, boolean>();
  const urls = new Map<number, string>();
  for (const token of tokens) {
    if (token.id === undefined || urls.has(token.id)) continue;
    const message = messages.get(token.id);
    const fail = (detail: string) => new Error(`Cannot resolve ${token.text}: ${detail}`);
    let url: string;
    try { url = messageUrl(session.realm, token.id, message); }
    catch { throw fail(lookupError()); }
    let allowed = options.allowPrivateReferences === true || !!destination && sameConversation(message, destination, options.selfUserId);
    if (!allowed && message.type === 'stream' && destination && ['stream', 'channel'].includes(destination.type)) {
      if (!publicStreams.has(message.stream_id)) {
        let result;
        try { result = await session.client.callEndpoint(`streams/${message.stream_id}`, 'GET', {}); }
        catch { throw fail(lookupError('TRANSPORT_ERROR')); }
        if (result?.result !== 'success') throw fail(lookupError(result?.code));
        // invite_only is the audience boundary. Web-public/history flags do not
        // widen or narrow this owner-approved rule; unknown metadata fails closed.
        publicStreams.set(message.stream_id, result.stream?.invite_only === false);
      }
      allowed = publicStreams.get(message.stream_id) === true;
    }
    if (!allowed) throw fail(`reference is outside the allowed destination audience; escape it as \\${token.text} or explicitly set allow_private_references: true`);
    urls.set(token.id, url);
  }
  let resolved = '', start = 0;
  for (const token of tokens) {
    resolved += content.slice(start, token.start);
    if (token.id === undefined) resolved += token.text;
    else {
      const url = urls.get(token.id)!;
      const replacement = inCode(content, token.start) ? url : labelled(messages.get(token.id), url);
      resolved += replacement;
      options.links?.push({token: token.text, url});
      options.replacements?.push(replacement);
    }
    start = token.end;
  }
  return resolved + content.slice(start);
}
