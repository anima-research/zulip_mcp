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

/** Only send_message's scalar private `to` is a documented comma list.
 * Array entries are atomic full-name/email/id queries (names can contain commas). */
export function recipientQueries(to: unknown): string[] {
  return (Array.isArray(to) ? to.map(String) : String(to ?? '').split(','))
    .map(value => value.trim());
}

/** Resolve once; the caller must send this exact destination after authorization.
 * Zulip JSON-decodes stream `to` before interpreting integers as ids and strings
 * as names. In particular, bare "12" is NOT the stream named "12". */
export async function canonicalDestination(client: any, destination: Record<string, any>,
  resolveUserId?: (query: string) => Promise<number>): Promise<Record<string, any>> {
  if (['private', 'direct'].includes(destination.type)) {
    const queries = recipientQueries(destination.to);
    if (!queries.length || queries.some(q => !q)) throw new Error('to must name at least one recipient');
    const ids = await Promise.all(queries.map(async q => resolveUserId ? resolveUserId(q) : /^\d+$/.test(q) ? Number(q) : NaN));
    if (ids.some(id => !positiveId(id))) throw new Error('Cannot resolve DM destination');
    return { ...destination, type: 'private', to: [...new Set(ids)] };
  }
  if (!['stream', 'channel'].includes(destination.type)) throw new Error('Unknown destination type');
  let to = destination.to;
  if (typeof to === 'string') {
    try { to = JSON.parse(to); } catch { /* An unquoted name is also accepted. */ }
  }
  if (Array.isArray(to) && to.length === 1) to = to[0];
  if (typeof to === 'string' && to) {
    let result;
    try { result = await client.callEndpoint('get_stream_id', 'GET', { stream: to }); }
    catch { throw new Error('Cannot resolve stream destination: temporarily unavailable; retry'); }
    if (result?.result !== 'success' || !positiveId(result.stream_id)) throw new Error('Cannot resolve stream destination');
    to = result.stream_id;
  }
  if (!positiveId(to)) throw new Error('Cannot resolve stream destination');
  return { ...destination, type: 'stream', to };
}

function sameConversation(message: any, destination: Record<string, any>, self?: number | null): boolean {
  if (message.type === 'stream') {
    return destination.type === 'stream' && destination.to === message.stream_id &&
      typeof destination.topic === 'string' && destination.topic.toLowerCase() === message.subject.toLowerCase();
  }
  if (message.type !== 'private' || destination.type !== 'private' || !positiveId(self)) return false;
  const ids = destination.to;
  if (!Array.isArray(ids) || ids.some(id => !positiveId(id)) || !Array.isArray(message.display_recipient)) return false;
  const canonical = (values: number[]) => [...new Set(values)].sort((a,b) => a-b).join(',');
  return canonical([...ids, self]) === canonical(message.display_recipient.map((r: any) => r.id));
}

/** Message-wide opt-in to labels, not a Zulip Markdown parser. Reject even
 * escaped/unmatched delimiters and closed fences; their state is irrelevant. */
function canLabelMessage(content: string): boolean {
  return !content.includes('`') && !content.includes('~~~') &&
    // Over-refuse any dollar (including unmatched/escaped math delimiters).
    !content.includes('$') &&
    // Outside exact tokens (including escaped ones), reject brackets and <
    // (time syntax, HTML and autolinks), even when escaped or unmatched.
    !/[\[\]<]/.test(content.replace(/\[\[msg:[0-9]+\]\]/g, '')) &&
    // Over-refuse tabs anywhere and four spaces at any list/quote nesting step.
    // Marker runs need not be valid Markdown; stop only at non-marker text.
    !/\t|^(?:[^\S\r\n]|[-*+>]|\d+[.)])* {4}/m.test(content) &&
    !/^ {0,3}(?:> *)*([*_-])(?:[ \t]*\1){2,}[ \t]*\r?$/m.test(content);
}

function labelled(message: any, url: string): string {
  // Port of Zulip 12.3 topic_link_util.py's get_fallback_markdown_link and
  // escape_invalid_stream_topic_characters. Zulip disables Markdown escapes;
  // replace each match once so generated entities are not escaped again.
  const mapping: Record<string, string> = {
    '`': '&#96;', '>': '&gt;', '*': '&#42;', '&': '&amp;',
    '$$': '&#36;&#36;', '[': '&#91;', ']': '&#93;', '|': '&#124;',
  };
  // Legacy metadata may contain line breaks, even though current Zulip rejects
  // them. Keep generated labels on one line (the URL still encodes the name).
  const escape = (s: string) => s.replace(/[\r\n]+/g, ' ')
    .replace(/[`>*&|[\]]|(\$\$)/g, match => mapping[match]);
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
    if (!allowed) throw fail(`reference is outside the allowed destination audience; escape it as \\${token.text} or use send_message with allow_private_references: true`);
    urls.set(token.id, url);
  }
  const useLabels = canLabelMessage(content);
  let resolved = '', start = 0;
  for (const token of tokens) {
    resolved += content.slice(start, token.start);
    if (token.id === undefined) resolved += token.text;
    else {
      const url = urls.get(token.id)!;
      const replacement = useLabels ? labelled(messages.get(token.id), url) : url;
      resolved += replacement;
      options.links?.push({token: token.text, url});
      options.replacements?.push(replacement);
    }
    start = token.end;
  }
  return resolved + content.slice(start);
}
