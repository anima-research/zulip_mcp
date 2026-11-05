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
    narrow = `stream/${operand}/topic/${hashEncode(message.subject)}`;
  } else if (message.type === 'private') {
    const recipients = message.display_recipient;
    if (!Array.isArray(recipients) || !recipients.length || recipients.some((r) => !positiveId(r?.id))) {
      throw new Error('Zulip returned no readable DM participants');
    }
    // Copy-message permalinks include everyone, including the bot, so the
    // link addresses the same conversation for other readers too.
    const ids = [...new Set<number>(recipients.map((r) => r.id))].sort((a, b) => a - b);
    narrow = `pm-with/${ids.join(',')}-${ids.length >= 3 ? 'group' : 'dm'}`;
  } else throw new Error('Zulip returned an unknown message type');
  // Legacy operators are accepted by both old and new Zulip web clients.
  return `${realm.replace(/\/+$/, '')}/#narrow/${narrow}/near/${id}`;
}

/** Resolve everything before the caller uploads, chunks, sends or edits anything. */
export async function resolveMessageLinks(content: string, session: Pick<ZulipSession, 'client' | 'realm'>): Promise<string> {
  const tokens = tokensIn(content);
  if (!tokens.length) return content;
  const links = new Map<number, string>();
  for (const token of tokens) {
    if (token.id === undefined || links.has(token.id)) continue;
    const fail = (description: string) => new Error(`Cannot resolve ${token.text}: ${description}`);
    let result;
    try {
      result = await session.client.messages.getById({ message_id: token.id });
    } catch {
      // Client errors can contain response-body bytes, URLs and credentials.
      // Never forward exception text (or attach the original as a cause).
      throw fail('transport or parse error');
    }
    if (result?.result !== 'success') {
      const safeCodes = new Set(['BAD_REQUEST', 'ACCESS_DENIED', 'UNAUTHORIZED', 'RATE_LIMIT_HIT', 'INTERNAL_SERVER_ERROR']);
      throw fail(safeCodes.has(result?.code) ? `Zulip API error ${result.code}` : 'message not found or not accessible');
    }
    try {
      links.set(token.id, messageUrl(session.realm, token.id, result.message));
    } catch {
      throw fail('message not found or not accessible');
    }
  }
  let resolved = '', start = 0;
  for (const token of tokens) {
    resolved += content.slice(start, token.start) + (token.id === undefined ? token.text : links.get(token.id)!);
    start = token.end;
  }
  return resolved + content.slice(start);
}
