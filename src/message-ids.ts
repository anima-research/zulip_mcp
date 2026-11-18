import { ZulipNoAnswer } from './zulip-api.js';

/** Agent boundary only. API calls, routing cursors and disk state remain numeric. */
export type IdMode = 'legacy' | 'compat' | 'strict';
const TAG = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const ID_FIELDS = new Set(['message_id', 'messageId', 'targetMessageId', 'oldest_id', 'newest_id',
  'sinceMessageId', 'talliedThroughMessageId', 'last_message_id', 'latest_message_id', 'last_read_message_id', 'lastReadMessageId', 'lastMessageId',
  'first_message_id', 'first_message_id_allowed_to_move', 'max_id', 'max_message_id', 'min_message_id',
  'latestMessageId', 'beforeMessageId', 'afterMessageId', 'scannedThrough']);
const ID_ARRAYS = new Set(['message_ids', 'messageIds', 'targetMessageIds', 'unverifiedMessageIds', 'unread_message_ids']);

export class MessageIds {
  constructor(readonly tag: string, readonly mode: IdMode = 'legacy') {
    if (!TAG.test(tag) || tag !== tag.trim()) throw new Error('ZULIP_REALM_TAG must match [a-z0-9][a-z0-9_-]{0,63}');
    if (!['legacy', 'compat', 'strict'].includes(mode)) throw new Error('ZULIP_ID_MODE must be legacy, compat or strict');
  }

  static fromEnv(realm: string, env: NodeJS.ProcessEnv = process.env): MessageIds {
    if (!env.ZULIP_ID_MODE || env.ZULIP_ID_MODE === 'legacy') return LEGACY_IDS;
    if (!['compat', 'strict'].includes(env.ZULIP_ID_MODE)) throw new Error('ZULIP_ID_MODE must be legacy, compat or strict');
    if (!env.ZULIP_REALM_TAG) throw new Error('ZULIP_REALM_TAG is required when ZULIP_ID_MODE is compat or strict');
    return new MessageIds(env.ZULIP_REALM_TAG, env.ZULIP_ID_MODE as IdMode);
  }

  format(id: number | string): number | string {
    if (this.mode === 'legacy' || (typeof id !== 'number' && typeof id !== 'string') || !/^[0-9]+$/.test(String(id)) || !Number.isSafeInteger(Number(id)) || Number(id) <= 0) return id;
    return `${this.tag}:${id}`;
  }

  parse(value: unknown, bare?: () => void, field = 'message_id'): number {
    const fail = (reason: string): never => {
      throw new Error(`${field}=${describe(value)}: ${reason}. Nothing was written.`);
    };
    const expected = this.mode === 'strict'
      ? `expected a tagged positive safe integer; use ${this.tag}:<n>`
      : 'expected a positive safe integer or <tag>:<n>';
    let numeric = value;
    let isBare = true;
    if (typeof value === 'string' && value.includes(':')) {
      isBare = false;
      const match = /^([a-z0-9][a-z0-9_-]{0,63}):([0-9]+)$/.exec(value);
      if (!match) return fail(expected);
      if (match[1] !== this.tag) return fail(`tag "${match[1]}" differs from this server's "${this.tag}"; use the matching server`);
      numeric = match[2];
    }
    if ((typeof numeric !== 'number' && (typeof numeric !== 'string' || !/^[0-9]+$/.test(numeric))) ||
        !Number.isSafeInteger(Number(numeric)) || Number(numeric) <= 0) {
      return fail(expected);
    }
    if (isBare && this.mode === 'strict') return fail(`bare ID is refused; use ${this.tag}:<n>`);
    if (isBare) bare?.();
    return Number(numeric);
  }

  /** Known structured message-id fields only; never rewrite user text or channel/user/stream IDs. */
  project<T>(value: T, receipt = false): T {
    if (this.mode === 'legacy') return value;
    const walk = (v: any, key = ''): any => {
      if (v === null || v === undefined || key === 'stream_ids') return v;
      if (ID_FIELDS.has(key) || (receipt && key === 'id')) return this.format(v);
      if (Array.isArray(v)) return (ID_ARRAYS.has(key) || (receipt && key === 'ids'))
        ? v.map(id => this.format(id)) : v.map(item => walk(item));
      // Project JSON records, not opaque objects with their own serialization.
      if (typeof v === 'object' && Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null) return v;
      if (typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, walk(x, k)]));
      return v;
    };
    return walk(value);
  }
}

export const LEGACY_IDS = new MessageIds('realm');

/** Keep the legacy tool schema byte-identical until the operator opts in. */
export function idToolDefinitions<T>(definitions: T, ids: MessageIds): T {
  if (ids.mode === 'legacy') return definitions;
  const copy = structuredClone(definitions) as any;
  for (const tool of copy) {
    if (tool.name === 'send_message') tool.description += ` In this mode use [[msg:${ids.tag}:123]] for message links; ${ids.mode === 'strict' ? 'bare IDs are refused' : 'bare IDs warn'}.`;
    for (const [key, field] of Object.entries(tool.inputSchema?.properties ?? {}) as [string, any][]) {
      if (key !== 'reply_to' && key !== 'message_id' && !(tool.name === 'fetch_history' && ['before', 'after'].includes(key))) continue;
      field.type = ids.mode === 'strict' ? 'string'
        : [...new Set(['string', ...(Array.isArray(field.type) ? field.type : [field.type])])];
      field.description += ` Use ${ids.tag}:<n>. ${ids.mode === 'compat' ? 'Bare numbers warn.' : 'Bare numbers are refused.'}`;
    }
  }
  return copy;
}

/** Runtime contract identity, not a source-revision or realm-authentication claim. */
export function messageIdServerInfo<T extends { name: string; version: string; _meta?: Record<string, unknown> }>(info: T, ids: MessageIds): T {
  if (ids.mode === 'legacy') return info;
  return { ...info, _meta: { ...info._meta,
    'zulip_mcp/messageIds': { version: 1, mode: ids.mode, realmTag: ids.tag },
  } };
}

export function describe(value: unknown): string {
  try { return JSON.stringify(value) ?? String(value); }
  catch { return typeof value === 'bigint' ? `${value}n` : '<unserializable value>'; }
}

const safeTransport = Symbol('safeTransport');
type TransportFacts = { operation?: 'Upload' | 'Attachment download'; status?: number; timeoutSeconds?: number; original?: unknown; completed?: boolean; refused?: boolean; retryAfterSeconds?: number };
const nativeTransportErrors = new WeakMap<Error, TransportFacts>();
/** Mark failures at the fetch boundary, not by their JavaScript error class. */
export function markTransportError(error: unknown, facts: TransportFacts = {}): unknown {
  if (error instanceof Error) nativeTransportErrors.set(error, facts);
  return error;
}

const loggedDiagnostics = new WeakSet<Error>();

/** Contextual boundaries must not log an error already logged by an inner boundary. */
export function hasLoggedDiagnostic(error: unknown): boolean {
  return error instanceof Error && loggedDiagnostics.has(error);
}

/** Agent-facing transport diagnostics: retain type/metadata, never expose raw transport text.
 * Copies may safely be annotated by callers, in every ID mode.
 */
export function messageIdError(error: unknown, ids: MessageIds, log = true): Error {
  const copy: Error = error instanceof DOMException
    ? Object.defineProperties(new DOMException(error.message, error.name), Object.getOwnPropertyDescriptors(error))
    : error instanceof Error
    ? Object.create(Object.getPrototypeOf(error), Object.getOwnPropertyDescriptors(error))
    : new Error(String(error));
  const nativeTransport = error instanceof Error && nativeTransportErrors.has(error);
  // Callers that already log contextual diagnostics opt out; keep original causes
  // on stderr only, never in the host-visible error copy.
  if (log && error instanceof Error && !(safeTransport in error) && !loggedDiagnostics.has(error)) {
    console.error('[zulip diagnostic]', nativeTransportErrors.get(error)?.original ?? error);
    loggedDiagnostics.add(error);
  }
  if (error instanceof Error && (loggedDiagnostics.has(error) || !log)) loggedDiagnostics.add(copy);
  if ((copy instanceof ZulipNoAnswer || nativeTransport) && !(safeTransport in copy)) {
    if (copy instanceof ZulipNoAnswer) {
      let detail = `Zulip transport failure (${copy.kind}).`;
      if (copy.kind === 'not-json') detail += ' The server returned an unreadable response; contact the host operator if it persists.';
      if (copy.method === 'GET' && copy.kind !== 'no-self-id' && copy.retryAfterSeconds === undefined) detail += ' Retry the read.';
      if (copy.kind === 'no-self-id') detail = "The server doesn't have the bot's own user id yet; it will retry on a later call.";
      if (copy.timeoutSeconds !== undefined) detail += ` Timeout: ${copy.timeoutSeconds} seconds.`;
      if (copy.status !== undefined) detail += ` HTTP ${copy.status}.`;
      if (copy.retryAfterSeconds !== undefined) detail += ` Retry after ${copy.retryAfterSeconds} seconds${copy.method === 'GET' ? ', then retry the read' : ''}.`;
      copy.message = new ZulipNoAnswer(copy.kind, copy.method, copy.path, copy.mayHaveBeenApplied,
        detail, copy.status, copy.retryAfterSeconds, copy.timeoutSeconds).message;
    } else {
      const facts = nativeTransportErrors.get(error as Error);
      const detail = ((facts?.operation === 'Upload' ? facts.refused : facts?.completed)
        ? `${facts?.operation ?? 'Request'} was refused by the server. Check the path, access permissions and server availability; ask the host operator if it persists.`
        : (facts?.completed ? `${facts.operation ?? 'Request'} returned no confirmed usable result. ` : `${facts?.operation ?? 'Transport'} failed before a complete answer was received. `)
        + (facts?.operation === 'Attachment download' ? 'Retry fetch_attachment with the same path.'
          : facts?.operation === 'Upload' ? 'No uploaded path was confirmed for this attempt. Ask the host operator to check Zulip uploads before retrying upload_file; retrying may create a duplicate.'
          : 'Retry reads; verify the outcome before repeating writes.'))
        + (facts?.status !== undefined ? ` HTTP ${facts.status}.` : '')
        + (facts?.timeoutSeconds !== undefined ? ` Timeout: ${facts.timeoutSeconds} seconds.` : '')
        + (facts?.retryAfterSeconds !== undefined ? ` Retry after ${facts.retryAfterSeconds} seconds.` : '');
      Object.defineProperty(copy, 'message', { value: detail, writable: true, configurable: true });
    }
    Object.defineProperty(copy, 'cause', { value: undefined, configurable: true });
    Object.defineProperty(copy, 'stack', { value: `${copy.name}: ${copy.message}`, configurable: true });
    Object.defineProperty(copy, safeTransport, { value: true });
  }
  return copy;
}
