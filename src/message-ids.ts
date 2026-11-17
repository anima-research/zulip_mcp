/** Agent boundary only. API calls, routing cursors and disk state remain numeric. */
export type IdMode = 'legacy' | 'compat' | 'strict';
const TAG = /^[a-z0-9][a-z0-9_-]{0,63}$/;
const ID_FIELDS = new Set(['message_id', 'messageId', 'targetMessageId', 'oldest_id', 'newest_id',
  'sinceMessageId', 'talliedThroughMessageId', 'last_message_id', 'latest_message_id', 'last_read_message_id', 'lastReadMessageId', 'lastMessageId',
  'first_message_id', 'first_message_id_allowed_to_move', 'max_id']);
const ID_ARRAYS = new Set(['message_ids', 'messageIds', 'targetMessageIds']);

export class MessageIds {
  constructor(readonly tag: string, readonly mode: IdMode = 'legacy') {
    if (!TAG.test(tag) || tag !== tag.trim()) throw new Error('ZULIP_REALM_TAG must match [a-z0-9][a-z0-9_-]{0,63}');
    if (!['legacy', 'compat', 'strict'].includes(mode)) throw new Error('ZULIP_ID_MODE must be legacy, compat or strict');
  }

  static fromEnv(realm: string, env: NodeJS.ProcessEnv = process.env): MessageIds {
    if (env.ZULIP_ID_MODE === undefined || env.ZULIP_ID_MODE === 'legacy') return LEGACY_IDS;
    if (!['compat', 'strict'].includes(env.ZULIP_ID_MODE)) throw new Error('ZULIP_ID_MODE must be legacy, compat or strict');
    if (env.ZULIP_REALM_TAG !== undefined) return new MessageIds(env.ZULIP_REALM_TAG, env.ZULIP_ID_MODE as IdMode);
    let url: URL;
    try { url = new URL(realm); }
    catch { throw new Error('Zulip realm (ZULIP_REALM or zuliprc site) must be an absolute URL to infer ZULIP_REALM_TAG'); }
    if (!url.hostname) throw new Error('Zulip realm URL must have a hostname to infer ZULIP_REALM_TAG');
    const label = url.hostname.split('.')[0].toLowerCase()
      .replace(/[^a-z0-9_-]/g, '-').replace(/^[^a-z0-9]+/, '').slice(0, 64) || 'realm';
    return new MessageIds(env.ZULIP_REALM_TAG ?? label, (env.ZULIP_ID_MODE ?? 'legacy') as IdMode);
  }

  format(id: number | string): number | string {
    if (this.mode === 'legacy' || (typeof id !== 'number' && typeof id !== 'string') || !/^[0-9]+$/.test(String(id)) || !Number.isSafeInteger(Number(id)) || Number(id) <= 0) return id;
    return `${this.tag}:${id}`;
  }

  parse(value: unknown, bare?: () => void, field = 'message_id'): number {
    const fail = (reason: string): never => {
      throw new Error(`${field}=${JSON.stringify(value)}: ${reason}. Nothing was written.`);
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
