import type { ZulipApi } from '../src/zulip-api.ts';
import { standInApi, type StandInCall } from './zulip-stand-in.ts';

/**
 * Messages carry their stream's current name, as Zulip's `display_recipient`
 * does, and a stream list comes sorted by name, as Zulip 12.4's does.
 */
export interface Realm {
  api: ZulipApi;
  calls: StandInCall[];
  streams: Map<number, string>;
  created: string[];
  subscribed: string[];
  sends: Record<string, unknown>[];
  reads: Record<string, unknown>[];
  staleList: Record<number, string> | null;
  listRefusals: number;
  registrations: number;
  hold(call: 'POST /register' | 'GET /streams' | 'GET /messages' | 'GET /messages/:id'): () => void;
  rename(streamId: number, name: string): void;
  archive(streamId: number): void;
  /** The bot sees a stream again, under the same id, as Zulip tells a client that regains access to one. */
  regain(streamId: number, name: string): void;
  create(name: string): number;
  /** The next poll is refused with BAD_EVENT_QUEUE_ID, as Zulip refuses a poll of a queue it has dropped. */
  gap(during: () => void): void;
  /** `noStreamId`: its event carries the stream's name alone, as some Zulip versions send it. */
  post(streamId: number, content: string, opts?: { topic?: string; mentioned?: boolean; noStreamId?: boolean }): number;
  react(messageId: number, emojiName: string, emojiCode: string): void;
  move(messageId: number, toStreamId: number): void;
  moveTopic(messageIds: number[], topic: string): void;
  edit(messageId: number, content: string, opts?: { mentioned?: boolean; topic?: string }): void;
  remove(messageIds: number[]): void;
}

export const SELF = 790;
const PERSON = { id: 7, full_name: 'Ann', email: 'ann@example.com' };

interface StoredMessage {
  id: number;
  streamId: number;
  topic: string;
  content: string;
  senderId: number;
  mentioned: boolean;
  timestamp: number;
}

export function realm(initial: Record<number, string>): Realm {
  const streams = new Map<number, string>(Object.entries(initial).map(([id, name]) => [Number(id), name]));
  const messages: StoredMessage[] = [];
  const events: Record<string, unknown>[] = [];
  const waiting: (() => void)[] = [];
  const created: string[] = [];
  const subscribed: string[] = [];
  const sends: Record<string, unknown>[] = [];
  const reads: Record<string, unknown>[] = [];
  let nextStreamId = 100;
  let nextMessageId = 1000;
  let nextEventId = 0;
  let queues = 0;
  let dropped = false;

  const queue = (event: Record<string, unknown>) => {
    if (dropped) return;
    events.push({ ...event, id: nextEventId++ });
    for (const wake of waiting.splice(0)) wake();
  };
  const list = () => [...streams].sort((x, y) => (x[1] < y[1] ? -1 : x[1] > y[1] ? 1 : 0)).map(([stream_id, name]) => ({ stream_id, name, invite_only: false, subscriber_count: 2 }));
  const idOf = (name: string) => [...streams].find(([, n]) => n === name)?.[0];
  const raw = (m: StoredMessage) => ({
    id: m.id,
    sender_id: m.senderId,
    sender_full_name: m.senderId === SELF ? 'Bot' : PERSON.full_name,
    sender_email: m.senderId === SELF ? 'bot-bot@example.com' : PERSON.email,
    type: 'stream',
    stream_id: m.streamId,
    display_recipient: streams.get(m.streamId) ?? `(archived ${m.streamId})`,
    subject: m.topic,
    content: m.content,
    timestamp: m.timestamp,
    flags: m.mentioned ? ['mentioned'] : [],
  });
  const store = (streamId: number, content: string, senderId: number, topic: string, mentioned: boolean, noStreamId = false): StoredMessage => {
    const m = { id: nextMessageId++, streamId, topic, content, senderId, mentioned, timestamp: 1_700_000_000 + nextMessageId };
    messages.push(m);
    const { stream_id: _, ...named } = raw(m);
    queue({ type: 'message', message: noStreamId ? named : raw(m), flags: raw(m).flags });
    return m;
  };
  const narrowed = (narrow: unknown[]): { streamId?: number; topic?: string; refused?: string } => {
    const out: { streamId?: number; topic?: string; refused?: string } = {};
    for (const n of narrow) {
      const [operator, operand] = Array.isArray(n) ? n : [(n as { operator: string }).operator, (n as { operand: unknown }).operand];
      if (operator === 'stream' || operator === 'channel') {
        const id = typeof operand === 'number' ? operand : idOf(String(operand));
        if (id === undefined || !streams.has(id)) out.refused = `Invalid narrow operator: unknown channel`;
        else out.streamId = id;
      } else if (operator === 'topic') out.topic = String(operand);
    }
    return out;
  };

  const holds = new Map<string, Promise<void>>();
  const { api, calls } = standInApi({
    'GET /users/me': { result: 'success', user_id: SELF },
    'POST /register': async (p) => {
      await holds.get('POST /register');
      self.registrations++;
      const types = p.event_types as string[] | undefined;
      dropped = false;
      return {
        result: 'success',
        queue_id: `q${++queues}`,
        last_event_id: nextEventId - 1,
        ...(types?.includes('stream') ? { streams: list() } : {}),
      };
    },
    'GET /events': async (p) => {
      const after = Number(p.last_event_id);
      while (!dropped && !events.some((e) => (e.id as number) > after)) await new Promise<void>((r) => waiting.push(r));
      if (dropped) return { result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: `Bad event queue ID: ${String(p.queue_id)}` };
      return { result: 'success', events: events.filter((e) => (e.id as number) > after) };
    },
    'DELETE /events': { result: 'success' },
    'GET /streams': async () => {
      await holds.get('GET /streams');
      if (self.listRefusals > 0) {
        self.listRefusals--;
        return { result: 'error', code: 'BAD_REQUEST', msg: 'the stream list is down' };
      }
      const stale = self.staleList;
      self.staleList = null;
      const answer = stale ? Object.entries(stale).map(([id, name]) => ({ stream_id: Number(id), name, invite_only: false, subscriber_count: 2 })) : list();
      return { result: 'success', streams: answer };
    },
    'GET /messages': async (p) => {
      reads.push(p);
      await holds.get('GET /messages');
      const where = narrowed((p.narrow as unknown[]) ?? []);
      if (where.refused) return { result: 'error', code: 'BAD_NARROW', msg: where.refused };
      const rows = messages
        .filter((m) => where.streamId === undefined || m.streamId === where.streamId)
        .filter((m) => where.topic === undefined || m.topic === where.topic);
      const anchor = p.anchor === 'newest' ? Infinity : Number(p.anchor);
      const before = Number(p.num_before ?? 0);
      const after = Number(p.num_after ?? 0);
      const older = rows.filter((m) => m.id < anchor || (p.include_anchor && m.id === anchor)).slice(-before || rows.length);
      const newer = rows.filter((m) => m.id > anchor).slice(0, after);
      const page = [...(before > 0 ? older : []), ...newer];
      return { result: 'success', messages: page.map(raw), found_newest: newer.length < after || after === 0, found_oldest: true };
    },
    'GET /messages/:id': async (_p, call) => {
      await holds.get('GET /messages/:id');
      const m = messages.find((x) => x.id === call.id);
      return m ? { result: 'success', message: raw(m) } : { result: 'error', code: 'BAD_REQUEST', msg: 'Invalid message(s)' };
    },
    'POST /messages': (p) => {
      sends.push(p);
      if (p.type !== 'stream' && p.type !== 'channel') return { result: 'success', id: nextMessageId++ };
      const id = typeof p.to === 'number' ? p.to : idOf(String(p.to));
      if (id === undefined || !streams.has(id)) {
        return { result: 'error', code: 'STREAM_DOES_NOT_EXIST', msg: `Channel '${String(p.to)}' does not exist`, stream: String(p.to) };
      }
      return { result: 'success', id: store(id, String(p.content), SELF, String(p.topic ?? ''), false).id };
    },
    'POST /users/me/subscriptions': (p) => {
      const names = (p.subscriptions as { name: string }[]).map((s) => s.name);
      subscribed.push(...names);
      for (const name of names) {
        if (idOf(name) !== undefined) continue;
        created.push(name);
        const id = nextStreamId++;
        streams.set(id, name);
        queue({ type: 'stream', op: 'create', streams: [{ stream_id: id, name }] });
      }
      return { result: 'success', subscribed: {}, already_subscribed: {} };
    },
    'POST /typing': { result: 'success' },
    'POST /messages/:id/reactions': { result: 'success' },
  });

  const self: Realm = {
    api,
    calls,
    streams,
    created,
    subscribed,
    sends,
    reads,
    staleList: null,
    listRefusals: 0,
    registrations: 0,
    hold(call) {
      let release!: () => void;
      holds.set(call, new Promise<void>((resolve) => { release = resolve; }));
      return () => { holds.delete(call); release(); };
    },
    rename(streamId, name) {
      streams.set(streamId, name);
      queue({ type: 'stream', op: 'update', stream_id: streamId, name, property: 'name', value: name });
    },
    archive(streamId) {
      streams.delete(streamId);
      queue({ type: 'stream', op: 'delete', stream_ids: [streamId], streams: [{ stream_id: streamId }] });
    },
    regain(streamId, name) {
      streams.set(streamId, name);
      queue({ type: 'stream', op: 'create', streams: [{ stream_id: streamId, name }] });
    },
    create(name) {
      const id = nextStreamId++;
      streams.set(id, name);
      queue({ type: 'stream', op: 'create', streams: [{ stream_id: id, name }] });
      return id;
    },
    gap(during) {
      dropped = true;
      during();
      for (const wake of waiting.splice(0)) wake();
    },
    post(streamId, content, opts = {}) {
      return store(streamId, content, PERSON.id, opts.topic ?? 'deploys', opts.mentioned ?? false, opts.noStreamId).id;
    },
    react(messageId, emojiName, emojiCode) {
      queue({ type: 'reaction', op: 'add', message_id: messageId, emoji_name: emojiName, emoji_code: emojiCode, reaction_type: 'unicode_emoji', user_id: PERSON.id, user: { user_id: PERSON.id, full_name: PERSON.full_name, email: PERSON.email } });
    },
    move(messageId, toStreamId) {
      const m = messages.find((x) => x.id === messageId)!;
      const from = m.streamId;
      m.streamId = toStreamId;
      queue({ type: 'update_message', message_id: messageId, message_ids: [messageId], user_id: PERSON.id, edit_timestamp: m.timestamp + 60, orig_subject: m.topic, stream_id: from, new_stream_id: toStreamId, flags: [] });
    },
    moveTopic(messageIds, topic) {
      const moved = messages.filter((x) => messageIds.includes(x.id));
      const from = moved[0].topic;
      for (const m of moved) m.topic = topic;
      queue({ type: 'update_message', message_id: messageIds[0], message_ids: messageIds, user_id: PERSON.id, edit_timestamp: moved[0].timestamp + 60, orig_subject: from, subject: topic, stream_id: moved[0].streamId, flags: [] });
    },
    edit(messageId, content, opts = {}) {
      const m = messages.find((x) => x.id === messageId)!;
      const before = { content: m.content, topic: m.topic };
      m.content = content;
      m.mentioned = opts.mentioned ?? m.mentioned;
      m.topic = opts.topic ?? m.topic;
      queue({
        type: 'update_message', message_id: messageId, message_ids: [messageId], user_id: PERSON.id, edit_timestamp: m.timestamp + 60,
        orig_content: before.content, content, ...(opts.topic === undefined ? {} : { orig_subject: before.topic, subject: opts.topic }),
        stream_id: m.streamId, flags: m.mentioned ? ['mentioned'] : [],
      });
    },
    remove(messageIds) {
      const gone = messages.filter((x) => messageIds.includes(x.id));
      for (const m of gone) messages.splice(messages.indexOf(m), 1);
      queue({ type: 'delete_message', message_ids: messageIds, message_type: 'stream', stream_id: gone[0].streamId, topic: gone[0].topic });
    },
  };
  return self;
}
