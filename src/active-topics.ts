import type { IncomingChannelMessage } from '@animalabs/mcpl-core';
import { assertApiSuccess } from './history.js';
import { headerField } from './message-line.js';

export const DEFAULT_ACTIVE_TOPICS = 6;
const STREAM_CAP = 128;
const TOPIC_CAP = 50;
const FETCH_CAP = 4;
export const DEFAULT_ACTIVE_TOPICS_TTL_MS = 60_000;

interface Topic {
  name: string;
  maxId: number;
  time?: string;
}
interface StreamTopics {
  topics: Topic[];
  /** Live events; reset at refresh start so only concurrent events are merged. */
  observed: Topic[];
  ready: boolean;
  pending: boolean;
  refreshAfter: number;
  shownAt?: number;
  shownList?: string;
}

/** Best-effort context only: no fetch is ever awaited on the delivery path.
 * LRU bounds stream memory; topic and in-flight caps also bound a stuck API.
 * Each refresh costs one topics GET: max_id supplies ordering; timestamps
 * are optional and come only from live events, never extra message GETs. */
export class ActiveTopicsCache {
  private readonly streams = new Map<number, StreamTopics>();
  private readonly limit: number;
  private fetching = 0;
  private cooldownUntil = 0;
  private readonly ttlMs: number;

  constructor(
    private readonly client: any,
    limit = DEFAULT_ACTIVE_TOPICS,
    private readonly now: () => number = Date.now,
    ttlMs = DEFAULT_ACTIVE_TOPICS_TTL_MS,
  ) {
    this.ttlMs = Number.isFinite(ttlMs) && ttlMs > 0 ? ttlMs : DEFAULT_ACTIVE_TOPICS_TTL_MS;
    this.limit = Number.isFinite(limit) ? Math.max(0, Math.min(TOPIC_CAP, Math.floor(limit))) : DEFAULT_ACTIVE_TOPICS;
  }

  private recent(topics: Topic[]): Topic[] {
    const byName = new Map<string, Topic>();
    for (const topic of topics) {
      const key = topic.name.toLowerCase();
      const old = byName.get(key);
      if (!old || topic.maxId >= old.maxId) byName.set(key, topic);
    }
    return [...byName.values()].sort((a, b) => b.maxId - a.maxId).slice(0, this.limit);
  }

  private entry(streamId: number): StreamTopics {
    let entry = this.streams.get(streamId);
    if (!entry) entry = { topics: [], observed: [], ready: false, pending: false, refreshAfter: 0 };
    this.streams.delete(streamId);
    this.streams.set(streamId, entry);
    if (this.streams.size > STREAM_CAP) this.streams.delete(this.streams.keys().next().value!);
    return entry;
  }

  /** Includes the bot's own stream events, even though those aren't delivered. */
  observe(streamId: number, name: string, maxId: number, time: string): void {
    if (!this.limit || !Number.isSafeInteger(streamId) || streamId <= 0) return;
    const entry = this.entry(streamId);
    const topic = { name, maxId, time };
    entry.observed = this.recent([...entry.observed, topic]);
    if (entry.ready) entry.topics = this.recent([...entry.topics, topic]);
  }

  /** Drop both warm lists and pending snapshots. No API work on change events.
   * Without a source stream id, conservatively invalidate all bounded entries. */
  invalidate(streamId: number | null): void {
    for (const [id, old] of this.streams) {
      if (streamId !== null && streamId !== id) continue;
      // Keep display history: invalidation alone is not a changed list.
      this.streams.set(id, { topics: [], observed: [], ready: false, pending: false,
        refreshAfter: 0, shownAt: old.shownAt, shownList: old.shownList });
    }
  }

  private noteRateLimit(value: any): void {
    const response = value?.response;
    const body = response?.data ?? value;
    const retryAfter = body?.retry_after ?? value?.retry_after;
    const seconds = typeof retryAfter === 'number' || typeof retryAfter === 'string' ? Number(retryAfter) : NaN;
    if ((Number.isFinite(seconds) && seconds >= 0) || value?.status === 429 || value?.statusCode === 429 ||
        response?.status === 429 || body?.code === 'RATE_LIMIT_HIT') {
      this.cooldownUntil = Math.max(this.cooldownUntil,
        this.now() + (Number.isFinite(seconds) && seconds >= 0 ? Math.max(1, seconds * 1000) : this.ttlMs));
    }
  }

  private refresh(streamId: number, entry: StreamTopics): void {
    if (entry.pending || this.fetching >= FETCH_CAP || this.now() < Math.max(entry.refreshAfter, this.cooldownUntil)) return;
    entry.pending = true;
    this.fetching++;
    const snapshot = this.recent([...entry.topics, ...entry.observed]);
    entry.observed = [];
    // Deferral matters: even a synchronously throwing client cannot affect
    // delivery. No timers or queued work accumulate behind hung requests.
    void Promise.resolve().then(async () => {
      if (this.now() < this.cooldownUntil || this.streams.get(streamId) !== entry) return;
      const result = await this.client.streams.topics.retrieve({ stream_id: streamId });
      this.noteRateLimit(result);
      assertApiSuccess(result, 'active topics');
      if (!Array.isArray(result?.topics)) throw new Error('missing topics');
      const topics: Topic[] = result.topics.filter((t: any) => typeof t?.name === 'string' && Number.isSafeInteger(t.max_id) && t.max_id > 0)
        .sort((a: any, b: any) => b.max_id - a.max_id).slice(0, this.limit)
        .map((row: any) => ({ name: row.name, maxId: row.max_id,
          time: snapshot.find(t => t.maxId === row.max_id && t.name.toLowerCase() === row.name.toLowerCase())?.time }));
      if (this.streams.get(streamId) !== entry) return; // evicted/invalidated: never resurrect it
      entry.topics = this.recent([...topics, ...entry.observed]);
      entry.observed = [];
      entry.ready = true;
    }).catch(error => {
      this.noteRateLimit(error);
      // Optional context: no tool error, log, or agent-facing failure. A warm
      // list stays usable, and a cold failure retries only after the TTL.
    }).finally(() => {
      entry.pending = false;
      entry.refreshAfter = this.now() + this.ttlMs;
      this.fetching--;
    });
  }

  decorate(message: IncomingChannelMessage, streamId: number | undefined): IncomingChannelMessage {
    const meta = message.metadata as Record<string, unknown> | undefined;
    if (!this.limit || meta?.isDM === true || streamId === undefined || !Number.isSafeInteger(streamId) || streamId <= 0) return message;
    const topic = typeof meta?.topic === 'string' ? meta.topic : message.threadId ?? '';
    this.observe(streamId, topic, Number(message.messageId), message.timestamp);
    const entry = this.entry(streamId);
    this.refresh(streamId, entry);
    if (!entry.ready || entry.topics.length === 0) return message;
    const activeTopics = entry.topics.map(t => ({
      name: headerField(t.name), ...(t.time ? { lastMessageTime: t.time } : {}), current: t.name.toLowerCase() === topic.toLowerCase(),
    }));
    const decorated = { ...message, metadata: { ...meta, activeTopics } };
    const list = JSON.stringify(activeTopics.map(t => t.name));
    if (entry.shownList === list && entry.shownAt !== undefined && this.now() - entry.shownAt < this.ttlMs) return decorated;
    entry.shownList = list;
    entry.shownAt = this.now();
    const text = '[active topics] ' + activeTopics.map(t => {
      const chars = Array.from(t.name);
      const name = JSON.stringify(chars.length > 60 ? chars.slice(0, 59).join('') + '…' : t.name);
      const label = `${t.current ? '*' : ''}${name}`;
      if (!t.lastMessageTime) return label;
      const seconds = Math.max(0, Math.floor((this.now() - Date.parse(t.lastMessageTime)) / 1000));
      const age = seconds < 60 ? `${seconds}s` : seconds < 3600 ? `${Math.floor(seconds / 60)}m` : seconds < 86400 ? `${Math.floor(seconds / 3600)}h` : `${Math.floor(seconds / 86400)}d`;
      return `${label} (${age})`;
    }).join('; ');
    return { ...decorated, content: [message.content[0], { type: 'text', text }, ...message.content.slice(1)] };
  }
}
