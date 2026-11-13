import { existsSync, mkdirSync, readFileSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { TextContent } from '@animalabs/mcpl-core';
import { dmKey, isDmKey, parseKey, streamKey, type ChannelKey, type KeyedMessage } from './channel-key.js';
import { dmChannelIdFor, parseDmChannelId } from './history.js';
import type { DirectorySnapshot, StreamDirectory } from './stream-directory.js';
import { messageLineHead } from './message-line.js';

export interface MissedTally {
  anchorId: number;
  talliedThrough: number;
  messages: number;
  characters: number;
}

interface DeliveryFile {
  watermarks?: Record<string, number>;
  missed?: Record<string, Partial<MissedTally>>;
  lastOpen?: string[];
  streams?: Record<string, string>;
  formerNames?: Record<string, number>;
}

interface Unresolved {
  watermark?: number;
  missed?: MissedTally;
  lastOpen?: boolean;
}

export type HeldStatus = 'queued' | 'failed' | 'rejected';

interface Held {
  status: HeldStatus;
  replayed: boolean;
  replyVerified: boolean;
}

const HELD_CAP_PER_CHANNEL = 50_000;

export class DeliveryState {
  writes = true;
  private unreadableFile = false;
  private copy: DirectorySnapshot | null = null;
  private watermarks = new Map<ChannelKey, number>();
  private missed = new Map<ChannelKey, MissedTally>();
  private lastOpen = new Set<ChannelKey>();
  private unresolved = new Map<string, Unresolved>();
  private held = new Map<ChannelKey, Map<number, Held>>();
  private reached = new Map<ChannelKey, number>();

  constructor(
    private readonly stateDir: string | null,
    private readonly sessionId: string,
    private readonly directory: StreamDirectory,
  ) {
    this.load();
    if (this.unresolved.size > 0) void this.directory.whenFilled().then(() => this.resolveByName());
  }

  private file(): string | null {
    return this.stateDir ? join(this.stateDir, `${this.sessionId}.delivery.json`) : null;
  }

  private load(): void {
    const path = this.file();
    if (!path || !existsSync(path)) return;
    try {
      const parsed = JSON.parse(readFileSync(path, 'utf-8')) as DeliveryFile;
      const saved = parsed.streams && typeof parsed.streams === 'object' ? parsed.streams : null;
      if (saved) {
        this.directory.load({ streams: saved, formerNames: parsed.formerNames ?? {} });
        this.copy = this.directory.snapshot();
      }
      const idBySavedName = new Map<string, number>();
      for (const [id, name] of Object.entries(saved ?? {})) if (typeof name === 'string') idBySavedName.set(name, Number(id));
      const keyOf = (chan: string): ChannelKey | null => {
        const dm = parseDmChannelId(chan);
        if (dm) return dmKey(dm);
        if (!chan.startsWith('zulip:')) return null;
        const id = idBySavedName.get(chan.slice('zulip:'.length));
        return id !== undefined && Number.isSafeInteger(id) && id > 0 ? streamKey(id) : null;
      };
      const place = (chan: string, apply: (key: ChannelKey) => void, hold: (u: Unresolved) => void): void => {
        const key = keyOf(chan);
        if (key !== null) return apply(key);
        if (saved || !chan.startsWith('zulip:')) {
          console.error(`[zulip-mcp] delivery state: dropping ${chan}, whose stream the file's own stream list does not hold`);
          return;
        }
        const name = chan.slice('zulip:'.length);
        const u = this.unresolved.get(name) ?? {};
        hold(u);
        this.unresolved.set(name, u);
      };
      for (const [chan, id] of Object.entries(parsed.watermarks ?? {})) {
        if (typeof id !== 'number' || !Number.isFinite(id) || id <= 0) continue;
        place(chan, (key) => this.watermarks.set(key, id), (u) => { u.watermark = id; });
      }
      for (const [chan, t] of Object.entries(parsed.missed ?? {})) {
        const tally: MissedTally = {
          anchorId: num(t.anchorId),
          talliedThrough: num(t.talliedThrough ?? t.anchorId),
          messages: num(t.messages),
          characters: num(t.characters),
        };
        place(chan, (key) => this.missed.set(key, tally), (u) => { u.missed = tally; });
      }
      for (const chan of parsed.lastOpen ?? []) {
        if (typeof chan !== 'string') continue;
        place(chan, (key) => this.lastOpen.add(key), (u) => { u.lastOpen = true; });
      }
    } catch (err) {
      this.unreadableFile = true;
      console.error('[zulip-mcp] Failed to load delivery state:', (err as Error).message);
    }
  }

  private resolveByName(): void {
    for (const [name, u] of this.unresolved) {
      const id = this.directory.idOf(name);
      if (id === undefined) {
        console.error(`[zulip-mcp] delivery state: dropping zulip:${name}: no Zulip stream has that name`);
        continue;
      }
      const key = streamKey(id);
      if (u.watermark !== undefined && !this.watermarks.has(key)) this.watermarks.set(key, u.watermark);
      if (u.missed && !this.missed.has(key)) this.missed.set(key, u.missed);
      if (u.lastOpen) this.lastOpen.add(key);
    }
    this.unresolved.clear();
    this.save();
  }

  directoryCopy(): DirectorySnapshot | null {
    return this.copy;
  }

  private fileKey(key: ChannelKey): string | null {
    const parsed = parseKey(key);
    if (parsed.kind === 'dm') return dmChannelIdFor(parsed.userIds);
    const name = this.directory.nameOf(parsed.streamId);
    return name === undefined ? null : `zulip:${name}`;
  }

  save(): void {
    const path = this.file();
    if (!path || !this.writes) return;
    try {
      mkdirSync(this.stateDir!, { recursive: true });
      const entries = <T>(map: Map<ChannelKey, T>): [string, T][] => {
        const out: [string, T][] = [];
        for (const [key, value] of map) {
          const chan = this.fileKey(key);
          if (chan !== null) out.push([chan, value]);
        }
        for (const [name, u] of this.unresolved) {
          const value = map === (this.watermarks as Map<ChannelKey, unknown>) ? u.watermark : map === (this.missed as Map<ChannelKey, unknown>) ? u.missed : undefined;
          if (value !== undefined) out.push([`zulip:${name}`, value as T]);
        }
        return out.sort((a, b) => a[0].localeCompare(b[0]));
      };
      const lastOpen = [...this.lastOpen].map((key) => this.fileKey(key)).filter((c): c is string => c !== null);
      for (const [name, u] of this.unresolved) if (u.lastOpen) lastOpen.push(`zulip:${name}`);
      const copy = this.copy !== null || this.directory.isFilled ? this.directory.snapshot() : null;
      const out: DeliveryFile = {
        watermarks: Object.fromEntries(entries(this.watermarks)),
        missed: Object.fromEntries(entries(this.missed)),
        lastOpen: lastOpen.sort(),
        ...(copy ?? {}),
      };
      const tmp = `${path}.${process.pid}.tmp`;
      writeFileSync(tmp, JSON.stringify(out, null, 2) + '\n');
      if (this.unreadableFile) {
        const aside = `${path}.unreadable-${new Date().toISOString().replace(/[:.]/g, '-')}`;
        try {
          renameSync(path, aside);
          console.error(`[zulip-mcp] The delivery state file that failed to load is moved aside to ${aside}.`);
        } catch (err) {
          if ((err as NodeJS.ErrnoException).code !== 'ENOENT') {
            rmSync(tmp, { force: true });
            console.error(`[zulip-mcp] Failed to save delivery state: the file that failed to load could not be moved aside: ${(err as Error).message}`);
            return;
          }
        }
        this.unreadableFile = false;
      }
      renameSync(tmp, path);
      if (copy) this.copy = copy;
    } catch (err) {
      console.error('[zulip-mcp] Failed to save delivery state:', (err as Error).message);
    }
  }

  watermark(key: ChannelKey): number | undefined {
    return this.watermarks.get(key);
  }

  advance(key: ChannelKey, messageId: number): boolean {
    const reached = Math.max(this.reached.get(key) ?? 0, this.watermarks.get(key) ?? 0, messageId);
    this.reached.set(key, reached);
    return this.settle(key);
  }

  advanceThrough(key: ChannelKey, messageId: number, afterId = 0): boolean {
    const held = this.held.get(key);
    if (held) {
      for (const id of held.keys()) if (id > afterId && id <= messageId) held.delete(id);
      if (held.size === 0) this.held.delete(key);
    }
    return this.advance(key, messageId);
  }

  acceptedThrough(key: ChannelKey): number | undefined {
    return this.reached.get(key) ?? this.watermarks.get(key);
  }

  restartReached(): void {
    this.reached = new Map(this.watermarks);
  }

  private settle(key: ChannelKey): boolean {
    const reached = this.reached.get(key);
    if (reached === undefined) return false;
    const floor = this.floor(key);
    const target = floor === undefined ? reached : Math.min(reached, floor - 1);
    const current = this.watermarks.get(key) ?? 0;
    if (target <= current) return false;
    this.watermarks.set(key, target);
    return true;
  }

  watermarkedChannels(): ChannelKey[] {
    return [...this.watermarks.keys()];
  }

  hold(key: ChannelKey, messageId: number, status: HeldStatus = 'queued'): void {
    if (!Number.isFinite(messageId) || messageId <= 0) return;
    if (messageId <= (this.watermarks.get(key) ?? 0)) return;
    let held = this.held.get(key);
    if (!held) {
      held = new Map();
      this.held.set(key, held);
    }
    const existing = held.get(messageId);
    if (existing) {
      existing.status = status;
    } else if (held.size >= HELD_CAP_PER_CHANNEL && messageId >= Math.max(...held.keys())) {
      console.error(`[zulip-mcp] ${this.fileKey(key) ?? key}: undelivered floor holds ${held.size} ids; not tracking ${messageId}`);
      return;
    } else {
      if (held.size >= HELD_CAP_PER_CHANNEL) held.delete(Math.max(...held.keys()));
      held.set(messageId, { status, replayed: false, replyVerified: false });
    }
    if (status !== 'queued' && this.watermarkBelowHolds(key)) this.save();
  }

  watermarkBelowHolds(key: ChannelKey): boolean {
    if (this.watermarks.has(key)) return false;
    const floor = this.floor(key);
    if (floor === undefined) return false;
    this.watermarks.set(key, floor - 1);
    return true;
  }

  release(key: ChannelKey, messageId: number): boolean {
    const held = this.held.get(key);
    if (!held?.delete(messageId)) return false;
    if (held.size === 0) this.held.delete(key);
    return this.settle(key);
  }

  refuse(key: ChannelKey, afterId: number, messageId: number): void {
    const held = this.held.get(key);
    if (!held) return;
    for (const [id, h] of held) if (id > afterId && id <= messageId) h.status = 'rejected';
    if (this.watermarkBelowHolds(key)) this.save();
  }

  failQueued(): void {
    for (const [key, held] of this.held) {
      let failed = false;
      for (const h of held.values()) {
        if (h.status !== 'queued') continue;
        h.status = 'failed';
        failed = true;
      }
      if (failed) this.watermarkBelowHolds(key);
    }
  }

  releaseAll(key: ChannelKey): boolean {
    if (!this.held.delete(key)) return false;
    return this.settle(key);
  }

  floor(key: ChannelKey): number | undefined {
    const held = this.held.get(key);
    if (!held || held.size === 0) return undefined;
    return Math.min(...held.keys());
  }

  heldChannels(): ChannelKey[] {
    return [...this.held.keys()];
  }

  heldIds(key: ChannelKey, opts: { replayable?: boolean } = {}): number[] {
    const held = this.held.get(key);
    if (!held) return [];
    return [...held.entries()]
      .filter(([, h]) => !opts.replayable || (h.status !== 'queued' && !h.replayed))
      .map(([id]) => id)
      .sort((a, b) => a - b);
  }

  markReplayed(key: ChannelKey, ids: number[]): void {
    const held = this.held.get(key);
    if (!held) return;
    for (const id of ids) {
      const h = held.get(id);
      if (h) h.replayed = true;
    }
  }

  verifiedReply(key: ChannelKey, messageId: number): boolean {
    return this.held.get(key)?.get(messageId)?.replyVerified === true;
  }

  verifyReply(key: ChannelKey, messageId: number): void {
    const h = this.held.get(key)?.get(messageId);
    if (h) h.replyVerified = true;
  }

  forgetReply(key: ChannelKey, messageId: number): void {
    const h = this.held.get(key)?.get(messageId);
    if (h) h.replyVerified = false;
  }

  clearReplayed(key: ChannelKey, ids?: number[]): void {
    const held = this.held.get(key);
    if (!held) return;
    for (const h of ids ? ids.map((id) => held.get(id)) : held.values()) if (h) h.replayed = false;
  }

  wasOpen(key: ChannelKey): boolean {
    return this.lastOpen.has(key);
  }

  lastOpenChannels(): ChannelKey[] {
    return [...this.lastOpen];
  }

  markOpen(key: ChannelKey): void {
    this.lastOpen.add(key);
    this.missed.delete(key);
  }

  markClosed(key: ChannelKey): void {
    if (!this.lastOpen.delete(key)) return;
    const anchor = this.watermarks.get(key) ?? 0;
    this.missed.set(key, { anchorId: anchor, talliedThrough: anchor, messages: 0, characters: 0 });
  }

  tally(key: ChannelKey): MissedTally | undefined {
    return this.missed.get(key);
  }

  talliedChannels(): ChannelKey[] {
    return [...this.missed.keys()];
  }

  countMissed(key: ChannelKey, m: { id: number; text: string }): boolean {
    const t = this.missed.get(key);
    if (!t || m.id <= t.talliedThrough) return false;
    t.messages += 1;
    t.characters += m.text.length;
    t.talliedThrough = m.id;
    return true;
  }

  backfillMissed(key: ChannelKey, ambient: { id: number; text: string }[], scannedThrough: number): void {
    const t = this.missed.get(key);
    if (!t) return;
    t.messages += ambient.length;
    t.characters += ambient.reduce((n, m) => n + m.text.length, 0);
    if (scannedThrough > t.talliedThrough) t.talliedThrough = scannedThrough;
  }
}

export type DeliveryReader = Pick<
  DeliveryState,
  | 'writes'
  | 'save'
  | 'directoryCopy'
  | 'watermark'
  | 'acceptedThrough'
  | 'watermarkedChannels'
  | 'floor'
  | 'heldChannels'
  | 'heldIds'
  | 'verifiedReply'
  | 'wasOpen'
  | 'lastOpenChannels'
  | 'tally'
  | 'talliedChannels'
>;

function num(v: unknown): number {
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}

export interface MissedView {
  id: number;
  mentioned: boolean;
  replyToSelf?: boolean;
  isDM?: boolean;
  topic: string;
  authorName: string;
  text: string;
  timestamp: Date;
  attachmentNames: string[];
}

export function viewOf(m: KeyedMessage): MissedView {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  const attachments = Array.isArray(meta.attachments) ? (meta.attachments as { name?: string }[]) : [];
  const text = m.content
    .filter((c): c is TextContent => c.type === 'text')
    .map((c) => c.text)
    .filter((t) => !t.startsWith('[attachments:'))
    .join('\n');
  return {
    id: Number(m.messageId),
    mentioned: meta.mentioned === true,
    replyToSelf: meta.replyToSelf === true,
    isDM: meta.isDM === true,
    topic: typeof meta.topic === 'string' ? meta.topic : (m.threadId ?? ''),
    authorName: m.author.name,
    text,
    timestamp: new Date(m.timestamp),
    attachmentNames: attachments.map((a) => a.name ?? 'attachment'),
  };
}

/**
 * A host that renders only the content blocks shows the model only the body,
 * so the who/where/when goes into it. `metadata.attributionHeader` is the
 * exact prefix added, so a host strategy that renders its own provenance
 * header can skip it, and one that scans message text can strip the prefix
 * first.
 */
export function attributeMessage(m: KeyedMessage, stream: string | null, formatTime: (d: Date) => string): KeyedMessage {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  if (meta.attributed === true) return m;
  const isDM = meta.isDM === true || isDmKey(m.key);
  const at = new Date(m.timestamp);
  const header = messageLineHead({
    id: m.messageId,
    time: Number.isNaN(at.getTime()) ? '' : formatTime(at),
    stream: isDM ? null : (stream ?? ''),
    topic: typeof meta.topic === 'string' ? meta.topic : (m.threadId ?? ''),
    author: m.author.name,
    mentioned: meta.mentioned === true,
  });
  const index = m.content.findIndex((c) => c.type === 'text');
  const content = [...m.content];
  const prefix = index < 0 ? header.trimEnd() : header;
  if (index < 0) content.unshift({ type: 'text', text: prefix });
  else content[index] = { type: 'text', text: prefix + (content[index] as TextContent).text };
  return { ...m, content, metadata: { ...meta, attributed: true, attributionHeader: prefix } };
}

export function isAddressed(m: { mentioned: boolean; replyToSelf?: boolean; isDM?: boolean }): boolean {
  return m.mentioned || m.replyToSelf === true || m.isDM === true;
}

export function selectMissed<T extends { mentioned: boolean; replyToSelf?: boolean; isDM?: boolean }>(
  msgs: T[],
  opts: { keepAll: boolean; vicinity: number },
): T[] {
  if (opts.keepAll) return msgs;
  const keep = new Set<number>();
  for (let i = 0; i < msgs.length; i++) {
    if (!isAddressed(msgs[i])) continue;
    for (let j = Math.max(0, i - opts.vicinity); j <= Math.min(msgs.length - 1, i + opts.vicinity); j++) keep.add(j);
  }
  return [...keep].sort((a, b) => a - b).map((i) => msgs[i]);
}

export const DEFAULT_MISSED_BLOCK_MAX_CHARS = 40_000;

export interface MissedBlockOptions {
  streamName: string;
  channelId: string;
  reason: 'backscroll' | 'mention' | 'addressed';
  count: number;
  formatTime: (d: Date) => string;
  maxChars?: number;
  moreBeyond?: boolean;
  newestScannedId?: number;
}

function historyPointer(channelId: string, cursor: 'before' | 'after', id: number | undefined): string {
  return `fetch_history(channel="${channelId}", ${cursor}=${id})`;
}

export function renderMissedBlock(msgs: MissedView[], opts: MissedBlockOptions): { text: string; elided: number } {
  const dm = parseDmChannelId(opts.channelId) !== null;
  const render = (m: MissedView): string => {
    const att = m.attachmentNames.length > 0 ? ` [attachments: ${m.attachmentNames.join(', ')}]` : '';
    const head = messageLineHead({
      id: m.id,
      time: Number.isNaN(m.timestamp.getTime()) ? '' : opts.formatTime(m.timestamp),
      stream: dm ? null : opts.streamName,
      topic: m.topic,
      author: m.authorName,
      mentioned: m.mentioned,
    });
    return `${head}${m.text}${att}`;
  };
  const lines = msgs.map(render);
  const budget = opts.maxChars ?? Infinity;
  const newestScanned = opts.newestScannedId ?? msgs[msgs.length - 1]?.id;

  const assemble = (elided: number, cut: string | null): string => {
    const attrs = [`stream="#${opts.streamName}"`, `channelId="${opts.channelId}"`, `count="${opts.count}"`];
    if (opts.reason !== 'backscroll') attrs.push(`lines="${msgs.length}"`);
    attrs.push(`reason="${opts.reason}"`);
    const head: string[] = [];
    const tail: string[] = [];
    if (elided > 0) {
      head.push(
        `[${elided} earlier line(s) elided (ids ${msgs[0].id}–${msgs[elided - 1].id}) to fit the catch-up budget — ` +
          `${historyPointer(opts.channelId, 'before', msgs[elided].id)} pages into them]`,
      );
      attrs.push(`elided="${elided}"`);
    }
    if (opts.moreBeyond) {
      tail.push(`[the catch-up ceiling was reached; newer messages exist — ${historyPointer(opts.channelId, 'after', newestScanned)} continues]`);
      attrs.push('truncated="true"');
    }
    const body = cut !== null ? [cut] : lines.slice(elided);
    return [`<missed ${attrs.join(' ')}>`, ...head, ...body, ...tail, '</missed>'].join('\n');
  };

  let elided = 0;
  let block = assemble(elided, null);
  while (block.length > budget && elided < lines.length - 1) {
    elided++;
    block = assemble(elided, null);
  }
  if (block.length > budget && lines.length > 0) {
    const last = msgs[msgs.length - 1];
    const marker = ` … [line cut to fit the catch-up budget — fetch_around(${last.id}) has the whole message]`;
    const room = budget - (assemble(elided, '').length + marker.length);
    const full = lines[lines.length - 1];
    block = assemble(elided, `${full.slice(0, Math.max(0, room))}${marker}`);
  }
  return { text: block, elided };
}
