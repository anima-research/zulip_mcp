import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import type { IncomingChannelMessage, TextContent } from '@animalabs/mcpl-core';
import { isDmChannelId } from './history.js';
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
}

export type HeldStatus = 'queued' | 'failed' | 'rejected';

interface Held {
  status: HeldStatus;
  replayed: boolean;
}

const HELD_CAP_PER_CHANNEL = 50_000;

export class DeliveryState {
  private watermarks = new Map<string, number>();
  private missed = new Map<string, MissedTally>();
  private lastOpen = new Set<string>();
  private held = new Map<string, Map<number, Held>>();
  private reached = new Map<string, number>();

  constructor(
    private readonly stateDir: string | null,
    private readonly sessionId: string,
  ) {
    this.load();
  }

  private file(): string | null {
    return this.stateDir ? join(this.stateDir, `${this.sessionId}.delivery.json`) : null;
  }

  private load(): void {
    const path = this.file();
    if (!path || !existsSync(path)) return;
    try {
      const parsed = JSON.parse(readFileSync(path, 'utf-8')) as DeliveryFile;
      for (const [chan, id] of Object.entries(parsed.watermarks ?? {})) {
        if (typeof id === 'number' && Number.isFinite(id) && id > 0) this.watermarks.set(chan, id);
      }
      for (const [chan, t] of Object.entries(parsed.missed ?? {})) {
        this.missed.set(chan, {
          anchorId: num(t.anchorId),
          talliedThrough: num(t.talliedThrough ?? t.anchorId),
          messages: num(t.messages),
          characters: num(t.characters),
        });
      }
      for (const id of parsed.lastOpen ?? []) if (typeof id === 'string') this.lastOpen.add(id);
    } catch (err) {
      console.error('[zulip-mcp] Failed to load delivery state:', (err as Error).message);
    }
  }

  save(): void {
    const path = this.file();
    if (!path) return;
    try {
      mkdirSync(this.stateDir!, { recursive: true });
      const out: DeliveryFile = {
        watermarks: Object.fromEntries([...this.watermarks].sort((a, b) => a[0].localeCompare(b[0]))),
        missed: Object.fromEntries([...this.missed].sort((a, b) => a[0].localeCompare(b[0]))),
        lastOpen: [...this.lastOpen].sort(),
      };
      const tmp = `${path}.${process.pid}.tmp`;
      writeFileSync(tmp, JSON.stringify(out, null, 2) + '\n');
      renameSync(tmp, path);
    } catch (err) {
      console.error('[zulip-mcp] Failed to save delivery state:', (err as Error).message);
    }
  }

  watermark(channelId: string): number | undefined {
    return this.watermarks.get(channelId);
  }

  advance(channelId: string, messageId: number): boolean {
    const reached = Math.max(this.reached.get(channelId) ?? 0, this.watermarks.get(channelId) ?? 0, messageId);
    this.reached.set(channelId, reached);
    return this.settle(channelId);
  }

  advanceThrough(channelId: string, messageId: number): boolean {
    const held = this.held.get(channelId);
    if (held) {
      for (const id of held.keys()) if (id <= messageId) held.delete(id);
      if (held.size === 0) this.held.delete(channelId);
    }
    return this.advance(channelId, messageId);
  }

  private settle(channelId: string): boolean {
    const reached = this.reached.get(channelId);
    if (reached === undefined) return false;
    const floor = this.floor(channelId);
    const target = floor === undefined ? reached : Math.min(reached, floor - 1);
    const current = this.watermarks.get(channelId) ?? 0;
    if (target <= current) return false;
    this.watermarks.set(channelId, target);
    return true;
  }

  watermarkedChannels(): string[] {
    return [...this.watermarks.keys()];
  }

  hold(channelId: string, messageId: number, status: HeldStatus = 'queued'): void {
    if (!Number.isFinite(messageId) || messageId <= 0) return;
    if (messageId <= (this.watermarks.get(channelId) ?? 0)) return;
    let held = this.held.get(channelId);
    if (!held) {
      held = new Map();
      this.held.set(channelId, held);
    }
    const existing = held.get(messageId);
    if (existing) {
      existing.status = status;
      return;
    }
    if (held.size >= HELD_CAP_PER_CHANNEL) {
      const highest = Math.max(...held.keys());
      if (messageId >= highest) {
        console.error(`[zulip-mcp] ${channelId}: undelivered floor holds ${held.size} ids; not tracking ${messageId}`);
        return;
      }
      held.delete(highest);
    }
    held.set(messageId, { status, replayed: false });
  }

  release(channelId: string, messageId: number): boolean {
    const held = this.held.get(channelId);
    if (!held?.delete(messageId)) return false;
    if (held.size === 0) this.held.delete(channelId);
    return this.settle(channelId);
  }

  releaseAll(channelId: string): boolean {
    if (!this.held.delete(channelId)) return false;
    return this.settle(channelId);
  }

  floor(channelId: string): number | undefined {
    const held = this.held.get(channelId);
    if (!held || held.size === 0) return undefined;
    return Math.min(...held.keys());
  }

  heldChannels(): string[] {
    return [...this.held.keys()];
  }

  heldIds(channelId: string, opts: { replayable?: boolean } = {}): number[] {
    const held = this.held.get(channelId);
    if (!held) return [];
    return [...held.entries()]
      .filter(([, h]) => !opts.replayable || (h.status !== 'queued' && !h.replayed))
      .map(([id]) => id)
      .sort((a, b) => a - b);
  }

  markReplayed(channelId: string, ids: number[]): void {
    const held = this.held.get(channelId);
    if (!held) return;
    for (const id of ids) {
      const h = held.get(id);
      if (h) h.replayed = true;
    }
  }

  wasOpen(channelId: string): boolean {
    return this.lastOpen.has(channelId);
  }

  lastOpenChannels(): string[] {
    return [...this.lastOpen];
  }

  markOpen(channelId: string): void {
    this.lastOpen.add(channelId);
    this.missed.delete(channelId);
  }

  markClosed(channelId: string): void {
    if (!this.lastOpen.delete(channelId)) return;
    const anchor = this.watermarks.get(channelId) ?? 0;
    this.missed.set(channelId, { anchorId: anchor, talliedThrough: anchor, messages: 0, characters: 0 });
  }

  tally(channelId: string): MissedTally | undefined {
    return this.missed.get(channelId);
  }

  talliedChannels(): string[] {
    return [...this.missed.keys()];
  }

  countMissed(channelId: string, m: { id: number; text: string }): boolean {
    const t = this.missed.get(channelId);
    if (!t) return false;
    t.messages += 1;
    t.characters += m.text.length;
    if (m.id > t.talliedThrough) t.talliedThrough = m.id;
    return true;
  }

  backfillMissed(channelId: string, ambient: { id: number; text: string }[], scannedThrough: number): void {
    const t = this.missed.get(channelId);
    if (!t) return;
    t.messages += ambient.length;
    t.characters += ambient.reduce((n, m) => n + m.text.length, 0);
    if (scannedThrough > t.talliedThrough) t.talliedThrough = scannedThrough;
  }
}

function num(v: unknown): number {
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}

export interface MissedView {
  id: number;
  mentioned: boolean;
  topic: string;
  authorName: string;
  text: string;
  timestamp: Date;
  attachmentNames: string[];
}

export function viewOf(m: IncomingChannelMessage): MissedView {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  const attachments = Array.isArray(meta.attachments) ? (meta.attachments as { name?: string }[]) : [];
  const text = m.content
    .filter((c): c is TextContent => c.type === 'text')
    .map((c) => c.text)
    .filter((t) => !t.startsWith('[attachments:'))
    .join('\n');
  return {
    id: Number(m.messageId),
    mentioned: meta.mentioned === true || meta.isDM === true,
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
export function attributeMessage(m: IncomingChannelMessage, formatTime: (d: Date) => string): IncomingChannelMessage {
  const meta = (typeof m.metadata === 'object' && m.metadata !== null ? m.metadata : {}) as Record<string, unknown>;
  if (meta.attributed === true) return m;
  const isDM = meta.isDM === true || isDmChannelId(m.channelId);
  const at = new Date(m.timestamp);
  const header = messageLineHead({
    id: m.messageId,
    time: Number.isNaN(at.getTime()) ? '' : formatTime(at),
    stream: isDM ? null : (m.channelId.startsWith('zulip:') ? m.channelId.slice('zulip:'.length) : m.channelId),
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

export function selectMissed<T extends { mentioned: boolean }>(
  msgs: T[],
  opts: { keepAll: boolean; vicinity: number },
): T[] {
  if (opts.keepAll) return msgs;
  const keep = new Set<number>();
  for (let i = 0; i < msgs.length; i++) {
    if (!msgs[i].mentioned) continue;
    for (let j = Math.max(0, i - opts.vicinity); j <= Math.min(msgs.length - 1, i + opts.vicinity); j++) keep.add(j);
  }
  return [...keep].sort((a, b) => a - b).map((i) => msgs[i]);
}

export const DEFAULT_MISSED_BLOCK_MAX_CHARS = 40_000;

export interface MissedBlockOptions {
  streamName: string;
  channelId: string;
  reason: 'backscroll' | 'mention';
  count: number;
  formatTime: (d: Date) => string;
  maxChars?: number;
  moreBeyond?: boolean;
  newestScannedId?: number;
}

function historyPointer(channelId: string, cursor: 'before' | 'after', id: number | undefined): string {
  return `fetch_history(channel="${channelId}", ${cursor}=${id})`;
}

export function renderMissedBlock(msgs: MissedView[], opts: MissedBlockOptions): string {
  const dm = isDmChannelId(opts.channelId);
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
    if (opts.reason === 'mention') attrs.push(`lines="${msgs.length}"`);
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
  return block;
}
