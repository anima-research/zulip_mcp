/**
 * Edits to the filters file are hot-applied: an allowlist change must never
 * require a restart (discord-mcpl #26).
 */

import { existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from 'node:fs';
import { dirname } from 'node:path';
import { createHash } from 'node:crypto';

export interface ZulipFilters {
  streams?: string[];
  dmUsers?: string[];
  mutedStreams?: string[];
  reactionChannels?: string[];
  suppressedReactionEmojis?: string[];
}

function uniqueStrings(values: string[] | undefined, map: (s: string) => string = (s) => s): string[] {
  return [...new Set((values ?? []).map((v) => map(String(v).trim())).filter(Boolean))];
}

function normalizeEmail(s: string): string {
  return s.includes('@') ? s.toLowerCase() : s;
}

function stripHash(s: string): string {
  return s.replace(/^#/, '');
}

/**
 * A glyph (☣️, as the host's baseline carries them) becomes its codepoints in
 * hex joined by '-', VS-16 stripped: the form Zulip reports as `emoji_code`
 * for unicode emoji, so a glyph-shaped entry and a name-shaped event meet on
 * the code.
 */
export function normalizeReactionEmoji(emoji: string): string {
  const s = emoji.replace(/\uFE0F/g, '').trim().replace(/^:|:$/g, '');
  if (/[^\x00-\x7F]/.test(s)) return [...s].map((c) => c.codePointAt(0)!.toString(16)).join('-');
  return s.toLowerCase();
}

export function normalizeFilters(f: ZulipFilters): ZulipFilters {
  const out: ZulipFilters = {};
  const streams = uniqueStrings(f.streams, stripHash);
  if (streams.length) out.streams = streams;
  const dmUsers = uniqueStrings(f.dmUsers, normalizeEmail);
  if (dmUsers.length) out.dmUsers = dmUsers;
  const muted = uniqueStrings(f.mutedStreams, stripHash);
  if (muted.length) out.mutedStreams = muted;
  const reactionChannels = uniqueStrings(f.reactionChannels);
  if (reactionChannels.length) out.reactionChannels = reactionChannels;
  if (f.suppressedReactionEmojis !== undefined) {
    out.suppressedReactionEmojis = uniqueStrings(f.suppressedReactionEmojis, normalizeReactionEmoji);
  }
  return out;
}

function splitList(raw: string | undefined): string[] {
  return (raw ?? '').split(',').map((s) => s.trim()).filter(Boolean);
}

export function parseFiltersFromEnv(env: NodeJS.ProcessEnv = process.env): ZulipFilters {
  return normalizeFilters({
    streams: splitList(env.ZULIP_STREAMS),
    dmUsers: splitList(env.ZULIP_DM_USERS),
    mutedStreams: splitList(env.ZULIP_MUTED_STREAMS),
  });
}

export function parseBaselineFromEnv(env: NodeJS.ProcessEnv = process.env): string[] {
  const raw = env.ZULIP_SUPPRESSED_REACTIONS_BASELINE ?? env.DISCORD_SUPPRESSED_REACTIONS_BASELINE;
  return uniqueStrings(splitList(raw), normalizeReactionEmoji);
}

export function loadFiltersFile(path: string): ZulipFilters | null {
  try {
    const raw: unknown = JSON.parse(readFileSync(path, 'utf-8'));
    if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) return null;
    const r = raw as Record<string, unknown>;
    const out: ZulipFilters = {};
    for (const key of ['streams', 'dmUsers', 'mutedStreams', 'reactionChannels', 'suppressedReactionEmojis'] as const) {
      if (!(key in r) || r[key] === null || r[key] === undefined) continue;
      const v = r[key];
      if (!Array.isArray(v) || v.some((x) => typeof x !== 'string' && typeof x !== 'number')) return null;
      out[key] = (v as unknown[]).map(String);
    }
    return normalizeFilters(out);
  } catch {
    return null;
  }
}

export function saveFiltersFile(path: string, filters: ZulipFilters): void {
  mkdirSync(dirname(path), { recursive: true });
  const tmp = `${path}.${process.pid}.tmp`;
  writeFileSync(tmp, JSON.stringify(normalizeFilters(filters), null, 2) + '\n');
  renameSync(tmp, path);
}

export function filtersFileMtime(path: string): number | null {
  try {
    return statSync(path).mtimeMs;
  } catch {
    return null;
  }
}

/** One missing observation is grace: a non-atomic editor removes the file mid-replace. */
const MISSING_POLLS_TOLERATED = 1;

export class FiltersFilePollTracker {
  private lastMtime: number | null;
  private missedPolls = 0;

  constructor(initialMtime: number | null) {
    this.lastMtime = initialMtime;
  }

  observe(mtime: number | null): 'none' | 'missing' | 'reload' {
    if (mtime === null) {
      this.missedPolls++;
      return this.missedPolls > MISSING_POLLS_TOLERATED ? 'missing' : 'none';
    }
    const reappeared = this.missedPolls > 0;
    this.missedPolls = 0;
    if (!reappeared && mtime === this.lastMtime) return 'none';
    this.lastMtime = mtime;
    return 'reload';
  }
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`);
    return `{${entries.join(',')}}`;
  }
  return JSON.stringify(value);
}

function sha256(s: string): string {
  return 'sha256:' + createHash('sha256').update(s).digest('hex');
}

export interface FiltersPlaneStatus {
  status: 'live' | 'stale';
  desiredState: 'ok' | 'invalid' | 'missing';
  path: string;
  effectiveDigest: string | null;
  loadedAt?: string;
  staleSince?: string;
}

export interface ReactionSuppressionStatus {
  status: 'not-configured' | 'configured-empty' | 'active' | 'stale';
  protectionActive: boolean;
  effectiveCount: number;
  effectiveDigest: string | null;
  baselineCount: number;
  source: 'file' | 'baseline' | 'file+baseline' | 'none';
}

export class FiltersPlane {
  private effective: ZulipFilters;
  private digest: string | null = null;
  private loadedAt: string | null = null;
  private broken: { since: string; desired: 'invalid' | 'missing' } | null = null;
  private listeners: ((next: ZulipFilters, prev: ZulipFilters) => void)[] = [];
  private poll: ReturnType<typeof setInterval> | null = null;
  private tracker: FiltersFilePollTracker | null = null;
  private readonly baseline: string[];

  constructor(
    readonly path: string,
    private readonly env: NodeJS.ProcessEnv = process.env,
    private readonly options: { pollMs?: number } = {},
  ) {
    this.effective = parseFiltersFromEnv(env);
    this.baseline = parseBaselineFromEnv(env);
  }

  start(): void {
    if (this.poll) return;
    if (existsSync(this.path)) {
      const fromFile = loadFiltersFile(this.path);
      if (!fromFile) {
        throw new Error(
          `the filters file ${this.path} exists but cannot be parsed (invalid JSON or a wrong-typed key); ` +
            'refusing to start on the env seed — repair the file, or remove it to re-seed from the environment',
        );
      }
      this.apply(fromFile, true);
      console.error(`[zulip-mcp] filters loaded from ${this.path}`);
    } else {
      try {
        saveFiltersFile(this.path, this.effective);
      } catch (err) {
        throw new Error(`cannot create the filters file ${this.path}: ${(err as Error).message}`);
      }
      this.apply(this.effective, true);
      console.error(`[zulip-mcp] filters file seeded from env -> ${this.path}`);
    }
    if (this.baseline.length > 0) {
      console.error(`[zulip-mcp] reaction-suppression baseline from the host: ${this.baseline.length} marker(s)`);
    }
    this.tracker = new FiltersFilePollTracker(filtersFileMtime(this.path));
    this.poll = setInterval(() => this.tick(), this.options.pollMs ?? 3000);
    (this.poll as { unref?: () => void }).unref?.();
  }

  stop(): void {
    if (this.poll) clearInterval(this.poll);
    this.poll = null;
  }

  tick(): void {
    if (!this.tracker) return;
    const action = this.tracker.observe(filtersFileMtime(this.path));
    if (action === 'none') return;
    if (action === 'missing') {
      if (this.markBroken('missing')) {
        console.error(`[zulip-mcp] filters file ${this.path} is MISSING — last-known-good filters stay in force until it reappears`);
      }
      return;
    }
    const next = loadFiltersFile(this.path);
    if (!next) {
      if (this.markBroken('invalid')) {
        console.error(`[zulip-mcp] filters file changed but is unparseable — keeping previous filters (${this.path})`);
      }
      return;
    }
    if (this.apply(next, false)) console.error(`[zulip-mcp] filters hot-reloaded from ${this.path}`);
  }

  current(): ZulipFilters {
    return this.effective;
  }

  onChange(listener: (next: ZulipFilters, prev: ZulipFilters) => void): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  update(edit: (current: ZulipFilters) => ZulipFilters): { ok: true; filters: ZulipFilters } | { ok: false; reason: string } {
    if (this.broken) {
      return {
        ok: false,
        reason: `the filters file ${this.path} is ${this.broken.desired} on disk; repair it (hot reload applies within seconds) and retry`,
      };
    }
    const next = normalizeFilters(edit(structuredClone(this.effective)));
    try {
      saveFiltersFile(this.path, next);
    } catch (err) {
      return { ok: false, reason: `could not write ${this.path}: ${(err as Error).message}` };
    }
    this.tracker = new FiltersFilePollTracker(filtersFileMtime(this.path));
    this.apply(next, false);
    return { ok: true, filters: this.effective };
  }

  streamAllowed(streamName: string): boolean {
    const allow = this.effective.streams;
    return !allow || allow.includes(streamName);
  }

  streamMuted(streamName: string): boolean {
    return this.effective.mutedStreams?.includes(streamName) ?? false;
  }

  dmAllowed(sender: { id: number; email: string }): boolean {
    const allow = this.effective.dmUsers;
    return !allow || allow.includes(String(sender.id)) || allow.includes(sender.email.toLowerCase());
  }

  reactionsVisible(channelId: string): boolean {
    return this.effective.reactionChannels?.includes(channelId) ?? false;
  }

  private effectiveSuppressed(): string[] {
    return [...new Set([...(this.effective.suppressedReactionEmojis ?? []), ...this.baseline])];
  }

  /** A custom emoji's code is its id, never compared against a glyph. */
  reactionSuppressed(emojiName: string, emojiCode?: string, reactionType?: string): boolean {
    const list = this.effectiveSuppressed();
    if (list.length === 0) return false;
    if (list.includes(normalizeReactionEmoji(emojiName))) return true;
    const unicode = reactionType === undefined || reactionType === 'unicode_emoji';
    return unicode && !!emojiCode && list.includes(emojiCode.toLowerCase());
  }

  planeStatus(): FiltersPlaneStatus {
    const loaded = this.loadedAt ? { loadedAt: this.loadedAt } : {};
    if (this.broken) {
      return {
        status: 'stale',
        desiredState: this.broken.desired,
        path: this.path,
        effectiveDigest: this.digest,
        staleSince: this.broken.since,
        ...loaded,
      };
    }
    return { status: 'live', desiredState: 'ok', path: this.path, effectiveDigest: this.digest, ...loaded };
  }

  suppressionStatus(): ReactionSuppressionStatus {
    const baselineCount = this.baseline.length;
    const fromFile = this.effective.suppressedReactionEmojis;
    const list = this.effectiveSuppressed();
    const n = list.length;
    const digest = n > 0 ? sha256(JSON.stringify([...list].sort())) : null;
    const source: ReactionSuppressionStatus['source'] =
      fromFile !== undefined && baselineCount > 0 ? 'file+baseline'
        : fromFile !== undefined ? 'file'
          : baselineCount > 0 ? 'baseline'
            : 'none';
    if (fromFile === undefined && baselineCount === 0) {
      return { status: 'not-configured', protectionActive: false, effectiveCount: 0, effectiveDigest: null, baselineCount, source };
    }
    if (this.broken) {
      return { status: 'stale', protectionActive: true, effectiveCount: n, effectiveDigest: digest, baselineCount, source };
    }
    return { status: n > 0 ? 'active' : 'configured-empty', protectionActive: n > 0, effectiveCount: n, effectiveDigest: digest, baselineCount, source };
  }

  private apply(filters: ZulipFilters, initial: boolean): boolean {
    const prev = this.effective;
    const normalized = normalizeFilters(filters);
    const digest = sha256(stableStringify(normalized));
    const changed = digest !== this.digest;
    this.broken = null;
    if (changed) this.loadedAt = new Date().toISOString();
    this.digest = digest;
    this.effective = normalized;
    if (changed && !initial) {
      for (const l of this.listeners) {
        try {
          l(normalized, prev);
        } catch (err) {
          console.error('[zulip-mcp] filters listener failed:', (err as Error).message);
        }
      }
    }
    return changed;
  }

  private markBroken(desired: 'invalid' | 'missing'): boolean {
    if (this.broken) {
      this.broken.desired = desired;
      return false;
    }
    this.broken = { since: new Date().toISOString(), desired };
    return true;
  }
}
