/**
 * The stream ids and names Zulip has told this server about:
 *   - a queue registration's own `streams` list replaces it;
 *   - a `stream` event changes it, in queue order;
 *   - any other list (discovery, a refresh, a miss) replaces it while no
 *     queue is registered, and otherwise only adds ids it does not hold;
 *   - a message's `display_recipient` never changes it.
 *
 * A name a stream had and no stream has now is kept as a former name of that
 * stream. A stream whose name reads as a direct-message channel id (`dm:42`)
 * never enters.
 */

import { parseDmChannelId } from './history.js';

export interface StreamEntry {
  stream_id: number;
  name: string;
}

export interface DirectoryChange {
  renamed: { id: number; from: string; to: string }[];
  removed: { id: number; name: string }[];
  added: { id: number; name: string }[];
}

/** The directory as the delivery state file keeps it. */
export interface DirectorySnapshot {
  streams: Record<string, string>;
  formerNames: Record<string, number>;
}

/** A `stream` event from Zulip's event queue, as far as the directory reads it. */
export interface ZulipStreamEvent {
  op?: unknown;
  streams?: unknown;
  stream_ids?: unknown;
  stream_id?: unknown;
  property?: unknown;
  value?: unknown;
}

type Listener = (change: DirectoryChange) => void;

export function isEmptyChange(change: DirectoryChange): boolean {
  return change.renamed.length === 0 && change.removed.length === 0 && change.added.length === 0;
}

export class StreamDirectory {
  private byId = new Map<number, string>();
  private byName = new Map<string, number>();
  private former = new Map<string, number>();
  private filled = false;
  private queueRegistered = false;
  private fillWaiters: (() => void)[] = [];
  private listed = false;
  private listedWaiters: (() => void)[] = [];
  private listeners: Listener[] = [];
  private reportedDmLike = new Set<string>();

  nameOf(streamId: number): string | undefined {
    return this.byId.get(streamId);
  }

  idOf(name: string): number | undefined {
    return this.byName.get(name);
  }

  /** The stream that had `name` before it was renamed, with its name now. */
  formerOf(name: string): { id: number; current: string } | undefined {
    const id = this.former.get(name);
    if (id === undefined) return undefined;
    const current = this.byId.get(id);
    return current === undefined ? undefined : { id, current };
  }

  /** Every stream id the directory holds. */
  ids(): number[] {
    return [...this.byId.keys()];
  }

  /** True once a stream list has filled it: a queue registration's, or a list read before any registration. */
  get isFilled(): boolean {
    return this.filled;
  }

  whenFilled(): Promise<void> {
    if (this.filled) return Promise.resolve();
    return new Promise((resolve) => this.fillWaiters.push(resolve));
  }

  /** Resolves at the first fill, or once a read of the list got no answer and what waits for one goes on with what the directory holds. */
  whenListed(): Promise<void> {
    if (this.listed) return Promise.resolve();
    return new Promise((resolve) => this.listedWaiters.push(resolve));
  }

  /** A read of the stream list got no answer: what waits for a list goes on with what the directory holds. */
  listUnanswered(): void {
    this.settleListed();
  }

  private settleListed(): void {
    if (this.listed) return;
    this.listed = true;
    for (const resolve of this.listedWaiters.splice(0)) resolve();
  }

  /**
   * Start again from a copy, as a connection does: not filled until its first
   * list, and no queue registered until its own. Tells no listener.
   */
  restart(copy: Partial<DirectorySnapshot> | null): void {
    this.byId = new Map();
    this.byName = new Map();
    this.former = new Map();
    this.filled = false;
    this.queueRegistered = false;
    if (copy) this.load(copy);
  }

  /** Called synchronously, in the same tick as the change it reports. */
  onChange(listener: Listener): void {
    this.listeners.push(listener);
  }

  /** Seed it from its copy on disk, before the first registration. */
  load(snapshot: Partial<DirectorySnapshot>): void {
    for (const [id, name] of Object.entries(snapshot.streams ?? {})) {
      const n = Number(id);
      if (Number.isSafeInteger(n) && n > 0 && typeof name === 'string' && this.admissible(name)) this.setName(n, name, emptyChange());
    }
    for (const [name, id] of Object.entries(snapshot.formerNames ?? {})) {
      if (typeof id === 'number' && this.byId.has(id) && !this.byName.has(name)) this.former.set(name, id);
    }
  }

  snapshot(): DirectorySnapshot {
    return {
      streams: Object.fromEntries([...this.byId].sort((a, b) => a[0] - b[0]).map(([id, name]) => [String(id), name])),
      formerNames: Object.fromEntries([...this.former].sort((a, b) => a[0].localeCompare(b[0]))),
    };
  }

  /** A queue registration's own list: it replaces what the directory held. */
  registration(list: unknown): DirectoryChange {
    this.queueRegistered = true;
    return this.replace(list);
  }

  /** A list read outside a registration: it replaces only while no queue has registered. */
  list(list: unknown): DirectoryChange {
    return this.queueRegistered ? this.addMissing(list) : this.replace(list);
  }

  private replace(list: unknown): DirectoryChange {
    const wanted = new Map<number, string>();
    for (const entry of entries(list)) if (this.admissible(entry.name)) wanted.set(entry.stream_id, entry.name);
    const change = emptyChange();
    for (const id of [...this.byId.keys()]) if (!wanted.has(id)) this.remove(id, change);
    for (const [id, name] of wanted) {
      const previous = this.byId.get(id);
      if (previous === undefined) change.added.push({ id, name });
      else if (previous !== name) {
        change.renamed.push({ id, from: previous, to: name });
        this.former.set(previous, id);
      }
    }
    this.byId = wanted;
    this.byName = new Map([...wanted].map(([id, name]) => [name, id]));
    this.dropShadowedFormerNames();
    const wasFilled = this.filled;
    this.filled = true;
    this.emit(change);
    if (!wasFilled) for (const resolve of this.fillWaiters.splice(0)) resolve();
    this.settleListed();
    return change;
  }

  /** A `stream` event: create adds, delete removes, a name update renames. */
  applyEvent(event: ZulipStreamEvent): DirectoryChange {
    const change = emptyChange();
    if (event.op === 'create') {
      for (const entry of entries(event.streams)) {
        if (this.admissible(entry.name)) this.setName(entry.stream_id, entry.name, change);
      }
    } else if (event.op === 'delete') {
      const ids = Array.isArray(event.stream_ids)
        ? event.stream_ids
        : entries(event.streams).map((s) => s.stream_id);
      for (const id of ids) if (typeof id === 'number') this.remove(id, change);
    } else if (event.op === 'update' && event.property === 'name' && typeof event.stream_id === 'number' && typeof event.value === 'string') {
      if (this.admissible(event.value)) this.setName(event.stream_id, event.value, change);
      else this.remove(event.stream_id, change);
    }
    this.dropShadowedFormerNames();
    this.emit(change);
    return change;
  }

  /** Only ids the directory does not hold are added. */
  private addMissing(list: unknown): DirectoryChange {
    const change = emptyChange();
    for (const entry of entries(list)) {
      if (this.byId.has(entry.stream_id) || this.byName.has(entry.name) || !this.admissible(entry.name)) continue;
      this.setName(entry.stream_id, entry.name, change);
    }
    this.dropShadowedFormerNames();
    this.emit(change);
    return change;
  }

  private setName(id: number, name: string, change: DirectoryChange): void {
    const previous = this.byId.get(id);
    if (previous === name) return;
    const holder = this.byName.get(name);
    if (holder !== undefined && holder !== id) this.byId.delete(holder);
    this.byId.set(id, name);
    this.byName.set(name, id);
    if (previous === undefined) {
      change.added.push({ id, name });
      return;
    }
    if (this.byName.get(previous) === id) this.byName.delete(previous);
    this.former.set(previous, id);
    change.renamed.push({ id, from: previous, to: name });
  }

  private remove(id: number, change: DirectoryChange): void {
    const name = this.byId.get(id);
    if (name === undefined) return;
    this.byId.delete(id);
    if (this.byName.get(name) === id) this.byName.delete(name);
    for (const [formerName, formerId] of this.former) if (formerId === id) this.former.delete(formerName);
    change.removed.push({ id, name });
  }

  /** A name some stream holds now is no stream's former name. */
  private dropShadowedFormerNames(): void {
    for (const name of [...this.former.keys()]) if (this.byName.has(name)) this.former.delete(name);
  }

  private admissible(name: string): boolean {
    if (parseDmChannelId(`zulip:${name}`) === null) return true;
    if (!this.reportedDmLike.has(name)) {
      this.reportedDmLike.add(name);
      console.error(`[zulip-mcp] stream #${name} is not served: its channel id would read as a direct-message conversation`);
    }
    return false;
  }

  private emit(change: DirectoryChange): void {
    if (isEmptyChange(change)) return;
    for (const listener of this.listeners) {
      try {
        listener(change);
      } catch (err) {
        console.error('[zulip-mcp] stream directory listener failed:', (err as Error).message);
      }
    }
  }
}

function emptyChange(): DirectoryChange {
  return { renamed: [], removed: [], added: [] };
}

function entries(list: unknown): StreamEntry[] {
  if (!Array.isArray(list)) return [];
  const out: StreamEntry[] = [];
  for (const s of list) {
    if (typeof s !== 'object' || s === null) continue;
    const { stream_id, name } = s as { stream_id?: unknown; name?: unknown };
    if (typeof stream_id === 'number' && Number.isSafeInteger(stream_id) && stream_id > 0 && typeof name === 'string' && name) {
      out.push({ stream_id, name });
    }
  }
  return out;
}
