import { dmKey, parseKey, streamKey, type ChannelKey } from './channel-key.js';
import { dmChannelIdFor, parseDmChannelId } from './history.js';
import type { StreamDirectory } from './stream-directory.js';

export type KeyAnswer = { key: ChannelKey } | { refusal: string } | { unknownName: string };

const PREFIX = 'zulip:';

export class ChannelIds {
  private heldByKey = new Map<ChannelKey, string>();
  private keyByHeld = new Map<string, ChannelKey>();
  private unsettled = new Set<ChannelKey>();
  private doubtful = new Set<string>();

  constructor(
    readonly directory: StreamDirectory,
    private readonly refreshStreams?: (why: string) => Promise<void>,
  ) {}

  channelIdOf(key: ChannelKey): string | null {
    const held = this.heldByKey.get(key);
    if (held !== undefined) return held;
    const fresh = this.freshIdOf(key);
    return fresh !== null && this.keyByHeld.has(fresh) ? null : fresh;
  }

  holderOf(channelId: string): ChannelKey | undefined {
    return this.keyByHeld.get(channelId);
  }

  freshIdOf(key: ChannelKey): string | null {
    const parsed = parseKey(key);
    if (parsed.kind === 'dm') return dmChannelIdFor(parsed.userIds);
    const name = this.directory.nameOf(parsed.streamId);
    return name === undefined ? null : PREFIX + name;
  }

  heldIdOf(key: ChannelKey): string | undefined {
    return this.heldByKey.get(key);
  }

  stale(): { key: ChannelKey; held: string; fresh: string | null }[] {
    const out: { key: ChannelKey; held: string; fresh: string | null }[] = [];
    for (const [key, held] of this.heldByKey) {
      const fresh = this.freshIdOf(key);
      if (fresh !== held || this.unsettled.has(key)) out.push({ key, held, fresh });
    }
    return out;
  }

  inDoubt(): string[] {
    return [...this.doubtful];
  }

  unanswered(keys: ChannelKey[], added: string[]): void {
    for (const key of keys) this.unsettled.add(key);
    for (const id of added) if (!this.keyByHeld.has(id)) this.doubtful.add(id);
  }

  answered(keys: ChannelKey[], removed: string[]): void {
    for (const key of keys) this.unsettled.delete(key);
    for (const id of removed) this.doubtful.delete(id);
  }

  noteHeld(key: ChannelKey, channelId: string): void {
    if (this.heldByKey.has(key) || this.keyByHeld.has(channelId)) return;
    this.heldByKey.set(key, channelId);
    this.keyByHeld.set(channelId, key);
    this.doubtful.delete(channelId);
  }

  move(key: ChannelKey, channelId: string): void {
    const old = this.heldByKey.get(key);
    if (old !== undefined) this.keyByHeld.delete(old);
    const other = this.keyByHeld.get(channelId);
    if (other !== undefined && other !== key) this.heldByKey.delete(other);
    this.heldByKey.set(key, channelId);
    this.keyByHeld.set(channelId, key);
    this.doubtful.delete(channelId);
  }

  forget(channelId: string): void {
    const key = this.keyByHeld.get(channelId);
    if (key === undefined) return;
    this.keyByHeld.delete(channelId);
    if (this.heldByKey.get(key) === channelId) this.heldByKey.delete(key);
  }

  reset(): void {
    this.heldByKey.clear();
    this.keyByHeld.clear();
    this.unsettled.clear();
    this.doubtful.clear();
  }

  keyOfDescriptor(descriptor: { id: string; address?: unknown }): ChannelKey | null {
    const address = typeof descriptor.address === 'object' && descriptor.address !== null
      ? (descriptor.address as { stream_id?: unknown; user_ids?: unknown; dm?: unknown })
      : {};
    if (address.dm === true && Array.isArray(address.user_ids)) return dmKey(address.user_ids.map(Number));
    if (typeof address.stream_id === 'number') return streamKey(address.stream_id);
    const answer = this.resolve(descriptor.id);
    return answer && 'key' in answer ? answer.key : null;
  }

  resolve(channelId: string, opts: { byName?: boolean } = {}): Exclude<KeyAnswer, { unknownName: string }> | null {
    const dm = parseDmChannelId(channelId);
    if (dm) return { key: dmKey(dm) };
    const held = opts.byName ? undefined : this.keyByHeld.get(channelId);
    if (held !== undefined) return { key: held };
    if (!channelId.startsWith(PREFIX)) return { refusal: `Unknown channel format: ${channelId}` };
    const name = channelId.slice(PREFIX.length);
    const id = this.directory.idOf(name);
    if (id !== undefined) return { key: streamKey(id) };
    const former = this.directory.formerOf(name);
    if (former) {
      return {
        refusal: `${channelId} is gone: Zulip stream #${name} was renamed #${former.current}, and its channel is ${PREFIX}${former.current}.`,
      };
    }
    return null;
  }

  async keyOf(channelId: string, opts: { byName?: boolean } = {}): Promise<KeyAnswer> {
    const first = this.resolve(channelId, opts);
    if (first) return first;
    await this.refreshStreams?.(`#${channelId.slice(PREFIX.length)}`);
    const second = this.resolve(channelId, opts);
    if (second) return second;
    return { unknownName: channelId.slice(PREFIX.length) };
  }
}
