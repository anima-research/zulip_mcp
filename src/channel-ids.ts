import { dmKey, parseKey, streamKey, type ChannelKey } from './channel-key.js';
import { dmChannelIdFor, parseDmChannelId } from './history.js';
import type { StreamDirectory } from './stream-directory.js';

/** A key; a refusal, for a former name, with the stream's new channel id in its words; or a name the directory does not hold. */
export type KeyAnswer = { key: ChannelKey } | { refusal: string } | { unknownName: string };

const PREFIX = 'zulip:';

/** The channel id the host holds for each channel key, and the key behind each channel id the host or a tool names. */
export class ChannelIds {
  private heldByKey = new Map<ChannelKey, string>();
  private keyByHeld = new Map<string, ChannelKey>();
  /** Keys that a change the host didn't answer named. */
  private unsettled = new Set<ChannelKey>();
  /** Ids that a change the host didn't answer added, which no key holds. */
  private doubtful = new Set<string>();

  /**
   * @param refreshStreams reads the stream list once, adding only what the
   *   directory does not hold; used before a name is refused as unknown.
   */
  constructor(
    readonly directory: StreamDirectory,
    private readonly refreshStreams?: () => Promise<void>,
  ) {}

  /** The id the host holds for a key; otherwise the id it would be told now. null when the host holds that id for another key. */
  channelIdOf(key: ChannelKey): string | null {
    const held = this.heldByKey.get(key);
    if (held !== undefined) return held;
    const fresh = this.freshIdOf(key);
    return fresh !== null && this.keyByHeld.has(fresh) ? null : fresh;
  }

  /** The key the host holds an id for, if any. */
  holderOf(channelId: string): ChannelKey | undefined {
    return this.keyByHeld.get(channelId);
  }

  /** `zulip:` and the stream's current name, or `zulip:dm:` and the user ids. */
  freshIdOf(key: ChannelKey): string | null {
    const parsed = parseKey(key);
    if (parsed.kind === 'dm') return dmChannelIdFor(parsed.userIds);
    const name = this.directory.nameOf(parsed.streamId);
    return name === undefined ? null : PREFIX + name;
  }

  heldIdOf(key: ChannelKey): string | undefined {
    return this.heldByKey.get(key);
  }

  /** Keys whose id at the host differs from the id they would be told now, or that a change the host didn't answer named. */
  stale(): { key: ChannelKey; held: string; fresh: string | null }[] {
    const out: { key: ChannelKey; held: string; fresh: string | null }[] = [];
    for (const [key, held] of this.heldByKey) {
      const fresh = this.freshIdOf(key);
      if (fresh !== held || this.unsettled.has(key)) out.push({ key, held, fresh });
    }
    return out;
  }

  /** Ids that a change the host didn't answer added and that no key holds now. The host may hold each or not. */
  inDoubt(): string[] {
    return [...this.doubtful];
  }

  /** The host didn't answer a change that named these keys and added these ids. */
  unanswered(keys: ChannelKey[], added: string[]): void {
    for (const key of keys) this.unsettled.add(key);
    for (const id of added) if (!this.keyByHeld.has(id)) this.doubtful.add(id);
  }

  /** The host answered a change that named these keys and removed these ids. */
  answered(keys: ChannelKey[], removed: string[]): void {
    for (const key of keys) this.unsettled.delete(key);
    for (const id of removed) this.doubtful.delete(id);
  }

  /** The host was told this id for this key. The first id told stands until `move`. */
  noteHeld(key: ChannelKey, channelId: string): void {
    if (this.heldByKey.has(key) || this.keyByHeld.has(channelId)) return;
    this.heldByKey.set(key, channelId);
    this.keyByHeld.set(channelId, key);
    this.doubtful.delete(channelId);
  }

  /** The host now holds `channelId` for this key in place of the old one. */
  move(key: ChannelKey, channelId: string): void {
    const old = this.heldByKey.get(key);
    if (old !== undefined) this.keyByHeld.delete(old);
    const other = this.keyByHeld.get(channelId);
    if (other !== undefined && other !== key) this.heldByKey.delete(other);
    this.heldByKey.set(key, channelId);
    this.keyByHeld.set(channelId, key);
    this.doubtful.delete(channelId);
  }

  /** The host no longer holds this id. */
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

  /** The key a descriptor this server built stands for. */
  keyOfDescriptor(descriptor: { id: string; address?: unknown }): ChannelKey | null {
    const address = typeof descriptor.address === 'object' && descriptor.address !== null
      ? (descriptor.address as { stream_id?: unknown; user_ids?: unknown; dm?: unknown })
      : {};
    if (address.dm === true && Array.isArray(address.user_ids)) return dmKey(address.user_ids.map(Number));
    if (typeof address.stream_id === 'number') return streamKey(address.stream_id);
    const answer = this.resolve(descriptor.id);
    return answer && 'key' in answer ? answer.key : null;
  }

  /**
   * A channel id named by the host or a tool, without reading the stream
   * list: a direct-message id, an id the host holds, a current name, or a
   * former name (refused, with the new id in the words). null for a name
   * the directory does not know. `byName` is for a stream name a tool was
   * given: the directory alone answers it, not an id the host holds.
   */
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

  /** `resolve`, and for a name the directory does not know, one read of the stream list first. */
  async keyOf(channelId: string, opts: { byName?: boolean } = {}): Promise<KeyAnswer> {
    const first = this.resolve(channelId, opts);
    if (first) return first;
    await this.refreshStreams?.();
    const second = this.resolve(channelId, opts);
    if (second) return second;
    return { unknownName: channelId.slice(PREFIX.length) };
  }
}
