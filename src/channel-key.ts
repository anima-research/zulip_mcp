import type { IncomingChannelMessage } from '@animalabs/mcpl-core';

declare const channelKeyBrand: unique symbol;

/** A channel inside the server: `s:<stream id>` for a stream, `d:<sorted user ids joined by +>` for a direct-message conversation. */
export type ChannelKey = string & { readonly [channelKeyBrand]: true };

export type ParsedKey = { kind: 'stream'; streamId: number } | { kind: 'dm'; userIds: number[] };

export function streamKey(streamId: number): ChannelKey {
  return `s:${streamId}` as ChannelKey;
}

export function dmKey(userIds: number[]): ChannelKey {
  return `d:${[...userIds].sort((a, b) => a - b).join('+')}` as ChannelKey;
}

export function parseKey(key: ChannelKey): ParsedKey {
  if (key.startsWith('s:')) return { kind: 'stream', streamId: Number(key.slice(2)) };
  return { kind: 'dm', userIds: key.slice(2).split('+').map(Number) };
}

export function isDmKey(key: ChannelKey): boolean {
  return key.startsWith('d:');
}

/** The stream id behind a key; null for a direct-message conversation. */
export function streamIdOfKey(key: ChannelKey): number | null {
  return key.startsWith('s:') ? Number(key.slice(2)) : null;
}

/** An incoming message as the server keeps it: keyed, with no channel id. */
export type KeyedMessage = Omit<IncomingChannelMessage, 'channelId'> & {
  key: ChannelKey;
  /** The stream's name as Zulip sent it with the message, for its message line only. */
  streamNameShown?: string;
};

export function toWire(message: KeyedMessage, channelId: string): IncomingChannelMessage {
  const { key: _key, streamNameShown: _shown, ...rest } = message;
  return { ...rest, channelId };
}
