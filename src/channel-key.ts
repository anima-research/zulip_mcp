import type { IncomingChannelMessage } from '@animalabs/mcpl-core';

declare const channelKeyBrand: unique symbol;

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

export function streamIdOfKey(key: ChannelKey): number | null {
  return key.startsWith('s:') ? Number(key.slice(2)) : null;
}

export type KeyedMessage = Omit<IncomingChannelMessage, 'channelId'> & {
  key: ChannelKey;
  streamNameShown?: string;
};

export function toWire(message: KeyedMessage, channelId: string): IncomingChannelMessage {
  const { key: _key, streamNameShown: _shown, ...rest } = message;
  return { ...rest, channelId };
}
