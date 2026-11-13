import type {
  ChannelDescriptor,
  ChannelsPublishResult,
  ContentBlock,
  ContextInjection,
} from '@animalabs/mcpl-core';
import type { ChannelKey, KeyedMessage } from '../channel-key.js';

export interface SentRecord {
  messageId: string;
  channelId: string;
  content: string;
}

export type PublishResult = ChannelsPublishResult & { messageIds?: string[] };

/**
 * `channels/publish` carries no thread information (SPEC 0.5 §14.3), so "reply
 * where the conversation is" is reconstructed server-side: Zulip maps threadId
 * to a topic.
 */
export interface RoutingHints {
  threadId?: string;
  metadata?: Record<string, unknown>;
}

export interface IncomingEntry {
  key: ChannelKey;
  messageId: string;
  ready(changes?: readonly ChangeEntry[]): Promise<{ message: KeyedMessage; newChannel?: ChannelDescriptor } | null>;
}

export type OnIncomingMessage = (entry: IncomingEntry) => void;

export interface PlatformSystemEvent {
  kind: 'gap' | 'degraded' | 'recovered';
  text: string;
  metadata?: Record<string, unknown>;
}

export type OnSystemEvent = (event: PlatformSystemEvent) => void;

export interface ReactionEvent {
  action: 'add' | 'remove';
  key: ChannelKey;
  messageId: string;
  emoji: string;
  /** Zulip's `emoji_code`: a unicode emoji's codepoints, a custom emoji's id, or
   *  the name of one of Zulip's own extra emoji. */
  emojiCode?: string;
  /** Zulip: 'unicode_emoji' | 'realm_emoji' | 'zulip_extra_emoji'. */
  emojiType?: string;
  reactorId: string;
  reactorName: string;
  onOwnMessage: boolean;
  messageSnippet: string | null;
  timestamp: Date;
}

export interface ReactionEntry extends ReactionEvent {
  placed(): ReactionEvent;
}

export type OnReaction = (entry: ReactionEntry) => void;

export interface MessageChangeEvent {
  kind: 'edit' | 'move' | 'delete';
  key: ChannelKey;
  messageId: string;
  messageIds: string[];
  authorId: string | null;
  authorName: string | null;
  authorEmail: string | null;
  /** Who made the change; null for a server-side change or when unknown
   *  (Zulip's delete event names no actor). */
  actorId: string | null;
  topic: string;
  previousTopic: string | null;
  movedToKey: ChannelKey | null;
  leftChannel: boolean;
  byBot: boolean;
  content: string | null;
  previousContent: string | null;
  /** The bot is mentioned in the message as it now reads (Zulip's verdict). */
  mentioned: boolean;
  replyToSelf?: boolean;
  replyToSelfVerified?: boolean;
  previouslyReplyToSelf?: boolean | null;
  /** null when the message was not in the adapter's cache: a GET reads only
   *  the post-change state. */
  previouslyMentioned: boolean | null;
  /** A deletion event for a message that had just moved to a stream the bot
   *  cannot see: Zulip reports lost visibility as a deletion. */
  vanished: boolean;
  isDM: boolean;
  onOwnMessage: boolean;
  timestamp: Date;
}

export interface ChangeEntry {
  key: ChannelKey;
  messageIds: number[];
  kind: 'edit' | 'move' | 'delete';
  ready(only?: readonly number[]): Promise<MessageChangeEvent | null>;
}

export type OnMessageChange = (entry: ChangeEntry) => void;

export interface ChannelHistoryQuery {
  limit: number;
  quoteLookupBudget?: { remainingMs: number };
  beforeMessageId?: string;
  afterMessageId?: string;
  timeoutMs?: number;
}

export interface ChannelHistoryPage {
  messages: KeyedMessage[];
  scannedThrough: number | null;
  reachedNewest: boolean;
  unverifiedMessageIds?: string[];
}

export interface PlatformAdapter {
  readonly type: string;

  discoverChannels(): Promise<ChannelDescriptor[]>;

  onSent?: ((sent: SentRecord) => void) | null;

  publish(
    target: ChannelKey | { streamName: string },
    descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult>;

  describeChannels?(keys: ChannelKey[]): Promise<ChannelDescriptor[]>;

  sendTyping?(
    key: ChannelKey,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void>;

  fetchContext(
    key: ChannelKey,
    descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<Omit<ContextInjection, 'namespace'> | null>;

  fetchHistory?(key: ChannelKey, query: ChannelHistoryQuery): Promise<ChannelHistoryPage>;

  /**
   * Without a subscription, a public stream's edits and reactions come only
   * for messages that mention the bot, and a private stream sends at most the
   * messages that mention it.
   */
  ensureSubscribed?(key: ChannelKey): Promise<void>;

  acknowledge?(key: ChannelKey | null, messageId: string, value?: string): Promise<string>;

  deleteMessage?(channelId: string, messageId: string): Promise<void>;

  noteSelfDeleted?(messageId: number): void;
  forgetSelfDeleted?(messageId: number): void;
  whereSeen?(messageId: string): { key: ChannelKey; topic: string } | null;

  startEvents(
    onMessage: OnIncomingMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnReaction,
    onMessageChange?: OnMessageChange,
  ): void;

  stopEvents(): void;

  refreshStreams?(why?: string, force?: boolean): Promise<void>;
}
