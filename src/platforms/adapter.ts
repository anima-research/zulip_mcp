
import type {
  ChannelDescriptor,
  ChannelsPublishResult,
  ContentBlock,
  ContextInjection,
  IncomingChannelMessage,
} from '@animalabs/mcpl-core';

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

export type OnIncomingMessage = (message: IncomingChannelMessage, newChannel?: ChannelDescriptor) => void;

export interface PlatformSystemEvent {
  kind: 'gap' | 'degraded' | 'recovered';
  text: string;
  metadata?: Record<string, unknown>;
}

export type OnSystemEvent = (event: PlatformSystemEvent) => void;

export interface ReactionEvent {
  action: 'add' | 'remove';
  channelId: string;
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

export type OnReaction = (event: ReactionEvent) => void;

export interface MessageChangeEvent {
  kind: 'edit' | 'move' | 'delete';
  channelId: string;
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
  movedToChannelId: string | null;
  content: string | null;
  previousContent: string | null;
  /** The bot is mentioned in the message as it now reads (Zulip's verdict). */
  mentioned: boolean;
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

export type OnMessageChange = (event: MessageChangeEvent) => void;

export interface ChannelHistoryQuery {
  limit: number;
  beforeMessageId?: string;
  afterMessageId?: string;
}

export interface ChannelHistoryPage {
  messages: IncomingChannelMessage[];
  scannedThrough: number | null;
  reachedNewest: boolean;
}

export interface PlatformAdapter {
  readonly type: string;

  discoverChannels(): Promise<ChannelDescriptor[]>;

  publish(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult>;

  describeChannels?(channelIds: string[]): Promise<ChannelDescriptor[]>;

  sendTyping?(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void>;

  fetchContext(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<ContextInjection | null>;

  fetchHistory?(channelId: string, query: ChannelHistoryQuery): Promise<ChannelHistoryPage>;

  ensureSubscribed?(channelId: string): Promise<void>;

  acknowledge?(channelId: string, messageId: string, value?: string): Promise<string>;

  deleteMessage?(channelId: string, messageId: string): Promise<void>;

  noteSelfDeleted?(messageId: number): void;
  forgetSelfDeleted?(messageId: number): void;

  startEvents(
    onMessage: OnIncomingMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnReaction,
    onMessageChange?: OnMessageChange,
  ): void;

  stopEvents(): void;
}
