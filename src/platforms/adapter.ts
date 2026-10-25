/**
 * PlatformAdapter — the seam between the platform-agnostic MCPL layer
 * (ChannelManager, ContextProvider) and the platform-specific client.
 *
 * One implementer today (Zulip), but the seam is the documented shape a
 * platform plugs into, and it costs nothing to keep. An adapter owns one
 * platform connection and is responsible for:
 *   - discovering channels and describing them (ChannelDescriptor)
 *   - delivering outgoing publishes (with thread routing where supported)
 *   - fetching recent history for context/beforeInference injections
 *   - streaming real-time incoming messages (self-filtered)
 *
 * Channel IDs are prefixed with the adapter's `type` ('zulip:...'); the MCPL
 * layer routes purely on that prefix and never inspects the remainder.
 */

import type {
  ChannelDescriptor,
  ChannelsPublishResult,
  ContentBlock,
  ContextInjection,
  IncomingChannelMessage,
} from '@animalabs/mcpl-core';

/** `messageIds` lists every platform message a publish produced (a long
 *  text is chunked); `messageId` is the last of them. */
export type PublishResult = ChannelsPublishResult & { messageIds?: string[] };

/**
 * `channels/publish` carries no thread information (SPEC 0.5 §14.3), so "reply
 * where the conversation is" is reconstructed server-side: Zulip maps threadId
 * to a topic.
 */
export interface RoutingHints {
  /** threadId of that message, if any. */
  threadId?: string;
  /** metadata of that message, if any. */
  metadata?: Record<string, unknown>;
}

/**
 * Delivery callback. `newChannel` is set when the message belongs to a
 * channel the adapter has not described before (a DM from a new
 * conversation) — the server registers it with the host before routing.
 */
export type OnIncomingMessage = (message: IncomingChannelMessage, newChannel?: ChannelDescriptor) => void;

/**
 * Out-of-band condition on a platform connection that the host/agent should
 * know about, surfaced as a synthetic system message on the platform's open
 * channels:
 *   - 'gap': real-time delivery lost coverage (e.g. a Zulip event queue
 *     expired and was re-registered from "now") — messages may have been
 *     missed and the agent should consult channel history if it matters.
 *   - 'degraded': the event source is failing repeatedly; delivery is
 *     unreliable until it recovers.
 *   - 'recovered': the event source resumed after a 'degraded' condition;
 *     real-time delivery is healthy again. Pairs with 'degraded' so the agent
 *     isn't left believing delivery is broken forever.
 */
export interface PlatformSystemEvent {
  kind: 'gap' | 'degraded' | 'recovered';
  /** Human-readable description, addressed to the agent. */
  text: string;
  metadata?: Record<string, unknown>;
}

export type OnSystemEvent = (event: PlatformSystemEvent) => void;

/** A reaction added to or removed from a message, resolved to its channel. */
export interface ReactionEvent {
  action: 'add' | 'remove';
  channelId: string;
  messageId: string;
  /** Emoji name in the platform's vocabulary (Zulip: 'thumbs_up'). */
  emoji: string;
  /** Zulip's `emoji_code`: a unicode emoji's codepoints, a custom emoji's id, or
   *  the name of one of Zulip's own extra emoji. */
  emojiCode?: string;
  /** Zulip: 'unicode_emoji' | 'realm_emoji' | 'zulip_extra_emoji'. */
  emojiType?: string;
  reactorId: string;
  reactorName: string;
  /** The reacted-to message was authored by the bot. */
  onOwnMessage: boolean;
  /** One-line snippet of the reacted-to message, or null when unknown. */
  messageSnippet: string | null;
  timestamp: Date;
}

export type OnReaction = (event: ReactionEvent) => void;

/**
 * A message the agent may have seen was edited, moved to another topic or
 * stream, or deleted — resolved to the channel the message lived in.
 * Adapters drop the bot's own edits before invoking the callback; a change
 * someone else made to the bot's message is news (`onOwnMessage`).
 */
export interface MessageChangeEvent {
  kind: 'edit' | 'move' | 'delete';
  channelId: string;
  /** The changed message; a move or bulk delete names the one acted on and
   *  lists every affected id in `messageIds`. */
  messageId: string;
  messageIds: string[];
  /** The message's author, when known (a deleted message no longer in the
   *  adapter's cache is placed by stream alone). */
  authorId: string | null;
  authorName: string | null;
  authorEmail: string | null;
  /** Who made the change; null for a server-side change or when unknown
   *  (Zulip's delete event names no actor). */
  actorId: string | null;
  /** The topic after the change; '' for a direct message. */
  topic: string;
  /** The topic before a move; null when the topic did not change. */
  previousTopic: string | null;
  /** The channel the message moved to, when it left this one. */
  movedToChannelId: string | null;
  /** The content after an edit, cleaned like a delivered message; null for a
   *  move or a delete. */
  content: string | null;
  /** The content before the change, cleaned; null when unknown. */
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
  /** The changed message was authored by the bot. */
  onOwnMessage: boolean;
  timestamp: Date;
}

export type OnMessageChange = (event: MessageChangeEvent) => void;

/** History request against one channel, in the platform's own id space. */
export interface ChannelHistoryQuery {
  limit: number;
  /** Exclusive: only messages older than this id. */
  beforeMessageId?: string;
  /** Exclusive: only messages newer than this id. */
  afterMessageId?: string;
}

/**
 * One page of channel history. `messages` is what may reach the agent —
 * the bot's own messages and disallowed senders are already removed — while
 * `scannedThrough` is the newest id the fetch actually covered, removed rows
 * included: the cursor a pager advances on, so a page of nothing but the
 * bot's own messages does not read as the end of history.
 */
export interface ChannelHistoryPage {
  /** Oldest first. */
  messages: IncomingChannelMessage[];
  /** Newest id scanned (filtered rows included); null when the page was empty. */
  scannedThrough: number | null;
  /** The platform reports nothing newer than this page. */
  reachedNewest: boolean;
}

export interface PlatformAdapter {
  /** Channel ID prefix and ChannelDescriptor.type, e.g. 'zulip'. */
  readonly type: string;

  /** Discover all channels visible to this connection. */
  discoverChannels(): Promise<ChannelDescriptor[]>;

  /**
   * Deliver content to a channel. `descriptor` is the registered descriptor
   * when known; adapters fall back to parsing the channelId.
   */
  publish(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult>;

  /**
   * Describe channels by id without enumerating the platform — for a channel
   * the bot just joined, whose descriptor the server wants to register
   * without a full discovery pass. Unknown ids are omitted. Optional.
   */
  describeChannels?(channelIds: string[]): Promise<ChannelDescriptor[]>;

  /**
   * Best-effort typing indicator. Optional — platforms without a usable
   * typing API simply omit it.
   */
  sendTyping?(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void>;

  /**
   * Fetch recent channel history formatted as a context injection for
   * context/beforeInference. Returns null when there's nothing to inject.
   */
  fetchContext(
    channelId: string,
    descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<ContextInjection | null>;

  /**
   * Channel history as incoming-shaped messages, oldest first. Optional —
   * without it channels/open cannot return backscroll and the reconnect
   * sweep has nothing to scan.
   */
  fetchHistory?(channelId: string, query: ChannelHistoryQuery): Promise<ChannelHistoryPage>;

  /**
   * Make sure the platform delivers events for this channel — Zulip only
   * sends stream events to subscribers. Optional; idempotent; best-effort.
   */
  ensureSubscribed?(channelId: string): Promise<void>;

  /** channels/acknowledge — mark a message as seen on the surface (a reaction). Returns the representation used. */
  acknowledge?(channelId: string, messageId: string, value?: string): Promise<string>;

  /** Delete one of the bot's own messages (rollback). */
  deleteMessage?(channelId: string, messageId: string): Promise<void>;

  /** A message the bot deleted through another path (a tool) — its delete
   *  event is the bot's own doing and must not surface as a change. */
  noteSelfDeleted?(messageId: number): void;
  /** That deletion failed: the message still exists. */
  forgetSelfDeleted?(messageId: number): void;

  /**
   * Start delivering real-time messages. Adapters filter the bot's own
   * messages before invoking the callback.
   *
   * `onSystemEvent` (optional, and optional for adapters to use) receives
   * out-of-band conditions — delivery gaps, degraded polling — so the host
   * can surface them to the agent instead of losing them in stderr.
   * `onMessageChange` (optional, and optional for adapters to use) receives
   * edits, moves and deletions of messages already delivered.
   */
  startEvents(
    onMessage: OnIncomingMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnReaction,
    onMessageChange?: OnMessageChange,
  ): void;

  /** Stop event delivery and release platform resources. */
  stopEvents(): void;
}
