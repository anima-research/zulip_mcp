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
 * Everything that crosses this seam names a channel by its key (see
 * channel-key.ts); channel ids are made only where something leaves for the
 * host, and descriptors carry the id the host is told. The rollback record
 * is the exception: `onSent` and `deleteMessage` carry the channel id.
 */

import type {
  ChannelDescriptor,
  ChannelsPublishResult,
  ContentBlock,
  ContextInjection,
} from '@animalabs/mcpl-core';
import type { ChannelKey, KeyedMessage } from '../channel-key.js';

/** A message this server sent, as the rollback record keeps it. */
export interface SentRecord {
  messageId: string;
  channelId: string;
  content: string;
}

/** `messageIds` lists every platform message a publish produced (a long
 *  text is chunked); `messageId` is the last of them. */
export type PublishResult = ChannelsPublishResult & { messageIds?: string[] };

/**
 * Routing hints for outgoing messages, derived by the MCPL layer from the
 * newest message ChannelManager.noteConversation took on the target channel
 * (its comment says which messages those are). The host's channels/publish
 * carries no thread information, so "reply where the conversation is" is
 * reconstructed server-side: Zulip maps threadId to a topic.
 */
export interface RoutingHints {
  /** threadId of that message, if any. */
  threadId?: string;
  /** metadata of that message, if any. */
  metadata?: Record<string, unknown>;
}

/**
 * A message from the event stream, placed in its channel when it arrives.
 * The server runs `ready` in the channel's turn: it does what may wait on
 * the platform, and answers null for a message that isn't for the host.
 * `newChannel` is set when the message belongs to a channel the adapter has
 * not described before (a DM from a new conversation) — the server
 * registers it with the host before routing.
 */
export interface IncomingEntry {
  key: ChannelKey;
  messageId: string;
  ready(changes?: readonly ChangeEntry[]): Promise<{ message: KeyedMessage; newChannel?: ChannelDescriptor } | null>;
}

export type OnIncomingMessage = (entry: IncomingEntry) => void;

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
  key: ChannelKey;
  messageId: string;
  /** Emoji name in the platform's vocabulary (Zulip: 'thumbs_up'). */
  emoji: string;
  /** The platform's code for the emoji (Zulip: codepoints for unicode emoji,
   *  the realm emoji id otherwise) — what a glyph-shaped suppression entry
   *  is matched against. */
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

export interface ReactionEntry extends ReactionEvent {
  placed(): ReactionEvent;
}

export type OnReaction = (entry: ReactionEntry) => void;

/**
 * A message the agent may have seen was edited, moved to another topic or
 * stream, or deleted — resolved to the channel the message lived in.
 * Adapters drop the bot's own edits that move nothing before invoking the
 * callback, and mark its moves `byBot`; a change someone else made to the
 * bot's message is news (`onOwnMessage`).
 */
export interface MessageChangeEvent {
  kind: 'edit' | 'move' | 'delete';
  key: ChannelKey;
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
  movedToKey: ChannelKey | null;
  /** The message moved to another stream, which `movedToKey` names when the adapter knows that stream. */
  leftChannel: boolean;
  /** The bot made the change. The adapter passes on only such a change that moves a message. */
  byBot: boolean;
  /** The content after an edit, cleaned like a delivered message; null for a
   *  move or a delete. */
  content: string | null;
  /** The content before the change, cleaned; null when unknown. */
  previousContent: string | null;
  /** The bot is mentioned in the message as it now reads (Zulip's verdict). */
  mentioned: boolean;
  /** Verified quote-reply addressing, independent of Zulip's mention flag. */
  replyToSelf?: boolean;
  /** True only for a resolved verdict (including content with no quote targets). */
  replyToSelfVerified?: boolean;
  previouslyReplyToSelf?: boolean | null;
  /** The bot was mentioned in the message as it read before the change, as
   *  far as the adapter knows (a deleted message keeps its last verdict);
   *  null when the message was not in the adapter's cache and only its
   *  post-change state could be read. */
  previouslyMentioned: boolean | null;
  /** A deletion event for a message that had just moved to a stream the bot
   *  cannot see: Zulip reports lost visibility as a deletion. */
  vanished: boolean;
  isDM: boolean;
  /** The changed message was authored by the bot. */
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

/** History request against one channel. */
export interface ChannelHistoryQuery {
  limit: number;
  /** Mutable allowance shared across a catch-up walk. Adapters subtract only
   *  milliseconds awaiting quote GETs, not history HTTP or host acceptance. */
  quoteLookupBudget?: { remainingMs: number };
  /** Exclusive: only messages older than this id. */
  beforeMessageId?: string;
  /** Exclusive: only messages newer than this id. */
  afterMessageId?: string;
  timeoutMs?: number;
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
  messages: KeyedMessage[];
  /** Newest id scanned (filtered rows included); null when the page was empty. */
  scannedThrough: number | null;
  /** The platform reports nothing newer than this page. */
  reachedNewest: boolean;
  /** Quote verdicts skipped for this page's GET/wait budget, not verified ambient. */
  unverifiedMessageIds?: string[];
}

export interface PlatformAdapter {
  /** Channel ID prefix and ChannelDescriptor.type, e.g. 'zulip'. */
  readonly type: string;

  /** Discover all channels visible to this connection. */
  discoverChannels(): Promise<ChannelDescriptor[]>;

  /** Set by the MCPL server: each message a publish posts is reported here
   *  as it is posted, so a rollback checkpoint can undo it. */
  onSent?: ((sent: SentRecord) => void) | null;

  /**
   * Deliver content to a channel. `descriptor` is the registered descriptor
   * when known. A stream name the directory does not hold is sent to as given.
   */
  publish(
    target: ChannelKey | { streamName: string },
    descriptor: ChannelDescriptor | undefined,
    content: ContentBlock[],
    hints?: RoutingHints,
  ): Promise<PublishResult>;

  /**
   * Describe channels — for a channel the bot just joined, whose descriptor
   * the server wants to register without a full discovery pass. A key the
   * adapter doesn't know can cost one read of the platform's channel list.
   * Unknown keys are omitted. Optional.
   */
  describeChannels?(keys: ChannelKey[]): Promise<ChannelDescriptor[]>;

  /**
   * Best-effort typing indicator. Optional — platforms without a usable
   * typing API simply omit it.
   */
  sendTyping?(
    key: ChannelKey,
    descriptor: ChannelDescriptor | undefined,
    metadata: Record<string, unknown> | undefined,
    op: 'start' | 'stop',
  ): Promise<void>;

  /**
   * Fetch recent channel history formatted as a context injection for
   * context/beforeInference. Returns null when there's nothing to inject.
   */
  fetchContext(
    key: ChannelKey,
    descriptor: ChannelDescriptor | undefined,
    historySize: number,
  ): Promise<Omit<ContextInjection, 'namespace'> | null>;

  /**
   * Channel history as incoming-shaped messages, oldest first. Optional —
   * without it channels/open cannot return backscroll and no catch-up
   * has anything to read.
   */
  fetchHistory?(key: ChannelKey, query: ChannelHistoryQuery): Promise<ChannelHistoryPage>;

  /**
   * Make sure the platform delivers events for this channel. Without a
   * subscription, a public stream's edits and reactions come only for
   * messages that mention the bot, and a private stream sends at most the
   * messages that mention it. Optional; idempotent.
   */
  ensureSubscribed?(key: ChannelKey): Promise<void>;

  /** channels/acknowledge — mark a message as seen on the surface (a reaction). Returns the representation used. */
  acknowledge?(key: ChannelKey | null, messageId: string, value?: string): Promise<string>;

  /** Delete one of the bot's own messages (rollback). */
  deleteMessage?(channelId: string, messageId: string): Promise<void>;

  /** A message the bot deleted through another path (a tool) — its delete
   *  event is the bot's own doing and must not surface as a change. */
  noteSelfDeleted?(messageId: number): void;
  /** That deletion failed: the message still exists. */
  forgetSelfDeleted?(messageId: number): void;
  placeOf?(messageId: string): { key: ChannelKey; topic: string } | null;

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

  refreshStreams?(why?: string, force?: boolean): Promise<void>;
}
