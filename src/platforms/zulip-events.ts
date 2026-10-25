/**
 * Failure shapes of the client stack (`zulip-js@2.1.0` → `isomorphic-fetch` →
 * `node-fetch@2.7.0`):
 *
 *   - Queue expiry (BAD_EVENT_QUEUE_ID) is an HTTP 400 with a valid JSON body
 *     `{ result: 'error', code: 'BAD_EVENT_QUEUE_ID', msg: ... }`. node-fetch
 *     does not throw on 4xx and the body parses, so zulip-js `retrieve`
 *     returns this object rather than throwing. Zulip offers no gap recovery
 *     for a dead queue: events between the last delivered message and
 *     re-registration are lost.
 *   - Transport failures (ECONNRESET, timeouts, non-JSON proxy/HTML error
 *     bodies) throw a node-fetch `FetchError`. A 502 HTML page rejects
 *     `response.json()` with `FetchError('invalid json response body at <url>
 *     reason: ...', 'invalid-json')`; a dropped connection rejects with
 *     `FetchError('request to <url> failed, reason: ...', 'system')`. Both
 *     embed the request URL, `?queue_id=...` included, so sniffing the message
 *     for 'queue_id' would misclassify a transient failure as expiry.
 */

import type { PlatformSystemEvent, OnSystemEvent } from './adapter.js';

export interface ZulipEventMessage {
  id: number;
  sender_id: number;
  sender_full_name: string;
  sender_email: string;
  display_recipient: string | { email: string; full_name: string; id: number }[];
  subject: string;
  content: string;
  timestamp: number;
  type: string;
  /** Present on stream messages; absent on direct messages. */
  stream_id?: number;
}

/** `flags` are the receiving user's message flags from the event envelope
 * (e.g. 'mentioned', 'wildcard_mentioned'), computed server-side by Zulip. */
export type OnZulipMessage = (streamName: string | null, message: ZulipEventMessage, flags: string[]) => void;

export interface ZulipReactionEvent {
  op: 'add' | 'remove';
  emoji_name: string;
  emoji_code: string;
  reaction_type: string;
  message_id: number;
  user_id: number;
  user?: { user_id?: number; full_name?: string; email?: string };
}

export type OnZulipReaction = (event: ZulipReactionEvent) => void;

/**
 * Zulip sends `orig_content` only when the content changed, and `content`
 * then and on a re-render that changed nothing the author wrote, such as a
 * link preview arriving (`rendering_only`). It sends `subject` only when the
 * topic changed, `orig_subject` with every move, and `new_stream_id` only
 * when the message moved to another stream.
 */
export interface ZulipUpdateMessageEvent {
  message_id: number;
  /** Every message the change touched — many for a topic move with `propagate_mode`. */
  message_ids?: number[];
  /** Who made the change — the acting user, despite the published docs
   *  calling it "the user who sent the message" (zerver/actions/
   *  message_edit.py sets it from the editor); null for a server-side change. */
  user_id?: number | null;
  edit_timestamp?: number;
  rendering_only?: boolean;
  content?: string;
  orig_content?: string;
  subject?: string;
  orig_subject?: string;
  propagate_mode?: string;
  stream_id?: number;
  new_stream_id?: number;
  /** The receiving user's flags after the change — `mentioned` is Zulip's
   *  verdict on the NEW content. */
  flags?: string[];
}

export interface ZulipDeleteMessageEvent {
  message_id?: number;
  message_ids?: number[];
  message_type?: 'stream' | 'private';
  stream_id?: number;
  topic?: string;
}

type UnixSeconds = number;

export type ZulipMessageChange =
  | {
      kind: 'edit';
      messageId: number;
      messageIds: number[];
      actorId: number | null;
      editedAt: UnixSeconds;
      content: string | null;
      origContent: string | null;
      /** Zulip omits `subject` for a stream-only move but still sends `orig_subject`. */
      topic: string | null;
      origTopic: string | null;
      streamId: number | null;
      newStreamId: number | null;
      flags: string[];
    }
  | {
      kind: 'delete';
      messageIds: number[];
      messageType: 'stream' | 'private';
      streamId: number | null;
      topic: string | null;
    };

export type OnZulipMessageChange = (change: ZulipMessageChange) => void;

export interface ZulipRetrieveResponse {
  events?: ({ id: number; type: string; message?: ZulipEventMessage; flags?: string[] }
    & Partial<ZulipReactionEvent>
    & Partial<ZulipUpdateMessageEvent>
    & Partial<ZulipDeleteMessageEvent>)[];
  result?: string;
  code?: string;
  msg?: string;
}

export interface ZulipEventClient {
  queues: {
    register(params: Record<string, unknown>): Promise<{
      queue_id: string;
      last_event_id: number;
      /** Present on a returned (not thrown) error: zulip-js hands 4xx bodies back as values. */
      result?: string;
      msg?: string;
      code?: string;
    }>;
  };
  events: {
    retrieve(params: { queue_id: string; last_event_id: number }): Promise<ZulipRetrieveResponse>;
  };
}

export interface ZulipEventLoopOptions {
  baseBackoffMs?: number;
  maxBackoffMs?: number;
  degradedThreshold?: number;
  sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_BASE_BACKOFF_MS = 2000;
const DEFAULT_MAX_BACKOFF_MS = 60_000;
const DEFAULT_DEGRADED_THRESHOLD = 3;

export class ZulipEventLoop {
  private stopped = false;
  private queueId: string | null = null;
  private pendingGap: {
    queueId: string;
    lastMessageId: number | null;
    lastMessageTimestamp: number | null;
  } | null = null;

  private lastMessageId: number | null = null;
  private lastMessageTimestamp: number | null = null;

  private consecutiveFailures = 0;
  private degradedActive = false;

  private readonly baseBackoffMs: number;
  private readonly maxBackoffMs: number;
  private readonly degradedThreshold: number;
  private readonly sleepFn: (ms: number) => Promise<void>;

  constructor(options: ZulipEventLoopOptions = {}) {
    this.baseBackoffMs = options.baseBackoffMs ?? DEFAULT_BASE_BACKOFF_MS;
    this.maxBackoffMs = options.maxBackoffMs ?? DEFAULT_MAX_BACKOFF_MS;
    this.degradedThreshold = options.degradedThreshold ?? DEFAULT_DEGRADED_THRESHOLD;
    this.sleepFn = options.sleep ?? ((ms) => new Promise(resolve => setTimeout(resolve, ms)));
  }

  async start(
    zulipClient: ZulipEventClient,
    onMessage: OnZulipMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnZulipReaction,
    onChange?: OnZulipMessageChange,
  ): Promise<void> {
    while (!this.stopped) {
      try {
        await this.pollLoop(zulipClient, onMessage, onSystemEvent, onReaction, onChange);
      } catch (error) {
        if (this.stopped) return;
        console.error('Zulip event loop error, restarting in 5s:', error);
        await this.sleep(5000);
      }
    }
  }

  stop(): void {
    this.stopped = true;
  }

  private async pollLoop(
    zulipClient: ZulipEventClient,
    onMessage: OnZulipMessage,
    onSystemEvent?: OnSystemEvent,
    onReaction?: OnZulipReaction,
    onChange?: OnZulipMessageChange,
  ): Promise<void> {
    // Two zulip-js quirks to work around:
    //   - Booleans crash FormData serialization; pass "true"/"false" as strings.
    //   - Arrays must be raw JS arrays (the library JSON.stringifies them);
    //     pre-stringified JSON produces "event_types is not a list" at Zulip.
    // Zulip delivers exactly the registered types: without 'update_message'
    // and 'delete_message' an edit to a message the agent was mentioned in
    // never arrived at all (#22).
    // `client_capabilities` must be pre-stringified: zulip-js JSON-encodes
    // arrays only.
    // Without bulk_message_deletion a topic deletion is one event per
    // message. `notification_settings_null` is spelled out at its documented
    // default (false): Zulip 12 validates client_capabilities as a whole
    // model and rejects the registration with "client_capabilities
    // [\"notification_settings_null\"] field is missing" when it is absent.
    const registration = await zulipClient.queues.register({
      event_types: ['message', 'reaction', 'update_message', 'delete_message'],
      all_public_streams: 'true',
      apply_markdown: 'false',
      client_capabilities: JSON.stringify({ notification_settings_null: false, bulk_message_deletion: true }),
    });

    // A refused registration comes back as a value, not a throw.
    if (registration?.result === 'error' || typeof registration?.queue_id !== 'string') {
      throw new Error(
        `Zulip event queue registration failed: ${registration?.msg ?? 'no queue_id in the response'}` +
          (registration?.code ? ` (${registration.code})` : ''),
      );
    }

    this.queueId = registration.queue_id;
    let lastEventId = registration.last_event_id;

    console.error(`Zulip event queue registered: ${this.queueId}`);

    if (this.pendingGap) {
      const gap = this.pendingGap;
      this.pendingGap = null;
      const since = gap.lastMessageId != null
        ? `after message id ${gap.lastMessageId}` +
          (gap.lastMessageTimestamp != null
            ? ` (${new Date(gap.lastMessageTimestamp * 1000).toISOString()})`
            : '')
        : 'since the last delivered message';
      this.emitSystemEvent(onSystemEvent, {
        kind: 'gap',
        text:
          `Zulip event queue ${gap.queueId} expired and was re-registered as ${this.queueId}. ` +
          `Messages arriving ${since} and before re-registration may have been missed. ` +
          `Check recent channel history from that point if continuity matters.`,
        metadata: {
          platform: 'zulip',
          expiredQueueId: gap.queueId,
          lastMessageId: gap.lastMessageId,
          lastMessageTimestamp: gap.lastMessageTimestamp,
          newQueueId: this.queueId,
        },
      });
    }

    while (!this.stopped) {
      try {
        const response = await zulipClient.events.retrieve({
          queue_id: this.queueId,
          last_event_id: lastEventId,
        });

        if (response?.result === 'error' && response.code === 'BAD_EVENT_QUEUE_ID') {
          console.error('Zulip event queue expired, re-registering...');
          this.pendingGap = {
            queueId: this.queueId ?? 'unknown',
            lastMessageId: this.lastMessageId,
            lastMessageTimestamp: this.lastMessageTimestamp,
          };
          return;
        }

        if (!response || !Array.isArray(response.events)) {
          this.consecutiveFailures++;
          const delay = this.backoffDelay(this.consecutiveFailures);
          console.error(
            `Zulip events.retrieve returned no events array (attempt ${this.consecutiveFailures}); retrying in ${delay}ms`,
          );
          this.maybeEmitDegraded(onSystemEvent, 'events.retrieve returned a malformed response (no events array)');
          await this.sleep(delay);
          continue;
        }

        this.maybeEmitRecovered(onSystemEvent);
        this.consecutiveFailures = 0;

        for (const event of response.events) {
          lastEventId = event.id;

          if (event.type === 'reaction' && onReaction && typeof event.message_id === 'number') {
            try {
              onReaction({
                op: event.op === 'remove' ? 'remove' : 'add',
                emoji_name: String(event.emoji_name ?? ''),
                emoji_code: String(event.emoji_code ?? ''),
                reaction_type: String(event.reaction_type ?? 'unicode_emoji'),
                message_id: event.message_id,
                user_id: Number(event.user_id),
                user: event.user,
              });
            } catch (handlerError) {
              console.error('Zulip event loop: onReaction handler threw:', handlerError);
            }
            continue;
          }

          if (event.type === 'update_message') {
            if (onChange && typeof event.message_id === 'number' && event.rendering_only !== true) {
              const contentChanged = typeof event.orig_content === 'string';
              // `orig_subject` accompanies every move; `subject` only a topic rename.
              const moved = typeof event.orig_subject === 'string';
              const topicChanged = moved && typeof event.subject === 'string' && event.subject !== event.orig_subject;
              const streamChanged = typeof event.new_stream_id === 'number';
              if (contentChanged || topicChanged || streamChanged) {
                try {
                  onChange({
                    kind: 'edit',
                    messageId: event.message_id,
                    messageIds: Array.isArray(event.message_ids) && event.message_ids.length > 0
                      ? event.message_ids.filter((id): id is number => typeof id === 'number')
                      : [event.message_id],
                    actorId: typeof event.user_id === 'number' ? event.user_id : null,
                    editedAt: typeof event.edit_timestamp === 'number' ? event.edit_timestamp : Math.floor(Date.now() / 1000),
                    content: contentChanged && typeof event.content === 'string' ? event.content : null,
                    origContent: contentChanged ? String(event.orig_content) : null,
                    topic: topicChanged ? String(event.subject) : null,
                    origTopic: moved ? String(event.orig_subject) : null,
                    streamId: typeof event.stream_id === 'number' ? event.stream_id : null,
                    newStreamId: streamChanged ? Number(event.new_stream_id) : null,
                    flags: Array.isArray(event.flags) ? event.flags : [],
                  });
                } catch (handlerError) {
                  console.error('Zulip event loop: onChange handler threw:', handlerError);
                }
              }
            }
            continue;
          }

          if (event.type === 'delete_message') {
            const ids = Array.isArray(event.message_ids)
              ? event.message_ids.filter((id): id is number => typeof id === 'number')
              : typeof event.message_id === 'number' ? [event.message_id] : [];
            if (onChange && ids.length > 0) {
              try {
                onChange({
                  kind: 'delete',
                  messageIds: ids,
                  messageType: event.message_type === 'private' ? 'private' : 'stream',
                  streamId: typeof event.stream_id === 'number' ? event.stream_id : null,
                  topic: typeof event.topic === 'string' ? event.topic : null,
                });
              } catch (handlerError) {
                console.error('Zulip event loop: onChange handler threw:', handlerError);
              }
            }
            continue;
          }

          if (event.type === 'message' && event.message) {
            const msg = event.message as ZulipEventMessage;
            // Anchor future gap markers on the last delivered message — the
            // event id is queue-local and useless once the queue is replaced.
            this.lastMessageId = msg.id;
            this.lastMessageTimestamp = msg.timestamp;

            // display_recipient is a string for stream messages, array for DMs
            const streamName = typeof msg.display_recipient === 'string'
              ? msg.display_recipient
              : null;

            const isStream = msg.type === 'stream' && streamName !== null;
            const isDm = msg.type === 'private';
            if (isStream || isDm) {
              // Zulip won't redeliver an event once the next poll acknowledges
              // it, so a handler failure is logged, not retried (at-most-once).
              try {
                onMessage(isStream ? streamName : null, msg, event.flags ?? []);
              } catch (handlerError) {
                console.error('Zulip event loop: onMessage handler threw:', handlerError);
              }
            }
          }
        }
      } catch (error: unknown) {
        if (this.stopped) return;

        const errMsg = error instanceof Error ? error.message : String(error);

        this.consecutiveFailures++;
        const delay = this.backoffDelay(this.consecutiveFailures);
        const nonJson = this.isNonJsonError(error, errMsg);
        if (nonJson) {
          console.error(
            `Zulip event poll got a non-JSON response (proxy/HTML error page?) (attempt ${this.consecutiveFailures}); retrying in ${delay}ms: ${errMsg}`,
          );
        } else {
          console.error(`Zulip event poll error (attempt ${this.consecutiveFailures}); retrying in ${delay}ms:`, error);
        }
        this.maybeEmitDegraded(
          onSystemEvent,
          nonJson ? 'upstream is returning non-JSON responses (proxy/HTML error pages)' : errMsg,
        );
        await this.sleep(delay);
      }
    }
  }

  private backoffDelay(consecutiveFailures: number): number {
    const exp = Math.min(consecutiveFailures - 1, 31);
    return Math.min(this.baseBackoffMs * 2 ** exp, this.maxBackoffMs);
  }

  /** node-fetch@2 rejects `response.json()` with a `FetchError` (type
   *  'invalid-json'), not a `SyntaxError`. */
  private isNonJsonError(error: unknown, errMsg: string): boolean {
    const name = (error as { name?: string })?.name;
    const type = (error as { type?: string })?.type;
    return (
      type === 'invalid-json' ||
      (name === 'FetchError' && /invalid json response body/i.test(errMsg)) ||
      /invalid json response body|unexpected token|not valid json|<html|<!doctype/i.test(errMsg)
    );
  }

  private maybeEmitDegraded(onSystemEvent: OnSystemEvent | undefined, reason: string): void {
    if (this.consecutiveFailures !== this.degradedThreshold) return;
    this.degradedActive = true;
    this.emitSystemEvent(onSystemEvent, {
      kind: 'degraded',
      text:
        `Zulip event polling has failed ${this.consecutiveFailures} times in a row (${reason}); ` +
        `real-time message delivery is degraded until it recovers.`,
      metadata: { platform: 'zulip', consecutiveFailures: this.consecutiveFailures, reason },
    });
  }

  private maybeEmitRecovered(onSystemEvent: OnSystemEvent | undefined): void {
    if (!this.degradedActive) return;
    const failures = this.consecutiveFailures;
    this.degradedActive = false;
    console.error(`Zulip event polling recovered after ${failures} consecutive failures`);
    this.emitSystemEvent(onSystemEvent, {
      kind: 'recovered',
      text:
        `Zulip event polling recovered after ${failures} consecutive failures; ` +
        `real-time message delivery is healthy again.`,
      metadata: { platform: 'zulip', recoveredAfter: failures },
    });
  }

  private emitSystemEvent(onSystemEvent: OnSystemEvent | undefined, event: PlatformSystemEvent): void {
    if (!onSystemEvent) return;
    try {
      onSystemEvent(event);
    } catch (error) {
      console.error('Zulip event loop: onSystemEvent callback threw:', error);
    }
  }

  private sleep(ms: number): Promise<void> {
    return this.sleepFn(ms);
  }
}
