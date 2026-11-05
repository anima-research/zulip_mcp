/**
 * Tool handlers and MCP resources — the stateful tool layer of this server.
 *
 * Owns the persistent monitoring state (`~/.zulip_mcp_state/<session>.json`):
 * which streams are monitored and the last-read message id per stream. The
 * MCPL layer (channels/context) is independent of this; tools and resources
 * are the plain-MCP surface every client gets.
 */

import { LOCATE_TIMEOUT_MS, withTimeout } from "./platforms/zulip.js";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { homedir } from "node:os";
import type { ContentBlock } from "@animalabs/mcpl-core";
import {
  cleanContent,
  fetchAttachmentBytes,
  parseZulipAttachmentUrl,
  toFetchResult,
} from "./content.js";
import type { SelfMove } from "./platforms/adapter.js";
import type { ZulipSession } from "./zulip-client.js";
import { assertApiSuccess, dmChannelIdFor, dmCounterparts, fetchAround, fetchHistory, parseDmChannelId, renderReactions, type ReactionSummary, type ZulipMessage, type ZulipRecipient, editedTrailer } from './history.js';
import { chunkMessage } from "./content.js";
import { messageLineHead } from "./message-line.js";
import { agentLineTimeFormatter } from "./timezone.js";
import {
  attachmentMarkdown,
  createZulipUploader,
  prepareAttachmentArg,
  prepareAttachments,
  resolveUploadPolicy,
  uploadPrepared,
  withAttachmentLinks,
  type UploadPolicy,
  type Uploader,
} from "./uploads.js";

/** What registering the streams `listen` joined achieved (MCPL mode only). */
export interface SubscriptionRegistration {
  announced: string[];
  local: string[];
  refused: string[];
  reason?: string;
}

/** A message this server sent through a tool — recorded for rollback. */
export interface SentRecord {
  messageId: string;
  channelId: string;
  content: string;
}

/** What of a message's reactions the model may see. */
export interface ReactionPolicy {
  /** `code`/`type` are Zulip's `emoji_code`/`reaction_type` when known. */
  suppressed(emojiName: string, emojiCode?: string, reactionType?: string): boolean;
}

const SHOW_ALL: ReactionPolicy = { suppressed: () => false };

/** Project reactions through the policy. */
export function projectReactions(reactions: ReactionSummary[], policy: ReactionPolicy): ReactionSummary[] {
  return reactions.filter((r) => !policy.suppressed(r.name, r.code, r.type));
}

/**
 * Withhold suppressed reactions from raw Zulip message payloads before any
 * model-visible serialization (the legacy `raw` format hands the payload to
 * the model as is).
 */
export function stripSuppressedReactions<T extends { reactions?: unknown }>(messages: T[], policy: ReactionPolicy): T[] {
  return messages.map((m) => {
    if (!Array.isArray(m.reactions)) return m;
    const kept = (m.reactions as { emoji_name?: unknown; emoji_code?: unknown; reaction_type?: unknown }[]).filter(
      (r) => !policy.suppressed(
        typeof r?.emoji_name === "string" ? r.emoji_name : "",
        typeof r?.emoji_code === "string" ? r.emoji_code : undefined,
        typeof r?.reaction_type === "string" ? r.reaction_type : undefined,
      ),
    );
    return { ...m, reactions: kept };
  });
}

export interface ChannelState {
  channelName: string;
  lastReadMessageId: number | string;
  subscribed: boolean;
}

interface SessionState {
  sessionId: string;
  userId?: string;
  monitoredChannels: Record<string, ChannelState>;
}

export interface ResourceDescriptor {
  uri: string;
  name: string;
  description: string;
  mimeType: string;
}

export interface ResourceContents {
  contents: { uri: string; mimeType: string; text: string }[];
}

/** MCP `tools/call` result shape, plus the MCPL §8 `state` checkpoint carrier. */
export interface ToolCallResult {
  content: ContentBlock[];
  isError?: boolean;
  state?: { featureSet: string; checkpoint: string; parent: string | null };
}

const STATE_DIR = join(homedir(), ".zulip_mcp_state");

// Helper function to parse date strings
export function parseDate(dateStr: string | undefined, defaultDate: Date): Date {
  if (!dateStr) return defaultDate;

  const lower = dateStr.toLowerCase();
  const now = new Date();

  if (lower === 'now') return now;
  if (lower === 'today') {
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    return today;
  }
  if (lower === 'yesterday') {
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    yesterday.setHours(0, 0, 0, 0);
    return yesterday;
  }

  return new Date(dateStr);
}

// Helper function to format messages
export function formatMessages(messages: any[], format: string): string {
  if (format === 'raw') {
    return JSON.stringify(messages, null, 2);
  }

  if (format === 'summary') {
    const summary = messages.map(msg => {
      const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
      const content = cleanContent(msg.content).substring(0, 80);
      return `[${time}] [${msg.subject}] ${msg.sender_full_name}: ${content}...`;
    }).join('\n');
    return `📊 ${messages.length} messages\n\n${summary}`;
  }

  // Detailed format
  const formatted = messages.map(msg => {
    const time = new Date(msg.timestamp * 1000).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit' });
    const date = new Date(msg.timestamp * 1000).toLocaleDateString('en-US');
    const content = cleanContent(msg.content);

    return `[${date} ${time}] 📝 Topic: ${msg.subject}\n👤 ${msg.sender_full_name}\n💬 ${content}\n`;
  }).join('\n' + '─'.repeat(80) + '\n\n');

  return `📊 Retrieved ${messages.length} messages\n${'='.repeat(80)}\n\n${formatted}`;
}

function clampInt(value: unknown, fallback: number, min: number, max: number): number {
  const n = typeof value === "number" && Number.isFinite(value) ? Math.floor(value) : fallback;
  return Math.max(min, Math.min(max, n));
}

function numberOrUndefined(value: unknown): number | undefined {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim() !== "" && Number.isFinite(Number(value))) return Number(value);
  return undefined;
}

/** UTC ISO seconds -- the line time when no agent formatter is supplied. */
export function utcLineTime(d: Date): string {
  return Number.isNaN(d.getTime()) ? "" : d.toISOString().replace(/\.\d{3}Z$/, "Z");
}

/**
 * One line per message in the shared line shape (message-line.ts), id first
 * so the agent can fetch_around(id). The anchor of a fetch_around window is
 * marked so it stands out. The runtime passes the agent line-time formatter,
 * so these lines read the same as live delivery and the `<missed>` block.
 */
export function formatHistoryLines(
  messages: ZulipMessage[],
  anchorId?: number,
  policy: ReactionPolicy = SHOW_ALL,
  selfUserId: number | null = null,
  formatTime: (d: Date) => string = utcLineTime,
): string {
  if (messages.length === 0) return "(no messages)";
  return messages.map((m) => {
    const head = messageLineHead({
      id: m.id,
      time: Number.isNaN(m.timestamp.getTime()) ? "" : formatTime(m.timestamp),
      stream: m.isDm ? null : (m.streamName ?? ""),
      topic: m.topic,
      author: m.authorName,
      mentioned: m.mentioned,
    });
    const mark = m.id === anchorId ? " <<" : "";
    const att = m.attachments.length > 0 ? ` [attachments: ${m.attachments.map((a) => a.path).join(", ")}]` : "";
    const reactions = renderReactions(projectReactions(m.reactions, policy), selfUserId);
    return `${head}${m.cleanContent}${editedTrailer(m)}${att}${reactions}${mark}`;
  }).join("\n");
}

/**
 * Shape a handler's return value into an MCP `tools/call` result. Handlers
 * that need to return image/non-text content set `_content` directly.
 */
export function toToolCallResult(result: unknown): ToolCallResult {
  if (result && typeof result === "object" && Array.isArray((result as any)._content)) {
    return { content: (result as any)._content };
  }
  return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
}

export class ZulipToolRuntime {
  private readonly zulipClient: any;
  private readonly sessionId: string;
  private readonly monitoredChannels = new Map<string, ChannelState>();

  private reactionPolicy: ReactionPolicy = SHOW_ALL;

  /** Set by the MCPL server: every message sent by a tool is reported here
   *  so a rollback checkpoint can undo it. */
  onSent: ((sent: SentRecord) => void) | null = null;
  /** Successful moves reconcile routing/cache without waiting for an echo. */
  onMoved: ((move: SelfMove) => void) | null = null;
  onMoveStarted: ((messageId: number) => ReadonlyMap<string, number>) | null = null;
  onMessageObserved: ((messageId: number) => void) | null = null;
  /** Called BEFORE delete_message is sent (the delete event can arrive before
   *  the response does), so the adapter recognises the echo as its own. */
  onDeleted: ((messageId: string) => void) | null = null;
  /** Called when that delete_message failed: the message still exists, and
   *  someone else's later deletion must surface. */
  onDeleteFailed: ((messageId: string) => void) | null = null;

  /** Set by the MCPL server: streams `listen` just subscribed the bot to, so
   *  they can be registered without waiting for a refresh or an event (#20).
   *  Awaited, and its result is reported by `listen`: an announcement the
   *  host did not confirm is the agent's business, not a stderr line. */
  onSubscribed: ((streams: string[]) => SubscriptionRegistration | Promise<SubscriptionRegistration | void> | void) | null = null;

  private readonly uploader: Uploader | null;
  private readonly uploadPolicy: UploadPolicy;
  /** Placement reads remain bounded; tests may use a shorter deadline. */
  private readonly moveReadTimeoutMs: number;
  /** Line time for fetch_history / fetch_around, shared with live delivery. */
  private readonly formatTime: (d: Date) => string;

  constructor(
    private readonly session: ZulipSession,
    private readonly stateDir: string = STATE_DIR,
    options: { uploader?: Uploader; uploadPolicy?: UploadPolicy; formatTime?: (d: Date) => string; moveReadTimeoutMs?: number } = {},
  ) {
    this.moveReadTimeoutMs = options.moveReadTimeoutMs ?? LOCATE_TIMEOUT_MS;
    this.zulipClient = session.client;
    this.sessionId = session.sessionId;
    this.formatTime = options.formatTime ?? agentLineTimeFormatter();
    this.uploader = options.uploader ?? (session.realm ? createZulipUploader(session) : null);
    this.uploadPolicy = options.uploadPolicy ?? resolveUploadPolicy(process.env, session.maxUploadBytes);
    this.loadState();
  }

  private requireUploader(): Uploader {
    if (!this.uploader) throw new Error("file uploads need the realm URL and bot credentials (ZULIP_REALM + ZULIP_EMAIL/ZULIP_API_KEY, or a zuliprc)");
    return this.uploader;
  }

  /** Validate and upload every `attachments` entry of a send call; throws before anything is sent. */
  private async uploadFor(args: Record<string, any>) {
    if (args.attachments === undefined || args.attachments === null) return [];
    if (Array.isArray(args.attachments) && args.attachments.length === 0) return [];
    const uploader = this.requireUploader();
    return uploadPrepared(uploader, await prepareAttachments(args.attachments, this.uploadPolicy), this.uploadPolicy);
  }

  /** Reaction suppression for history rendering (the filters plane). */
  setReactionPolicy(policy: ReactionPolicy): void {
    this.reactionPolicy = policy;
  }

  /** Resolve and check a reply before uploading files or sending any chunks. */
  private async replyDestination(args: Record<string, any>) {
    const id = numberOrUndefined(args.reply_to);
    if (!Number.isSafeInteger(id)) throw new Error("reply_to must be an integer message id");
    if (args.to !== undefined || args.topic !== undefined) throw new Error("reply_to sets the destination; omit to/topic");
    const result = await this.zulipClient.messages.getById({ message_id: id });
    if (result?.result === "error") {
      // Keep the API code as well as its wording: missing/deleted/inaccessible
      // messages must never fall back to a caller-supplied destination.
      throw new Error(`Zulip refused reply_to ${id}${result.code ? ` (${result.code})` : ""}: ${result.msg ?? "unknown error"}`);
    }
    const message = result?.message;
    const unreadable = () => new Error(`reply_to ${id} has no readable destination`);
    if (!message || !["stream", "private"].includes(message.type)) throw unreadable();
    const type = args.type === "direct" ? "private" : args.type === "channel" ? "stream" : args.type;
    if (type !== undefined && type !== message.type) {
      throw new Error(`reply_to ${id}: type disagrees with the referenced message`);
    }
    if (message.type === "stream") {
      const name = message.display_recipient;
      if (typeof name !== "string" || !name || typeof message.subject !== "string") throw unreadable();
      return { type: message.type, to: message.stream_id ?? name, topic: message.subject, channelId: `zulip:${name}` };
    }

    const recipients: ZulipRecipient[] = message.display_recipient;
    if (!Array.isArray(recipients) || recipients.length === 0 || recipients.some((r) => !Number.isSafeInteger(r?.id))) {
      throw unreadable();
    }
    const ids = recipients.map((r) => r.id);
    return { type: message.type, to: ids, topic: undefined, channelId: dmChannelIdFor(dmCounterparts(recipients, this.session.selfUserId).map((r) => r.id)) };
  }

  // ── Persistent monitoring state ──

  private stateFile(): string {
    return join(this.stateDir, `${this.sessionId}.json`);
  }

  private loadState(): void {
    const stateFile = this.stateFile();
    if (existsSync(stateFile)) {
      try {
        const data = JSON.parse(readFileSync(stateFile, "utf-8")) as SessionState;
        this.monitoredChannels.clear();

        if (data.monitoredChannels) {
          Object.values(data.monitoredChannels).forEach(channel => {
            this.monitoredChannels.set(channel.channelName, channel);
          });
        }

        console.error(`Loaded state for session ${this.sessionId}: ${this.monitoredChannels.size} Zulip channels`);
      } catch (error) {
        console.error(`Failed to load state: ${error}`);
      }
    }
  }

  private saveState(): void {
    if (!this.sessionId) return;

    try {
      if (!existsSync(this.stateDir)) {
        mkdirSync(this.stateDir, { recursive: true });
      }

      const state: SessionState = {
        sessionId: this.sessionId,
        monitoredChannels: Object.fromEntries(this.monitoredChannels),
      };

      writeFileSync(this.stateFile(), JSON.stringify(state, null, 2));
    } catch (error) {
      console.error(`Failed to save state: ${error}`);
    }
  }

  // ── Tools ──

  /** Execute a tool. Returns the raw handler result; see toToolCallResult. */
  async handleToolCall(name: string, args: any): Promise<any> {
    const zulipClient = this.zulipClient;
    const monitoredChannels = this.monitoredChannels;
    args = args ?? {};

    switch (name) {
      case "start_monitoring": {
        const channels: string[] = args.channels;
        const results: any[] = [];

        for (const channelName of channels) {
          try {
            // Get latest message to establish baseline
            const messages = await zulipClient.messages.retrieve({
              anchor: "newest",
              num_before: 1,
              num_after: 0,
              narrow: [["stream", channelName]],
            });
            for (const message of messages.messages ?? []) this.onMessageObserved?.(message.id);

            const lastMessageId = messages.messages.length > 0
              ? messages.messages[0].id
              : 0;

            monitoredChannels.set(channelName, {
              channelName,
              lastReadMessageId: lastMessageId,
              subscribed: true,
            });

            results.push({
              channel: channelName,
              status: "monitoring",
              last_message_id: lastMessageId,
            });
          } catch (error) {
            results.push({
              channel: channelName,
              status: "error",
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

        this.saveState();

        return {
          session_id: this.sessionId,
          monitored_count: monitoredChannels.size,
          channels: results,
        };
      }

      case "stop_monitoring": {
        const channels: string[] = args.channels;

        if (!channels || channels.length === 0) {
          // Stop all monitoring
          const stopped = Array.from(monitoredChannels.keys());
          monitoredChannels.clear();
          this.saveState();
          return {
            message: "Stopped monitoring all channels",
            stopped_channels: stopped,
          };
        }

        const stopped: string[] = [];
        for (const channel of channels) {
          if (monitoredChannels.has(channel)) {
            monitoredChannels.delete(channel);
            stopped.push(channel);
          }
        }

        this.saveState();

        return {
          stopped_channels: stopped,
          still_monitoring: Array.from(monitoredChannels.keys()),
        };
      }

      case "listen": {
        const channels: string[] = args.channels;
        if (!Array.isArray(channels) || channels.length === 0) {
          return { error: "channels array is required and must be non-empty" };
        }
        try {
          const result = await zulipClient.users.me.subscriptions.add({
            subscriptions: channels.map(name => ({ name })),
          });
          // A stream the bot just joined is not in the host's channel list
          // yet; registering it here is what makes it openable without a
          // refresh or a restart (#20).
          const unauthorized: string[] = Array.isArray(result?.unauthorized) ? result.unauthorized : [];
          const joined = channels.filter((name) => !unauthorized.includes(name));
          const registration = result?.result === "success" && joined.length > 0
            ? await this.onSubscribed?.(joined)
            : undefined;
          return {
            result: result?.result,
            subscribed: result?.subscribed ?? {},
            already_subscribed: result?.already_subscribed ?? {},
            unauthorized: result?.unauthorized ?? [],
            msg: result?.msg,
            ...(registration
              ? {
                  registration,
                  note: registration.announced.length > 0
                    ? `Registered ${registration.announced.length} channel(s) with the host; they can be opened now.`
                    : registration.refused.length > 0
                      ? `The host refused ${registration.refused.join(", ")}.`
                      : registration.local.length > 0
                        ? `The host did not confirm ${registration.local.join(", ")} (${registration.reason ?? "no answer"}); usable here and re-announced on the next refresh.`
                        : undefined,
                }
              : {}),
          };
        } catch (error) {
          return {
            error: error instanceof Error ? error.message : String(error),
          };
        }
      }

      case "unlisten": {
        const channels: string[] = args.channels;
        if (!Array.isArray(channels) || channels.length === 0) {
          return { error: "channels array is required and must be non-empty" };
        }
        try {
          const result = await zulipClient.users.me.subscriptions.remove({
            subscriptions: JSON.stringify(channels),
          });
          return {
            result: result?.result,
            removed: result?.removed ?? [],
            not_removed: result?.not_removed ?? [],
            msg: result?.msg,
          };
        } catch (error) {
          return {
            error: error instanceof Error ? error.message : String(error),
          };
        }
      }

      case "get_monitored_channels": {
        const channels = Array.from(monitoredChannels.values());
        return {
          monitored_count: channels.length,
          channels: channels.map(c => ({
            name: c.channelName,
            last_read_message_id: c.lastReadMessageId,
            subscribed: c.subscribed,
          })),
        };
      }

      case "get_channel_history": {
        // Parse dates
        const today = new Date();
        today.setHours(0, 0, 0, 0);
        const now = new Date();

        const startDate = parseDate(args.start_date, today);
        const endDate = parseDate(args.end_date, now);
        const startTimestamp = Math.floor(startDate.getTime() / 1000);
        const endTimestamp = Math.floor(endDate.getTime() / 1000);

        // Build narrow filters
        const narrow: any[] = [["stream", args.channel]];
        if (args.topic) {
          narrow.push(["topic", args.topic]);
        }

        // Retrieve messages
        const maxMessages = args.max_messages || 500;
        const result = await zulipClient.messages.retrieve({
          anchor: "newest",
          num_before: maxMessages,
          num_after: 0,
          narrow: narrow,
        });
        for (const message of result.messages ?? []) this.onMessageObserved?.(message.id);

        // Filter by date range
        const filteredMessages = result.messages.filter((msg: any) => {
          return msg.timestamp >= startTimestamp && msg.timestamp <= endTimestamp;
        });

        // Auto-monitor: Start monitoring this channel if requested (default: true)
        const autoMonitor = args.auto_monitor !== false;
        let monitoringStatus = "not_monitored";

        if (autoMonitor && result.messages.length > 0) {
          const latestMessageId = Math.max(...result.messages.map((m: any) => m.id));

          if (!monitoredChannels.has(args.channel)) {
            monitoredChannels.set(args.channel, {
              channelName: args.channel,
              lastReadMessageId: latestMessageId,
              subscribed: true,
            });
            monitoringStatus = "started_monitoring";
            this.saveState();
          } else {
            // Update last read
            const state = monitoredChannels.get(args.channel)!;
            state.lastReadMessageId = latestMessageId;
            monitoringStatus = "updated_read_position";
            this.saveState();
          }
        } else if (monitoredChannels.has(args.channel)) {
          monitoringStatus = "already_monitored";
        }

        // Format output — suppressed reaction markers never reach the model, raw format included.
        const format = args.format || "detailed";
        const formattedOutput = formatMessages(stripSuppressedReactions(filteredMessages, this.reactionPolicy), format);

        return {
          channel: args.channel,
          topic: args.topic,
          start_date: startDate.toISOString(),
          end_date: endDate.toISOString(),
          message_count: filteredMessages.length,
          total_retrieved: result.messages.length,
          monitoring_status: monitoringStatus,
          formatted_history: formattedOutput,
        };
      }

      case "get_unread_messages": {
        const channelsToCheck = args.channels || Array.from(monitoredChannels.keys());

        if (channelsToCheck.length === 0) {
          return {
            message: "No channels being monitored. Use start_monitoring first.",
            unread_messages: [],
          };
        }

        const allUnreadMessages: any[] = [];
        const channelResults: any[] = [];

        for (const channelName of channelsToCheck) {
          const state = monitoredChannels.get(channelName);

          if (!state) {
            channelResults.push({
              channel: channelName,
              status: "not_monitored",
              unread_count: 0,
            });
            continue;
          }

          try {
            // Get messages since last read
            const result = await zulipClient.messages.retrieve({
              anchor: "newest",
              num_before: 200,
              num_after: 0,
              narrow: [["stream", channelName]],
            });
            for (const message of result.messages ?? []) this.onMessageObserved?.(message.id);

            const unreadMessages = result.messages.filter((msg: any) =>
              msg.id > state.lastReadMessageId
            );

            allUnreadMessages.push(...unreadMessages.map((msg: any) => ({
              ...msg,
              channel: channelName,
            })));

            // Update last read if mark_as_read is true
            if (args.mark_as_read !== false && unreadMessages.length > 0) {
              state.lastReadMessageId = Math.max(...unreadMessages.map((m: any) => m.id));
            }

            channelResults.push({
              channel: channelName,
              status: "checked",
              unread_count: unreadMessages.length,
              latest_message_id: unreadMessages.length > 0 ? unreadMessages[0].id : state.lastReadMessageId,
            });
          } catch (error) {
            channelResults.push({
              channel: channelName,
              status: "error",
              error: error instanceof Error ? error.message : String(error),
            });
          }
        }

        // Sort by timestamp
        allUnreadMessages.sort((a, b) => a.timestamp - b.timestamp);

        // Save state if we marked messages as read
        if (args.mark_as_read !== false && allUnreadMessages.length > 0) {
          this.saveState();
        }

        // Format output — suppressed reaction markers never reach the model, raw format included.
        const format = args.format || "detailed";
        const formattedOutput = formatMessages(stripSuppressedReactions(allUnreadMessages, this.reactionPolicy), format);

        return {
          total_unread: allUnreadMessages.length,
          channels_checked: channelResults,
          formatted_messages: formattedOutput,
        };
      }

      case "send_message": {
        args = { ...args, reply_to: args.reply_to ?? undefined, to: args.to ?? undefined,
          topic: args.topic ?? undefined, type: args.type ?? undefined };
        if (args.reply_to === undefined && (args.to === undefined || args.type === undefined)) throw new Error("type and to are required without reply_to");
        const hasText = typeof args.content === "string" && args.content.trim() !== "";
        const hasFiles = Array.isArray(args.attachments) && args.attachments.length > 0;
        if (!hasText && !hasFiles) throw new Error("content is required (or at least one attachment)");
        const reply = args.reply_to !== undefined ? await this.replyDestination(args) : undefined;
        if (reply) args = { ...args, type: reply.type, to: reply.to, topic: reply.topic };
        const uploaded = await this.uploadFor(args);
        const content = withAttachmentLinks(hasText ? args.content : "", uploaded);
        const ids: number[] = [];
        let last: any = null;
        const channelId = reply?.channelId ?? (args.type === "private" && Array.isArray(args.to) && args.to.every((t: unknown) => typeof t === "number")
          ? dmChannelIdFor(args.to as number[])
          : `zulip:${Array.isArray(args.to) ? args.to.join(",") : String(args.to ?? "")}`);
        for (const chunk of chunkMessage(content)) {
          last = await zulipClient.messages.send({ type: args.type, to: args.to, topic: args.topic, content: chunk });
          if (last?.result === "error") throw new Error(last.msg ?? "Zulip refused the message");
          ids.push(last.id);
          this.onSent?.({ messageId: String(last.id), channelId, content: chunk });
        }
        return {
          ...last,
          ...(uploaded.length > 0 ? { attachments: uploaded } : {}),
          ...(ids.length > 1 ? { ids, note: `Sent as ${ids.length} messages (content exceeded the realm's message length).` } : {}),
        };
      }

      case "send_dm": {
        const wanted: string[] = Array.isArray(args.to) ? args.to.map(String) : [String(args.to ?? "")];
        if (wanted.length === 0 || wanted.some((w) => !w.trim())) throw new Error("to must name at least one recipient");
        const hasText = typeof args.content === "string" && args.content.trim() !== "";
        const hasFiles = Array.isArray(args.attachments) && args.attachments.length > 0;
        if (!hasText && !hasFiles) throw new Error("content is required (or at least one attachment)");
        const ids = await Promise.all(wanted.map((w) => this.resolveUserId(w)));
        const uploaded = await this.uploadFor(args);
        const content = withAttachmentLinks(hasText ? args.content : "", uploaded);
        const channelId = dmChannelIdFor(ids);
        const sent: number[] = [];
        let result: any = null;
        for (const chunk of chunkMessage(content)) {
          result = await zulipClient.messages.send({ type: "private", to: ids, content: chunk });
          if (result?.result === "error") throw new Error(result.msg ?? "Zulip refused the message");
          sent.push(result.id);
          this.onSent?.({ messageId: String(result.id), channelId, content: chunk });
        }
        return {
          ...result,
          to_user_ids: ids,
          ...(uploaded.length > 0 ? { attachments: uploaded } : {}),
          ...(sent.length > 1 ? { ids: sent, note: `Sent as ${sent.length} messages.` } : {}),
        };
      }

      case "upload_file": {
        const uploader = this.requireUploader();
        const prepared = await prepareAttachmentArg({ file: args.file, data: args.data, name: args.name, mime_type: args.mime_type }, this.uploadPolicy);
        try {
          const data = await prepared.read();
          const file = await uploader.upload({ name: prepared.name, mimeType: prepared.mimeType, data });
          return { ...file, size: data.length, markdown: attachmentMarkdown(file) };
        } finally {
          await prepared.close();
        }
      }

      case "edit_message":
      case "move_messages": {
        const moving = name === "move_messages";
        if (!Number.isInteger(args.message_id) || args.message_id <= 0) throw new Error("message_id is required");
        const operation = `${moving ? "moving" : "editing"} message ${args.message_id}`;
        if (!moving) {
          if (typeof args.content !== "string") throw new Error("content must be a string");
          if (args.topic !== undefined || args.stream_id !== undefined) throw new Error("use move_messages for topic or stream moves");
          if (args.propagate_mode !== undefined && args.propagate_mode !== "change_one") {
            throw new Error("propagate_mode requires topic or stream_id; use move_messages");
          }
        } else {
          if (args.content !== undefined) throw new Error("use edit_message for content edits");
          if (args.topic !== undefined && typeof args.topic !== "string") throw new Error("topic must be a string");
          if (args.stream_id !== undefined && (!Number.isInteger(args.stream_id) || args.stream_id <= 0)) {
            throw new Error("stream_id must be a positive integer");
          }
          if (args.propagate_mode !== undefined && !["change_one", "change_later", "change_all"].includes(args.propagate_mode)) {
            throw new Error("propagate_mode must be change_one, change_later, or change_all");
          }
          if (args.topic === undefined && args.stream_id === undefined) throw new Error("topic or stream_id is required");
        }
        // zulip-js includes message_id in the query and sends no PATCH body.
        // Keep the id only in the path, with the exposed edit fields in the body.
        if (!this.session.realm || !this.session.authHeader) {
          throw new Error(`Cannot ${moving ? "move" : "edit"} messages: realm or bot credentials are unknown`);
        }
        // The original placement decides the propagation range even when the
        // acted-on message is not the current conversation's source.
        const moveEpochs = moving ? this.onMoveStarted?.(args.message_id) : undefined;
        let original: any = null;
        if (moving) {
          try {
            const before = await withTimeout<any>(zulipClient.messages.getById({ message_id: args.message_id, apply_markdown: false }), this.moveReadTimeoutMs, "before move placement");
            this.onMessageObserved?.(before.message?.id);
            if (before?.result === "success") original = before.message;
          } catch { /* Cache/event reconciliation remains available. */ }
        }
        const source = typeof original?.display_recipient === "string" && typeof original?.subject === "string"
          ? { channelId: `zulip:${original.display_recipient}`, topic: original.subject } : null;
        const newStreamId = moving && args.stream_id !== undefined && args.stream_id !== original?.stream_id
          ? args.stream_id : null;
        let movedTo: { channelId: string | null; stream?: string | null; topic: string | null } | undefined;
        if (moving) {
          movedTo = { channelId: null, topic: args.topic === undefined ? source?.topic ?? null : null };
        }
        const apiUrl = zulipClient.config?.apiURL ?? `${this.session.realm}/api/v1`;
        const editFields = new URLSearchParams();
        for (const field of moving ? ["topic", "propagate_mode", "stream_id"] : ["content"]) {
          if (args[field] !== undefined) editFields.set(field, String(args[field]));
        }
        const response = await fetch(`${apiUrl}/messages/${encodeURIComponent(args.message_id)}`, {
          method: "PATCH",
          headers: { Authorization: this.session.authHeader },
          body: editFields,
        });
        const body = await response.text();
        let result: any = null;
        try {
          result = JSON.parse(body);
        } catch {
          // Proxies can return HTML or an empty body; preserve the HTTP status.
        }
        if (!response.ok || result?.result === "error") {
          let detail = typeof result?.msg === "string" ? result.msg : body;
          detail = detail.replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim();
          const encodedAuth = this.session.authHeader.replace(/^Basic\s+/i, "");
          const credentials = [this.session.authHeader, encodedAuth, ...Buffer.from(encodedAuth, "base64").toString().split(":")];
          for (const credential of credentials.filter(Boolean)) detail = detail.replaceAll(credential, "[redacted]");
          detail = detail.slice(0, 160);
          const fields = ["code", "first_message_id_allowed_to_move", "total_messages_in_topic", "total_messages_allowed_to_move"];
          const structured = fields.flatMap((field) => {
            const value = result?.[field];
            if (field === "code" ? typeof value !== "string" : value !== null && (typeof value !== "number" || !Number.isFinite(value))) return [];
            let safe = String(value).replace(/<[^>]*>/g, "").replace(/\s+/g, " ").trim();
            for (const credential of credentials.filter(Boolean)) safe = safe.replaceAll(credential, "[redacted]");
            return [`${field}=${safe.slice(0, 160)}`];
          });
          if (structured.length) detail += ` (${structured.join(", ")})`;
          const diagnostic = [response.ok ? "" : `HTTP ${response.status}`, detail].filter(Boolean).join(": ");
          throw new Error(`Zulip refused ${operation}${diagnostic ? `: ${diagnostic}` : ""}`);
        }
        assertApiSuccess(result, operation);
        if (result?.result !== "success") {
          throw new Error(`Zulip refused ${operation}: HTTP ${response.status}`);
        }
        if (result.ignored_parameters_unsupported?.length) {
          throw new Error(`Zulip refused ${operation}: ignored_parameters_unsupported: ${JSON.stringify(result.ignored_parameters_unsupported)}`);
        }
        if (moving) {
          // Never report a failed follow-up read as a failed move: retrying
          // would repeat an already successful write. Only stored subjects
          // confirm a requested (possibly normalized/truncated) topic.
          let destinationChannelId = newStreamId === null ? source?.channelId ?? null : null;
          let storedTopic = args.topic === undefined ? source?.topic ?? null : null;
          try {
            const placed = await withTimeout<any>(zulipClient.messages.getById({ message_id: args.message_id, apply_markdown: false }), this.moveReadTimeoutMs, "placed move placement");
            this.onMessageObserved?.(placed.message?.id);
            if (placed?.result === "success" &&
                (args.stream_id === undefined || placed.message?.stream_id === args.stream_id)) {
              if (typeof placed.message?.display_recipient === "string") destinationChannelId = `zulip:${placed.message.display_recipient}`;
              if (typeof placed.message?.subject === "string") storedTopic = placed.message.subject;
            }
          } catch { /* The bot may have lost visibility after moving. */ }
          if (movedTo && destinationChannelId === null && args.stream_id !== undefined) {
            try {
              const streams = await withTimeout<any>(zulipClient.streams.retrieve({ include_public: true, include_subscribed: true }), this.moveReadTimeoutMs, "streams move placement");
              const destination = streams?.streams?.find((stream: any) => stream.stream_id === args.stream_id);
              if (typeof destination?.name === "string") destinationChannelId = `zulip:${destination.name}`;
            } catch { /* Preserve the successful move even if lookup fails. */ }
          }
          const confirmedMove: SelfMove = {
            moveEpochs, messageId: args.message_id, source, propagateMode: args.propagate_mode ?? "change_one",
            newStreamId, destinationChannelId, topic: storedTopic, likelyTopic: args.topic,
          };
          this.onMoved?.(confirmedMove);
          if (movedTo) {
            movedTo = { channelId: destinationChannelId, stream: destinationChannelId?.replace(/^zulip:/, "") ?? null, topic: storedTopic };
            return {
              ...result,
              moved_to: movedTo,
              ...(movedTo.channelId === null || movedTo.topic === null
                ? { routing_note: "Move succeeded, but destination routing could not be fully resolved; inspect list_streams/get_stream_topics before publishing." }
                : {}),
            };
          }
        }
        return result;
      }

      case "delete_message": {
        // Noted before the call: the delete event can arrive before the
        // response does. Un-noted on failure: nothing will echo.
        this.onDeleted?.(String(args.message_id));
        try {
          const result = await zulipClient.messages.deleteById({
            message_id: args.message_id,
          });
          assertApiSuccess(result, `deleting message ${args.message_id}`);
          return result;
        } catch (err) {
          this.onDeleteFailed?.(String(args.message_id));
          throw err;
        }
      }

      case "list_streams": {
        const result = await zulipClient.streams.retrieve({
          include_public: args.include_public ?? true,
          include_subscribed: args.include_subscribed ?? true,
        });
        assertApiSuccess(result, "listing streams");

        // Format streams nicely
        const streams = result.streams || [];
        const formatted = streams
          .sort((a: any, b: any) => b.stream_weekly_traffic - a.stream_weekly_traffic)
          .map((s: any) => {
            const activity = s.is_recently_active ? "🟢" : "⚪";
            const privacy = s.invite_only ? "🔒" : "🌐";
            const traffic = s.stream_weekly_traffic > 0 ? `${s.stream_weekly_traffic} msgs/wk` : "inactive";
            return `${activity} ${privacy} **${s.name}** (${s.subscriber_count} subs, ${traffic})${s.description ? `\n   └─ ${s.description}` : ""}`;
          })
          .join("\n");

        return {
          total_streams: streams.length,
          formatted_list: `📋 **${streams.length} Streams**\n\n${formatted}`,
          stream_ids: Object.fromEntries(streams.map((s: any) => [s.name, s.stream_id])),
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "get_stream_topics": {
        const result = await zulipClient.streams.topics.retrieve({
          stream_id: args.stream_id,
        });
        assertApiSuccess(result, `topics of stream ${args.stream_id}`);

        const topics = result.topics || [];
        const formatted = topics
          .map((t: any, idx: number) => `${idx + 1}. **${t.name}** (latest msg: ${t.max_id})`)
          .join("\n");

        return {
          topic_count: topics.length,
          formatted_list: `📑 **${topics.length} Topics**\n\n${formatted}`,
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "list_users": {
        const result = await zulipClient.users.retrieve({
          client_gravatar: args.client_gravatar || false,
        });
        assertApiSuccess(result, "listing users");

        const users = result.members || [];
        const formatted = users
          .filter((u: any) => u.is_active)
          .map((u: any) => {
            const status = u.is_bot ? "🤖" : "👤";
            const role = u.is_admin ? " (admin)" : u.is_owner ? " (owner)" : "";
            return `${status} **${u.full_name}** <${u.email}>${role}`;
          })
          .join("\n");

        return {
          total_users: users.length,
          active_users: users.filter((u: any) => u.is_active).length,
          formatted_list: `👥 **${users.length} Users** (${users.filter((u: any) => u.is_active).length} active)\n\n${formatted}`,
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "get_user_profile": {
        const result = await zulipClient.users.me.getProfile();
        assertApiSuccess(result, "fetching the bot profile");
        return result;
      }

      case "add_reaction": {
        const emoji = await this.resolveEmoji(String(args.emoji_name ?? ""));
        const result = await zulipClient.reactions.add({ message_id: args.message_id, ...emoji });
        if (result?.result === "error") throw new Error(result.msg ?? "Zulip refused the reaction");
        return result;
      }

      case "remove_reaction": {
        const emoji = await this.resolveEmoji(String(args.emoji_name ?? ""));
        const result = await zulipClient.reactions.remove({ message_id: args.message_id, ...emoji });
        if (result?.result === "error") throw new Error(result.msg ?? "Zulip refused removing the reaction");
        return result;
      }

      case "list_emojis": {
        const emoji = await this.realmEmoji();
        const list = Object.values(emoji)
          .filter((e) => !e.deactivated)
          .map((e) => ({ name: e.name, id: e.id, token: `:${e.name}:` }))
          .sort((a, b) => a.name.localeCompare(b.name));
        return { count: list.length, emojis: list };
      }

      case "find_user": {
        const usersResult = await zulipClient.users.retrieve();
        assertApiSuccess(usersResult, "listing users");
        const members = usersResult.members || [];
        const query = args.query.toLowerCase();

        const matches = members.filter((user: any) =>
          user.full_name.toLowerCase().includes(query) ||
          user.email.toLowerCase().includes(query)
        );

        if (matches.length === 0) {
          return {
            found: false,
            message: `No users found matching "${args.query}"`,
          };
        }

        const formatted = matches.map((user: any) => {
          const status = user.is_bot ? "🤖" : "👤";
          return `${status} **${user.full_name}** <${user.email}>\n   └─ User ID: ${user.user_id}\n   └─ Mention format: @**${user.full_name}**`;
        }).join("\n\n");

        return {
          found: true,
          match_count: matches.length,
          formatted_list: `👥 Found ${matches.length} user${matches.length !== 1 ? 's' : ''}:\n\n${formatted}`,
          users: matches.map((u: any) => ({
            user_id: u.user_id,
            full_name: u.full_name,
            email: u.email,
            is_bot: u.is_bot,
            mention_syntax: `@**${u.full_name}**`,
          })),
        };
      }

      case "fetch_history": {
        const channel = String(args.channel ?? "").trim().replace(/^#/, "");
        if (!channel) throw new Error("channel is required");
        // A DM conversation reads by its channel id (`zulip:dm:<ids>`, the
        // form every catch-up note and history line quotes); anything else
        // is a stream name or stream channel id.
        const dmIds = parseDmChannelId(channel.startsWith("zulip:") ? channel : `zulip:${channel}`);
        const streamName = channel.startsWith("zulip:") ? channel.slice("zulip:".length) : channel;
        const limit = clampInt(args.limit, 50, 1, 1000);
        const page = await fetchHistory(zulipClient, {
          ...(dmIds ? { dmUserIds: dmIds } : { streamName, topic: typeof args.topic === "string" && args.topic ? args.topic : undefined }),
          limit,
          before: numberOrUndefined(args.before),
          after: numberOrUndefined(args.after),
        });
        for (const message of page.messages) this.onMessageObserved?.(message.id);
        return {
          channel: dmIds ? dmChannelIdFor(dmIds) : streamName,
          channelId: dmIds ? dmChannelIdFor(dmIds) : `zulip:${streamName}`,
          topic: dmIds ? undefined : args.topic,
          count: page.messages.length,
          oldest_id: page.messages[0]?.id ?? null,
          newest_id: page.messages[page.messages.length - 1]?.id ?? null,
          reached_oldest: page.foundOldest,
          reached_newest: page.foundNewest,
          formatted_history: formatHistoryLines(page.messages, undefined, this.reactionPolicy, this.session.selfUserId, this.formatTime),
        };
      }

      case "fetch_around": {
        const messageId = numberOrUndefined(args.message_id);
        if (messageId === undefined) throw new Error("message_id must be a number");
        const limit = clampInt(args.limit, 50, 1, 200);
        const page = await fetchAround(zulipClient, messageId, limit);
        for (const message of page.messages) this.onMessageObserved?.(message.id);
        const anchor = page.messages.find((m) => m.id === messageId);
        return {
          message_id: messageId,
          channel: anchor?.streamName ?? null,
          channelId: anchor?.streamName ? `zulip:${anchor.streamName}` : null,
          topic: anchor?.topic ?? null,
          count: page.messages.length,
          oldest_id: page.messages[0]?.id ?? null,
          newest_id: page.messages[page.messages.length - 1]?.id ?? null,
          formatted_history: formatHistoryLines(page.messages, messageId, this.reactionPolicy, this.session.selfUserId, this.formatTime),
        };
      }

      case "fetch_attachment": {
        // Validation (realm host + /user_uploads/ prefix, with the URL
        // dot-segment normalization invariant) lives in parseZulipAttachmentUrl.
        const url = parseZulipAttachmentUrl(String(args.path || ""), this.session.realm);

        const headers: Record<string, string> = {};
        if (this.session.authHeader) headers["Authorization"] = this.session.authHeader;
        const name = decodeURIComponent(url.pathname.split("/").pop() || "attachment");
        return toFetchResult(await fetchAttachmentBytes(url.toString(), name, { headers }));
      }

      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  }

  // ── Emoji resolution ──

  private emojiCache: { at: number; emoji: Record<string, { id: string; name: string; deactivated?: boolean }> } | null = null;

  private async realmEmoji(): Promise<Record<string, { id: string; name: string; deactivated?: boolean }>> {
    const now = Date.now();
    if (this.emojiCache && now - this.emojiCache.at < 300_000) return this.emojiCache.emoji;
    const result = await this.zulipClient.emojis.retrieve();
    // An API error is an error, not an empty realm — and never cached as one.
    assertApiSuccess(result, "listing realm emoji");
    const emoji = (result?.emoji ?? {}) as Record<string, { id: string; name: string; deactivated?: boolean }>;
    this.emojiCache = { at: now, emoji };
    return emoji;
  }

  /** `:name:` or `name` → the reaction params Zulip wants: a realm emoji
   *  needs its code and type; anything else is tried as unicode by name. */
  private async resolveEmoji(raw: string): Promise<{ emoji_name: string; emoji_code?: string; reaction_type: string }> {
    const name = raw.trim().replace(/^:|:$/g, "");
    if (!name) throw new Error("emoji_name is required");
    try {
      const realm = Object.values(await this.realmEmoji()).find((e) => e.name === name && !e.deactivated);
      if (realm) return { emoji_name: name, emoji_code: realm.id, reaction_type: "realm_emoji" };
    } catch {
      // Realm emoji lookup is best-effort; fall through to unicode.
    }
    return { emoji_name: name, reaction_type: "unicode_emoji" };
  }

  // ── User resolution ──

  private userCache: { at: number; members: any[] } | null = null;

  private async members(): Promise<any[]> {
    const now = Date.now();
    if (this.userCache && now - this.userCache.at < 60_000) return this.userCache.members;
    const result = await this.zulipClient.users.retrieve({});
    assertApiSuccess(result, "listing users");
    const members = Array.isArray(result?.members) ? result.members : [];
    this.userCache = { at: now, members };
    return members;
  }

  /**
   * A user id from a name, email, or id. Exact, case-insensitive matches
   * only — a near-miss fails loudly rather than messaging the wrong person,
   * and an ambiguous name lists the candidates with their ids.
   */
  async resolveUserId(query: string): Promise<number> {
    const q = query.trim();
    if (/^\d+$/.test(q)) return Number(q);
    const members = await this.members();
    const lower = q.toLowerCase();
    const byEmail = members.filter((u: any) => typeof u.email === "string" && u.email.toLowerCase() === lower);
    if (byEmail.length === 1) return byEmail[0].user_id;
    const byName = members.filter((u: any) => typeof u.full_name === "string" && u.full_name.toLowerCase() === lower);
    if (byName.length === 1) return byName[0].user_id;
    if (byName.length > 1) {
      const options = byName.map((u: any) => `${u.full_name} <${u.email}> (id ${u.user_id})`).join("; ");
      throw new Error(`"${q}" matches ${byName.length} users — pass an id or email instead: ${options}`);
    }
    throw new Error(`No user matches "${q}" (by full name, email, or id). Use find_user to search.`);
  }

  // ── Resources ──

  listResources(): ResourceDescriptor[] {
    const resources: ResourceDescriptor[] = [
      {
        uri: "zulip://unread/summary",
        name: "Zulip - Unread Messages Summary",
        description: "Count of unread messages across all monitored Zulip channels",
        mimeType: "text/plain",
      },
      {
        uri: "zulip://monitoring/status",
        name: "Zulip - Monitoring Status",
        description: "Current Zulip monitoring state and channel list",
        mimeType: "application/json",
      },
    ];

    // Add a resource for each monitored Zulip channel
    for (const channel of this.monitoredChannels.keys()) {
      resources.push({
        uri: `zulip://channel/${channel}/unread`,
        name: `Zulip #${channel} - Unread Messages`,
        description: `Unread message count for Zulip #${channel}`,
        mimeType: "text/plain",
      });
    }

    return resources;
  }

  async readResource(uri: string): Promise<ResourceContents> {
    const zulipClient = this.zulipClient;
    const monitoredChannels = this.monitoredChannels;

    // Handle unread summary
    if (uri === "zulip://unread/summary") {
      let totalUnread = 0;
      const channelSummaries: string[] = [];

      for (const [channelName, state] of monitoredChannels) {
        try {
          const result = await zulipClient.messages.retrieve({
            anchor: "newest",
            num_before: 50,
            num_after: 0,
            narrow: [["stream", channelName]],
          });
          for (const message of result.messages ?? []) this.onMessageObserved?.(message.id);

          const unread = result.messages.filter((msg: any) => msg.id > state.lastReadMessageId);
          totalUnread += unread.length;

          if (unread.length > 0) {
            channelSummaries.push(`📬 #${channelName}: ${unread.length} unread`);
          }
        } catch (error) {
          // Skip channels with errors
        }
      }

      const summary = totalUnread > 0
        ? `🔔 ${totalUnread} unread message${totalUnread !== 1 ? 's' : ''}\n\n${channelSummaries.join('\n')}`
        : "✅ No unread messages";

      return { contents: [{ uri, mimeType: "text/plain", text: summary }] };
    }

    // Handle monitoring status
    if (uri === "zulip://monitoring/status") {
      const status = {
        session_id: this.sessionId,
        monitored_count: monitoredChannels.size,
        channels: Array.from(monitoredChannels.values()).map(c => ({
          name: c.channelName,
          last_read_message_id: c.lastReadMessageId,
        })),
      };

      return { contents: [{ uri, mimeType: "application/json", text: JSON.stringify(status, null, 2) }] };
    }

    // Handle individual channel unread count
    const channelMatch = uri.match(/^zulip:\/\/channel\/([^/]+)\/unread$/);
    if (channelMatch) {
      const channelName = channelMatch[1];
      const state = monitoredChannels.get(channelName);

      if (!state) {
        return { contents: [{ uri, mimeType: "text/plain", text: `Channel "${channelName}" is not being monitored` }] };
      }

      try {
        const result = await zulipClient.messages.retrieve({
          anchor: "newest",
          num_before: 50,
          num_after: 0,
          narrow: [["stream", channelName]],
        });
        for (const message of result.messages ?? []) this.onMessageObserved?.(message.id);

        const unread = result.messages.filter((msg: any) => msg.id > state.lastReadMessageId);
        const count = unread.length;

        const text = count > 0
          ? `📬 ${count} unread message${count !== 1 ? 's' : ''} in #${channelName}`
          : `✅ No unread messages in #${channelName}`;

        return { contents: [{ uri, mimeType: "text/plain", text }] };
      } catch (error) {
        return { contents: [{ uri, mimeType: "text/plain", text: `Error checking #${channelName}: ${error}` }] };
      }
    }

    throw new Error(`Unknown resource: ${uri}`);
  }
}
