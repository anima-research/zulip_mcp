/**
 * Tool handlers and MCP resources — the stateful tool layer of this server.
 *
 * Owns the persistent monitoring state (`~/.zulip_mcp_state/<session>.json`):
 * which streams are monitored and the last-read message id per stream. The
 * MCPL layer (channels/context) is independent of this; tools and resources
 * are the plain-MCP surface every client gets.
 */

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
import type { ZulipSession } from "./zulip-client.js";
import { mayHaveBeenApplied, ZulipNoAnswer, type ZulipApi } from "./zulip-api.js";
import { dmChannelIdFor, dmCounterparts, fetchAround, fetchHistory, parseDmChannelId, renderReactions, type HistoryQuery, type ReactionSummary, type ZulipMessage, type ZulipRecipient, editedTrailer } from './history.js';
import { prepareOutgoing, SendRefused, sendMessage, ZULIP_MAX_MESSAGE_LENGTH, type Destination } from "./outgoing.js";
import type { SentRecord } from "./platforms/adapter.js";
import { linkMessages, MessageLinkRefused, type Audience } from "./message-links.js";
import { parseKey, streamKey, type ChannelKey } from './channel-key.js';
import type { ChannelIds } from './channel-ids.js';
import { messageLineHead } from "./message-line.js";
import { agentLineTimeFormatter } from "./timezone.js";
import {
  attachmentMarkdown,
  createZulipUploader,
  prepareAttachmentArg,
  prepareAttachments,
  resolveUploadPolicy,
  uploadPrepared,
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

const MOVE_SCOPES: Record<string, string> = { one: "change_one", later: "change_later", all: "change_all" };

const MOVE_NOTE = "To answer this message where it is now, use send_message with reply_to.";

// The host ends a tool call at 60 s by default (agent-framework's DEFAULT_REQUEST_TIMEOUT_MS). A move makes up
// to three requests of 15 s each before this wait, so the call's bounds add up to 50 s (anima-research/zulip_mcp#60).
const FOLLOW_WAIT_MS = 5_000;

export type FollowSeen = "sure" | "unsure" | { follows: number | undefined };

export interface AnswerHints {
  followed(key: ChannelKey): { from?: number; topic?: string } | undefined;
  follow(key: ChannelKey, from: number, readTopic: string | undefined, topic: string): Promise<FollowSeen>;
}

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

function positiveIdArg(name: string, value: unknown): number {
  const id = typeof value === "string" && /^[0-9]+$/.test(value) ? Number(value) : value;
  if (typeof id !== "number" || !Number.isSafeInteger(id) || id <= 0) throw new Error(`${name} must be a positive integer`);
  return id;
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
      stream: m.isDm ? null : (m.streamNameShown ?? ""),
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

function postedBefore(ids: number[]): string {
  if (ids.length === 0) return "";
  const one = ids.length === 1;
  const list = one ? String(ids[0]) : `${ids.slice(0, -1).join(", ")} and ${ids[ids.length - 1]}`;
  return `Posted as message${one ? "" : "s"} ${list} (${one ? "part 1" : `parts 1 to ${ids.length}`}) before the next part failed; ` +
    `sending the whole text again posts ${one ? "that part" : "those parts"} twice. `;
}

export class ZulipToolRuntime {
  private readonly api: ZulipApi;
  private readonly sessionId: string;
  private readonly monitoredChannels = new Map<string, ChannelState>();

  private reactionPolicy: ReactionPolicy = SHOW_ALL;

  /** Set by the MCPL server: every message sent by a tool is reported here
   *  so a rollback checkpoint can undo it. */
  onSent: ((sent: SentRecord) => void) | null = null;
  /** Called BEFORE delete_message is sent (the delete event can arrive before
   *  the response does), so the adapter recognises the echo as its own. */
  onDeleted: ((messageId: string) => void) | null = null;
  onDeleteFailed: ((messageId: string) => void) | null = null;

  /** Set by the MCPL server: streams `listen` just subscribed the bot to, so
   *  they can be registered without waiting for a refresh or an event (#20).
   *  Awaited, and its result is reported by `listen`: an announcement the
   *  host did not confirm is the agent's business, not a stderr line. */
  onSubscribed: ((streams: string[]) => SubscriptionRegistration | Promise<SubscriptionRegistration | void> | void) | null = null;

  channelIds: ChannelIds | null = null;

  answerHints: AnswerHints | null = null;

  private async streamIdsByName(names: string[], opts: { readList?: boolean } = {}): Promise<Map<string, number>> {
    const out = new Map<string, number>();
    const ids = this.channelIds;
    if (!ids) return out;
    const renamed: string[] = [];
    for (const name of names) {
      const answer = opts.readList === false
        ? ids.resolve(`zulip:${name}`, { byName: true }) ?? { unknownName: name }
        : await ids.keyOf(`zulip:${name}`, { byName: true });
      if ("key" in answer) {
        const parsed = parseKey(answer.key);
        if (parsed.kind === "stream") out.set(name, parsed.streamId);
        continue;
      }
      const former = "refusal" in answer ? ids.directory.formerOf(name) : undefined;
      if (former) renamed.push(`Zulip stream #${name} was renamed #${former.current}.`);
    }
    if (renamed.length > 0) throw new Error(`${renamed.join(" ")} Nothing went to Zulip.`);
    return out;
  }

  private async streamIdForName(name: string): Promise<number> {
    const known = (await this.streamIdsByName([name], { readList: false })).get(name);
    if (known !== undefined) return known;
    let id: unknown;
    try {
      id = (await this.api.request<{ stream_id?: unknown }>("GET", "/get_stream_id", `looking up stream ${JSON.stringify(name)}`, { stream: name })).stream_id;
    } catch (err) {
      if (err instanceof ZulipNoAnswer) throw err;
      const text = (err as Error).message;
      throw new Error(`${text}${/[.!?]$/.test(text) ? "" : "."} list_streams gives each stream's name.`);
    }
    if (typeof id !== "number" || !Number.isSafeInteger(id) || id <= 0) throw new Error(`Zulip's answer to looking up stream ${JSON.stringify(name)} gave no stream id.`);
    return id;
  }

  private async followOwnMove(messageId: number, streamId: number, topic: string, scope: string): Promise<boolean> {
    const hints = this.answerHints;
    const key = streamKey(streamId);
    const read = hints?.followed(key);
    const from = read?.from;
    if (!hints || from === undefined || (from !== messageId && scope === "one")) return true;
    let followedTopic = topic;
    if (from !== messageId) {
      const followed = await this.followedTopic(from, streamId);
      if (followed === "elsewhere") return true;
      if (followed === undefined) return false;
      followedTopic = followed.topic;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const late = new Promise<boolean>((resolve) => { timer = setTimeout(() => resolve(false), FOLLOW_WAIT_MS); });
    try {
      const seen = hints.follow(key, from, read?.topic, followedTopic);
      return await Promise.race([seen.then((it) => it === "sure" || (typeof it === "object" && scope === "one")), late]);
    } finally {
      clearTimeout(timer);
    }
  }

  private async followedTopic(messageId: number, streamId: number): Promise<{ topic: string } | "elsewhere" | undefined> {
    let message: any;
    try {
      message = (await this.api.request<{ message?: any }>("GET", `/messages/${messageId}`, `reading message ${messageId}`)).message;
    } catch {
      return undefined;
    }
    if (!Number.isSafeInteger(message?.stream_id) || message.stream_id <= 0) return undefined;
    if (message.stream_id !== streamId) return "elsewhere";
    return typeof message.subject === "string" ? { topic: message.subject } : undefined;
  }

  private readonly uploader: Uploader | null;
  private readonly uploadPolicy: UploadPolicy;
  /** Line time for fetch_history / fetch_around, shared with live delivery. */
  private readonly formatTime: (d: Date) => string;
  private readonly maxMessageLength: number;

  constructor(
    private readonly session: ZulipSession,
    private readonly stateDir: string = STATE_DIR,
    options: { uploader?: Uploader; uploadPolicy?: UploadPolicy; formatTime?: (d: Date) => string; maxMessageLength?: number } = {},
  ) {
    this.api = session.api;
    this.sessionId = session.sessionId;
    this.formatTime = options.formatTime ?? agentLineTimeFormatter();
    this.maxMessageLength = options.maxMessageLength ?? ZULIP_MAX_MESSAGE_LENGTH;
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

  private async send(destination: Destination, content: string, args: Record<string, any>, channelId: string) {
    const posted: number[] = [];
    try {
      return await sendMessage(this.api, destination, content, {
        limit: this.maxMessageLength,
        unlinkable: "refuse",
        upload: () => this.uploadFor(args),
        onChunk: (chunk) => {
          posted.push(chunk.id);
          this.onSent?.({ messageId: String(chunk.id), channelId, content: chunk.content });
        },
      });
    } catch (err) {
      const before = postedBefore(posted);
      if (err instanceof SendRefused) throw new Error(`${before}${before ? "Zulip: " : ""}${err.answer.msg ?? "Zulip refused the message"}`);
      if (err instanceof MessageLinkRefused) throw new Error(`${err.message} Nothing was sent.`);
      if (before && err instanceof Error) err.message = `${before}${err.message}`;
      throw err;
    }
  }

  /** Resolve and check a reply before uploading files or sending any chunks. */
  private async replyDestination(args: Record<string, any>) {
    const id = numberOrUndefined(args.reply_to);
    if (!Number.isSafeInteger(id)) throw new Error("reply_to must be an integer message id");
    if (args.to !== undefined) throw new Error("reply_to sets the destination; omit to");
    const result = await this.api.requestAnswer<{ message?: any }>("GET", `/messages/${id}`);
    if (result.result === "error") {
      // Keep the API code as well as its wording: missing/deleted/inaccessible
      // messages must never fall back to a caller-supplied destination.
      throw new Error(`Zulip refused reply_to ${id}${result.code ? ` (${result.code})` : ""}: ${result.msg ?? "unknown error"}`);
    }
    const message = result.message;
    const unreadable = () => new Error(`reply_to ${id} has no readable destination`);
    if (!message || !["stream", "private"].includes(message.type)) throw unreadable();
    const type = args.type === "direct" ? "private" : args.type === "channel" ? "stream" : args.type;
    if (type !== undefined && type !== message.type) {
      throw new Error(`reply_to ${id}: type disagrees with the referenced message`);
    }
    if (message.type === "stream") {
      const name = message.display_recipient;
      if (typeof name !== "string" || !name || typeof message.subject !== "string") throw unreadable();
      const named = `zulip:${name}`;
      // The receipt names the stream by the id the host holds for it, or by none
      // while the host holds its name's id for another stream.
      const channelId = this.channelIds != null && Number.isSafeInteger(message.stream_id) ? this.channelIds.channelIdOf(streamKey(message.stream_id)) : named;
      return args.topic === undefined
        ? { type: message.type, to: message.stream_id ?? name, topic: message.subject, channelId: channelId ?? named, receipt: { channelId, channel: name, topic: message.subject } }
        : { type: message.type, to: message.stream_id ?? name, topic: args.topic, channelId: channelId ?? named, receipt: { channelId, channel: name } };
    }
    if (args.topic !== undefined) throw new Error(`reply_to ${id} is a direct message, which has no topic; omit topic`);

    const recipients: ZulipRecipient[] = message.display_recipient;
    if (!Array.isArray(recipients) || recipients.length === 0 || recipients.some((r) => !Number.isSafeInteger(r?.id))) {
      throw unreadable();
    }
    const ids = recipients.map((r) => r.id);
    const others = dmCounterparts(recipients, this.session.selfUserId).map((r) => r.id);
    const channelId = dmChannelIdFor(others);
    return { type: message.type, to: ids, topic: undefined, channelId, receipt: { channelId, to_user_ids: others } };
  }

  private async historyTarget(channel: string): Promise<{ query: Pick<HistoryQuery, 'dmUserIds' | 'streamId' | 'streamName'>; channel: string; channelId: string | null; isDm: boolean }> {
    const asId = channel.startsWith("zulip:") ? channel : `zulip:${channel}`;
    const dmIds = parseDmChannelId(asId);
    if (dmIds) return { query: { dmUserIds: dmIds }, channel: dmChannelIdFor(dmIds), channelId: dmChannelIdFor(dmIds), isDm: true };
    const ids = this.channelIds;
    const answer = ids ? await ids.keyOf(asId, { byName: !channel.startsWith("zulip:") }) : null;
    if (answer && "refusal" in answer) throw new Error(answer.refusal);
    if (ids && answer && "key" in answer) {
      const parsed = parseKey(answer.key);
      if (parsed.kind === "stream") {
        const name = ids.directory.nameOf(parsed.streamId) ?? "";
        return { query: { streamId: parsed.streamId }, channel: name, channelId: ids.channelIdOf(answer.key), isDm: false };
      }
    }
    const name = asId.slice("zulip:".length);
    return { query: { streamName: name }, channel: name, channelId: asId, isDm: false };
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
    const api = this.api;
    const monitoredChannels = this.monitoredChannels;
    args = args ?? {};

    switch (name) {
      case "start_monitoring": {
        const channels: string[] = args.channels;
        const results: any[] = [];

        for (const channelName of channels) {
          try {
            // Get latest message to establish baseline
            const messages = await api.request<{ messages: any[] }>("GET", "/messages", `the newest message in #${channelName}`, {
              anchor: "newest",
              num_before: 1,
              num_after: 0,
              narrow: [["stream", channelName]],
            });

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
          await this.streamIdsByName(channels);
          const result = await api.requestAnswer<any>("POST", "/users/me/subscriptions", {
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
          await this.streamIdsByName(channels);
          const result = await api.requestAnswer<any>("DELETE", "/users/me/subscriptions", {
            subscriptions: channels,
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
        const result = await api.request<{ messages: any[] }>("GET", "/messages", `history of #${args.channel}`, {
          anchor: "newest",
          num_before: maxMessages,
          num_after: 0,
          narrow: narrow,
        });

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
            const result = await api.request<{ messages: any[] }>("GET", "/messages", `unread messages in #${channelName}`, {
              anchor: "newest",
              num_before: 200,
              num_after: 0,
              narrow: [["stream", channelName]],
            });

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
        if (args.reply_to === undefined && args.topic === undefined && (args.type === "stream" || args.type === "channel")) {
          throw new Error("topic is required for a stream message: name one, or use reply_to to answer a message where it is");
        }
        const reply = args.reply_to !== undefined ? await this.replyDestination(args) : undefined;
        if (reply) args = { ...args, type: reply.type, to: reply.to, topic: reply.topic };
        const isDm = args.type === "private" || args.type === "direct";
        let channelId = reply?.channelId ?? (args.type === "private" && Array.isArray(args.to) && args.to.every((t: unknown) => typeof t === "number")
          ? dmChannelIdFor(args.to as number[])
          : `zulip:${Array.isArray(args.to) ? args.to.join(",") : String(args.to ?? "")}`);
        if (!reply && !isDm && typeof args.to === "string") {
          const streamId = (await this.streamIdsByName([args.to])).get(args.to);
          if (streamId !== undefined) {
            channelId = this.channelIds?.channelIdOf(streamKey(streamId)) ?? channelId;
            args = { ...args, to: streamId };
          }
        }
        const { chunks, last, uploaded } = await this.send({ type: args.type, to: args.to, topic: args.topic }, hasText ? args.content : "", args, channelId);
        const ids = chunks.map((c) => c.id);
        return {
          ...last,
          ...(reply ? reply.receipt : {}),
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
        const { chunks, last: result, uploaded } = await this.send({ type: "private", to: ids }, hasText ? args.content : "", args, dmChannelIdFor(ids));
        const sent = chunks.map((c) => c.id);
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

      case "edit_message": {
        const messageId = positiveIdArg("message_id", args.message_id);
        if (typeof args.content !== "string") throw new Error("content must be a string");
        const edited = async (): Promise<Audience> => {
          const { message } = await api.request<{ message?: { type?: unknown; stream_id?: unknown } }>("GET", `/messages/${messageId}`, `reading message ${messageId}`, { apply_markdown: false, allow_empty_topic_name: true });
          return message?.type === "stream" && typeof message.stream_id === "number" ? { kind: "stream", id: message.stream_id } : { kind: "direct" };
        };
        const tabled = prepareOutgoing(args.content);
        let content: string;
        try {
          content = await linkMessages(api, tabled, edited, "refuse");
        } catch (err) {
          if (err instanceof MessageLinkRefused) throw new Error(`${err.message} The message wasn't edited.`);
          throw err;
        }
        const length = [...content].length;
        if (length > this.maxMessageLength) {
          const changes = [
            ...(tabled === args.content ? [] : ["a blank line above each table under a line of text"]),
            ...(content === tabled ? [] : ["each [[msg:ID]] as its link"]),
          ];
          const posted = changes.length === 0 ? "" : ` as it would be posted, with ${changes.join(" and ")} (${[...args.content].length} as written)`;
          throw new Error(`content is ${length} characters${posted}, above the message length of ${this.maxMessageLength}; an edit can't be split as a send is`);
        }
        return api.request("PATCH", `/messages/${messageId}`, `editing message ${args.message_id}`, { content });
      }

      case "delete_message": {
        // Noted before the call: the delete event can arrive before the
        // response does. Un-noted once the delete surely failed: nothing will echo.
        const messageId = positiveIdArg("message_id", args.message_id);
        this.onDeleted?.(String(args.message_id));
        try {
          return await api.request("DELETE", `/messages/${messageId}`, `deleting message ${args.message_id}`);
        } catch (err) {
          if (!mayHaveBeenApplied(err)) this.onDeleteFailed?.(String(args.message_id));
          throw err;
        }
      }

      case "move_messages": {
        const messageId = positiveIdArg("message_id", args.message_id);
        const topic = args.topic ?? undefined;
        const stream = args.stream ?? undefined;
        if (topic === undefined && stream === undefined) throw new Error("Give a topic, a stream, or both.");
        if (topic !== undefined && typeof topic !== "string") throw new Error("topic must be a string.");
        if (stream !== undefined && (typeof stream !== "string" || stream === "")) throw new Error("stream must be a stream's name.");
        const scope = args.scope;
        if (typeof scope !== "string" || !Object.hasOwn(MOVE_SCOPES, scope)) throw new Error("scope must be one, later or all.");
        const streamId = stream === undefined ? undefined : await this.streamIdForName(stream);
        await api.request("PATCH", `/messages/${messageId}`, `moving message ${messageId}`, {
          propagate_mode: MOVE_SCOPES[scope],
          ...(topic !== undefined ? { topic } : {}),
          ...(streamId !== undefined ? { stream_id: streamId } : {}),
        });
        const asked = { ...(topic !== undefined ? { topic } : {}), ...(stream !== undefined ? { stream } : {}), scope };
        let message: any;
        try {
          message = (await api.request<{ message?: any }>("GET", `/messages/${messageId}`, `reading message ${messageId} back`)).message;
        } catch (err) {
          return { moved: true, asked, readBackError: (err as Error).message, note: MOVE_NOTE };
        }
        const name = message?.display_recipient;
        if (message?.type !== "stream" || typeof name !== "string" || name === "" || typeof message.subject !== "string" || !Number.isSafeInteger(message.stream_id) || message.stream_id <= 0) {
          return { moved: true, asked, readBackError: `Zulip's answer gave no stream and topic for message ${messageId}.`, note: MOVE_NOTE };
        }
        const answersFollow = stream === undefined && (await this.followOwnMove(messageId, message.stream_id, message.subject, scope));
        return {
          channelId: this.channelIds ? this.channelIds.channelIdOf(streamKey(message.stream_id)) : `zulip:${name}`,
          channel: name,
          topic: message.subject,
          ...(answersFollow ? {} : { note: MOVE_NOTE }),
        };
      }

      case "list_streams": {
        const result = await api.request<{ streams?: any[] }>("GET", "/streams", "listing streams", {
          include_public: args.include_public ?? true,
          include_subscribed: args.include_subscribed ?? true,
        });

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
        const byName = args.stream !== undefined && args.stream !== null;
        const byId = args.stream_id !== undefined && args.stream_id !== null;
        if (byName === byId) throw new Error("Give exactly one of stream and stream_id.");
        if (byName && (typeof args.stream !== "string" || args.stream === "")) throw new Error("stream must be a stream's name.");
        const givenId = byId ? positiveIdArg("stream_id", args.stream_id) : undefined;
        const limit = args.limit ?? undefined;
        if (limit !== undefined && !(typeof limit === "number" && Number.isInteger(limit) && limit > 0)) {
          throw new Error("limit must be a positive integer.");
        }
        const streamId = byName ? await this.streamIdForName(args.stream) : givenId;
        const result = await api.request<{ topics?: any[] }>("GET", `/users/me/${streamId}/topics`, `topics of stream ${streamId}`);

        const topics = result.topics || [];
        const shown = limit === undefined ? topics : topics.slice(0, limit);
        const formatted = shown
          .map((t: any, idx: number) => `${idx + 1}. **${t.name}** (latest msg: ${t.max_id})`)
          .join("\n");
        const heading = shown.length < topics.length
          ? `${topics.length} Topics, the newest ${shown.length} shown`
          : `${topics.length} Topics`;

        return {
          topic_count: topics.length,
          formatted_list: `📑 **${heading}**\n\n${formatted}`,
          ...(args.verbose && { raw_data: result }),
        };
      }

      case "list_users": {
        const result = await api.request<{ members?: any[] }>("GET", "/users", "listing users", {
          client_gravatar: args.client_gravatar || false,
        });

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
        return api.request("GET", "/users/me", "fetching the bot profile");
      }

      case "add_reaction": {
        const messageId = positiveIdArg("message_id", args.message_id);
        const emoji = await this.resolveEmoji(String(args.emoji_name ?? ""));
        const result = await api.requestAnswer("POST", `/messages/${messageId}/reactions`, emoji);
        if (result.result === "error") throw new Error(result.msg ?? "Zulip refused the reaction");
        return result;
      }

      case "remove_reaction": {
        const messageId = positiveIdArg("message_id", args.message_id);
        const emoji = await this.resolveEmoji(String(args.emoji_name ?? ""));
        const result = await api.requestAnswer("DELETE", `/messages/${messageId}/reactions`, emoji);
        if (result.result === "error") throw new Error(result.msg ?? "Zulip refused removing the reaction");
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
        const usersResult = await api.request<{ members?: any[] }>("GET", "/users", "listing users");
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
        const target = await this.historyTarget(channel);
        const limit = clampInt(args.limit, 50, 1, 1000);
        const page = await fetchHistory(api, {
          ...target.query,
          ...(target.isDm ? {} : { topic: typeof args.topic === "string" && args.topic ? args.topic : undefined }),
          limit,
          before: numberOrUndefined(args.before),
          after: numberOrUndefined(args.after),
        });
        return {
          channel: target.channel,
          channelId: target.channelId,
          topic: target.isDm ? undefined : args.topic,
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
        const page = await fetchAround(api, messageId, limit);
        const anchor = page.messages.find((m) => m.id === messageId);
        const shown = anchor?.streamNameShown ?? null;
        const known = this.channelIds != null && anchor?.streamId != null;
        const held = known ? this.channelIds!.channelIdOf(streamKey(anchor!.streamId!)) : null;
        return {
          message_id: messageId,
          channel: shown,
          channelId: known ? held : shown ? `zulip:${shown}` : null,
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
    // An API error is an error, not an empty realm — and never cached as one.
    const result = await this.api.request<{ emoji?: Record<string, { id: string; name: string; deactivated?: boolean }> }>("GET", "/realm/emoji", "listing realm emoji");
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
    const result = await this.api.request<{ members?: unknown }>("GET", "/users", "listing users", {});
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
    const api = this.api;
    const monitoredChannels = this.monitoredChannels;

    // Handle unread summary
    if (uri === "zulip://unread/summary") {
      let totalUnread = 0;
      const channelSummaries: string[] = [];

      for (const [channelName, state] of monitoredChannels) {
        try {
          const result = await api.request<{ messages: any[] }>("GET", "/messages", `unread messages in #${channelName}`, {
            anchor: "newest",
            num_before: 50,
            num_after: 0,
            narrow: [["stream", channelName]],
          });

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
        const result = await api.request<{ messages: any[] }>("GET", "/messages", `messages in #${channelName}`, {
          anchor: "newest",
          num_before: 50,
          num_after: 0,
          narrow: [["stream", channelName]],
        });

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
