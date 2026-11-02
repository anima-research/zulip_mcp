/**
 * MCP tool definitions for the Zulip server. Handlers live in tool-runtime.ts.
 */

export interface ToolDefinition {
  name: string;
  description: string;
  inputSchema: Record<string, unknown>;
  /** MCP `_meta`. `tools/list` adds `mcpl/class` from src/tool-classes.ts. */
  _meta?: Record<string, unknown>;
}

export const toolDefinitions: ToolDefinition[] = [
  {
    name: "start_monitoring",
    description:
      "Start monitoring one or more channels. This enables tracking of read/unread messages and allows efficient message retrieval.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Channel names to start monitoring (e.g., ['analysts', 'general'])",
        },
      },
      required: ["channels"],
    },
  },
  {
    name: "stop_monitoring",
    description:
      "Stop monitoring one or more channels.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Channel names to stop monitoring. If not provided, stops all.",
        },
      },
    },
  },
  {
    name: "listen",
    description:
      "Subscribe the bot to one or more Zulip streams. Required before the bot can receive real-time message events from a stream — event queues with all_public_streams only deliver events for streams the bot is subscribed to. Subscription persists server-side across session restarts.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Stream names to subscribe to (e.g., ['tracker-miner-f', 'infra']).",
        },
      },
      required: ["channels"],
    },
  },
  {
    name: "unlisten",
    description:
      "Unsubscribe the bot from one or more Zulip streams. After this, the bot stops receiving real-time events for those streams until listen is called again. Unsubscription persists server-side.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Stream names to unsubscribe from.",
        },
      },
      required: ["channels"],
    },
  },
  {
    name: "get_monitored_channels",
    description:
      "List all currently monitored channels and their state.",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
  {
    name: "get_channel_history",
    description:
      "Get message history from a channel with convenient date/time filtering. Returns formatted, readable messages from the specified time range.",
    inputSchema: {
      type: "object",
      properties: {
        channel: {
          type: "string",
          description: "Channel/stream name (e.g., 'analysts', 'general')",
        },
        topic: {
          type: "string",
          description: "Optional: filter to specific topic within the channel",
        },
        start_date: {
          type: "string",
          description: "Start date/time in ISO format or 'today', 'yesterday' (e.g., '2025-11-03', '2025-11-03T09:00:00'). Defaults to start of today.",
        },
        end_date: {
          type: "string",
          description: "End date/time in ISO format or 'now' (e.g., '2025-11-03T17:00:00'). Defaults to now.",
        },
        max_messages: {
          type: "number",
          description: "Maximum number of messages to retrieve (default: 500)",
          default: 500,
        },
        format: {
          type: "string",
          enum: ["detailed", "summary", "raw"],
          description: "Output format: 'detailed' (full formatted), 'summary' (brief), 'raw' (JSON)",
          default: "detailed",
        },
        auto_monitor: {
          type: "boolean",
          description: "Automatically start monitoring this channel and mark messages as read (default: true)",
          default: true,
        },
      },
      required: ["channel"],
    },
  },
  {
    name: "get_unread_messages",
    description:
      "Get unread messages from monitored channels. Only works for channels you're actively monitoring.",
    inputSchema: {
      type: "object",
      properties: {
        channels: {
          type: "array",
          items: { type: "string" },
          description: "Optional: specific channels to check. If not provided, checks all monitored channels.",
        },
        format: {
          type: "string",
          enum: ["detailed", "summary", "raw"],
          description: "Output format: 'detailed', 'summary', or 'raw'",
          default: "detailed",
        },
        mark_as_read: {
          type: "boolean",
          description: "Mark messages as read after retrieving (default: true)",
          default: true,
        },
      },
    },
  },
  {
    name: "send_message",
    description:
      "Send a message to a stream or as a direct message. For streams, provide 'stream' and 'topic'. For DMs, provide 'to' as user email(s). " +
      "Pass 'attachments' to send files (root-relative paths under a configured upload root, or base64) with the message.",
    inputSchema: {
      type: "object",
      properties: {
        type: {
          type: "string",
          enum: ["stream", "private"],
          description: "Type of message: 'stream' for stream messages, 'private' for direct messages",
        },
        to: {
          type: "string",
          description: "For stream: stream name. For private: comma-separated email addresses",
        },
        topic: {
          type: "string",
          description: "Topic for stream messages (required for type='stream')",
        },
        content: {
          type: "string",
          description: "The message content (supports Markdown). May be empty when attachments are given.",
        },
        attachments: {
          type: "array",
          description:
            "Files to attach (max 10). Each entry is either { file: '<root>/<path>' } — a file under one of the server's configured " +
            "upload roots, e.g. 'notes/report.pdf'; absolute paths are refused and with no roots configured local files are unavailable — " +
            "or { data: '<base64>', name: 'report.pdf' }. Optional mime_type on either (guessed from the extension otherwise). " +
            "Files are uploaded to Zulip first, then linked at the end of the message (images get a preview). With attachments, content may be empty.",
          items: {
            type: "object",
            properties: {
              file: { type: "string", description: "Root-relative path: <root>/<path> under a configured upload root" },
              data: { type: "string", description: "Base64-encoded bytes (requires name)" },
              name: { type: "string", description: "Filename shown in Zulip" },
              mime_type: { type: "string", description: "MIME type (optional)" },
            },
          },
        },
      },
      required: ["type", "to"],
    },
  },
  {
    name: "send_dm",
    description:
      "Send a direct message to a Zulip user, identified by full name, email, or numeric user id. To reply " +
      "to a DM you received, pass the sender's name or id (both are in the message metadata). Names are " +
      "matched exactly (case-insensitive); an ambiguous name errors and lists the candidates with their " +
      "ids, which are always a valid address. For group DMs pass several recipients.",
    inputSchema: {
      type: "object",
      properties: {
        to: {
          type: "array",
          items: { type: "string" },
          description: "One or more recipients: full name, email, or user id each",
        },
        content: { type: "string", description: "The message content (supports Markdown). May be empty when attachments are given." },
        attachments: {
          type: "array",
          description:
            "Files to attach (max 10): { file: '<root>/<path>' } (under a configured upload root, e.g. 'notes/report.pdf') " +
            "or { data: '<base64>', name: 'x.pdf' } each, optional mime_type. Uploaded first, then linked at the end of the message.",
          items: {
            type: "object",
            properties: {
              file: { type: "string", description: "Root-relative path: <root>/<path> under a configured upload root" },
              data: { type: "string", description: "Base64-encoded bytes (requires name)" },
              name: { type: "string", description: "Filename shown in Zulip" },
              mime_type: { type: "string", description: "MIME type (optional)" },
            },
          },
        },
      },
      required: ["to"],
    },
  },
  {
    name: "upload_file",
    description:
      "Upload a file to Zulip without sending a message. Returns the `/user_uploads/...` path, the absolute url, and " +
      "the markdown link to embed in any message body (send_message, send_dm, edit_message). Use send_message's " +
      "'attachments' instead when the file simply goes with a new message.",
    inputSchema: {
      type: "object",
      properties: {
        file: { type: "string", description: "Root-relative path: <root>/<path> under a configured upload root (never absolute)" },
        data: { type: "string", description: "Base64-encoded bytes (alternative to file; requires name)" },
        name: { type: "string", description: "Filename shown in Zulip (defaults to the file's basename)" },
        mime_type: { type: "string", description: "MIME type (optional)" },
      },
    },
  },
  {
    name: "edit_message",
    description: "Edit the content of your own Zulip messages by ID (subject to the realm's message-edit time limit). Use move_messages to rename topics or move messages.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: { type: "number", description: "ID of the message to edit" },
        content: { type: "string", description: "The new message content (supports Markdown, replaces the old content entirely)" },
      },
      required: ["message_id", "content"],
    },
  },
  {
    name: "move_messages",
    description: "Rename a topic or move Zulip messages by message ID. Provide topic or stream_id. Zulip defaults to change_one (this message only); use change_all for the whole topic. Realm permissions and move time limits apply, including to other people's messages. Use list_streams to find a destination stream_id. Content edits use edit_message separately. Zulip posts a Notification Bot breadcrumb in the destination topic by default. To resolve a topic, rename it to ✔ <topic> with change_all. After a cross-stream move, confirm with send_message to moved_to's stream and topic, not channels/publish (the destination channel may have a newer conversation).",
    inputSchema: {
      type: "object",
      properties: {
        message_id: { type: "number", description: "ID of the message to move" },
        topic: { type: "string", description: "New topic name" },
        stream_id: { type: "integer", minimum: 1, description: "Destination stream ID" },
        propagate_mode: {
          type: "string",
          enum: ["change_one", "change_later", "change_all"],
          description: "Requires topic or stream_id. Zulip defaults to change_one (this message only); change_later moves this and later messages in the same topic; change_all renames/moves the whole topic. Omit to use the server default.",
        },
      },
      required: ["message_id"],
    },
  },
  {
    name: "delete_message",
    description: "Delete a Zulip message by ID. You can delete your own messages, and if you have permissions, others' messages too.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: {
          type: "number",
          description: "ID of the message to delete",
        },
      },
      required: ["message_id"],
    },
  },
  {
    name: "list_streams",
    description: "Get all streams in the Zulip organization",
    inputSchema: {
      type: "object",
      properties: {
        include_public: {
          type: "boolean",
          description: "Include public streams",
          default: true,
        },
        include_subscribed: {
          type: "boolean",
          description: "Include subscribed streams",
          default: true,
        },
        verbose: {
          type: "boolean",
          description: "Include full raw API response (warning: very large)",
          default: false,
        },
      },
    },
  },
  {
    name: "get_stream_topics",
    description: "Get all topics in a specific stream",
    inputSchema: {
      type: "object",
      properties: {
        stream_id: {
          type: "number",
          description: "ID of the stream",
        },
        verbose: {
          type: "boolean",
          description: "Include full raw API response (warning: very large)",
          default: false,
        },
      },
      required: ["stream_id"],
    },
  },
  {
    name: "list_users",
    description: "Get all users in the Zulip organization",
    inputSchema: {
      type: "object",
      properties: {
        client_gravatar: {
          type: "boolean",
          description: "Whether to include gravatar URLs",
          default: false,
        },
        verbose: {
          type: "boolean",
          description: "Include full raw API response (warning: very large)",
          default: false,
        },
      },
    },
  },
  {
    name: "get_user_profile",
    description: "Get the profile of the authenticated user/bot",
    inputSchema: {
      type: "object",
      properties: {},
    },
  },
  {
    name: "add_reaction",
    description: "Add an emoji reaction to a message",
    inputSchema: {
      type: "object",
      properties: {
        message_id: {
          type: "number",
          description: "ID of the message",
        },
        emoji_name: {
          type: "string",
          description: "Name of the emoji (e.g., 'thumbs_up', 'heart', 'rocket')",
        },
      },
      required: ["message_id", "emoji_name"],
    },
  },
  {
    name: "remove_reaction",
    description: "Remove this bot's own emoji reaction from a message.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: { type: "number", description: "ID of the message" },
        emoji_name: { type: "string", description: "Name of the emoji previously added by this bot" },
      },
      required: ["message_id", "emoji_name"],
    },
  },
  {
    name: "list_emojis",
    description:
      "List the realm's custom emoji. Use the `name` with add_reaction, or `:name:` inside message content to render it. " +
      "Standard unicode emoji are always available by name (e.g. 'thumbs_up', 'eyes') and are not listed here.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "set_reaction_visibility",
    description:
      "Opt a channel in or out of showing emoji reactions live. When ON, reactions added or removed on ANY message in " +
      "that channel appear in your context as they happen. They carry only reaction tags (chat:reaction, " +
      "chat:reaction-remove), so a wake policy keyed on tags leaves you asleep; a policy that wakes on everything in " +
      "the channel wakes on them too. Default OFF, persisted across restarts. (Reactions on history you fetch always " +
      "show via fetch_history regardless of this setting.)",
    inputSchema: {
      type: "object",
      properties: {
        channel: { type: "string", description: "Stream name, or channel id ('zulip:general', 'zulip:dm:42')" },
        visible: { type: "boolean", description: "true = surface live reactions from this channel; false = stop." },
      },
      required: ["channel", "visible"],
    },
  },
  {
    name: "find_user",
    description: "Find a Zulip user by name or email to get their ID for mentions. Use @**username** format in messages to mention.",
    inputSchema: {
      type: "object",
      properties: {
        query: {
          type: "string",
          description: "Name or email to search for",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "fetch_history",
    description:
      "Fetch message history from a stream (optionally one topic) or a DM conversation, oldest first. By default returns " +
      "the most recent messages. Use `before` (a message id) to scroll further back — pass the id of " +
      "the oldest message you have seen to page backwards. Use `after` (a message id) to fetch only " +
      "messages newer than a given point. Message ids are realm-global and increase over time, so " +
      "they work as cursors across streams and topics. Each line leads with the id so you can " +
      "fetch_around(id) for more context.",
    inputSchema: {
      type: "object",
      properties: {
        channel: { type: "string", description: "Stream name (e.g. 'general'), stream channel id ('zulip:general'), or DM conversation id ('zulip:dm:42')" },
        topic: { type: "string", description: "Optional: limit to one topic (streams only)" },
        limit: { type: "number", description: "Max messages to fetch (default 50, max 1000)" },
        before: { type: "number", description: "Only messages older than this message id (exclusive)" },
        after: { type: "number", description: "Only messages newer than this message id (exclusive)" },
      },
      required: ["channel"],
    },
  },
  {
    name: "fetch_around",
    description:
      "Scroll to a specific message and fetch the surrounding context: the message itself plus roughly " +
      "half the window on either side, in whatever stream and topic it lives in. Message ids come " +
      "from incoming messages, fetch_history lines, and <missed> catch-up blocks.",
    inputSchema: {
      type: "object",
      properties: {
        message_id: { type: "number", description: "Message to centre the window on" },
        limit: { type: "number", description: "Total window size (default 50, max 200)" },
      },
      required: ["message_id"],
    },
  },
  {
    name: "channel_missed",
    description:
      "Report how much ambient (non-mention) traffic you have missed in a stream since the host CLOSED " +
      "its channel — returns missed message and character counts. Mentions always reach you and are " +
      "not counted. Counts are durable across restarts and backfill downtime on reconnect. Useful for " +
      "deciding whether to reopen the channel.",
    inputSchema: {
      type: "object",
      properties: {
        channel: { type: "string", description: "Stream name (e.g. 'general') or channel id ('zulip:general')" },
      },
      required: ["channel"],
    },
  },
  {
    name: "filters_get",
    description:
      "Show the active event filters: which streams may deliver events to you (null = every stream the bot can " +
      "see), which users may DM you (null = anyone), and which streams are muted. Reports the filters plane's " +
      "desired-vs-effective state (live / stale) and the reaction-suppression state as a count and " +
      "digest — the suppressed entries themselves are operator-owned and never shown here.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "filters_update",
    description:
      "Hot-adjust the event filters — takes effect immediately and persists, no restart. If the stream allowlist is " +
      "unrestricted, the first removal materializes it as the list of all current streams first, so nothing silently " +
      "drops. Newly allowed streams are registered right away. Reaction-suppression entries are operator-owned and " +
      "cannot be carried by this tool. Refused (without writing) while the filters file is unreadable on disk.",
    inputSchema: {
      type: "object",
      properties: {
        addStreams: { type: "array", items: { type: "string" }, description: "Stream names to allow" },
        removeStreams: { type: "array", items: { type: "string" }, description: "Stream names to stop delivering" },
        setDmUsers: {
          type: "array",
          items: { type: "string" },
          description: "Replace the DM allowlist with these user ids/emails. CAREFUL: an empty array means UNRESTRICTED (anyone may DM). Omit to leave DMs unchanged.",
        },
      },
    },
  },
  {
    name: "mute_channel",
    description:
      "Mute a stream entirely: no ambient messages, no wake on mentions, nothing tallied. Use this to stay out of a " +
      "stream that keeps pulling you in. Persisted across restarts. Reverse with unmute_channel.",
    inputSchema: {
      type: "object",
      properties: { channel: { type: "string", description: "Stream name or channel id ('zulip:general')" } },
      required: ["channel"],
    },
  },
  {
    name: "unmute_channel",
    description:
      "Un-mute a stream: mentions reach you again, and ambient traffic once the host opens the channel. Persisted.",
    inputSchema: {
      type: "object",
      properties: { channel: { type: "string", description: "Stream name or channel id ('zulip:general')" } },
      required: ["channel"],
    },
  },
  {
    name: "refresh_channels",
    description:
      "Re-scan every stream and DM conversation the bot can currently see and register any the host does not yet " +
      "know about — use it if you were added to a stream after startup and it is not in your channel list.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "fetch_attachment",
    description:
      "Fetch a Zulip user-upload attachment by path and return its bytes inline. " +
      "Images (png/jpg/jpeg/gif/webp) return as an image content block usable by vision models. " +
      "Text-ish MIME types (text/*, JSON, CSV, YAML) return as `content_text` so the model can read directly. " +
      "Other binaries return as `base64`. Paths look like '/user_uploads/X/Yy/Zz/name.ext' and appear in " +
      "incoming message attachment refs. Only paths under /user_uploads/ on the configured realm are allowed.",
    inputSchema: {
      type: "object",
      properties: {
        path: {
          type: "string",
          description: "Attachment path (e.g. '/user_uploads/2/Ab/cd/screenshot.png') or full Zulip URL.",
        },
      },
      required: ["path"],
    },
  },
];
