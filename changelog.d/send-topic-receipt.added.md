- `send_message` accepts `topic` with a stream `reply_to`, retaining the referenced
  stream while choosing a new topic. Private replies refuse nonempty topics;
  explicit `to` and conflicting types are still refused.
- `send_message` receipts include `type`, `channelId`, and `stream`/`topic` or
  `recipients`; `send_dm` also includes its type, recipients, and channel ID.
  Existing API fields, attachments, and chunk IDs are retained.
- Strict-shaped `send_message` calls may use null or empty strings for unused
  `type`, `to`, `topic`, and `reply_to`; numeric/string zero reply IDs are absent.
  Standalone stream sends now require a non-empty trimmed topic for every absent
  spelling, deliberately refusing Zulip 10+ general chat rather than relying on
  realm/server behavior. Reply topic overrides are trimmed and type-checked before
  uploads. Type aliases normalize in both payloads and receipts; non-reply receipt
  destinations echo normalized input rather than confirming server placement.
- Nullable placeholders cover `move_messages` topic/stream_id/propagate_mode,
  `fetch_history` cursors (0 is absent), and `get_unread_messages` channels ([] is
  absent). For `filters_update`, null leaves fields unchanged; `setDmUsers: []`
  deliberately retains its existing UNRESTRICTED meaning, never a placeholder.
  The README lists remaining tools outside the strict-placeholder contract.
