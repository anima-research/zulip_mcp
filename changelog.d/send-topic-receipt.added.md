- `send_message` accepts `topic` with a stream `reply_to`, retaining the referenced
  stream while choosing a new topic.
- `send_message` receipts include `type`, `channelId`, and `stream`/`topic` or
  `recipients`; `send_dm` also includes its type, recipients, and channel ID.
  Existing API fields, attachments, and chunk IDs are retained. Non-reply
  destinations and reply topic overrides echo input, not confirmed placement.
- Strict-shaped `send_message` calls may use null or empty strings for unused
  `type`, `to`, `topic`, and `reply_to`; numeric/string zero reply IDs are absent.
  Type aliases normalize in both payloads and receipts.
- Nullable placeholders cover `move_messages` topic/stream_id/propagate_mode,
  `fetch_history` cursors, and `get_unread_messages` channels ([] is absent).
  For `filters_update`, null leaves fields unchanged. The README lists remaining
  tools outside the strict-placeholder contract.
