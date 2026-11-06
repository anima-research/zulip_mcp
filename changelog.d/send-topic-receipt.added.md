- `send_message` accepts `topic` with a stream `reply_to`, retaining the referenced
  stream while choosing a new topic. Private replies refuse nonempty topics;
  explicit `to` and conflicting types are still refused.
- `send_message` receipts include `type`, `channelId`, and `stream`/`topic` or
  `recipients`; `send_dm` also includes its type, recipients, and channel ID.
  Existing API fields, attachments, and chunk IDs are retained.
- Strict-shaped `send_message` calls may use null or empty strings for unused
  `type`, `to`, `topic`, and `reply_to`. Explicitly empty stream topics without
  `reply_to` remain errors. Published destination schemas now allow null (including
  `move_messages` topic/stream_id); `reply_to: 0` is also treated as absent.
