- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`. Hosts can allowlist moves separately
  from `edit_message`, which remains content-only for the bot's own messages.
  Zulip enforces realm permissions and move limits.
