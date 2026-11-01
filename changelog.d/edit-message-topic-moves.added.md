- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`. Hosts can allowlist moves separately
  from `edit_message`, which remains content-only for the bot's own messages.
  Zulip enforces realm permissions and move limits.
- The bot's own moves reconcile message placement and current reply/typing
  hints without delivering a change marker. Cross-stream moves return
  `moved_to: {channelId, topic}` for publishing into the destination; unresolved
  routing fields are null with a diagnostic if Zulip cannot supply them.
- Move-limit errors include the Zulip code and partial-move retry fields.
- Moves out of filtered streams also update cached placement: the source move
  marker stays filtered, while later reactions, edits and deletions are judged
  against the destination stream, including snippets of messages now there.
