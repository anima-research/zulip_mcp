- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`. Hosts can allowlist moves separately
  from `edit_message`, which remains content-only for the bot's own messages.
  Zulip enforces realm permissions and move limits.
- The bot's own moves reconcile message placement and current reply/typing
  hints without delivering a change marker. Successful tool moves reconcile
  before returning, without depending on the event echo. Echo tokens are
  capped at 16, expire after 60 seconds, and clear on queue registration/stop.
  Intervening events invalidate affected tokens; only unchanged cached tool
  placements can acknowledge without replay. Ambiguous or expired self moves
  read current stored placement per message, preserving overlapping ranges
  and genuine later moves with the same transition. Failed placement reads
  leave routing unconfirmed rather than publishing to stale placement. Typing stops in the
  topic where its run began. Cross-stream moves return
  `moved_to: {channelId, topic}` for confirmation via `send_message` with an
  explicit stream and topic, not a channel-only publish. Source-channel
  publishes are refused and source typing stops after the move; unresolved
  routing fields are null with a diagnostic if Zulip cannot supply them.
- Move-limit errors include the Zulip code and partial-move retry fields.
- Moves out of filtered streams also update cached placement: the source move
  marker stays filtered, while later reactions, edits and deletions are judged
  against the destination stream, including snippets of messages now there.
