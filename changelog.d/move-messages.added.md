- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`. Hosts can allowlist moves separately
  from `edit_message`, which remains content-only for the bot's own messages.
  Zulip enforces realm permissions and move limits.
- The bot's own moves reconcile message placement and current reply/typing
  hints without delivering a change marker. Successful tool moves reconcile
  before returning, without depending on the event echo. Echo tokens are
  capped at 16, expire after 60 seconds, and clear on queue registration/stop.
  Unrelated applied events invalidate affected tokens. Matching completed
  tokens acknowledge without replay only while cache provenance remains a
  recognized local move, including later tool results in overlapping ranges.
  Ambiguous, expired, or unmatched self echoes issue zero placement GETs,
  regardless of range size or uncached IDs. They invalidate affected cached
  placement and exact-message reply routes locally; channel-only publishing
  fails closed until a later real message or tool result re-establishes routing.
  Normal reaction/edit lookups and tool source/result reads are unchanged.
  Typing stops in the topic where its run began. Cross-stream moves return
  `moved_to: {channelId, topic}` for confirmation via `send_message` with an
  explicit stream and topic, not a channel-only publish. Source-channel
  publishes are refused and source typing stops after the move; unresolved
  routing fields are null with a diagnostic if Zulip cannot supply them.
- Move-limit errors include the Zulip code and partial-move retry fields.
- Moves out of filtered streams also update cached placement: the source move
  marker stays filtered, while later reactions, edits and deletions are judged
  against the destination stream, including snippets of messages now there.
