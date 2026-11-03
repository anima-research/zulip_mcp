- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`, separately allowlisted from
  content-only `edit_message`. Zulip enforces permissions and move limits;
  errors include partial-move retry fields.
- Tool success confirms only an anchor not invalidated during the call. Other
  references a range move may have touched are refused with the anchor destination
  as a hint.
- On refusal use `send_message` with explicit bare stream/topic. Cross-stream
  results include `moved_to: {channelId, stream, topic}`; unknown fields are null.
  Inspect `list_streams` / `get_stream_topics` when needed.
- Co-moved routing re-establishes only at a new non-bot message in that channel,
  never from an existing/moved copy, echo, read, reaction or self-event. The bot's
  own moves emit no move notice; typing stops where its run began.
