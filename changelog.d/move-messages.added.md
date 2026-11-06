- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`, separately allowlisted from
  content-only `edit_message`. Zulip enforces permissions and move limits;
  errors include partial-move retry fields.
- Tool success confirms only an anchor not invalidated during the call. Other
  references a range move may have touched are refused, never confirmed by
  echoes. Only the exact anchor or a known-source topic match gets a destination
  hint, with the stored (or, after a failed read, requested) topic labelled likely.
  Unknown-source conversations get only their own reply_to recovery, no fallback.
- A deletion of the followed message refuses replies if no cached message
  remains in that stream/topic: the messages were deleted or moved where this
  bot can't see them. Other deletions leave routing unchanged. Visible third-party
  moves refuse replies following affected IDs, with the event's destination hint.
  Moves out of visibility arrive as ordinary deletions, not a distinct notice.
- Refusals say Nothing was posted and include messageId and a bare stream.
  For move refusals, resolve current placement with
  `send_message({reply_to: <messageId>, content})`. A deletion/hidden-move
  refusal means that message cannot be replied to: to answer a newer message
  use `send_message({reply_to: <its id>, content})`; otherwise choose an explicit
  destination. Use a known bare stream/topic, or private recipient for DMs.
  Same-stream and cross-stream results include `moved_to: {channelId, stream, topic}`;
  unknown fields are null. Check list_streams/get_stream_topics if needed.
- Only a new message (but not this bot's own messages) above the refusal's
  message-id floor clears refusal: in any topic of an open stream, or accepted as a push on a closed channel.
  In the stream/topic the conversation left, only bot senders are ignored
  (including old-topic Notification Bot notices); a person's new message there,
  including a delete-and-repost, clears refusal. Destination breadcrumbs can. Routing then follows
  that message's topic. Existing/moved copies, echoes, reads,
  reactions, edit markers and this bot's own messages cannot clear it. Explicit
  sends bypass refusal one reply at a time, not clear it. The bot's own moves emit no change marker;
  typing stops where its run began.
