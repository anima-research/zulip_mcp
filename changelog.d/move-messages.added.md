- `move_messages` renames topics or moves messages across streams with optional
  `topic`, `stream_id`, and `propagate_mode`, separately allowlisted from
  content-only `edit_message`. Zulip enforces permissions and move limits;
  errors include partial-move retry fields.
- Tool success confirms only an anchor not invalidated during the call. Other
  references a range move may have touched are refused with the anchor's stored
  destination topic labelled likely, never confirmed by echoes.
- Third-party deletions leave routing unchanged. Third-party moves refuse replies
  following affected message IDs, with the event's destination stream/topic hint.
- Refusals say Nothing was posted and include messageId and a bare stream.
  On upstream main (#39), recover with `send_message({reply_to: <messageId>})`;
  otherwise use an explicit bare stream/topic, or private recipient for DMs.
  Same-stream and cross-stream results include `moved_to: {channelId, stream, topic}`;
  unknown fields are null. Check list_streams/get_stream_topics if needed.
- Only a non-bot message above the refusal's message-id floor clears refusal:
  in any topic of an open stream, or accepted as a push on a closed channel.
  Routing then follows that message's topic. Existing/moved copies, echoes, reads,
  reactions, edit markers and bot messages cannot clear it. Explicit sends bypass
  refusal one reply at a time, not clear it. The bot's own moves emit no notice;
  typing stops where its run began.
