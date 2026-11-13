- `move_messages` moves a message to another topic, another stream, or both:
  the one message, the message and those after it in its topic, or its whole
  topic (`scope`). Zulip decides whether the bot may move it, and the tool
  passes none of Zulip's notice switches, so Zulip's defaults decide which
  "moved" notices it posts. The result says where the message is afterwards,
  as Zulip reads it back; where that read fails or names no place, it says the
  move was made, with the read's error. Its `channelId` is `null` where the
  host holds the stream's name's id for another stream, or the server's list
  doesn't name the stream yet. After a move that names no stream, the tool
  waits up to 5 s for the channel to take the move in, so that a plain answer
  right after the result goes where the move put the message it answers. Where
  the tool can't say that a plain answer lands right, as after a move that
  names a stream, a read that fails or gives no place, or a wait that reaches
  its 5 s, the result says to answer the message with `send_message`'s
  `reply_to`. The tool belongs to `zulip.messaging` and is on by default; a
  realm limits it with Zulip's permissions for moving messages.
