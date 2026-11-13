- `move_messages` moves a message to another topic, another stream, or both:
  the one message, the message and those after it in its topic, or its whole
  topic (`scope`). Zulip decides whether the bot may move it, and the tool
  passes none of Zulip's notice switches, so Zulip's defaults decide which
  "moved" notices it posts. The result says where the message is afterwards,
  as Zulip reads it back; where that read fails or names no place, it says
  the move was made, with the read's error. A call that names a stream also says to answer the
  message with `send_message`'s `reply_to`. The tool belongs to
  `zulip.messaging` and is on by default; a realm limits it with Zulip's
  permissions for moving messages.
