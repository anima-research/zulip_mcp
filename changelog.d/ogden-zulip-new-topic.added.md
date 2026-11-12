- `send_message` takes `topic` beside `reply_to` when that message is a stream
  message, and sends to its stream in the given topic.
- After a send with `reply_to`, `send_message`'s result says where the message
  went: `channelId` and `channel`, the stream's name, for a stream message,
  with `topic` when none was given, and `channelId` and `to_user_ids`, the
  other participants, for a direct message.
