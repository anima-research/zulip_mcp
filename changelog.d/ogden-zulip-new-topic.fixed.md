- `send_message` refuses a stream message that has neither `topic` nor
  `reply_to`, before uploading or sending. The error says to name a topic, or
  to use `reply_to` to answer a message where it is. Such a message used to
  land in a topic named "undefined", though the tool's text already said a
  stream message needs a topic.
