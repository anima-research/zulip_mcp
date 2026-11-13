- `get_stream_topics` takes the stream by its name, as `stream`, beside
  `stream_id`, and a `limit` that lists only the newest topics. A call gives
  exactly one of `stream` and `stream_id`. `topic_count` still counts every
  topic, and the list's first line says when `limit` cut it. `send_message`'s
  `topic` text now points to it, so an agent can see a stream's recent topics
  before it starts one (#51). With MCPL, the old name of a rename that the
  server saw, where no stream has that name now, is refused with the new name
  before anything goes to Zulip, as the send tools refuse it.
