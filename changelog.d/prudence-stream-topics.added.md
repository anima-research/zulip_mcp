- `get_stream_topics` takes the stream by its name, as `stream`, beside
  `stream_id`, and a `limit` that lists only the newest topics. A call gives
  exactly one of `stream` and `stream_id`. A `stream_id` that isn't a positive
  integer, given as a number or as a string of digits, is refused before Zulip
  is asked; it used to go into the request's path as written. `topic_count` still counts every
  topic, and the list's first line says when `limit` cut it. `send_message`'s
  `topic` text now points to it, so an agent can see a stream's recent topics
  before it starts one (#51).
