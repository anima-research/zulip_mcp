- On live delivery, the fetches of one message's attachments share one 15 s
  bound, where each fetch had 15 s of its own. An attachment not fetched by
  then gets the "could not fetch" note with its path for `fetch_attachment`,
  and an attachment after it isn't fetched. So a message waits at most 15 s
  for its attachments, where it could wait 15 s for each.
- On a channel where the host has accepted nothing, a message that isn't
  accepted now gives the channel a watermark just below it, for example
  because the host refused it, the connection ended first, or
  `zulip.messaging` was disabled. So a later direct message of that
  conversation carries no reply note, and a `sinceLastSeen` open reads
  forward from that message, where it read the newest messages.
- After a reconnect within one process, a channel's watermark follows only
  what the new connection's host accepts. It used to jump, once a held
  message was settled, to the newest message that an earlier connection's
  host had accepted. In one sequence the next catch-up now repeats messages
  that the host already has: the host refused a message and reconnected, a
  `sinceLastSeen` open whose limit cut its history settled that message, and
  the connection ended before the host accepted the catch-up block.
- A message waiting for its quote lookup, or for a read of the stream list
  for its stream id, now holds back only its own channel. It held back every
  later event: up to 3 s for a quote lookup, and up to 15 s for the read.
- While the host hasn't answered a stream's rename, nothing of that stream
  goes to the host: agent-framework drops the old id as it reads the
  announcement, before it answers. A host that doesn't answer delays the
  stream's messages by the announcement's 5 s bound, after which they go
  under the old id.
- A read from history that fills a hole and gets no answer from Zulip, or
  that Zulip refuses by its rate limit, is now tried again: after 2 s, then
  at waits that double up to 60 s, or after Zulip's `retry-after` where that
  is longer. These are the catch-up at a connection's start, the recovery
  after Zulip's event queue broke, and the read after a channel gave up its
  waiting messages. Meanwhile the channel's later messages wait behind the
  read, so the messages of the hole reach the host first, in order. Such a
  read used to be logged and skipped, and the messages of that stretch were
  never offered once the host accepted a later one. A read that Zulip
  refuses for another reason still ends the hole, as before.
