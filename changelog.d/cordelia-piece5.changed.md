- On live delivery, the fetches of one message's attachments share one 15 s
  bound, where each fetch had 15 s of its own. An attachment whose fetch
  that bound cuts gets the "could not fetch" note with its path for
  `fetch_attachment`, and so does each later attachment the server would
  fetch, which it doesn't fetch then. So a message waits at most 15 s for
  its attachments' fetches, where it could wait 15 s for each.
- On a channel where the host has accepted nothing, a message that isn't
  accepted now gives the channel a watermark just below it, for example
  because the host refused it, the connection ended first, or
  `zulip.messaging` was disabled. So a later direct message of that
  conversation carries no reply note, and a `sinceLastSeen` open reads
  forward from that message, where it read the newest messages. While
  `zulip.messaging` is disabled the watermark comes as the message comes in,
  so a conversation's first direct message then carries no reply note
  either.
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
- While the host hasn't answered a stream's rename, none of that stream's
  messages, or the lines about them, go to the host: agent-framework drops
  the old id as it reads the announcement, before it answers. A host that
  doesn't answer delays the stream's messages by the announcement's 5 s
  bound, after which they go under the old id.
- A read from history that fills a hole and gets no answer from Zulip, or
  that Zulip refuses by its rate limit, is now tried again: after 2 s, then
  at waits that double up to 60 s, or after Zulip's `retry-after` where that
  is longer. These are the catch-up at a connection's start, the read of the
  hole after Zulip's event queue broke, and the read after a channel gave up
  its waiting messages. Meanwhile the channel's later messages wait behind
  the read, so the messages of the hole reach the host first, in order. Such
  a read used to be logged and skipped, and the messages of that stretch
  were never offered once the host accepted a later one. A read that Zulip
  refuses for another reason still ends the hole, as before. Once a
  channel's wait for its history is set to reach 90 s, every open channel
  gets one line naming the channels whose reads wait then, and one more line
  when no read waits any more. The first line names no channel that is muted
  or outside the allowlist, and goes only if it names one. With no channel
  open, or while `zulip.messaging` is disabled, both lines go to the log,
  and the first line goes to the open channels at the read's next failed
  try once a channel is open and `zulip.messaging` is enabled. A channel
  whose stream's rename is with
  the host at that moment may not get them: they go there under the old id,
  and the host may drop them. After a break, each open channel's notice
  of the break counts what had come when the notice reached that channel,
  and says how many channels still wait for a read of their history.
