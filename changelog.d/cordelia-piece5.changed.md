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
