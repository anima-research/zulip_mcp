- On live delivery, a message whose attachments were still being fetched could
  reach the host after a later message of its channel. If the host then
  refused it, having accepted the later one, it was not offered again. Each
  channel's messages, reactions and changes are now handled one at a time, in
  the order the server received them, so a reaction or an edit follows the
  message it is about. A message waiting for its attachments holds back only
  its own channel.
- A host's `channels/open` or `channels/close` of a channel the server
  already knows no longer waits for the requests ahead of it, such as a tool
  call that waits for the host. It waits for the job running on its own
  channel, such as one message being handled. So a host that opens a channel
  before it answers that channel's announcement, as agent-framework 0.18.0
  and older did, no longer stalls for 5 s behind a tool call that announced
  the channel.
- A message still waiting for its attachments when the host opened its
  channel came both in the open's history and again as a message. The
  history an open returns now ends below the first message of its channel
  that the server hasn't handed on yet, and each such message follows the
  open's answer once, as a message.
- After a restart, a message the host hadn't accepted wasn't offered again
  when the host had accepted nothing on its channel before it, such as one
  the host refused, or one that was waiting or on its way when the
  connection ended. Such a channel had no watermark on disk. It now gets one
  just below that message, so the next start offers it.
- A message that arrived while the host had `zulip.messaging` disabled was
  dropped, unless it was already waiting in the batch. It is now held, and
  offered once the host enables `zulip.messaging` again.
- A stream muted, or left out of the allowlist, while an addressed message
  of its closed channel waited for its attachments no longer has that
  message pushed: the mute and the allowlist are checked again when the
  attachments are in.
- A message could reach the host twice when its event arrived after an
  open's `sinceLastSeen` history had already returned it: once in the
  history, and again as a message. An event for a message that its channel
  has already dealt with is now dropped.
- An open whose history was a channel's newest messages counted every
  message below that history as seen, including ones the server hadn't
  handed on yet, such as at a connection's start before the channel's
  catch-up. If the connection ended before the catch-up, those messages were
  never offered. Such an open now counts as seen only what its channel had
  already dealt with, and the catch-up block moves the watermark when the
  host accepts it.
- A catch-up block the host refused was lost once the host accepted a later
  message of its channel: the watermark moved past the block. The block's
  first message is now held, so the watermark stays below it, and the block
  is offered again when the host next accepts something.
- A message offered again from history on an open channel, after the host
  refused it, its batch was lost or Zulip's event queue broke, now has its
  attachments inline, as on live delivery. It had the reference note only.
- An offer that the host refused, or that was lost, after its stream was
  muted or left out of the allowlist was held again, so the channel's
  watermark stayed below it until the host accepted something else there.
  Such an offer is now let go.
- After Zulip's event queue broke, a message could reach the host twice
  when both its event and the recovery's read of its channel's history
  brought it: once recovered, and once as a message. The recovery now reads
  only up to the first message that waits on its channel, and an event for a
  message that the recovery already handed on is dropped.
- After Zulip's event queue broke, the recovery read from the watermark, so
  it offered again the messages the host had accepted above one that it
  refused. It now offers again only what the host hasn't accepted.
- After an open whose `sinceLastSeen` history its limit cut, a break of
  Zulip's event queue handed the channel, as recovered, the messages above
  that history, if the host had accepted nothing there since. Those messages
  had been counted as missed while the channel was closed. The recovery now
  starts above the newest message its channel has dealt with.
