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
