- On live delivery, a message whose attachments were still being fetched could
  reach the host after a later message of its channel. If the host then
  refused it, having accepted the later one, it was not offered again. Each
  channel's messages, reactions and changes are now handled one at a time, in
  the order the server received them, so a reaction or an edit follows the
  message it is about. A message waiting for its attachments holds back only
  its own channel.
