- Edits, topic moves and deletions of messages the agent has seen now reach
  it. The event queue registers `update_message` and `delete_message` with
  `bulk_message_deletion` (it asked only for `message` and `reaction`, so
  Zulip never sent them); a change surfaces as one `[edited]` / `[moved]` /
  `[deleted]` line in the shared message shape, tagged `chat:edited` /
  `chat:deleted` plus the addressing tag the message would carry
  (`chat:mention` / `chat:dm` / `chat:ambient`). It is as visible as its
  message was: open channels see changes to accepted or offered messages
  and to the bot's own, closed channels are pushed only addressed ones
  (a deleted mention included); re-renders and the bot's own edits and
  deletions never surface; a cross-stream move is reported where the
  message was seen, and a move into a stream the bot cannot see is
  reported as "no longer visible" rather than deleted. Change lines don't retarget reply routing, and
  reaction lines no longer do; the one exception is a move of the message a
  plain answer follows, to another topic in its stream. `PlatformAdapter` gains optional
  `onMessageChange` (on `startEvents`) and `noteSelfDeleted`. History and
  backscroll render `(edited)` / `(moved)` trailers with
  `metadata.editedAt` / `movedAt` (#22).
