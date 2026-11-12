- On live delivery, the fetches of one message's attachments share one 15 s
  bound, where each fetch had 15 s of its own. An attachment not fetched by
  then gets the "could not fetch" note with its path for `fetch_attachment`,
  and an attachment after it isn't fetched. So a message waits at most 15 s
  for its attachments, where it could wait 15 s for each.
