- Treat incoming stream quote replies to the connector's own messages as addressed replies (`chat:reply`, not `chat:mention`),
  using the quoted message's sender rather than the silent mention's label.
  Message handlers are awaited to preserve delivery order, with quote lookups
  limited to at most the first two distinct targets per incoming message and a
  shared three-second budget per message. A poll batch containing several quote
  replies can delay later events by up to that budget for each message.
  Channel history uses the same classifier for refused-push recovery, reconnect
  sweeps, backscroll and queue-gap recovery: page/cache authors first, then at
  most eight uncached target GETs within a shared three-second wait budget per
  page. Unresolved targets fail closed.
  Content edits recompute reply addressing from the new raw Markdown; moves
  and deletions retain the last known reply verdict separately from mentions.
  Translated heading punctuation (spaces, including NBSP, and full-width
  colons) is accepted. Forwarding-style ` in …:` / ` to …:` headings are
  deliberately excluded, including such first headings in multi-quotes.
  Authors with bot-like email addresses (`-bot@`) do not receive inferred
  quote addressing; explicit mentions and DMs are unchanged.
  Stopping event delivery during a lookup or poll drops stale callbacks and
  the rest of the stopped batch, including across connection restarts.
