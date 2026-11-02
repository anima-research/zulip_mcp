- Treat stream quote replies to the connector's own messages as addressed
  replies (`chat:reply`, not `chat:mention`, with `origin.isReply` on pushes),
  using the quoted message's sender rather than the silent mention's label.
  Message handlers are awaited to preserve delivery order, with quote lookups
  limited to at most the first two distinct targets per incoming message and a
  shared three-second wait budget per message. A poll batch containing several
  quote replies can delay later events by up to that budget for each message.
  Channel history uses the same classifier for refused-push recovery, reconnect
  sweeps, backscroll and queue-gap recovery: page/cache authors first, then at
  most eight uncached target GETs within a shared three-second wait budget per
  page, including reconnect sweeps and tally backfill. Budget-starved rows
  stop the initial closed-channel block short of the first deferred row and
  receive one retry walk over all deferred rows in the same catch-up call.
  Quote waiting across all pages and both walks is capped at six seconds
  (three seconds per pass); rows still unverified then fail closed.
  Content edits recompute reply addressing from the new raw Markdown; moves
  and deletions retain the last known reply verdict separately from mentions.
  Translated heading punctuation (spaces, including NBSP, and full-width
  colons) is accepted. Forwarding-style ` in …:` / ` to …:` headings are
  deliberately excluded, including such first headings in multi-quotes.
  Authors with bot-like email addresses (`-bot@`) do not receive inferred
  quote addressing; explicit mentions and DMs are unchanged.
  Stopping event delivery during a lookup or poll drops stale callbacks and
  the rest of the stopped batch, including across connection restarts.
  Failed or missing quote-target lookups (not timeouts) are negatively cached for
  three minutes (bounded to 2,000 ids), without suppressing independent
  reaction or edit placement retries. Budgets limit waiting, not request
  lifetime: the underlying zulip-js GET cannot be cancelled by the timer.
  The delivery model, edit tags and line-label semantics are documented in
  the README alongside the remaining lookup and heading-format limits.
  Addressed-only catch-up blocks use `reason="addressed"` when their count
  includes quote replies, retaining `reason="mention"` for mention-only counts.
  Unaccepted live reply verdicts survive fail-closed history reclassification
  during sweeps and retries; go-live deduplication distinguishes scanning from
  actual acceptance of a buffered reply.
  Staged catch-up progress is also committed on graceful shutdown and before
  connection reset, so accepted blocks are not replayed solely due to that window.
  Quote edits clear a pending live verdict only when the new content has no
  quote target or all inspected targets were verified as not authored by the bot.
  Releasing a muted/disallowed channel or a message gone from replay history
  also clears its pending reply verdict, preventing stale history overlays.
