- `fetch_around` stays inside the anchor's conversation; without a narrow
  Zulip answered with the realm-wide timeline (#16).
- `fetch_attachment` refuses a protocol downgrade from the realm's scheme
  (an `http` URL on an `https` realm), which would have sent the bot's
  credentials in clear.
- A message that arrives before its channel's catch-up at a connection's
  start waits for that catch-up and follows it, in order, so a live
  delivery cannot advance the watermark over the offline gap, and a later
  live message cannot overtake it. Past 5,000 waiting messages, the channel
  with the most gives its waiting messages up to a read from history in
  their place. A `channel_open` with backscroll
  answered before the catch-up cannot hide the gap from it.
- Catch-up pages past a page holding nothing but the bot's own messages
  instead of reading it as the end of history.
- `ZULIP_MISSED_BLOCK_MAX_CHARS` is a hard cap on the whole `<missed>`
  block, header and notes included; a single over-long line is cut and
  says so. Catch-up notes point at DM conversations by channel id, which
  `fetch_history` now reads.
- Suppressed reaction markers are withheld from the legacy `raw` history
  format too.
- Shutdown (EOF, `SIGTERM`, `SIGINT`) waits for deliveries still fetching
  their attachments, flushes the last delivery window and writes state
  atomically.
