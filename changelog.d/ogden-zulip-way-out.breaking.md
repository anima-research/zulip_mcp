- **Operators:** a numeric variable whose value isn't only decimal digits,
  with white space around them allowed, is now ignored at start with a line
  saying so, and its default is used. Write the number alone, such as `250`
  for `250ms`. Such a value used to read as the number it began with, with no
  line, so `ZULIP_MAX_MESSAGE_LENGTH=1e4` read as 1. This covers
  `ZULIP_MAX_MESSAGE_LENGTH`, `ZULIP_BACKSCROLL_DEFAULT`, `ZULIP_CATCHUP_LIMIT`,
  `ZULIP_MISSED_BLOCK_MAX_CHARS`, `MCPL_BATCH_WINDOW_MS` and
  `MCPL_CONTEXT_HISTORY_SIZE`.
