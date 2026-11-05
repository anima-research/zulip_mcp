- `send_message` and `edit_message` resolve exact `[[msg:ID]]` tokens anywhere,
  including code, fences, quotes and tables, to native Zulip message-link URLs
  for streams/topics and DMs. Escape with `\[[msg:ID]]` to send the literal
  token without lookup (the backslash is removed). All referenced messages
  must be readable by the bot or the call fails before uploads, sends or edits.
  Up to 20 unescaped token occurrences are allowed; duplicate IDs are fetched
  once per call. Bare numbers and other text are unchanged, and existing links
  in replacement edit content stay as supplied.
