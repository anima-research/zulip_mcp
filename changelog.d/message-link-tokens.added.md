- `send_message`, `send_dm`, `edit_message` and MCPL publishes expand exact
  `[[msg:ID]]` tokens atomically, before uploads or writes. By default only
  same-conversation references, or non-invite-only-stream references into a
  stream, are allowed. Tools offer explicit `allow_private_references: true`;
  results return `links: [{token, url}]`. Escape with `\[[msg:ID]]` to send
  a literal token (one backslash removed). At most 20 unescaped occurrences
  are allowed, with one message read per distinct ID including `reply_to`.
  Links use Zulip-escaped labelled Markdown only in unambiguous messages;
  messages with backticks, fences, indentation or horizontal rules use bare
  Copy-link URLs throughout, with modern
  `channel`/`dm` operators. Sends never split expanded links; edits fail before
  PATCH if content exceeds the configured message limit in Unicode code points,
  even without tokens. Realm limits come from event registration, with an env
  override. Bare numbers and pre-existing links are unchanged.
  DM recipient arrays preserve atomic full-name queries, including commas.
