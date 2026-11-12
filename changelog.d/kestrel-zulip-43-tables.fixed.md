- A table written directly under a line of text shows as a table in Zulip.
  `send_message`, `send_dm`, a plain answer and `edit_message` put one blank
  line above its header row, which Zulip's parser needs there. Nothing else
  of a message changes, and a table directly under a list item or a quoted
  line is left as written. The added line counts toward the message length,
  so a send at exactly the length goes out as two messages, and such an edit
  is refused.
