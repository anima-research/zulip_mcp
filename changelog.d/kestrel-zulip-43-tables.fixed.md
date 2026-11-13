- A table written directly under a line of text shows as a table in Zulip.
  `send_message`, `send_dm`, a plain answer and `edit_message` put one blank
  line above its header row wherever Zulip then shows more tables. A table
  directly under a list item or a quoted line is left as written, and so is
  one above a rule with another table below it and no blank line between
  them, which the blank line would merge into one. Nothing else of a message
  changes. The added line counts toward the message length, so a send at
  exactly the length goes out as two messages, and such an edit is refused
  with the length as it would be posted.
