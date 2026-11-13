- In a message's text, `[[msg:ID]]` becomes a link to the stream message with
  that id, in the Markdown link form Zulip's web client writes, with a `|`
  written as `&#124;`. It holds for `send_message`, `send_dm`, a plain answer
  and `edit_message`, and `send_message`'s description says so. A token inside
  a code or math fence, inside inline code or in a spoiler fence's title line
  is sent as written, and a text with no token is sent as it was before. A
  token is replaced in an indented code block, in inline math, inside another
  link, and inside inline code that crosses a list line in a spoiler fence.
  A tool call refuses, before anything is uploaded or sent, a token for a
  message the bot can't read, for a direct message, or for a message in a
  private stream when the text goes anywhere but that stream, and a token
  whose read fails. A plain answer posts such a token as written, and after a
  read that Zulip doesn't answer, it posts the rest of its tokens as written,
  unread. A text with tokens reads each message it names from Zulip at the
  call. It also reads that message's stream, unless the destination names that
  stream by its id. The link counts toward the message length.
