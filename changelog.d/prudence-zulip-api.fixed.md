- A Zulip call that gets no answer ends with an error after 15 s, or after the
  caller's own shorter budget. It used to wait without end, until the server
  restarted. The error says whether the request may have been applied. File
  uploads and `fetch_attachment`'s download have no bound yet.
- An event long poll that gets no answer for 90 s fails. The loop asks again
  with its backoff and counts it toward its `degraded` marker, as for other
  failures. It used to wait without end.
- `edit_message` sends its text in the request body, so a long edit is applied.
  Removing a reaction and unsubscribing send their parameters in the body too.
- `edit_message` refuses an edit above the message length before Zulip is
  asked, with both lengths in the error. The length is `ZULIP_MAX_MESSAGE_LENGTH`
  (10000 by default), counted in code points. Zulip itself would cut such an
  edit to its limit without a word. It also refuses an edit with no `content`,
  which used to send the text "undefined" as the content, and one whose
  `message_id` isn't a positive integer (#36).
- Where Zulip gives no JSON answer, the error says what happened: no answer
  within the bound, a failed connection, or an answer that isn't JSON, with its
  HTTP status. It says whether the request may have been applied. It used to
  show the HTTP library's own text. A JSON refusal with a 5xx status on any
  request other than a GET gains the same sentence.
- `get_channel_history`, the per-channel errors of `start_monitoring` and
  `get_unread_messages`, and a channel's resource text show
  `Zulip refused <what>: <Zulip's message>` where Zulip refuses. They used to
  show "Cannot read properties of undefined".
- A zuliprc file with no `[api]` section fails at start with a message that says
  so. A zuliprc file without `email` or `key` takes them from `ZULIP_EMAIL` and
  `ZULIP_API_KEY` for its API calls too. Those calls used to send "undefined"
  in place of the missing value.
