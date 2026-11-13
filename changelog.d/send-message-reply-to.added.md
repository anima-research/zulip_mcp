- `send_message` accepts `reply_to` to derive a stream/topic or complete DM
  conversation from a message ID. With `reply_to`, omit `to`; supplying it,
  a `topic` for a direct message, a conflicting type, or an unreadable ID is
  refused before uploading or sending. `type` is optional in the schema and inferred for replies;
  calls without `reply_to` still require both `type` and `to` at runtime. Null
  optional send arguments are treated as absent.
  Reply IDs also accept integer-valued strings; explicit `channel`/`direct`
  types normalize to `stream`/`private` before comparison.
