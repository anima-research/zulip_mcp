- `send_message` accepts `reply_to` to derive a stream/topic or complete DM
  conversation from a message ID. With `reply_to`, omit `to` and `topic`;
  supplying either, a conflicting type, or an unreadable ID is refused before
  uploading or sending. The tool schema requires `type`; direct runtime callers
  may omit it with `reply_to` to infer the type. Calls without `reply_to` require
  both `type` and `to`.
