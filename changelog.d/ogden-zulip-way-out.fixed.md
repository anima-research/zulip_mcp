- `send_message` and `send_dm` split a long message at `ZULIP_MAX_MESSAGE_LENGTH`,
  as a plain answer already did. They split at 10,000 characters whatever it
  said, so where the server's limit was below 10,000, Zulip cut a longer
  message off.
- A rollback deletes the parts of a plain answer that went out before Zulip
  refused it, or stopped answering, partway, as it already did for the tools'
  sends.
- A `ZULIP_MAX_MESSAGE_LENGTH` of 0 is ignored at start, with a line saying so,
  as a negative value already was. It made a plain answer split forever.
