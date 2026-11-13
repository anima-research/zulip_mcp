- `listen` and `unlisten` no longer say that the bot gets no events from a
  stream it isn't subscribed to. The server's event queue registers with
  `all_public_streams`, so without a subscription a public stream still sends
  its new messages, with mentions of the bot flagged, and the edits of and
  reactions to the messages that mention the bot. A private stream sends at
  most the messages that mention the bot.
