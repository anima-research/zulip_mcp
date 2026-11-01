- Treat incoming stream quote replies to the connector's own messages as addressed replies (`chat:reply`, not `chat:mention`),
  using the quoted message's sender rather than the silent mention's label.
  Message handlers are awaited to preserve delivery order, with quote lookups
  limited to at most the first two distinct targets per incoming message and a
  shared three-second budget per message. A poll batch containing several quote
  replies can delay later events by up to that budget for each message.
