- Where the start's read of the bot's own user id fails, a later request reads
  it first, and fails unsent if that read fails too. The server used to go on
  without the id until it stopped, so the agent got the bot's own messages
  back, and a direct message's channel had the bot among its counterparts. A
  refusal with a status from 400 to 499 other than 429, as Zulip gives an
  incoming-webhook bot, isn't such a failure: the server goes on without the
  id, as before, and doesn't read it again.
