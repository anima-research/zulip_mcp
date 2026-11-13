- Where the start's read of the bot's own user id fails, each later request to
  Zulip reads it first until Zulip gives it, but for the server's setup
  requests and a file's upload or download. A request whose read fails too
  fails unsent. After three of the event loop's polls fail that way, the agent
  gets its `degraded` notice with the read's reason, and `recovered` once a
  poll gets through. The server used to go on without the id until it stopped,
  so the agent got the bot's own messages back, and a direct message's channel
  had the bot among its counterparts. A refusal with a status from 400 to 499
  other than 429, as Zulip gives an incoming-webhook bot, isn't such a
  failure: the server goes on without the id, as before, and doesn't read it
  again.
