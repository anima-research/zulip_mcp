- When Zulip refuses the `ZULIP_PASSWORD` exchange, or answers it with no API
  key, the start logs Zulip's words and says that the server has no API key.
  The start goes on, as before. It used to log nothing, and each later call
  that needs one was refused with no line that said why.
- At start, a refused read of the bot's profile or of the realm's settings is
  logged with Zulip's words. So is a profile answer with no user id. The
  profile's line says that the self-filter is off for this run, so the bot's
  own messages come back to the agent. That line now gives a failed read's
  error message in place of its stack. A refused read used to log nothing.
- A `ZULIP_SUBSCRIBE` that Zulip refuses logs the failure line with Zulip's
  words. It used to log the success line with empty lists.
- An event poll that Zulip refuses for another reason than an expired queue
  logs Zulip's message and code, and the `degraded` marker's reason names
  them. It used to say "returned no events array". The backoff is as before.
- A `ZULIP_BACKSCROLL_CHANNELS` limit counts only where it is digits alone,
  with space around them allowed. Any other entry, such as `general:1e3`, is
  skipped with the line that names it. Such an entry used to give the stream a
  limit of its leading digits, here 1.
