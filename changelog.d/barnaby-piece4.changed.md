- An archived stream, or one the bot lost access to, is removed at the host
  with one `channels/changed`, and the agent gets one line on the channel if
  it is open. Its channel used to stay registered and go quiet.
- The delivery state file also holds `streams` and `formerNames`, the
  server's copy of the stream names. Its other keys keep their form, so an
  earlier release reads the file as before.
- A run without MCPL writes no delivery state file. It used to write one when
  it stopped.
- `ZULIP_SUBSCRIBE` subscribes once a client has connected and the stream
  list has been read: with MCPL at the event queue's registration, and
  without MCPL at one read of the list. It used to subscribe as the process
  started.
