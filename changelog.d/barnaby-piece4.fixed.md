- With MCPL, a renamed stream keeps its place. The agent gets one line on
  the channel, if it is open, naming the old and the new channel id, and the
  host gets one `channels/changed` that removes `zulip:<old name>` and adds
  `zulip:<new name>`. The watermark, the missed count, the mute, the
  allowlist entry and the reaction setting carry to the new name. A stream
  that was open at last sight and was renamed while the server was down, or
  while no host was connected, gets one push from its new channel when a host
  next connects, and then its backlog, as any stream open at last sight does.
  A rename used to
  go unnoticed: the channel went quiet, and its place and its filters
  entries were lost.
- A channel id under a stream's former name is refused by `channels/open`,
  `channels/publish` and, with MCPL, `fetch_history`, and the refusal names
  the new channel id. With MCPL, `send_message`, `listen` and `unlisten` given
  a former name refuse the call, naming the new name, and send nothing to
  Zulip. A name another stream has taken since is that stream's. It used to go to Zulip by the old name, so it failed,
  or reached a new stream that had taken the name.
- `channels/publish` and the history reads behind context, backscroll and,
  with MCPL, `fetch_history` reach a stream the server knows by its id, so
  one made during a rename reaches the stream. They used to go by its name.
- A stream whose name reads as a direct-message channel id, such as `dm:42`,
  isn't served, and the log says so. Its answers used to go to that user.
- `ZULIP_SUBSCRIBE` and `ZULIP_BACKSCROLL_CHANNELS` entries under a former
  name the server knows, from a rename it saw or from its state file, get a
  log line naming the new name, and `ZULIP_SUBSCRIBE` neither subscribes nor
  creates anything for one. It used to create an empty stream under the old
  name.
- An operator's edit of the filters file is kept when the server writes the
  file before its next poll, at a filters tool's call or at a rename. The
  server's write used to replace it with nothing said.
