- **Callers sending stream messages:** standalone `send_message` now requires a
  non-empty trimmed topic for every absent spelling (omitted, null, empty, or
  whitespace-only). This refuses general-chat sends accepted by Zulip 10+;
  reply without an override to retain an existing unnamed topic instead.
  Send topics, including reply overrides, are trimmed; override types are
  checked before uploads.
- **Callers paging history from the start:** `fetch_history` now treats `after: 0`
  as absent, selecting the newest page by default instead of the oldest.
  Use `after: 1` to read from the oldest. `before: 0` is also absent.
- **Callers resetting DM filters:** send `filters_update` with `setDmUsers: []`
  alone to make DMs UNRESTRICTED. Combining that reset with non-empty
  `addStreams` or `removeStreams` is now refused without writing; use
  `setDmUsers: null` for stream-only updates.
