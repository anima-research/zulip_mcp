- User-directory lookups now surface Zulip API errors instead of caching an
  empty directory, so `send_dm` by name or email reports the real failure
  (including rate limits) and retries failed lookups.
