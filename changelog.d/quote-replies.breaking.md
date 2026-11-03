- **Gate policies:** match DM/private tags to keep receiving closed-DM catch-up
  under mention-only policies. Closed-DM catch-up now carries `chat:dm` and
  `chat:private`, rather than a synthesized `chat:mention`; unmentioned DMs
  have `mentioned: false` and `origin.isMention: false`.
