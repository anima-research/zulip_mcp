- **Gate policies:** match DM/private tags to keep receiving closed-DM catch-up
  under mention-only policies. Closed-DM catch-up now carries `chat:dm` and
  `chat:private`, rather than a synthesized `chat:mention`; unmentioned DMs
  have `mentioned: false` and `origin.isMention: false`.
- Catch-up may emit multiple `<missed>` blocks per channel when deferred
  quote verification splits an accepted prefix from the retry block.
- Addressed-only catch-up blocks use `reason="addressed"` when their count
  includes quote replies; mention-only counts retain `reason="mention"`.
- Message events are handled in order on each channel; a quote lookup
  delays the later events of its own channel by up to three seconds per
  message.
