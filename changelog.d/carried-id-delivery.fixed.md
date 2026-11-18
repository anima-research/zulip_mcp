- Save completed restart by-ID reads and their failure counters before retrying
  catch-up history; catch-up failures no longer discard or repeat that pass.
- Keep open-channel saved IDs until an actual host request, not just batch
  enqueue. Closing a channel or revoking incoming capabilities before flush
  leaves unoffered IDs saved. Policy withdrawals recheck current policy.
- Persist refused/failed holds even when a watermark already exists. Saved-list
  overflow cannot resurrect a dropped current-run ID or silence a new lower-ID
  drop warning; in-memory watermark holds remain unchanged.
- Preserve refusal codes after successful uploads when Zulip omits its message.
  Authorization retry advice requires access repair, upload uncertainty names
  the original operation, and failed move read-backs name fetch_around rather
  than suggesting another move. Polling authorization failures request host
  operator repair.
