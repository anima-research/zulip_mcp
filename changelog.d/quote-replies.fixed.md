- Preserve unaccepted live reply verdicts through reconnect sweeps and deferred
  verification. Inconclusive history reads and content edits retain a cached
  verified reply only for unchanged quote targets on eligible messages.
- Recompute reply addressing on content edits; moves and deletions retain the
  current cached verdict. Open-channel change lines include address flags.
- Bound quote lookups across live delivery and history, retry transient failures,
  and cache missing or inaccessible targets independently of error language.
- Heal refused closed-channel deliveries once per attempt, including failed or
  empty history reads, while retaining eligibility after local policy aborts.
