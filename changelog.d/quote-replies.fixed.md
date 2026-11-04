- Preserve unaccepted live reply verdicts through fail-closed history reads,
  reconnect sweeps and deferred verification. Accepted catch-up progress
  survives shutdown and reset without re-delivering already accepted blocks.
- Recompute reply addressing on content edits; moves and deletions retain the
  last confident verdict. Open-channel change lines include address flags.
- Bound quote lookups across live delivery and history, retry transient failures,
  and cache missing or inaccessible targets independently of error language.
- Heal refused closed-channel deliveries once per attempt, including failed or
  empty history reads, while retaining eligibility after local policy aborts.
- Keep pre-live drains disjoint across reconnects.
