- Persist a bounded held-ID list beside the delivery watermark, up to 50 IDs
  per channel across runs. Refused/failed holds are saved immediately (also
  while a retry is pending); queued work is saved at an orderly stop. Bulk
  failure/refusal batches write state once after updating their holds.
- On restart, listed IDs still above the watermark follow ordinary catch-up.
  IDs it has passed are read individually before catch-up, independently of
  its limit. Eligible rows are offered once: a closed-channel `<missed>` block
  of listed rows, oldest first, or messages on an open channel. Unselected
  closed-stream ambient rows are withdrawn. These offers do not move the
  watermark and are removed from the list at the actual host-request boundary,
  regardless of the answer; open-channel batches do not retry them after an
  uncertain send. Channel close/capability withdrawal before flush preserves
  unoffered IDs. Policy-filtered empty reads are not evidence of deletion.
- Completed by-ID results and per-ID failure counts are saved before retryable
  catch-up. Failed reads stop that start's pass; three failed starts drop that
  ID. Overflow drops the oldest saved IDs without resurrecting current-run
  evictions. Drops log their IDs/ranges and reasons. The watermark retains its
  existing settlement rules, and never-held rows are not covered by the list.
