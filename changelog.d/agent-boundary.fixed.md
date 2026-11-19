- Tool errors: `listen`/`unlisten` validation, explicit Zulip refusals and
  transport failures set the tool error flag instead of returning a
  successful result that holds an error. An explicit Zulip refusal uses
  Zulip's message if present, otherwise a generic error, not the refusal
  code. A `listen` whose streams are all unauthorized still succeeds and
  lists them as unauthorized.
- Unread totals are null when any channel check fails, and the unread
  summary labels partial counts “At least” instead of reporting no unread
  messages.
- Agent-facing transport errors keep the HTTP status, timeout, Retry-After
  and whether a write may have been applied, but no longer show raw request
  paths, response bodies, causes or stacks; the original error goes to stderr
  once. Reads that fail are offered for retry, except that HTTP 401/403 ask
  for an access repair first. A move whose read-back fails still reports the
  move and points to `fetch_around` rather than another move.
- Uploads: only complete responses with status 400, 401, 403, 404, 405, 413,
  415, 422 or 429 count as refusals. Any other failure, 5xx and proxy errors
  and malformed successes included, is reported as possibly stored, so a
  retry may duplicate the file. HTTP 413 refusals reported as transport errors
  suggest reducing or omitting the attachment; a complete Zulip JSON refusal
  does not add this advice. A failed send or publish lists the attachment
  paths already uploaded and warns that retrying it uploads them again.
- Attachments: before fetching, `fetch_attachment` and inline attachments
  require the resolved URL to match the configured realm's origin and be
  under `/user_uploads/`. The resolved URL cannot contain credentials or
  encoded path separators. Redirects follow native fetch behavior and are
  not checked again. An inline failure marker gives the path and one remedy,
  plus the HTTP status when available. For 401/403 it asks the host operator
  to fix authorization because retrying alone will not help; for 404 it
  asks to check the path, and for other HTTP statuses it suggests retrying
  with `fetch_attachment`. Invalid input paths and an invalid configured
  realm are reported separately.
- Catch-up: when even a `<missed>` block's header cannot fit its size cap,
  a notice names the omitted count and ID range with a `fetch_around` anchor,
  and `fetch_around` now returns a usable `channelId` for DMs. Channel
  arguments in catch-up pointers are JSON-escaped, so quotes and backslashes
  in stream names work. Catch-up rechecks mute and allowlist rules after
  history returns, and overlapping catch-up scans may repeat a row rather
  than drop one.
- Event polling: refusals keep their HTTP status and Retry-After, and the
  degraded notice says polling retries automatically and when authorization
  needs the host operator.
- `channels/incoming` results must give a boolean acceptance and name each
  offered message once; other entries are ignored without logging the
  payload, and the stderr line says whether delivery is unconfirmed or the
  event cannot be replayed from history.
- Invalid acknowledgement IDs are described safely, and the reported server
  version comes from package.json.
