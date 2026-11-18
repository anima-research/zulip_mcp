- In every ID mode, listen/unlisten validation, Zulip refusals and transport failures now set the tool error flag instead of returning successful error objects.
- Unread totals are null if any channel check fails; the unread summary reports unknown state and labels partial counts “At least”, rather than claiming no unread messages. Per-channel resource errors use the same safe diagnostics.
- Attachment downloads distinguish invalid input paths from invalid configured realm origins (including queries, fragments, credentials and unsupported paths; /api suffixes remain supported), before any fetch.
- Transport diagnostics are sanitized on every agent-facing path in every ID mode, preserving HTTP status, timeout duration, retry facts and write uncertainty. Read failures recommend retrying the read; upload failures explain that no path was confirmed and require an operator check before retrying. Response-body failures retain known HTTP status, and self-ID gates retain transport facts and refusal status in every mode.
- Request/event and native transport boundaries retain original errors and stacks on stderr in every ID mode, including best-effort message-link lookup failures. This is not a promise for every internal fallback or state-file catch. Timeout duration metadata is non-enumerable and absent when unknown, avoiding extra error-inspection fields.
- Move read-back failures retain successful move outcomes with operation/ID context and safe transport facts in every mode.
- Server version comes from package.json in every mode.
- Oversized catch-up headers use a bounded notice naming the omitted count/range and a fetch_around recovery anchor with fetch_history paging guidance; DM anchors now return a usable channelId in every mode. Acceptance acknowledges the scan like ordinary elision, with no-repeat across restart only at or below the saved watermark; an older hold can pin it, and accepted later ranges are not separately persisted and may recur. Both ordinary and compact catch-up scans reserve their entire pending interval (including existing ambient holds); failed or rejected delivery still holds the range back. Recovery is limited to fetched rows and existing caps.
- Invalid acknowledgement IDs are described safely even when their JSON value cannot be string-converted.
- Attachment URL authorization is guarded after resolving against the realm: protocol-relative URLs, backslashes and percent-encoded foreign hosts cannot bypass the initial same-origin check in any ID mode. Redirect destinations retain native fetch behavior and are not checked again; credential-bearing URLs and encoded separators are rejected. Attachment body-read failures and stalls preserve an already-received HTTP status (including 206).

  Main-suite legacy contract audit (main `1d1ce5d5559b20ddaad6098a204e8bd94a8bafcd`, 856 tests; the 16 differences identified at r12):
  - 103, `renderMissedBlock` hard-cap hint: reverted the unrelated legacy wording change; ordinary legacy truncation still says `fetch_around(7)`. Tagged hints still name the tagged argument. This is not an R1-2 bug fix.
  - 181, message-link read failure: R1-2 safe transport text replaces raw request details; “Nothing was sent” remains.
  - 197, multipart send timeout: R1-2 sanitized text keeps the posted-part receipt and write uncertainty.
  - 198, multipart send error identity: R1-2 returns a safe copy, preserving error kind/metadata instead of mutating/reusing the raw error.
  - 200, publish error identity: the same R1-2 safe-copy rule; posted-part receipt and error kind remain.
  - 668, refused listen: R1-2 throws instead of returning a successful `{error}` object; no registration occurs.
  - 733, move read-back: R1-2 adds operation/ID context and safe facts while retaining `moved:true`, the requested move and reply guidance.
  - 737, move stream lookup failure: R1-2 sanitizes transport text with operation/ID context; no move or misleading list-streams hint.
  - 792, bounded request timeout: R1-2 safe text keeps the known duration and retry guidance; connection closure remains.
  - 808, attachment idle timeout: R1-2 safe download diagnostic replaces the filename/raw text and keeps the duration and retry guidance; progressing downloads still finish.
  - 809, default timeout: R1-2 reports the same 15-second bound as a safe fact instead of raw request text.
  - 822, listen/unlisten refusals: R1-2 rejects the tool call instead of returning refusal data as success.
  - 832, self-ID tool gate: R1-2 removes raw request/refusal text, keeps status and the unsent guarantee.
  - 838, self-ID degraded notice: R1-2 uses the same safe self-ID reason/status; degradation/recovery semantics remain.
  - 840, HTML/proxy poll failure: R1-2 safe `not-json` diagnostic replaces raw non-JSON text, preserving HTTP 502 and backoff.
  - 842, poll timeout: R1-2 safe timeout diagnostic replaces raw “no answer” text; backoff and degraded accounting remain.

- Completed HTTP attachment refusals retain status and suggest corrective checks rather than claiming an incomplete transport failure or recommending an identical retry. Inline body-read errors retain status too.
- Transport and self-ID replacement errors retain their original exceptions as operator-only causes. Safe boundary copies remove raw causes and stack text; contextual callers avoid duplicate transport logs. Ordinary local boundary exceptions are logged too.
- Completed HTTP upload refusals, including 5xx JSON and non-JSON bodies, report refusal/status and Retry-After rather than incomplete-transport uncertainty. Incomplete bodies and malformed successful responses retain status and duplicate-upload uncertainty; ordinary 4xx Zulip JSON refusals remain unchanged.
- Non-JSON and body-read transport failures retain Retry-After headers. Event-poll degraded notices explain server retries, not an agent read command.
- Compat warning decoration leaves non-record results intact; the warning remains on stderr. Enabled host-result mapping requires boolean acceptance, drops duplicate normalized IDs, and never logs raw malformed payloads.
- Legacy audit corrections: catch-up pointers retain the original unescaped channel bytes; ordinary acknowledgement refusals retain their unprefixed reason. DM fetch_around channel fields change only as the documented compact-notice recovery error fix. The added message-ID startup diagnostic is emitted only in compat/strict modes; default and explicit legacy startup output match main.

- Batch upload failures report paths confirmed earlier in the batch and warn that retrying the original send reuploads them. Enabled host-result diagnostics distinguish invalid/unmatched IDs, non-boolean acceptance and duplicate normalized IDs without dumping payloads.
