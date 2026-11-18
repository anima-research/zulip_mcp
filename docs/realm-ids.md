# Realm-tagged message IDs

Message IDs are realm-local in Zulip. Deployments connected to several realms can
opt in to tagged IDs to reject a message reference sent to the wrong server:

```sh
export ZULIP_ID_MODE=strict
export ZULIP_REALM_TAG=research
```

- Unset, empty, or `legacy`: existing numeric IDs, tool schemas, message-link handling,
  and handshake metadata are unchanged. `ZULIP_REALM_TAG` is unused.
- `compat`: emit `research:123`, accept matching tagged IDs and bare positive safe
  integers (including decimal strings). Bare tool inputs warn on stderr and in
  successful record results' `warnings` array, or in the error text when a call fails.
  Primitive, array and null successes are preserved unchanged; their warning is stderr-only.
- `strict`: emit tagged IDs and reject bare inputs.

Tags match `[a-z0-9][a-z0-9_-]{0,63}`. Enabled modes require an explicit,
distinct, stable `ZULIP_REALM_TAG` per realm; there is no hostname inference.
Tags are routing guards, not authentication. Invalid enabled configuration fails
before Zulip requests. Startup logs the active mode and tag once.

The contract covers message-ID tool inputs (`message_id`, `reply_to`, history
`before`/`after`), receipts, raw/history output, MCPL message delivery and history,
publish receipts, acknowledgments, and history-open cursors. Active Markdown
links use `[[msg:research:123]]`; code spans/fences stay literal. In compat/strict,
an odd-length run of backslashes before any token (numeric, tagged, or malformed)
keeps it literal; an even-length run leaves it active. Legacy retains upstream
behavior: backslashes do not escape numeric tokens. Active foreign tags and
malformed IDs are rejected before the tool's API calls or uploads. Message text, user/stream/channel
IDs, event IDs and checkpoint tokens are not rewritten. Zero/negative sentinel
values remain numeric, not usable message references. MCPL publish retains its
as-typed policy: invalid, foreign and strict bare link tokens remain literal;
tool calls reject them. Zulip API calls, routing
cursors and persisted state remain numeric. No server-state migration is needed.
For existing agents, transition through compat until bare IDs in the host
chronicle and agent context have been replaced or aged out, then switch to strict.

## Host compatibility check

In both MCP and MCPL, an enabled server's `initialize` response includes:

```json
{
  "serverInfo": {
    "name": "zulip-mcp-server",
    "version": "<package release version>",
    "_meta": {
      "zulip_mcp/messageIds": {
        "version": 1,
        "mode": "strict",
        "realmTag": "research"
      }
    }
  }
}
```

A host requiring this contract should check the **live** `serverInfo.name`, an
approved released package version, and `_meta["zulip_mcp/messageIds"]`: contract
`version === 1`, mode `strict` or `compat`, and the expected `realmTag`. Absent or
unknown metadata is unsupported; environment configuration alone is not proof.
Also check the live `add_reaction` tool schema accepts a string `message_id`, its
required emoji argument, the MCPL capability grant when applicable, and reaction
suppression policy. Revalidate after reconnection or config replacement.

No released version is claimed by this change: consumers must pin the upstream
release that first includes it once published. A source SHA copied into host
configuration is not a substitute for these runtime checks. Handshake metadata
is a server self-description, not cryptographic provenance of its executable.

Itemized host results are correlated only to IDs in the delivered batch. Compat
accepts bare decimal string references too; strict requires the matching tag.
Synthetic event IDs must be echoed exactly. Invalid/unmatched entries, non-boolean acceptance and duplicate normalized IDs are
ignored independently; diagnostics omit the returned payload, never settling an unrelated message.

All ID modes keep transport timeout seconds, HTTP status, retry advice and
write-outcome uncertainty in agent-facing errors, but omit raw transport paths
and bodies. Original transport diagnostics go to stderr. Errors marked at the native fetch boundary (not arbitrary local TypeErrors) are replaced with safe uncertainty guidance;
ordinary Zulip refusals and validation messages retain their text; upload refusals append received Retry-After advice. Completed HTTP
attachment refusals report their status and corrective checks, not an incomplete
download or a blind retry. Complete upload responses with rejection statuses
400, 401, 403, 404, 405, 413, 415, 422 or 429 count as refusals. Other upload
responses, including all 5xx/proxy failures, incomplete bodies and malformed
successful responses, retain duplicate-upload uncertainty; a complete body alone
does not prove that nothing was stored. Attachment URLs use the origin of an HTTP(S) realm URL, including
`/api` or other configured path suffixes; only the initial URL is checked for same-origin
`/user_uploads/` authorization. Redirect destinations follow native fetch policy
and are not revalidated. Credential-bearing URLs and encoded path separators are
rejected before fetching. Failed unread
checks report unknown state instead of claiming that no messages are unread.

When a long channel ID forces the bounded catch-up fallback, the compact notice
names the omitted count and first/last message IDs. Its `fetch_around` pointer
recovers the first message and context, including a pageable `channelId` for DMs; `fetch_history`
can then page through the omitted range. Acceptance acknowledges the omissions,
records progress through the scan, including unselected messages and earlier
holds within that scan; the durable watermark advances only as far as remaining
holds permit. Ordinary catch-up starts after the saved watermark, but a separate bounded list
also persists up to 50 held IDs per channel and reads those passed by the
watermark by ID on the next start, offering selected rows once and withdrawing
unselected closed-stream ambient rows (see the README recovery limits).
An earlier hold outside the scan can pin the watermark: acceptance of a later range is not independently persisted, and that
range can be offered again, including after restart. Explicit history reads remain available. Recovery covers fetched rows only,
not an unfetched interval or overflow beyond the existing held-ID cap.
Unread aggregates are null when any channel check failed; summary counts are
labelled “At least” in that case.

Legacy compatibility corrections: catch-up pointers now JSON-escape channel
arguments in every mode so quotes and backslashes remain usable. Ordinary
acknowledgement refusals retain their original reason without a context prefix. The compact-notice recovery error fix also makes
`fetch_around` return DM channel fields in every mode, rather than an unusable null.
No successful stream read or message-ID representation changes in legacy mode.
No earlier live-contract run is offered as evidence for this revision; its code,
server and runtime versions were not recorded with the cited result.

Pending catch-up offers do not exclude overlapping history scans: this can repeat
already offered rows, but must not suppress an unoffered addressed tail. Catch-up
answers from an older connection generation cannot clear or recreate current
holds. Accepted answers still drain on shutdown before a replacement begins.
