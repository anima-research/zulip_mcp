# Realm-tagged message IDs

Message IDs are realm-local in Zulip. Deployments connected to several realms can
opt in to tagged IDs to reject a message reference sent to the wrong server:

```sh
export ZULIP_ID_MODE=strict
export ZULIP_REALM_TAG=research
```

- Unset, empty, or `legacy`: existing numeric IDs, tool schemas, message-link handling,
  and handshake identity are unchanged. `ZULIP_REALM_TAG` is unused.
- `compat`: emit `research:123`, accept matching tagged IDs and bare positive safe
  integers (including decimal strings). Bare tool inputs warn on stderr and in
  successful results' `warnings` array, or in the error text when a call fails.
- `strict`: emit tagged IDs and reject bare inputs.

Tags match `[a-z0-9][a-z0-9_-]{0,63}`. If omitted or empty in an enabled mode, the tag is
inferred from the first hostname label. **Set an explicit, distinct tag per realm**:
that inference is not globally unique. Tags are routing guards, not authentication.
Invalid enabled configuration fails before Zulip requests.

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
cursors and persisted state remain numeric. No state migration is needed.

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
Synthetic event IDs must be echoed exactly. Invalid/unmatched entries are logged
and ignored independently, never settling an unrelated message.

Enabled modes keep transport timeout seconds, HTTP status, retry advice and
write-outcome uncertainty in agent-facing errors, but omit raw transport paths
and bodies. Original transport diagnostics go to stderr. Native fetch TypeError,
AbortError and TimeoutError messages are replaced with safe uncertainty guidance;
ordinary Zulip refusals and validation messages remain unchanged. Failed unread
checks report unknown state instead of claiming that no messages are unread.
