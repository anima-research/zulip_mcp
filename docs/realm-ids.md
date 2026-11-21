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
keeps it literal; an even-length run leaves it active. In legacy, backslashes
do not escape numeric tokens. Active foreign tags and
malformed IDs are rejected before the tool's API calls or uploads. Message text, user/stream/channel
IDs, event IDs and checkpoint tokens are not rewritten. Zero/negative sentinel
values remain numeric, not usable message references. MCPL publish retains its
as-typed policy: invalid, foreign and strict bare link tokens remain literal;
tool calls reject them. In compat, a bare link token in a publish is treated
like a tagged token and warns once on stderr. Zulip API calls, routing
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

A source SHA copied into host configuration is not a substitute for these
runtime checks. Handshake metadata is a server self-description, not
cryptographic provenance of its executable.

Itemized host results are correlated only to IDs in the delivered batch. Compat
accepts bare decimal string references too; strict requires the matching tag.
Synthetic event IDs must be echoed exactly. Invalid or unmatched entries,
non-boolean acceptance and duplicate normalized IDs are ignored independently;
diagnostics omit the returned payload and never settle an unrelated message.
