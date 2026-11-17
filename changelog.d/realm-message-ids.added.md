- Opt-in realm-tagged message IDs via `ZULIP_ID_MODE=compat|strict` and `ZULIP_REALM_TAG`, including reaction tools and MCP/MCPL boundaries. Enabled servers advertise contract version, mode and realm tag in initialize serverInfo metadata; unset behavior is unchanged.
- Enabled-mode transport errors retain safe timeout, HTTP status and retry facts, log originals, and also cover native fetch failures. Listen/unlisten failures set the tool error flag; unread checks distinguish unknown from empty. Oversized catch-up identifiers fall back to a bounded message recovery pointer. Legacy output is unchanged.

Enabled modes validate message-link tokens before sends and edits; strict mode rejects bare tokens such as `[[msg:123]]` (use a realm-tagged ID).
