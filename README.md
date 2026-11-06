# Zulip MCP Server

A Zulip server for AI agents, speaking plain **MCP** (Model Context Protocol) to
any client and **MCPL** (MCP Live) to hosts that support it — live delivery of
stream messages and DMs, host-managed channels, catch-up after downtime, a
hot-reloadable filters plane, and a stateful tool surface for reading and
writing Zulip.

Built on [`@animalabs/mcpl-core`](https://github.com/anima-research/mcpl-core-ts),
the same substrate as [discord-mcpl](https://github.com/anima-research/discord-mcpl)
and [slack-mcpl](https://github.com/anima-research/slack-mcpl). Zulip only — the
Discord and Slack adapters that once lived here moved to those servers.

## What you get

**Plain MCP (Claude Code, Cursor, any MCP client)**

- 29 tools: stream/topic history with natural dates or id cursors,
  `fetch_around`, sending to streams and DMs, editing, deleting, reactions,
  user lookup, attachments, and a persistent read/unread monitor.
- Resources: `zulip://unread/summary`, `zulip://monitoring/status`,
  `zulip://channel/{stream}/unread`.

**MCPL hosts (connectome-host and friends)**

- Every stream and DM conversation the bot can see is a channel the host can
  open and close. Opening a channel subscribes the bot to the stream first
  (Zulip only delivers events to subscribers) — a stream the bot cannot
  subscribe to fails the open rather than opening onto silence — and can
  return backscroll atomically.
- Delivery model: messages on **open** channels arrive as `channels/incoming`;
  **mentions, quote replies and DMs on closed channels** arrive as
  `push/event` so they always reach the agent; ambient traffic on closed channels is dropped and counted
  (`channel_missed`). Message events are handled in order, and a quote lookup
  delays later events in the same batch by up to three seconds per message.
  Stopping delivery mid-batch drops the rest of that batch, and the next sweep
  recovers what it can.

  A stream quote reply counts as addressed when a message link ending in
  `/near/N` immediately heads a `quote` fence and N resolves to a message
  the bot authored. It carries `metadata.replyToSelf` on channel messages,
  `origin.isReplyToBot` on pushes, and `chat:reply`, not `chat:mention`.
  Push origins and channel-message metadata expose umbrella `isMention`
  (explicit mention or reply to the bot), plus `isExplicitMention` and `isReplyToBot` for precise gate
  matching. `metadata.mentioned` remains Zulip's verdict, and the line has no
  `(mention)` label unless Zulip reports one. When Zulip already reports an
  explicit mention, quote classification is skipped and `isReplyToBot` is
  false even if the message also quotes the bot. Silent mentions and links alone
  do not count. The same rule applies to live delivery, content edits, and
  channel history used by backscroll, refused-push healing, reconnect sweeps
  and queue-gap recovery. Removing the quote removes reply addressing;
  moves and deletions retain the current cached verdict. Inconclusive history
  reads and content edits retain a cached verified reply only when the quote
  targets are unchanged and the message remains eligible (not an explicit
  mention, DM, self-authored or bot-authored message). Only the first two
  distinct quote targets are checked; page/cache authors are preferred,
  otherwise live messages/edits share a three-second wait budget each and
  history reads share eight target GETs and a three-second wait budget per
  page. Closed-channel catch-up additionally shares at most six seconds of
  quote-lookup waiting across all pages and both walks. The initial walk
  waits at most three seconds; the retry walk uses whatever remains of the
  shared six-second allowance. Only time awaiting quote GETs spends it, not
  history HTTP or host acceptance. Go-live can wait about six seconds per
  channel with uncached quote targets, in addition to those other waits.
  Other channels have separate call allowances.
  Cache/page matches remain usable after exhaustion. All uncached quote-target GETs, live and
  history, also share a process-wide token bucket: a burst of 30 and refill
  of 30 per minute (one token every two seconds). A page, channel, reconnect
  or additional adapter cannot reset it. Exhaustion reports budget-skipped
  rows without waiting for a token or negative-caching the target; ordinary
  sends, reactions and edit-placement lookups do not spend these tokens.
  This limits quote traffic only, not all Zulip API requests. The sweep can
  exhaust the bucket shared with live delivery: live quotes to uncached
  targets then degrade to ambient, with no immediate retry (a later sweep
  may revisit them). Cached targets and explicit mentions remain usable.
  A budget-starved quote row stops the
  initial catch-up block short of that row; the single retry stays within
  the original catch-up ceiling. Rows still unverified on retry without a
  retained verified verdict fail closed, allowing the backlog to proceed
  without another gap or reconnect. An unknown bot identity fails closed;
  unreadable targets fail closed except for the unchanged-target cached
  verdict described above. An unaccepted live-verified
  reply keeps its verdict through sweep deduplication and retries even when
  history cannot verify it again. Only API errors with
  `result: "error"` and `code: "BAD_REQUEST"` (missing or inaccessible targets)
  are cached for three minutes. On pre-5.0 servers without a code, the legacy
  `msg: "Invalid message(s)"` is used instead. Other API errors, missing message objects, mismatched message ids,
  absent/invalid numeric sender ids, non-JSON responses, transport errors and
  timeouts are uncached and eligible for the retry pass. This is not full
  response-schema validation.
  The wait budget does not cancel the GET or bound history-page HTTP latency.
  Locale/forward-heading limits and the bot-author exclusion are detailed
  below.

- Catch-up: a persisted per-channel watermark advances only when the host
  has accepted a message (itemized `channels/incoming` results, acknowledged
  `push/event`). During the pre-live sweep, scan advancement is staged while
  buffered live replies are reconciled; it is committed at go-live, graceful
  shutdown, or before a new connection resets the staged state, preserving
  accepted catch-up progress while refused replies retain their holds and
  pending replies without a delivery hold cap the committed scan below their
  ids; held replies preserve the scanned target while flooring the watermark. It
  never advances past a message the host was offered but did not
  accept — a refusal, a batch lost to a transport failure — which is
  replayed from history once the host answers again. On the next connection
  one or more `<missed>` blocks per channel deliver what arrived meanwhile (full
  backscroll for channels the host had open, addressed message ± 7 for the
  rest). Deferred quote verification can split a channel into an accepted
  prefix block and a retry block; consumers must not assume one block per
  channel. Closed-DM catch-up uses `chat:dm` and `chat:private`, not a
  synthesized `chat:mention`; unmentioned DMs have `metadata.mentioned: false`
  and `origin.isMention: false`. Policies that previously matched DM catch-up
  via mention tags or flags should match the DM/private tags instead.
  Addressed-only blocks use `reason="mention"` when every counted
  addressed message is a mention, otherwise `reason="addressed"`; full
  backscroll uses `reason="backscroll"`. Blocks are paged up to `ZULIP_CATCHUP_LIMIT` and hard-capped in size. A Zulip
  event-queue expiry is healed the same way — open channels replayed, closed
  channels' addressed messages pushed — not merely reported. Live events
  that arrive before the sweep has run are held and released in order after it, so a
  live delivery can never jump the watermark over the offline gap.
- RFC-001 tags on every message (`chat:mention`, `chat:reply`, `chat:dm`,
  `chat:ambient`, `chat:from-bot`, `chat:has-image`, `chat:reaction`, …) for the host's wake
  policy. Images are inlined on live delivery, downsampled to model-max.
- Reactions, opt-in per channel, carry only reaction tags so a tag-keyed
  wake policy ignores them; operator-owned suppression of reaction markers
  plus the host-injected baseline; rollback checkpoints minted by every
  messaging tool; acknowledge by reaction; typing indicators routed to the
  active topic.
- Edits, topic moves and deletions of messages the agent has seen arrive as
  `[edited]` / `[moved]` / `[deleted]` lines carrying the message's own id
  (`chat:edited` / `chat:deleted`, plus `chat:mention` / `chat:reply` when
  the edited message mentions / quote-replies to the bot); history marks
  edited messages `(edited)`.

## Installation

```bash
npm install
npm run build
```

Requires Node 20+. `npm test` runs the suite (`node --test`, no network).

## Configuration

Credentials, via environment or a zuliprc file:

```bash
export ZULIP_REALM=https://your-org.zulipchat.com
export ZULIP_EMAIL=your-bot@your-org.zulipchat.com
export ZULIP_API_KEY=your-api-key
# or
export ZULIP_RC_PATH=/path/to/zuliprc
```

Everything else is optional. `.env.example` lists every variable; the ones you
are likely to touch:

| Variable | Default | Meaning |
|---|---|---|
| `ZULIP_SESSION_ID` | bot email (from env or zuliprc) | Keys the persistent state files (monitoring, delivery, filters); set it when two sessions of one bot share a state dir |
| `ZULIP_STATE_DIR` | `~/.zulip_mcp_state` | Where those files live |
| `ZULIP_SUBSCRIBE` | — | Streams to subscribe the bot to on startup |
| `ZULIP_FILTERS_FILE` | `<state dir>/<session>.filters.json` | The filters plane file (hot-reloaded) |
| `ZULIP_STREAMS`, `ZULIP_DM_USERS`, `ZULIP_MUTED_STREAMS` | — | Seed for the filters file on first materialization |
| `ZULIP_SUPPRESSED_REACTIONS_BASELINE` | `DISCORD_SUPPRESSED_REACTIONS_BASELINE` | Host-owned reaction markers withheld from the model; re-read every start, never persisted (connectome-host injects the `DISCORD_` name into every MCPL child) |
| `ZULIP_CATCHUP_LIMIT` | 3000 | Per-channel ceiling for catch-up and gap recovery (max 10000). For an always-open desk channel a few hundred is plenty |
| `ZULIP_MISSED_BLOCK_MAX_CHARS` | 40000 | Size cap on one `<missed>` block; the oldest lines are elided with a `fetch_history` pointer |
| `ZULIP_ATTRIBUTE_DELIVERY` | true | Prefix live, pushed and recovered bodies with `[time id=N] [#stream > topic] Author: `; `false` for a host that renders the structured fields itself (see *What the model reads*) |
| `ZULIP_BACKSCROLL_DEFAULT`, `ZULIP_BACKSCROLL_CHANNELS` | 500 | History cap on `channels/open`, per stream as `general:50,dev:200` |
| `ZULIP_INLINE_IMAGES`, `ZULIP_INLINE_IMAGES_MAX`, `ZULIP_ATTACHMENT_INLINE_MAX_BYTES` | true, 4, 5120 | Attachment inlining on live delivery |
| `ZULIP_UPLOAD_ROOTS` | — | Named directories local-file attachments may come from, `notes=./notes,out=/srv/out`. Unset: local files refused, base64 only |
| `ZULIP_UPLOAD_MAX_BYTES` | realm's cap, else 25 MiB | Per-file ceiling for outbound uploads; the realm's `max_file_upload_size_mib` is read at start. A message carries at most 10 files within 4× this |
| `AGENT_TIMEZONE`, `AGENT_TIMESTAMP_STYLE` | system, `full` | Agent-visible time on every message line: delivery, `<missed>`, injected history, `fetch_history` |
| `MCPL_ENABLED` | true | `false` forces plain-MCP mode even for MCPL hosts |

### Plain MCP client (Claude Code, Cursor)

```json
{
  "mcpServers": {
    "zulip": {
      "command": "node",
      "args": ["/path/to/zulip-mcp/build/index.js"],
      "env": {
        "ZULIP_RC_PATH": "/path/to/zuliprc",
        "ZULIP_SESSION_ID": "my-agent"
      }
    }
  }
}
```

### MCPL host

The server negotiates MCPL when the host advertises `experimental.mcpl` in
`initialize`. It stays inert until the host's `featureSets/update` Request
establishes the capability grant (SPEC 0.5 §5.3 — absence is denial), then
registers channels and runs the catch-up sweep. Stdio is the default
transport; `--tcp <port>` serves one connection at a time on localhost.

Feature sets: `zulip.messaging` (channels, push events, tools, rollback),
`zulip.history` (the read tools), `zulip.context` (recent history injected
before inference for open channels).

Every `zulip.messaging` tool result carries `state.checkpoint` (SPEC §8);
`state/rollback` to a checkpoint deletes what the bot sent after it —
tool sends and `channels/publish` alike. Disabling `zulip.messaging` stops
its delivery (incoming and push) and its tools at once.

**Wake policy.** Closed-channel mentions, quote replies and DMs, new-DM
announcements and `<missed>` catch-up blocks arrive as `push/event`, not `channels/incoming`.
A host whose gate defaults to skip needs a policy on the `mcpl:push-event`
scope or they land in context without a turn — for connectome-host's gate:

```json
{ "name": "addressed-push",
  "match": { "scope": ["mcpl:push-event"], "tagsAny": ["chat:addressed", "zulip:missed"] },
  "behavior": "always" }
```

(the gate matches on `tagsAny` / `tagsAll` / `tagsNone`; the host expands
`chat:mention`, `chat:reply` and `chat:dm` into `chat:addressed`). Conversely,
reactions on an open channel are ordinary `channels/incoming` messages carrying only
`chat:reaction` / `chat:reaction-remove` — a policy keyed on tags ignores
them, but an unconditional "always wake on this channel" policy wakes on
them too; add `"tagsNone": ["chat:reaction", "chat:reaction-remove"]` to it.

**Quote heading formats.** Reply detection accepts translated link text,
whitespace between the link label and URL or before the colon (including
non-breaking spaces), and ASCII or full-width colons. Headings ending in
` in …:` or ` to …:` are deliberately excluded: these also describe
forwarded messages, not necessarily replies. Thus the first quote in a
multi-message quote using a forwarding-style heading does not count; a
later ordinary reply heading can still count.

Quote-based addressing is disabled for authors whose email looks like a bot
account (the `-bot@` heuristic), to avoid bots waking each other by quoting.
Explicit Zulip mentions and DMs from bots retain their usual addressing.

### Edits, moves and deletions

A change to a message is as visible as the message was. An edit, a topic
move or a deletion of a message the host has accepted (or been offered) on
an open channel, or of the bot's own message, surfaces on that channel as
one line in the shared shape, carrying the message's own id:

```
[edited] [10:42 id=77] [#general > deploys] Ann: ship it tomorrow
[moved] [10:43 id=77] [#general > deploys-2] Ann: topic changed from "deploys" (3 messages) [by user 12]
[deleted] [10:44 id=77] [#general > deploys-2] Ann: message deleted — was: "ship it tomorrow"
```

On a closed channel only an addressed change is pushed — the mention the
agent is about to answer was rewritten or deleted, a quote reply changed,
or a DM it is reading changed — the same rule as for messages. A change to a message the host was
never offered is dropped. Zulip re-renders (a link preview arriving) are
not edits and never surface; the bot's own edits and deletions (rollback,
`delete_message`) are its own doing and never surface either. A moved
message is reported on the channel it left, naming where it went.

The lines carry `chat:edited` (moves add `zulip:moved`) or `chat:deleted`,
plus the same addressing tag a message would: `chat:mention` when the
message mentions the bot as it now reads, `chat:reply` for a verified quote
reply to the bot (including after an edit), `chat:dm` for a DM, else
`chat:ambient` — so a tag-keyed wake policy wakes on an edit exactly when
it would wake on the message, and a debounced ambient policy debounces
ambient edits. An unconditional per-channel policy wakes on every change;
add `"tagsNone": ["chat:edited", "chat:deleted"]` to it if that is unwanted.
`metadata` carries `change`, `targetMessageId(s)`, `previousContent`,
`previousTopic`, `movedToChannelId`, `actorId`, `mentioned` and
`previouslyMentioned`, plus `replyToSelf` and `previouslyReplyToSelf` for
quote addressing (previous verdicts are absent when only the post-change
state could be read). A change line never sets `threadId`. A deletion leaves reply routing
unchanged unless it removes the conversation's source message (including a
reference in another stream after a move) and no cached
message remains in that stream and topic. Then channel-only replies refuse:
"the message this conversation followed was deleted or moved where this bot
can't see it, so it can't be replied to". A visible third-party move of a conversation's source message
refuses channel-only replies, with the event's stream/topic as its destination
hint; it does not silently retarget the reply. The bot's own moves emit no change
marker. For host integrations:

1. A tool result confirms the anchor unless intervening movement invalidated
   that reference. A channel-only publish follows it or refuses. Other references
   a range move may have touched become unconfirmed, never confirmed by echoes.
   Only the exact anchor or a non-stale reference with known placement
   matching the source topic gets a destination hint: the stored topic (or requested topic if the read failed)
   is labelled **likely**, not confirmed. Others refuse without a stream/topic
   fallback; use their own `messageId` with `reply_to`. Only an exact anchor
   may seed a destination channel's conversation slot, never a co-moved hint.
2. Refusals say **Nothing was posted** and include `messageId` and a bare
   `stream`. For a move refusal, use
   `send_message({reply_to: <messageId>, content})` to resolve the message's current
   placement. A deletion/hidden-move refusal means that message cannot be replied
   to: to answer a newer message use `send_message({reply_to: <its id>, content})`;
   otherwise choose an explicit destination. Use an explicit bare stream and
   topic only when known (check `list_streams` / `get_stream_topics` if unknown).
   Never use a stream fallback for a DM; use its private recipient.
   Same-stream and cross-stream tool results include
   `moved_to: {channelId, stream, topic}`; unknown fields are null.
3. On an open channel, a new message (but not this bot's own messages)
   **above the refusal's message-id floor** clears refusal and routes replies
   to that message's topic. In the stream/topic the conversation left, only
   bot senders (identified by `looksLikeBotEmail`) are ignored: a person's new
   message there, including a delete-and-repost, clears refusal.
   A Notification Bot notice in the old topic does not clear refusal; a new
   destination breadcrumb can. On a closed channel, only such a message accepted
   as a push does so. Existing/moved copies, this bot's own messages, echoes,
   reads, reactions and
   edit markers do not clear refusal. Explicit sends bypass it for one reply;
   they do not clear it. Typing stops in the topic where its run began.

A move into a stream the bot cannot see
arrives from Zulip as an ordinary deletion, indistinguishable from deleting
the messages. The empty-cached-topic rule above refuses replies when the
followed message disappears with the last cached message of its topic.
Synthetic ids (`edit:77:…`) never advance the delivery watermark; the message's own id in the line is what `fetch_around`
takes (a deleted id can no longer be fetched). History and backscroll
render `(edited)` and `(moved)` trailers with `metadata.editedAt` /
`movedAt`.

### Known limitations of move reconciliation

- Deletions and moves out of visibility are indistinguishable. Refusal depends
  on the followed message disappearing with the last cached message in its
  topic, not proof that the whole topic was deleted. A newer human message in
  that same topic clears refusal; an old-topic bot notice does not. A deleted
  or inaccessible id cannot be used with `reply_to`: use a newer readable
  message's id, or choose an explicit destination. Explicit sends bypass
  refusal for that reply but do not clear it.
- Reply routing and refusals live in memory; they do not survive a reconnect
  or restart. Routing re-seeds from the next incoming message or history on
  channel open, or falls back to topic `mcpl` when neither supplies a topic.

## Channels

| Channel id | What it is |
|---|---|
| `zulip:<stream>` | A stream. Topics are threads: incoming messages carry the topic as `threadId`; publishes go to the topic of the newest of these there, else `mcpl`: a message queued for the host while the stream is open, even one the batch then withholds or the host refuses; while the stream is closed, an addressed message the host accepted as a push, or a `<missed>` block it accepted, from its newest addressed message or, in a full backscroll, its newest message; or the newest message of the history `channels/open` returned, unless that history was fetched before a given message. Streams visible at startup are registered then; one the bot joins later announces itself (see below). |
| `zulip:dm:<ids>` | A DM conversation, keyed by the other parties' sorted user ids (`zulip:dm:42`, `zulip:dm:7+42`). Discovered from recent DM history and announced on the fly (`channels/changed`) when someone new writes. |

Descriptors carry `capabilities.history` (`maxMessages`, `supportsBeforeMessage`,
`supportsSinceLastSeen`); `channels/open` may ask for history and gets it
before the lifecycle commits.

**Channels that appear after startup.** The bot does not have to be
restarted to use a stream it was added to later. Four things register one,
whichever happens first: the `listen` tool registers what it subscribes the
bot to; every message from the stream carries its descriptor, the way a DM
from a new conversation does, so the mention that wakes the agent also
makes the channel openable; `refresh_channels` re-enumerates everything
visible; and `channels/open` on an unknown exact `channelId` re-enumerates
once before it fails. Only a channel none of these can find answers
`-32023 Unknown channel`.

A `channels/changed` announcement the host never confirms is not a refusal:
the channel stays usable here and is re-announced on the next explicit
registration (`refresh_channels`, `listen`, a widened allowlist — never the
message path, where an announcement sits in front of a delivery).
`refresh_channels` reports it under `pendingAnnouncement`, and `listen`
under `registration`. The retry set is capped at 100 channels; past that the
oldest stops being retried and stays usable. One consequence worth knowing:
`channels/list` can name a channel the host never confirmed.

A host that opens or closes a channel has confirmed it, whatever became of
the announcement, and it leaves the retry set at that point. Against
agent-framework that is the only confirmation available for an announcement
made inside a tool call: it reconciles (a blocking `channels/open` or
`channels/close` per descriptor) before it answers, and this server serves
one request at a time, so the answer cannot arrive until the tool returns.
Without that rule the backlog would be retried on every refresh forever.

Silence is not refusal either. A host that itemizes only the descriptors it
changed leaves the rest unstated, and they are treated as pending, not
refused. An itemized `accepted: false` IS the host's answer: the channel is
unregistered and closed, it is not announced again, and `refresh_channels`
lists it under `refused`. Asking again is an agent decision — an explicit
`refresh_channels` retries refused channels.

An announcement waits 5 seconds, not the protocol default of 30. This
server answers one request at a time, and agent-framework's
`channels/changed` handler reconciles (a blocking `channels/open` per
descriptor) *before* it answers, while its `channels/register` handler
answers first. So an announcement made inside a tool call cannot be
answered until that call returns. Timing out quickly keeps the channel
usable, lets the host's own `channels/open` be served, and leaves the
re-announcement to the next refresh; descriptors are recorded before the
announcement goes out so that open succeeds when it lands.

## Filters plane

One JSON file is the desired state for what reaches the agent. It always exists
once the server has started (seeded from the environment), is authoritative
from then on, and is hot-reloaded within seconds — no change here ever needs a
restart.

```json
{
  "streams": ["general", "dev"],
  "dmUsers": ["42", "ann@example.com"],
  "mutedStreams": ["random"],
  "reactionChannels": ["zulip:general"],
  "suppressedReactionEmojis": ["biohazard"]
}
```

- `streams` — allowlist (absent = every stream the bot can see). Gates
  discovery and delivery on every surface: live, catch-up, gap recovery,
  backscroll on open, context injection, reactions.
- `dmUsers` — who may DM the bot (absent = anyone), judged per sender on
  every surface. Empty means unrestricted, deliberately: unsetting a
  variable must not silently lose every DM.
- `mutedStreams` — nothing from these reaches the agent on any surface:
  live delivery, mentions, backscroll on open, context injection, reactions,
  catch-up and gap recovery. The pull tools (`fetch_history`, …) still work.
- `reactionChannels` — channels showing live reactions.
- `suppressedReactionEmojis` — reaction markers withheld from every
  model-visible surface. Operator-owned: the agent's tools cannot carry this
  key, and `filters_get` reports it only as a count and digest. Entries are
  emoji names (`biohazard`) or glyphs (☣️); a glyph matches a Zulip reaction
  on its codepoints. The host's baseline (`*_SUPPRESSED_REACTIONS_BASELINE`,
  glyphs as connectome-host injects them) is added on top at every start
  and is never written into the file.

Every key is an authorization list: a wrong-typed value makes the file
invalid rather than reading as "unrestricted". While running, an
unparseable or vanished file keeps the last-known-good filters in force and
marks the plane stale; updates from the tools are refused until it is
repaired. At start there is no last-known-good, so a file that exists but
cannot be parsed is a startup failure — as is one that cannot be created —
not a run on the env seed (which, unset, means everything). Repair the
file, or remove it to re-seed from the environment.

## Tools

**Reading**
`fetch_history` (stream/topic or a DM conversation by channel id,
`before`/`after` id cursors, ids on every line),
`fetch_around` (window centred on a message, within its conversation),
`get_channel_history` (natural dates), `get_unread_messages`,
`list_streams`, `get_stream_topics`, `list_users`, `find_user`,
`get_user_profile`, `fetch_attachment`, `list_emojis`.

**Writing**
`send_message`, `send_dm` (by name, email, or id), `upload_file`,
`edit_message` (own-message content only), `move_messages` (topic/stream moves),
`delete_message`, `add_reaction`, `remove_reaction`.

`send_message` accepts an integer `reply_to` message ID (integer-valued strings also work): `{"reply_to":12345,"content":"Thanks!"}`.
Replies use the message's current stream/topic or complete DM recipient list, without quoting it.
Omit `to` and `topic`; `type` is inferred, and an explicit type must match (`channel`/`direct` normalize to `stream`/`private`).
Supplied destinations, conflicting types, or unreadable IDs fail before uploads or sends; lookup errors retain Zulip's API code.
Without `reply_to`, provide `type` and `to` (plus `topic` for streams). No additional permissions are needed.

Both send tools take an optional `attachments` array; each entry is a local
file (`{ "file": "notes/report.pdf" }`) or inline bytes
(`{ "data": "<base64>", "name": "chart.png" }`), with an optional
`mime_type` (guessed from the extension otherwise; Zulip previews an image
only when its declared type is `image/*`). Files are uploaded to the realm
first and linked at the end of the message, the way the Zulip client
attaches them. `content` may be omitted when there are attachments.
`upload_file` does the upload alone and returns the `/user_uploads/...`
path, the URL, and the markdown link, for embedding in an `edit_message` or
anywhere in a body. On the MCPL publish path, `image` and `audio` blocks
with inline data are uploaded the same way. Uploads happen before the send;
a send that then fails leaves them unreferenced, and Zulip garbage-collects
unclaimed uploads after a week.

**Local files are confined to upload roots.** Tool input is influenced by
message content from untrusted senders, and this server runs with the
host's filesystem and environment, so a `file` is never an arbitrary path.
It is `<root>/<path>`, where `<root>` names a directory exported in
`ZULIP_UPLOAD_ROOTS` (`notes=./notes,out=/srv/agent/out`; relative to the
server's cwd, which under a host is the host's). The path is resolved
against that root, symlinks followed, and must land inside it; absolute
paths and other roots are refused with the available names in the error.
With no roots configured, local-file attachments are refused and only
base64 `data` works. A root that does not exist is a startup failure. To
let an agent attach what it writes in its workspace, mount the workspace
and name it as a root at the same path, e.g. `ZULIP_UPLOAD_ROOTS=workspace=./workspace`,
so the mount-prefixed path the agent already knows is the attachment path.
The check is bound to the file actually opened, not to its pathname (via
`/proc/self/fd`), so a directory swapped for a symlink mid-request is
caught too. That makes local-file attachments Linux-only: on other
platforms Node has no descriptor-relative resolution, a pathname re-check
would be the very race the guard exists for, and so `file` is refused
there with a pointer to base64 `data`. What the check cannot see is a hard
link created inside a root to a file outside it: that needs
a local writer in the root (and, with `fs.protected_hardlinks=1`, ownership
of the target), so export roots only writers you trust can reach.

Limits: one file up to the realm's advertised cap (or `ZULIP_UPLOAD_MAX_BYTES`),
at most 10 files per message, 4× the per-file cap in total. The per-file
ceiling is enforced on the bytes read, not only on `stat`; the aggregate
budget is checked on declared sizes before any read and again on the bytes
actually read; base64 is measured before it is decoded. Files are read and
uploaded one at a time, so peak memory is one file in three copies (the
bytes, the multipart body, and the copy Node's fetch makes of the request
body). On the MCPL publish path a media block with malformed base64 fails
the publish; blocks the server cannot upload (no uploader, or a URI) are
dropped as before.

**Attention**
`listen` / `unlisten` (Zulip stream subscription), `start_monitoring` /
`stop_monitoring` / `get_monitored_channels` (read cursors for the plain-MCP
unread tools), `channel_missed`, `mute_channel` / `unmute_channel`,
`set_reaction_visibility`, `filters_get` / `filters_update`, `refresh_channels`.

Message ids are realm-global and monotonic, which makes them cursors: every
history line, `<missed>` block, and incoming message leads with one so the
agent can `fetch_around` it.

**What the model reads.** A delivered message carries its author, topic and
id as structured fields, but agent-framework's context strategies render
only the content blocks, so the body itself leads with them. Every place the
model reads a Zulip message line uses one shape: live delivery
(`channels/incoming`, `push/event`), messages recovered onto an open channel
after a gap or a refused batch, the `<missed>` catch-up block, the recent
history injected before inference, and `fetch_history` / `fetch_around`:

```
[2026-09-14T11:42:52+03:00 id=17206924] [#qa > router] Mykhailo Buialo (mention): do you have the same issue
[2026-09-14T11:42:52+03:00 id=17206925] [DM] Bo: hey, got a minute?
```

The time follows `AGENT_TIMEZONE` / `AGENT_TIMESTAMP_STYLE`; `full` drops its
`[Zone]` suffix on these lines (the offset fixes the instant), and `none`
keeps the id. `(mention)` marks a stream message that Zulip says mentions
the bot, not a quote-only reply; a direct message says `[DM]` instead. The reply affordance on a new DM precedes
the line; the attachment note and inlined images follow it. Header fields are
folded onto one line but not escaped, so a topic containing `]: ` reads
ambiguously to a regex. The backscroll returned on `channels/open` is not
prefixed: the host hands it to the agent as the `channel_open` tool result,
JSON that already shows each message's author and topic. The legacy
`get_channel_history` / `get_unread_messages` formats are unchanged.

**Hosts that render provenance themselves.** A prefixed message is stamped
`attributed: true` with `attributionHeader` (the exact prefix added) in its
metadata, and in the `push/event` origin, which agent-framework stores as the
message metadata on that path. A host strategy that builds its own
provenance header can skip it when the stamp is present, and one that scans
message text can strip the prefix first. connectome-host's `frontdesk`
strategy renders such a header (`[zulip · #stream · topic "t" · @Author ·
time · msg N]`); a frontdesk that does not yet honour the stamp shows both.
A host that renders the fields itself and cannot read the stamp sets
`ZULIP_ATTRIBUTE_DELIVERY=false` for bare bodies, in the environment the
server actually starts with: under connectome-host that is the host's own
environment or the `env` of the server's entry in `mcpl-servers.json`. A
recipe's `env` reaches the server only when the recipe defines the server
itself (`command` or `url`); for a server defined in `mcpl-servers.json` it
is not merged.

**Gate filters see the prefix.** agent-framework's wake gate matches a
policy's `match.filter` (substring or regex) against the joined text of the
message, which now starts with the line head. A pattern anchored with `^` on
the body stops matching, and a keyword that also appears in a stream, topic
or author name matches every message there. Anchor on the body after the
head (`\] [^:]*: pattern`), or match on channel, scope or tags instead.

## State on disk

Under `ZULIP_STATE_DIR`, keyed by session:

- `<session>.json` — the plain-MCP monitor (streams, last-read ids)
- `<session>.delivery.json` — watermarks, missed tallies, last-open channels
- `<session>.filters.json` — the filters plane (unless `ZULIP_FILTERS_FILE`)

Put `ZULIP_STATE_DIR` on storage that survives a redeploy. In a container
the default `~/.zulip_mcp_state` lives in the writable layer and goes with
the image: every rebuild wipes the watermarks (the next start anchors
catch-up at "now") and the filters file (mutes, reaction visibility and
allowlist edits made through the tools are gone; the file re-seeds from the
environment). Under connectome-host, point it into the data volume, e.g.
`"ZULIP_STATE_DIR": "${DATA_DIR}/<agent>/zulip-state"`.

## Notes for operators

- **Subscription is not optional.** Zulip delivers stream events only to
  subscribers, even with `all_public_streams` on the event queue. Opening a
  channel subscribes the bot (and fails if it cannot — a private stream the
  bot was not invited to answers `success` with the stream under
  `unauthorized`, which this server treats as a refusal); `ZULIP_SUBSCRIBE`
  and `listen` do it explicitly. `listen` alone leaves the channel *closed*:
  ambient is tallied and addressed messages become push events — the host must open
  the channel to receive its traffic.
- **State from 2.x.** The plain-MCP monitor cursors (`<session>.json`) are
  read as before but are not migrated into delivery watermarks: the first
  3.x start anchors catch-up at "now".
- **No per-call timeouts** on the Zulip API yet: the serve loop handles one
  host request at a time, so a hung Zulip call stalls the requests behind
  it. The host's own timeout abandons its request but does not unstall the
  loop; a server in that state needs a restart.
- **zulip-js quirks** (in `platforms/zulip-events.ts`): booleans in POST bodies
  must be strings, arrays must be raw arrays; API errors come back as values
  (`result: 'error'`), which this server turns into thrown errors.
- **Debugging delivery:** run the server standalone with the env of the recipe
  and watch stderr — hosts do not always capture MCPL child stderr.

## Development

```bash
npm run build      # tsc → build/
npm test           # node --test test/*.test.ts (via tsx)
npm run watch
```

`test/server.test.ts` drives the real `McplConnection` over an in-memory stream
pair through the handshake, the policy exchange, registration, delivery,
catch-up, and the tools — the fastest way to see the wire behaviour.

## License

MIT
