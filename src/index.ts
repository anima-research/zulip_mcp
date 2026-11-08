#!/usr/bin/env node
/**
 * Zulip MCP/MCPL server — CLI entry point.
 *
 * Usage:
 *   zulip-mcp-server                 # stdio transport (default; MCP-compatible)
 *   zulip-mcp-server --stdio
 *   zulip-mcp-server --tcp <port>    # TCP transport for MCPL hosts
 *
 * Environment:
 *   ZULIP_REALM / ZULIP_EMAIL / ZULIP_API_KEY   - bot credentials
 *   ZULIP_RC_PATH                               - alternative: a zuliprc file
 *   ZULIP_SESSION_ID                            - persistent monitoring state id
 *   ZULIP_SUBSCRIBE                             - comma-separated streams to join on startup
 *   ZULIP_STATE_DIR                             - where monitoring + delivery state lives
 *                                                 (default ~/.zulip_mcp_state)
 *   ZULIP_CATCHUP_LIMIT                         - per-channel ceiling for the reconnect
 *                                                 catch-up sweep and gap recovery (3000)
 *   ZULIP_ACTIVE_TOPICS                        - recent topics in stream events (6; 0 disables, max 50)
 *   ZULIP_BACKSCROLL_DEFAULT                    - history cap per channel on channels/open (500)
 *   ZULIP_BACKSCROLL_CHANNELS                   - per-stream caps, "general:50,dev:200"
 *   ZULIP_FILTERS_FILE                          - the filters plane file (default
 *                                                 <state dir>/<session>.filters.json); hot-reloaded
 *   ZULIP_STREAMS / ZULIP_DM_USERS /            - env seed for the filters file on first
 *   ZULIP_MUTED_STREAMS                           materialization (then the file is authoritative)
 *   ZULIP_SUPPRESSED_REACTIONS_BASELINE         - host-injected reaction-suppression seed
 *   AGENT_TIMEZONE / AGENT_TIMESTAMP_STYLE      - agent-visible timestamps (IANA zone; full|compact|time|none)
 *   ZULIP_ATTRIBUTE_DELIVERY                    - "false" delivers bare bodies; default renders
 *                                                 "[time id=N] [#stream > topic] Author: " into each
 *   ZULIP_MAX_MESSAGE_LENGTH                    - the realm's max message length; longer sends are split (10000)
 *   ZULIP_INLINE_IMAGES                         - "false" to stop inlining images on live delivery
 *   ZULIP_INLINE_IMAGES_MAX                     - images inlined per message (4)
 *   ZULIP_ATTACHMENT_INLINE_MAX_BYTES           - text attachments inlined at or under this size (5120; max 256KiB)
 *   ZULIP_UPLOAD_ROOTS                          - named directories local-file attachments may come from,
 *                                                 "notes=./notes,out=/srv/out"; unset = local files refused
 *   ZULIP_UPLOAD_MAX_BYTES                      - per-file ceiling for outbound uploads (the realm's
 *                                                 advertised cap, else 25MiB); 10 files / 4x that per message
 *   MCPL_ENABLED                                - "false" forces plain-MCP mode
 *   MCPL_BATCH_WINDOW_MS                        - channels/incoming batching window (500)
 *   MCPL_CONTEXT_HISTORY_SIZE                   - messages injected per open channel (20)
 */

import * as net from 'node:net';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { McplConnection } from '@animalabs/mcpl-core';
import { isMainModule } from './content.js';
import { fetchCapped, resolveInlineOptions } from './attachments.js';
import { parseZulipAttachmentUrl } from './content.js';
import { FiltersPlane } from './filters.js';
import { DEFAULT_ACTIVE_TOPICS, DEFAULT_ACTIVE_TOPICS_TTL_MS } from './active-topics.js';
import { DEFAULT_BACKSCROLL, ZulipAdapter } from './platforms/zulip.js';
import { DEFAULT_CATCHUP_LIMIT, ZulipMcplServer } from './server.js';
import { DEFAULT_MISSED_BLOCK_MAX_CHARS } from './delivery.js';
import { ZulipToolRuntime } from './tool-runtime.js';
import { LOCAL_FILES_SUPPORTED, createZulipUploader, resolveUploadPolicy } from './uploads.js';
import { agentLineTimeFormatter } from './timezone.js';
import { initializeZulipClient } from './zulip-client.js';

export { fetchAttachmentBytes, extractZulipAttachments, cleanContent, cleanMarkdown } from './content.js';
export { formatMessages } from './tool-runtime.js';

const SERVER_INFO = { name: 'zulip-mcp-server', version: '3.0.0' };

function intEnv(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = parseInt(raw, 10);
  if (!Number.isFinite(n) || n < 0) {
    console.error(`[zulip-mcp] ignoring ${name}=${JSON.stringify(raw)} (not a non-negative integer); using ${fallback}`);
    return fallback;
  }
  return n;
}

/** "general:50,dev:200" → Map { general → 50, dev → 200 }. Bad entries are reported and skipped. */
export function parseBackscrollLimits(raw: string | undefined): Map<string, number> {
  const out = new Map<string, number>();
  if (!raw) return out;
  for (const entry of raw.split(',')) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const sep = trimmed.lastIndexOf(':');
    const name = sep > 0 ? trimmed.slice(0, sep).trim() : '';
    const n = sep > 0 ? parseInt(trimmed.slice(sep + 1), 10) : NaN;
    if (!name || !Number.isFinite(n) || n < 0) {
      console.error(`[zulip-mcp] ignoring ZULIP_BACKSCROLL_CHANNELS entry ${JSON.stringify(trimmed)} (want "stream:limit")`);
      continue;
    }
    out.set(name.replace(/^#/, ''), n);
  }
  return out;
}

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const tcpIdx = args.indexOf('--tcp');
  const tcpPort = tcpIdx >= 0 ? parseInt(args[tcpIdx + 1], 10) : undefined;
  if (tcpIdx >= 0 && !(tcpPort! > 0)) {
    console.error('Usage: zulip-mcp-server [--stdio | --tcp <port>]');
    process.exit(1);
  }

  const session = await initializeZulipClient();
  const stateDir = process.env.ZULIP_STATE_DIR || join(homedir(), '.zulip_mcp_state');
  const filters = new FiltersPlane(process.env.ZULIP_FILTERS_FILE || join(stateDir, `${session.sessionId}.filters.json`));
  filters.start();
  // Outbound uploads: a bad ZULIP_UPLOAD_ROOTS is a startup failure.
  const uploadPolicy = resolveUploadPolicy(process.env, session.maxUploadBytes);
  const uploader = session.realm ? createZulipUploader(session) : undefined;
  if (uploadPolicy.roots.size > 0) {
    console.error(`[zulip-mcp] upload roots: ${[...uploadPolicy.roots].map(([n, d]) => `${n}=${d}`).join(', ')}`);
    if (!LOCAL_FILES_SUPPORTED) console.error(`[zulip-mcp] ZULIP_UPLOAD_ROOTS is set but local-file attachments are Linux-only on this platform (${process.platform}); base64 data still works`);
  }
  // One line-time formatter for every surface (and one warning for a bad AGENT_TIMEZONE).
  const formatTime = agentLineTimeFormatter();
  const adapter = new ZulipAdapter(session.client, session.selfUserId, session.sessionId, {
    formatTime,
    uploader,
    uploadPolicy,
    activeTopics: intEnv('ZULIP_ACTIVE_TOPICS', DEFAULT_ACTIVE_TOPICS),
    activeTopicsTtlMs: intEnv('ZULIP_ACTIVE_TOPICS_TTL_MS', DEFAULT_ACTIVE_TOPICS_TTL_MS),
    backscrollDefault: intEnv('ZULIP_BACKSCROLL_DEFAULT', DEFAULT_BACKSCROLL),
    backscrollLimits: parseBackscrollLimits(process.env.ZULIP_BACKSCROLL_CHANNELS),
    filters,
    maxMessageLength: session.maxMessageLength,
  });
  const tools = new ZulipToolRuntime(session, stateDir, { uploader, uploadPolicy, formatTime });
  tools.setReactionPolicy({
    suppressed: (name, code, type) => filters.reactionSuppressed(name, code, type),
  });
  const server = new ZulipMcplServer(adapter, tools, {
    serverInfo: SERVER_INFO,
    mcplEnabled: process.env.MCPL_ENABLED !== 'false',
    batchWindowMs: intEnv('MCPL_BATCH_WINDOW_MS', 500),
    contextHistorySize: intEnv('MCPL_CONTEXT_HISTORY_SIZE', 20),
    stateDir,
    sessionId: session.sessionId,
    catchupLimit: intEnv('ZULIP_CATCHUP_LIMIT', DEFAULT_CATCHUP_LIMIT),
    missedBlockMaxChars: intEnv('ZULIP_MISSED_BLOCK_MAX_CHARS', DEFAULT_MISSED_BLOCK_MAX_CHARS),
    attributeDelivery: process.env.ZULIP_ATTRIBUTE_DELIVERY !== 'false',
    formatTime,
    filters,
    attachments: {
      // Same validation as fetch_attachment: only /user_uploads/ on the realm
      // host may ever see the bot's credentials.
      source: {
        fetch: (path, maxBytes) => {
          const url = parseZulipAttachmentUrl(path, session.realm);
          const headers: Record<string, string> = session.authHeader ? { Authorization: session.authHeader } : {};
          return fetchCapped(url.toString(), headers, maxBytes);
        },
      },
      inline: resolveInlineOptions(),
    },
  });

  if (tcpPort) {
    console.error(`[zulip-mcp] Listening on TCP port ${tcpPort}`);
    const tcpServer = net.createServer();
    tcpServer.listen(tcpPort, '127.0.0.1');
    await new Promise<void>((resolve) => tcpServer.once('listening', resolve));

    // One connection at a time.
    while (true) {
      const conn = await McplConnection.acceptTcp(tcpServer);
      console.error('[zulip-mcp] Client connected');
      await server.serve(conn);
      console.error('[zulip-mcp] Client disconnected, waiting for next...');
    }
  }

  // Stdio: stdout is the protocol channel, so everything else logs to stderr.
  const conn = McplConnection.fromStreams(process.stdin, process.stdout);

  // A host that stops us with a signal rather than EOF still gets the last
  // batch and a persisted watermark file.
  let stopping = false;
  const stop = async (why: string) => {
    if (stopping) return;
    stopping = true;
    console.error(`[zulip-mcp] ${why}; shutting down`);
    await server.shutdown();
    filters.stop();
    process.exit(0);
  };
  process.once('SIGTERM', () => void stop('SIGTERM'));
  process.once('SIGINT', () => void stop('SIGINT'));

  await server.serve(conn);
  await stop('stdin closed');
}

// Only auto-start when run as a CLI. Importing this module (e.g. from tests)
// should not boot the server or require Zulip credentials. isMainModule
// realpaths argv[1] so the guard also passes when launched through an npm
// bin symlink (npx zulip-mcp-server).
if (isMainModule(import.meta.url, process.argv[1])) {
  main().catch((error) => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });
}
