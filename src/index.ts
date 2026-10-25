#!/usr/bin/env node

import * as net from 'node:net';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { McplConnection } from '@animalabs/mcpl-core';
import { isMainModule } from './content.js';
import { fetchCapped, resolveInlineOptions } from './attachments.js';
import { parseZulipAttachmentUrl } from './content.js';
import { FiltersPlane } from './filters.js';
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
  const uploadPolicy = resolveUploadPolicy(process.env, session.maxUploadBytes);
  const uploader = session.realm ? createZulipUploader(session) : undefined;
  if (uploadPolicy.roots.size > 0) {
    console.error(`[zulip-mcp] upload roots: ${[...uploadPolicy.roots].map(([n, d]) => `${n}=${d}`).join(', ')}`);
    if (!LOCAL_FILES_SUPPORTED) console.error(`[zulip-mcp] ZULIP_UPLOAD_ROOTS is set but local-file attachments are Linux-only on this platform (${process.platform}); base64 data still works`);
  }
  const formatTime = agentLineTimeFormatter();
  const adapter = new ZulipAdapter(session.client, session.selfUserId, session.sessionId, {
    formatTime,
    uploader,
    uploadPolicy,
    backscrollDefault: intEnv('ZULIP_BACKSCROLL_DEFAULT', DEFAULT_BACKSCROLL),
    backscrollLimits: parseBackscrollLimits(process.env.ZULIP_BACKSCROLL_CHANNELS),
    filters,
    maxMessageLength: process.env.ZULIP_MAX_MESSAGE_LENGTH ? intEnv('ZULIP_MAX_MESSAGE_LENGTH', 10000) : undefined,
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

if (isMainModule(import.meta.url, process.argv[1])) {
  main().catch((error) => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });
}
