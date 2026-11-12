/**
 * Zulip client bootstrap — credentials, realm/auth capture for direct HTTP
 * (attachments), bot identity for the self-filter, and startup subscriptions.
 */

import { readFileSync } from 'node:fs';
import { parse as parseIni } from 'ini';
import { httpTransport, zulipApi, type ZulipApi } from './zulip-api.js';
import type { StreamDirectory } from './stream-directory.js';

export interface ZulipSession {
  /** Zulip's API, with the session's credentials. */
  api: ZulipApi;
  /** Numeric bot user id when the profile fetch succeeded; null = no self-filter. */
  selfUserId: number | null;
  /** Realm URL without trailing slash, for direct `/user_uploads/` fetches. */
  realm: string;
  /** `Basic …` header for direct fetches; empty when credentials are unknown. */
  authHeader: string;
  /** Persistent-state id (ZULIP_SESSION_ID, else the bot email, else 'default'). */
  sessionId: string;
  /** The realm's advertised per-file upload cap (`max_file_upload_size_mib`), bytes; null when unknown. */
  maxUploadBytes?: number | null;
}

export interface ZulipClientOptions {
  /** The bound for a call that passes none, in milliseconds; 15 s by default. */
  timeoutMs?: number;
}

function basic(email: string, apiKey: string): string {
  return "Basic " + Buffer.from(`${email}:${apiKey}`).toString("base64");
}

export async function initializeZulipClient(env: NodeJS.ProcessEnv = process.env, options: ZulipClientOptions = {}): Promise<ZulipSession> {
  const zuliprc = env.ZULIP_RC_PATH;
  let realm = "";
  let email = "";
  let apiKey = "";
  let password = "";
  let apiUrl: string;

  if (zuliprc) {
    const api = parseIni(readFileSync(zuliprc, "utf-8")).api as Record<string, unknown> | undefined;
    if (!api) throw new Error(`The zuliprc file at ${zuliprc} has no [api] section`);
    const text = (v: unknown) => (typeof v === "string" ? v : "");
    realm = text(api.site);
    email = text(api.email) || env.ZULIP_EMAIL || env.ZULIP_USERNAME || "";
    apiKey = text(api.key) || env.ZULIP_API_KEY || "";
    apiUrl = `${realm}/api/v1`;
  } else {
    realm = env.ZULIP_REALM || "";
    email = env.ZULIP_USERNAME || env.ZULIP_EMAIL || "";
    apiKey = env.ZULIP_API_KEY || "";
    password = env.ZULIP_PASSWORD || "";
    if (!realm) {
      throw new Error(
        "ZULIP_REALM must be set (or provide ZULIP_RC_PATH for zuliprc file)"
      );
    }
    if (!email) {
      throw new Error(
        "ZULIP_USERNAME/ZULIP_EMAIL must be set (or provide ZULIP_RC_PATH)"
      );
    }
    if (!apiKey && !password) {
      throw new Error(
        "Either ZULIP_API_KEY or ZULIP_PASSWORD must be set (or provide ZULIP_RC_PATH)"
      );
    }
    apiUrl = realm.endsWith("/api") ? `${realm}/v1` : `${realm}/api/v1`;
  }

  // Final fallback: parse the zuliprc file directly if any field is still missing.
  if ((!realm || !email || !apiKey) && zuliprc) {
    try {
      const raw = readFileSync(zuliprc, "utf-8");
      const parsed: Record<string, string> = {};
      for (const line of raw.split(/\r?\n/)) {
        const m = line.match(/^\s*(email|key|site)\s*=\s*(.+?)\s*$/);
        if (m) parsed[m[1]] = m[2];
      }
      if (!realm && parsed.site) realm = parsed.site;
      if (!email && parsed.email) email = parsed.email;
      if (!apiKey && parsed.key) apiKey = parsed.key;
    } catch (err) {
      console.error("Failed to parse zuliprc for direct-HTTP credentials:", err);
    }
  }

  if (!apiKey && password) {
    const exchange = zulipApi(httpTransport({ apiUrl, authHeader: "", timeoutMs: options.timeoutMs }));
    const answer = await exchange.requestAnswer<{ api_key?: unknown }>("POST", "/fetch_api_key", { username: email, password });
    apiKey = answer.result === "success" && typeof answer.api_key === "string" ? answer.api_key : "";
  }

  realm = realm.replace(/\/+$/, "");
  const authHeader = email && apiKey ? basic(email, apiKey) : "";
  const api = zulipApi(httpTransport({ apiUrl, authHeader: basic(email, apiKey), timeoutMs: options.timeoutMs }));

  // Keys the persistent state files. The bot's identity is the natural
  // default whichever way the credentials arrived (env or zuliprc); two
  // sessions of one bot sharing a state dir must set ZULIP_SESSION_ID.
  const sessionId = env.ZULIP_SESSION_ID || email || env.ZULIP_USERNAME || "default";

  // Fail-open: if profile fetch fails, leave selfUserId null (no self-filter).
  let selfUserId: number | null = null;
  try {
    const profile = await api.requestAnswer<{ user_id?: unknown }>("GET", "/users/me");
    if (profile.result === "success" && typeof profile.user_id === "number") {
      selfUserId = profile.user_id;
      console.error(`Zulip MCP bot user_id: ${selfUserId}`);
    }
  } catch (err) {
    console.error("Failed to fetch bot profile for self-filter:", err);
  }

  // The realm's upload cap, so the outbound ceiling is what the server will
  // actually accept rather than a guess. Fail-open: unknown → null.
  let maxUploadBytes: number | null = null;
  try {
    const settings = await api.requestAnswer<{ max_file_upload_size_mib?: unknown }>("GET", "/server_settings", {});
    const mib = settings.result === "success" ? settings.max_file_upload_size_mib : undefined;
    if (typeof mib === "number" && Number.isFinite(mib) && mib > 0) maxUploadBytes = Math.floor(mib * 1024 * 1024);
  } catch (err) {
    console.error("Failed to read realm server settings (upload cap):", err);
  }

  console.error(`Zulip MCP initialized with session: ${sessionId}`);
  return { api, selfUserId, realm, authHeader, sessionId, maxUploadBytes };
}

/**
 * A former name isn't subscribed, since Zulip's subscribe call creates a
 * stream for a name that no stream has.
 */
export async function subscribeConfigured(api: ZulipApi, directory: StreamDirectory, setting: string | undefined): Promise<void> {
  const named = (setting ?? "").split(",").map((s) => s.trim()).filter(Boolean);
  if (named.length === 0) return;
  await directory.whenListed();
  const streams: string[] = [];
  for (const name of named) {
    const former = directory.formerOf(name);
    if (former) {
      console.error(`[zulip-mcp] ZULIP_SUBSCRIBE names #${name}, which Zulip stream ${former.id} had before it was renamed #${former.current}; nothing is subscribed or created for it`);
      continue;
    }
    streams.push(name);
  }
  if (streams.length === 0) return;
  try {
    const result = await api.requestAnswer<{ subscribed?: unknown; already_subscribed?: unknown }>("POST", "/users/me/subscriptions", {
      subscriptions: streams.map((name) => ({ name })),
    });
    const subscribed = result.result === "success" ? result.subscribed ?? {} : {};
    const already = result.result === "success" ? result.already_subscribed ?? {} : {};
    console.error(`Zulip MCP auto-subscribed: new=${JSON.stringify(subscribed)} already=${JSON.stringify(already)}`);
  } catch (err) {
    console.error(`Zulip MCP auto-subscribe failed for [${streams.join(", ")}]:`, err);
  }
}
