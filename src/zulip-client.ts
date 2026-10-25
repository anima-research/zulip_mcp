import { readFileSync } from 'node:fs';
import zulipInit from 'zulip-js';

export interface ZulipSession {
  client: any;
  selfUserId: number | null;
  realm: string;
  authHeader: string;
  sessionId: string;
  maxUploadBytes?: number | null;
}

export async function initializeZulipClient(env: NodeJS.ProcessEnv = process.env): Promise<ZulipSession> {
  const config: any = {
    realm: env.ZULIP_REALM || "",
  };

  if (env.ZULIP_RC_PATH) {
    config.zuliprc = env.ZULIP_RC_PATH;
  } else {
    config.username = env.ZULIP_USERNAME || env.ZULIP_EMAIL;
    config.apiKey = env.ZULIP_API_KEY;
    config.password = env.ZULIP_PASSWORD;
  }

  if (!config.realm && !config.zuliprc) {
    throw new Error(
      "ZULIP_REALM must be set (or provide ZULIP_RC_PATH for zuliprc file)"
    );
  }

  if (!config.zuliprc && !config.username) {
    throw new Error(
      "ZULIP_USERNAME/ZULIP_EMAIL must be set (or provide ZULIP_RC_PATH)"
    );
  }

  if (!config.zuliprc && !config.apiKey && !config.password) {
    throw new Error(
      "Either ZULIP_API_KEY or ZULIP_PASSWORD must be set (or provide ZULIP_RC_PATH)"
    );
  }

  const client: any = await zulipInit(config);

  // Capture realm + auth for direct fetches (zulip-js doesn't expose user_uploads).
  const resolved = (client && client.config) || config;
  let realm = (resolved.realm || config.realm || "").replace(/\/+$/, "");
  let email = resolved.username || config.username || env.ZULIP_EMAIL || env.ZULIP_USERNAME || "";
  let apiKey = resolved.apiKey || config.apiKey || env.ZULIP_API_KEY || "";

  if ((!realm || !email || !apiKey) && config.zuliprc) {
    try {
      const raw = readFileSync(config.zuliprc, "utf-8");
      const parsed: Record<string, string> = {};
      for (const line of raw.split(/\r?\n/)) {
        const m = line.match(/^\s*(email|key|site)\s*=\s*(.+?)\s*$/);
        if (m) parsed[m[1]] = m[2];
      }
      if (!realm && parsed.site) realm = parsed.site.replace(/\/+$/, "");
      if (!email && parsed.email) email = parsed.email;
      if (!apiKey && parsed.key) apiKey = parsed.key;
    } catch (err) {
      console.error("Failed to parse zuliprc for direct-HTTP credentials:", err);
    }
  }

  const authHeader = email && apiKey
    ? "Basic " + Buffer.from(`${email}:${apiKey}`).toString("base64")
    : "";

  const sessionId = env.ZULIP_SESSION_ID || email || env.ZULIP_USERNAME || "default";

  let selfUserId: number | null = null;
  try {
    const profile = await client.users.me.getProfile();
    if (profile && typeof profile.user_id === "number") {
      selfUserId = profile.user_id;
      console.error(`Zulip MCP bot user_id: ${selfUserId}`);
    }
  } catch (err) {
    console.error("Failed to fetch bot profile for self-filter:", err);
  }

  let maxUploadBytes: number | null = null;
  try {
    const settings = await client.callEndpoint("server_settings", "GET", {});
    const mib = settings?.max_file_upload_size_mib;
    if (typeof mib === "number" && Number.isFinite(mib) && mib > 0) maxUploadBytes = Math.floor(mib * 1024 * 1024);
  } catch (err) {
    console.error("Failed to read realm server settings (upload cap):", err);
  }

  if (env.ZULIP_SUBSCRIBE) {
    const streams = env.ZULIP_SUBSCRIBE.split(",").map(s => s.trim()).filter(Boolean);
    if (streams.length > 0) {
      try {
        const result = await client.users.me.subscriptions.add({
          subscriptions: streams.map(name => ({ name })),
        });
        const subscribed = result?.subscribed ?? {};
        const already = result?.already_subscribed ?? {};
        console.error(`Zulip MCP auto-subscribed: new=${JSON.stringify(subscribed)} already=${JSON.stringify(already)}`);
      } catch (err) {
        console.error(`Zulip MCP auto-subscribe failed for [${streams.join(", ")}]:`, err);
      }
    }
  }

  console.error(`Zulip MCP initialized with session: ${sessionId}`);
  return { client, selfUserId, realm, authHeader, sessionId, maxUploadBytes };
}
