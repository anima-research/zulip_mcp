import { randomBytes } from 'node:crypto';
import type { ContractEnv } from './env.ts';

export function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now().toString(36)}-${randomBytes(3).toString('hex')}`;
}

export async function until<T>(probe: () => Promise<T | undefined> | T | undefined, what: string, ms = 30_000): Promise<T> {
  const deadline = Date.now() + ms;
  while (true) {
    const value = await probe();
    if (value !== undefined) return value;
    if (Date.now() > deadline) throw new Error(`timed out after ${ms} ms waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 100));
  }
}

export interface ZulipMessage {
  id: number;
  sender_id: number;
  type: 'stream' | 'private';
  display_recipient: string | { id: number; email: string }[];
  subject: string;
  content: string;
}

const RATE_LIMIT_TRIES = 5;

export class PlainApi {
  constructor(
    private readonly realm: string,
    private readonly email: string,
    private readonly apiKey: string,
  ) {}

  async call(method: 'GET' | 'POST' | 'PATCH' | 'DELETE', path: string, params: Record<string, unknown> = {}): Promise<any> {
    const form = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
      form.append(key, typeof value === 'string' ? value : JSON.stringify(value));
    }
    const url = new URL(`${this.realm}/api/v1/${path}`);
    if (method === 'GET') url.search = form.toString();
    for (let tries = 1; ; tries++) {
      const res = await fetch(url, {
        method,
        headers: {
          Authorization: `Basic ${Buffer.from(`${this.email}:${this.apiKey}`).toString('base64')}`,
          ...(method === 'GET' ? {} : { 'Content-Type': 'application/x-www-form-urlencoded' }),
        },
        ...(method === 'GET' ? {} : { body: form.toString() }),
      });
      const body = await res.json();
      if (res.status !== 429 || tries === RATE_LIMIT_TRIES) return body;
      // Wait the seconds that Zulip's 429 names, as any client must.
      const seconds = Number(res.headers.get('retry-after') ?? body?.['retry-after']);
      await new Promise((r) => setTimeout(r, (Number.isFinite(seconds) && seconds > 0 ? seconds : 1) * 1000));
    }
  }

  async ok(method: 'GET' | 'POST' | 'PATCH' | 'DELETE', path: string, params: Record<string, unknown> = {}): Promise<any> {
    const body = await this.call(method, path, params);
    if (body?.result !== 'success') throw new Error(`${method} ${path} failed: ${body?.msg ?? JSON.stringify(body)}`);
    return body;
  }

  /** A message by id: its raw Markdown rather than its rendering, and an empty topic as "" rather than its display name. */
  async message(id: number | string): Promise<ZulipMessage> {
    return (await this.ok('GET', `messages/${id}`, { apply_markdown: false, allow_empty_topic_name: true })).message;
  }

  async myId(): Promise<number> {
    return (await this.ok('GET', 'users/me')).user_id;
  }
}

export function apis(env: ContractEnv): { user: PlainApi; bot: PlainApi } {
  return {
    user: new PlainApi(env.realm, env.user.email, env.user.apiKey),
    bot: new PlainApi(env.realm, env.bot.email, env.bot.apiKey),
  };
}

export async function newStream(env: ContractEnv, prefix: string): Promise<string> {
  const name = uniqueName(prefix);
  const { user, bot } = apis(env);
  await user.ok('POST', 'users/me/subscriptions', {
    subscriptions: [{ name }],
    principals: [await user.myId(), await bot.myId()],
  });
  return name;
}
