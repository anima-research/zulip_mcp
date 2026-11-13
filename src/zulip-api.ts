export type Method = 'GET' | 'POST' | 'PATCH' | 'DELETE';

export interface RequestOptions {
  timeoutMs?: number;
}

export interface ZulipRefusal {
  result: 'error';
  msg: string;
  code?: string;
  [field: string]: unknown;
}

export type ZulipSuccess<T extends object> = T & { result: 'success' };

export interface ZulipApi {
  request<T extends object = Record<string, unknown>>(
    method: Method, path: string, what: string, params?: Record<string, unknown>, opts?: RequestOptions,
  ): Promise<ZulipSuccess<T>>;
  requestAnswer<T extends object = Record<never, never>>(
    method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions,
  ): Promise<ZulipSuccess<T> | ZulipRefusal>;
}

export interface TransportAnswer {
  status: number;
  body: unknown;
}

export type Transport = (
  method: Method, path: string, params: Record<string, unknown> | undefined, opts: RequestOptions | undefined,
) => Promise<TransportAnswer>;

export type NoAnswerKind = 'timeout' | 'network' | 'not-json';

export class ZulipNoAnswer extends Error {
  constructor(
    readonly kind: NoAnswerKind,
    readonly method: Method,
    readonly path: string,
    readonly mayHaveBeenApplied: boolean,
    detail: string,
    readonly status?: number,
  ) {
    super(`${detail} ${mayHaveBeenApplied
      ? 'The request may have been applied: check before sending it again.'
      : method === 'GET' ? '' : 'The request was not sent.'}`.trim());
    this.name = 'ZulipNoAnswer';
  }
}

const WRITE_MAY_HAVE_BEEN_APPLIED = 'Zulip answered with a server error, so the request may have been applied: check before sending it again.';

export function isRefusal(answer: unknown): answer is ZulipRefusal {
  return typeof answer === 'object' && answer !== null && (answer as { result?: unknown }).result === 'error';
}

export class ZulipRefused extends Error {
  readonly code: string | undefined;
  readonly retryAfterSeconds: number | undefined;

  constructor(what: string, answer: ZulipRefusal, readonly mayHaveBeenApplied = false) {
    super(`Zulip refused ${what}: ${answer.msg ?? answer.code ?? 'unknown error'}`);
    this.code = typeof answer.code === 'string' ? answer.code : undefined;
    const after = answer['retry-after'];
    this.retryAfterSeconds = typeof after === 'number' && Number.isFinite(after) && after >= 0 ? after : undefined;
  }
}

export function mayHaveBeenApplied(err: unknown): boolean {
  return (err instanceof ZulipNoAnswer || err instanceof ZulipRefused) && err.mayHaveBeenApplied;
}

export function zulipApi(transport: Transport): ZulipApi {
  const answer = async (method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions) => {
    const { status, body } = await transport(method, path, params, opts);
    if (method !== 'GET' && status >= 500 && isRefusal(body)) {
      const text = body.msg || body.code || 'unknown error';
      return { body: { ...body, msg: `${text}${/[.!?]$/.test(text) ? '' : '.'} ${WRITE_MAY_HAVE_BEEN_APPLIED}` }, serverError: true };
    }
    return { body, serverError: false };
  };
  return {
    async request<T extends object>(method: Method, path: string, what: string, params?: Record<string, unknown>, opts?: RequestOptions) {
      const { body, serverError } = await answer(method, path, params, opts);
      if (isRefusal(body)) throw new ZulipRefused(what, body, serverError);
      return body as ZulipSuccess<T>;
    },
    async requestAnswer<T extends object>(method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions) {
      return (await answer(method, path, params, opts)).body as ZulipSuccess<T> | ZulipRefusal;
    },
  };
}

function encodeParams(params: Record<string, unknown> | undefined): URLSearchParams {
  const encoded = new URLSearchParams();
  for (const [key, value] of Object.entries(params ?? {})) {
    if (value === undefined) continue;
    encoded.append(key, typeof value === 'string' ? value : JSON.stringify(value));
  }
  return encoded;
}

export const DEFAULT_TIMEOUT_MS = 15_000;
export const LONG_TIMEOUT_MS = 90_000;

export interface HttpTransportConfig {
  apiUrl: string;
  authHeader: string;
  timeoutMs?: number;
}

const NOT_SENT_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN', 'EHOSTUNREACH', 'ENETUNREACH', 'UND_ERR_CONNECT_TIMEOUT', 'ERR_INVALID_URL', 'UNABLE_TO_VERIFY_LEAF_SIGNATURE']);
const notSent = (code: string) => NOT_SENT_CODES.has(code) || code.includes('CERT');

function causeCode(err: unknown): string | undefined {
  let current: unknown = err;
  for (let depth = 0; depth < 4 && current && typeof current === 'object'; depth++) {
    const code = (current as { code?: unknown }).code;
    if (typeof code === 'string') return code;
    current = (current as { cause?: unknown }).cause;
  }
  return undefined;
}

function duration(ms: number): string {
  return ms < 1000 ? `${ms} ms` : `${ms / 1000} s`;
}

export function httpTransport(config: HttpTransportConfig): Transport {
  return async (method, path, params, opts) => {
    const timeoutMs = opts?.timeoutMs ?? config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const encoded = encodeParams(params);
    let url = `${config.apiUrl}${path}`;
    const init: RequestInit = { method, headers: config.authHeader ? { Authorization: config.authHeader } : {} };
    if (method === 'GET') {
      const query = encoded.toString();
      if (query) url += `?${query}`;
    } else if (encoded.toString()) {
      init.body = encoded;
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    init.signal = controller.signal;
    const write = method !== 'GET';
    let status: number;
    let text: string;
    try {
      const response = await fetch(url, init);
      status = response.status;
      text = await response.text();
    } catch (err) {
      if (controller.signal.aborted) {
        throw new ZulipNoAnswer('timeout', method, path, write, `Zulip gave no answer to ${method} ${path} within ${duration(timeoutMs)}.`);
      }
      const code = causeCode(err);
      const unsent = code !== undefined && notSent(code);
      throw new ZulipNoAnswer('network', method, path, write && !unsent,
        unsent
          ? `Couldn't reach Zulip for ${method} ${path} (${code}).`
          : `The connection to Zulip failed during ${method} ${path}${code ? ` (${code})` : ''}.`);
    } finally {
      clearTimeout(timer);
    }
    try {
      return { status, body: JSON.parse(text) };
    } catch {
      throw new ZulipNoAnswer('not-json', method, path, write,
        `Zulip's answer to ${method} ${path} wasn't JSON (HTTP ${status}).`, status);
    }
  };
}
