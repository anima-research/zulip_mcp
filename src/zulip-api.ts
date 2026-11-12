export type Method = 'GET' | 'POST' | 'PATCH' | 'DELETE';

export interface RequestOptions {
  /** Ends the request after this long, in milliseconds. */
  timeoutMs?: number;
}

/** Zulip's answer when it refused the request. */
export interface ZulipRefusal {
  result: 'error';
  msg: string;
  code?: string;
  [field: string]: unknown;
}

/** A successful answer, with the fields a call site reads. */
export type ZulipSuccess<T extends object> = T & { result: 'success' };

export interface ZulipApi {
  /**
   * Zulip's answer. Throws `Zulip refused <what>: <msg>` when its `result` is
   * "error", and ZulipNoAnswer when Zulip gave no JSON answer.
   */
  request<T extends object = Record<string, unknown>>(
    method: Method, path: string, what: string, params?: Record<string, unknown>, opts?: RequestOptions,
  ): Promise<ZulipSuccess<T>>;
  /**
   * Zulip's JSON answer whatever its `result`, for a site that reads a refusal
   * itself. Throws ZulipNoAnswer when Zulip gave no JSON answer.
   */
  requestAnswer<T extends object = Record<never, never>>(
    method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions,
  ): Promise<ZulipSuccess<T> | ZulipRefusal>;
}

/** What a transport hands back: the HTTP status and the parsed JSON body. */
export interface TransportAnswer {
  status: number;
  body: unknown;
}

/** Sends one request and parses its JSON answer, or throws ZulipNoAnswer. */
export type Transport = (
  method: Method, path: string, params: Record<string, unknown> | undefined, opts: RequestOptions | undefined,
) => Promise<TransportAnswer>;

export type NoAnswerKind = 'timeout' | 'network' | 'not-json';

/** Zulip gave no JSON answer: the request timed out, the connection failed, or the body wasn't JSON. */
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

/** The error a checking site throws for a refusal: `Zulip refused <what>: <msg>`. */
export function refusalError(what: string, answer: ZulipRefusal): Error {
  return new Error(`Zulip refused ${what}: ${answer.msg ?? answer.code ?? 'unknown error'}`);
}

/** Builds the two entry points on a transport. */
export function zulipApi(transport: Transport): ZulipApi {
  const answer = async (method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions): Promise<unknown> => {
    const { status, body } = await transport(method, path, params, opts);
    if (method !== 'GET' && status >= 500 && isRefusal(body)) {
      return { ...body, msg: `${body.msg ?? body.code ?? 'unknown error'}. ${WRITE_MAY_HAVE_BEEN_APPLIED}` };
    }
    return body;
  };
  return {
    async request<T extends object>(method: Method, path: string, what: string, params?: Record<string, unknown>, opts?: RequestOptions) {
      const body = await answer(method, path, params, opts);
      if (isRefusal(body)) throw refusalError(what, body);
      return body as ZulipSuccess<T>;
    },
    async requestAnswer<T extends object>(method: Method, path: string, params?: Record<string, unknown>, opts?: RequestOptions) {
      return (await answer(method, path, params, opts)) as ZulipSuccess<T> | ZulipRefusal;
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

const DEFAULT_TIMEOUT_MS = 15_000;

export interface HttpTransportConfig {
  /** The API's address, such as `https://chat.example.com/api/v1`. */
  apiUrl: string;
  /** `Basic …`, or empty to send no Authorization header. */
  authHeader: string;
  /** The bound for a call that passes none; 15 s by default. */
  timeoutMs?: number;
}

/** Error codes from Node's fetch that mean the request was never sent. */
const NOT_SENT_CODES = new Set(['ECONNREFUSED', 'ENOTFOUND', 'EAI_AGAIN']);

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

/** Sends each request over HTTP, ended at its bound. */
export function httpTransport(config: HttpTransportConfig): Transport {
  return async (method, path, params, opts) => {
    const timeoutMs = opts?.timeoutMs ?? config.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const encoded = encodeParams(params);
    let url = `${config.apiUrl}${path}`;
    const init: RequestInit = { method, headers: config.authHeader ? { Authorization: config.authHeader } : {} };
    if (method === 'GET') {
      const query = encoded.toString();
      if (query) url += `?${query}`;
    } else {
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
      const notSent = code !== undefined && NOT_SENT_CODES.has(code);
      throw new ZulipNoAnswer('network', method, path, write && !notSent,
        notSent
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
