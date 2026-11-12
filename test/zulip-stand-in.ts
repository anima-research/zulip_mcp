import { zulipApi, type Method, type TransportAnswer, type ZulipApi } from '../src/zulip-api.ts';

export interface StandInCall {
  method: Method;
  path: string;
  params: Record<string, unknown>;
}

/** An answer with an explicit HTTP status. */
export function withStatus(status: number, body: unknown): TransportAnswer {
  return { status, body };
}

type Route = unknown | ((params: Record<string, unknown>, call: StandInCall) => unknown);

/**
 * A ZulipApi whose transport answers by "METHOD /path" (below /api/v1) with the
 * call site's own values, unencoded. A route's answer is the JSON body, sent
 * with 200, or 400 when it is a refusal; `withStatus` sets another. A call with
 * no route throws, naming it.
 */
export function standInApi(routes: Record<string, Route>): { api: ZulipApi; calls: StandInCall[] } {
  const calls: StandInCall[] = [];
  const api = zulipApi(async (method, path, params) => {
    const call: StandInCall = { method, path, params: params ?? {} };
    calls.push(call);
    const key = `${method} ${path}`;
    if (!(key in routes)) throw new Error(`no stand-in route for ${key}`);
    const route = routes[key];
    const answer = typeof route === 'function' ? await route(call.params, call) : route;
    if (answer && typeof answer === 'object' && 'status' in answer && 'body' in answer) return answer as TransportAnswer;
    const refused = (answer as { result?: unknown } | null)?.result === 'error';
    return { status: refused ? 400 : 200, body: answer };
  });
  return { api, calls };
}
