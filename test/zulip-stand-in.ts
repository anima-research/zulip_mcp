import { ZulipNoAnswer, zulipApi, type Method, type TransportAnswer, type ZulipApi } from '../src/zulip-api.ts';

export interface StandInCall {
  method: Method;
  path: string;
  id?: number;
  params: Record<string, unknown>;
}

type Route = unknown | ((params: Record<string, unknown>, call: StandInCall) => unknown);

export function standInApi(routes: Record<string, Route>): { api: ZulipApi; calls: StandInCall[] } {
  const calls: StandInCall[] = [];
  const answerFor = async (call: StandInCall): Promise<TransportAnswer> => {
    const { method, path } = call;
    const exact = `${method} ${path}`;
    const key = exact in routes ? exact : `${method} ${path.replace(/\/\d+(?=\/|$)/g, '/:id')}`;
    if (!(key in routes)) throw new Error(`no stand-in route for ${exact}`);
    const route = routes[key];
    const answer = typeof route === 'function' ? await route(call.params, call) : route;
    const refused = (answer as { result?: unknown } | null)?.result === 'error';
    return { status: refused ? 400 : 200, body: answer };
  };
  const api = zulipApi(async (method, path, params, opts) => {
    const segment = path.match(/\/(\d+)(?=\/|$)/)?.[1];
    const call: StandInCall = { method, path, params: params ?? {}, ...(segment === undefined ? {} : { id: Number(segment) }) };
    calls.push(call);
    const answering = answerFor(call);
    const bound = opts?.timeoutMs;
    if (bound === undefined) return answering;
    answering.catch(() => {});
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new ZulipNoAnswer('timeout', method, path, method !== 'GET', `Zulip gave no answer to ${method} ${path} within ${bound} ms.`)), bound);
      timer.unref();
    });
    try {
      return await Promise.race([answering, timeout]);
    } finally {
      clearTimeout(timer);
    }
  });
  return { api, calls };
}
