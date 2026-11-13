import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { once } from 'node:events';

interface Recorded {
  method: string;
  rawPath: string;
  path: string;
  query: URLSearchParams;
  body: string;
  contentType: string | undefined;
  authorization: string | undefined;
  closed: Promise<void>;
}

type Answer =
  | { json: unknown; status?: number; delayMs?: number }
  | { html: string; status: number }
  | 'hang'
  | 'drop'
  | 'stall-body'
  | { slow: string[]; everyMs: number };

const defaults: Record<string, Answer> = {
  'GET /users/me': { json: { result: 'success', user_id: 790, email: 'bot@example.com' } },
  'GET /server_settings': { json: { result: 'success', max_file_upload_size_mib: 25 } },
};

export async function realm(answers: Record<string, Answer | ((r: Recorded) => Answer)>) {
  const requests: Recorded[] = [];
  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    const chunks: Buffer[] = [];
    req.on('data', (chunk) => chunks.push(chunk));
    req.on('end', () => {
      const url = new URL(req.url ?? '/', 'http://realm.test');
      const path = url.pathname.replace(/^\/api\/v1/, '');
      const recorded: Recorded = {
        method: req.method ?? '',
        rawPath: url.pathname,
        path,
        query: url.searchParams,
        body: Buffer.concat(chunks).toString(),
        contentType: req.headers['content-type'],
        authorization: req.headers.authorization,
        closed: once(res, 'close').then(() => undefined),
      };
      requests.push(recorded);
      const found = answers[`${recorded.method} ${path}`] ?? defaults[`${recorded.method} ${path}`];
      const answer = typeof found === 'function' ? found(recorded) : found;
      if (answer === undefined) {
        res.statusCode = 404;
        res.setHeader('content-type', 'application/json');
        res.end(JSON.stringify({ result: 'error', msg: `no stand-in for ${recorded.method} ${path}` }));
      } else if (answer === 'hang') {
      } else if (answer === 'drop') {
        req.socket.destroy();
      } else if (answer === 'stall-body') {
        res.statusCode = 200;
        res.setHeader('content-type', 'application/json');
        res.write('{"result":"success","streams":[');
      } else if ('slow' in answer) {
        res.statusCode = 200;
        res.setHeader('content-type', 'text/plain');
        const parts = [...answer.slow];
        const next = () => {
          const part = parts.shift();
          if (part === undefined) { res.end(); return; }
          res.write(part);
          setTimeout(next, answer.everyMs);
        };
        next();
      } else if ('html' in answer) {
        res.statusCode = answer.status;
        res.setHeader('content-type', 'text/html');
        res.end(answer.html);
      } else {
        const send = () => {
          res.statusCode = answer.status ?? 200;
          res.setHeader('content-type', 'application/json');
          res.end(JSON.stringify(answer.json));
        };
        if (answer.delayMs) setTimeout(send, answer.delayMs);
        else send();
      }
    });
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const port = (server.address() as { port: number }).port;
  return {
    url: `http://127.0.0.1:${port}`,
    requests,
    close: () => { server.closeAllConnections(); server.close(); },
  };
}
