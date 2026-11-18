import type { McplConnection } from '@animalabs/mcpl-core';

/**
 * mcpl-core 0.3 has no public request-written hook. Isolate its synchronous
 * writeLine boundary here: returning from sendRequest alone is NOT evidence of
 * a write (closed connections and throwing writers return rejected promises).
 * A write accepted with backpressure still counts; later transport failure is
 * uncertain delivery, not proof that the host did not receive the request.
 */
export function requestWritten(conn: McplConnection, method: string, params: unknown, written?: () => void): Promise<unknown> {
  if (!written) return conn.sendRequest(method, params);
  const transport = conn as unknown as {
    writeLine(json: string): void;
    writer: { destroyed?: boolean; writableEnded?: boolean };
  };
  const write = transport.writeLine;
  transport.writeLine = function (json) {
    if (transport.writer.destroyed || transport.writer.writableEnded) throw new Error('host writer is closed');
    write.call(conn, json);
    // All response continuations run after this synchronous commit, including
    // an immediate loopback answer. A failed write never removes saved IDs.
    const request = JSON.parse(json) as { method?: string; id?: unknown };
    if (request.method === method && request.id !== undefined) written();
  };
  try {
    return conn.sendRequest(method, params);
  } finally {
    transport.writeLine = write;
  }
}
