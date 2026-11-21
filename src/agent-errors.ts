import { ZulipNoAnswer } from './zulip-api.js';

const safeTransport = Symbol('safeTransport');
type TransportFacts = { attachmentName?: string; operation?: 'Upload' | 'Attachment download'; status?: number; timeoutSeconds?: number; original?: unknown; completed?: boolean; refused?: boolean; retryAfterSeconds?: number };
const nativeTransportErrors = new WeakMap<Error, TransportFacts>();
/** Mark failures at the fetch boundary, not by their JavaScript error class. */
export function markTransportError(error: unknown, facts: TransportFacts = {}): unknown {
  if (error instanceof Error) nativeTransportErrors.set(error, facts);
  return error;
}

/** The safe facts of a download failure marked at the native fetch boundary, if any. */
export function attachmentDownloadFailure(error: unknown): { refused: boolean; status?: number; timeoutSeconds?: number; retryAfterSeconds?: number } | undefined {
  const facts = error instanceof Error ? nativeTransportErrors.get(error) : undefined;
  if (facts?.operation !== 'Attachment download') return undefined;
  return { refused: facts.completed === true, status: facts.status, timeoutSeconds: facts.timeoutSeconds, retryAfterSeconds: facts.retryAfterSeconds };
}

const loggedDiagnostics = new WeakSet<Error>();

/** Contextual boundaries must not log an error already logged by an inner boundary. */
export function hasLoggedDiagnostic(error: unknown): boolean {
  return error instanceof Error && loggedDiagnostics.has(error);
}

/** Preserve operator-only originals and share ownership across contextual boundaries. */
export function logDiagnostic(error: unknown, context = '[zulip diagnostic]'): void {
  if (error instanceof Error) {
    if (safeTransport in error || loggedDiagnostics.has(error)) return;
    loggedDiagnostics.add(error);
  }
  console.error(context, error instanceof Error ? nativeTransportErrors.get(error)?.original ?? error : error);
}

/** Agent-facing transport diagnostics: retain type/metadata, never expose raw transport text.
 * Copies may safely be annotated by callers, in every ID mode.
 */
export function agentSafeError(error: unknown, log = true): Error {
  const copy: Error = error instanceof DOMException
    ? Object.defineProperties(new DOMException(error.message, error.name), Object.getOwnPropertyDescriptors(error))
    : error instanceof Error
    ? Object.create(Object.getPrototypeOf(error), Object.getOwnPropertyDescriptors(error))
    : new Error(String(error));
  const nativeTransport = error instanceof Error && nativeTransportErrors.has(error);
  // Callers that already log contextual diagnostics opt out; keep original causes
  // on stderr only, never in the host-visible error copy.
  if (log) logDiagnostic(error);
  if (error instanceof Error && (loggedDiagnostics.has(error) || !log)) loggedDiagnostics.add(copy);
  if ((copy instanceof ZulipNoAnswer || nativeTransport) && !(safeTransport in copy)) {
    if (copy instanceof ZulipNoAnswer) {
      let detail = copy.kind === 'no-self-id'
        ? "The server doesn't have the bot's own user id yet; it will retry on a later call."
        : `Zulip transport failure (${copy.kind}).`;
      if (copy.kind === 'not-json') detail += ' The server returned an unreadable response; contact the host operator if it persists.';
      if ([401, 403].includes(copy.status ?? 0)) detail += ' The host operator must repair authorization before retrying.';
      else if (copy.method === 'GET' && copy.kind !== 'no-self-id' && copy.retryAfterSeconds === undefined) detail += ' Retry the read.';
      if (copy.timeoutSeconds !== undefined) detail += ` Timeout: ${copy.timeoutSeconds} seconds.`;
      if (copy.status !== undefined) detail += ` HTTP ${copy.status}.`;
      if (copy.retryAfterSeconds !== undefined) detail += [401, 403].includes(copy.status ?? 0)
        ? ` Server Retry-After: ${copy.retryAfterSeconds} seconds.`
        : ` Retry after ${copy.retryAfterSeconds} seconds${copy.method === 'GET' ? ', then retry the read' : ''}.`;
      copy.message = new ZulipNoAnswer(copy.kind, copy.method, copy.path, copy.mayHaveBeenApplied,
        detail, copy.status, copy.retryAfterSeconds, copy.timeoutSeconds).message;
    } else {
      const facts = nativeTransportErrors.get(error as Error);
      const authorization = [401, 403].includes(facts?.status ?? 0);
      const detail = ((facts?.operation === 'Upload' ? facts.refused : facts?.completed)
        ? `${facts?.operation ?? 'Request'} was refused by the server. Check the path, access permissions and server availability; ask the host operator if it persists.`
        : (facts?.completed ? `${facts.operation ?? 'Request'} returned no confirmed usable result. ` : `${facts?.operation ?? 'Transport'} failed before a complete answer was received. `)
        + (facts?.operation === 'Attachment download' ? 'Retry fetch_attachment with the same path.'
          : facts?.operation === 'Upload' ? 'No uploaded path was confirmed for this attempt. Ask the host operator to check Zulip uploads before retrying the attachment upload in the original operation; retrying may create a duplicate.'
          : 'Retry reads; verify the outcome before repeating writes.'))
        + (facts?.attachmentName !== undefined ? ` Attachment: ${JSON.stringify(facts.attachmentName)}.` : '')
        + (facts?.operation === 'Upload' && facts.refused && facts.status === 413 ? ' Reduce the attachment size or omit it before retrying.' : '')
        + (authorization ? ' The host operator must repair authorization; retries alone cannot fix access.' : '')
        + (facts?.status !== undefined ? ` HTTP ${facts.status}.` : '')
        + (facts?.timeoutSeconds !== undefined ? ` Timeout: ${facts.timeoutSeconds} seconds.` : '')
        + (facts?.retryAfterSeconds !== undefined ? authorization
          ? ` Server Retry-After: ${facts.retryAfterSeconds} seconds; access must be corrected first.`
          : ` Retry after ${facts.retryAfterSeconds} seconds.` : '');
      Object.defineProperty(copy, 'message', { value: detail, writable: true, configurable: true });
    }
    Object.defineProperty(copy, 'cause', { value: undefined, configurable: true });
    Object.defineProperty(copy, 'stack', { value: `${copy.name}: ${copy.message}`, configurable: true });
    Object.defineProperty(copy, safeTransport, { value: true });
  }
  return copy;
}
