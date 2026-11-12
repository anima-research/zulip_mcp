import { isRefusal, type ZulipApi, type ZulipRefusal, type ZulipSuccess } from './zulip-api.js';
import { withAttachmentLinks, type UploadedFile } from './uploads.js';

export interface Destination {
  type: unknown;
  to: unknown;
  topic?: unknown;
}

export interface SentChunk {
  id: number;
  content: string;
}

export interface SendResult {
  chunks: SentChunk[];
  last: ZulipSuccess<{ id: number }>;
}

/** A message this server sent, as the rollback record keeps it. */
export interface SentRecord {
  messageId: string;
  channelId: string;
  content: string;
}

export class SendRefused extends Error {
  constructor(readonly answer: ZulipRefusal, readonly sent: SentChunk[]) {
    super(answer.msg ?? answer.code ?? 'Zulip refused the message');
    this.name = 'SendRefused';
  }
}

/** Zulip's default `max_message_length` realm setting. */
export const ZULIP_MAX_MESSAGE_LENGTH = 10000;

/**
 * Split text into messages of at most `limit` characters, preferring
 * paragraph boundaries, then line boundaries, then a hard cut. Boundary
 * whitespace at the split is dropped; nothing else is.
 */
export function chunkMessage(text: string, limit: number = ZULIP_MAX_MESSAGE_LENGTH): string[] {
  const out: string[] = [];
  let rest = text;
  while (rest.length > 0) {
    if (rest.length <= limit) {
      out.push(rest);
      break;
    }
    const window = rest.slice(0, limit + 1);
    let cut = window.lastIndexOf('\n\n');
    if (cut <= 0) cut = window.lastIndexOf('\n');
    if (cut <= 0) cut = limit;
    out.push(rest.slice(0, cut));
    rest = rest.slice(cut).replace(/^\n+/, '');
  }
  return out.filter((c) => c.length > 0);
}

export function prepareOutgoing(content: string): string {
  return content;
}

export async function sendMessage(
  api: ZulipApi,
  destination: Destination,
  content: string,
  opts: { limit: number; uploaded?: UploadedFile[]; onChunk?: (chunk: SentChunk) => void },
): Promise<SendResult> {
  const parts = chunkMessage(withAttachmentLinks(prepareOutgoing(content), opts.uploaded ?? []), opts.limit);
  if (parts.length === 0) throw new Error('sendMessage was given no text and no files');
  const chunks: SentChunk[] = [];
  let last: ZulipSuccess<{ id: number }> | undefined;
  for (const part of parts) {
    const answer = await api.requestAnswer<{ id: number }>('POST', '/messages', { ...destination, content: part });
    if (isRefusal(answer)) throw new SendRefused(answer, chunks);
    const chunk = { id: answer.id, content: part };
    chunks.push(chunk);
    last = answer;
    opts.onChunk?.(chunk);
  }
  return { chunks, last: last! };
}
