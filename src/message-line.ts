/**
 * A message line's header fields, such as the author's name and the topic,
 * are not escaped: a name or topic containing `]: ` still reads ambiguously
 * to a regex. Zulip already refuses control characters and some punctuation
 * in display names; topics are the realistic source.
 */

export interface MessageLineHead {
  id: number | string;
  time: string;
  stream: string | null;
  topic: string;
  author: string;
  mentioned: boolean;
}

const FIRST_NON_C0_CONTROL = 0x20;
const DELETE = 0x7f;
const LINE_SEPARATOR = 0x2028;
const PARAGRAPH_SEPARATOR = 0x2029;

function isLineBreaking(codePoint: number): boolean {
  return (
    codePoint < FIRST_NON_C0_CONTROL ||
    codePoint === DELETE ||
    codePoint === LINE_SEPARATOR ||
    codePoint === PARAGRAPH_SEPARATOR
  );
}

export function headerField(value: string): string {
  let out = '';
  let inRun = false;
  for (const ch of value) {
    if (isLineBreaking(ch.codePointAt(0) ?? 0)) {
      if (!inRun) out += ' ';
      inRun = true;
    } else {
      out += ch;
      inRun = false;
    }
  }
  return out;
}

export function messageLineHead(h: MessageLineHead): string {
  const where = h.stream === null ? '[DM]' : `[#${headerField(h.stream)} > ${headerField(h.topic)}]`;
  const mark = h.stream !== null && h.mentioned ? ' (mention)' : '';
  const time = headerField(h.time);
  return `[${time ? `${time} ` : ''}id=${h.id}] ${where} ${headerField(h.author)}${mark}: `;
}
