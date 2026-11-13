/*
 * HASH_REPLACEMENTS, encodeHashComponent, the slug in streamSlug, and
 * MARKDOWN_SYNTAX with escapeMarkdownSyntax are ported from Zulip 12.4,
 * web/src/internal_url.ts (encodeHashComponent, stream_id_to_slug,
 * encode_slug) and web/src/topic_link_util.ts
 * (html_escape_markdown_syntax_characters), as messageLink's label and URL
 * follow topic_link_util.ts's message link. Its notice:
 *
 * Copyright 2012–2015 Dropbox, Inc., 2015–2021 Kandra Labs, Inc., and contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this project except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * BACKTICK is Python-Markdown 3.10.3's BACKTICK_RE
 * (markdown/inlinepatterns.py), which Zulip 12.4 registers as its first
 * inline pattern. Its licence:
 *
 * BSD 3-Clause License
 *
 * Copyright 2007, 2008 The Python Markdown Project (v. 1.7 and later)
 * Copyright 2004, 2005, 2006 Yuri Takhteyev (v. 0.2-1.6b)
 * Copyright 2004 Manfred Stienstra (the original version)
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice, this
 *    list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * 3. Neither the name of the copyright holder nor the names of its
 *    contributors may be used to endorse or promote products derived from
 *    this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

import { CODE, HEADING, RULE, expandTabs, markdownBlocks, nextMatching } from './markdown-blocks.js';
import { tableTest } from './tables.js';
import { ZulipNoAnswer, isRefusal, type ZulipApi } from './zulip-api.js';

// ── Where a token is ──

const TOKEN = /\[\[msg:([0-9]+)\]\]/g;
const BACKTICK = /(?:(?<!\\)((?:\\{2})+)(?=`+)|(?<!\\)(`+)(.+?)(?<!`)\2(?!`))/gs;
/** Zulip's MarkdownListPreprocessor.LI_RE (zerver/lib/markdown/__init__.py at tag 12.4). */
const LIST_ITEM = /^[ ]*(?:[*+-]|\d\.)[ ]+/;

interface MessageToken {
  start: number;
  end: number;
  /** The id as written. */
  digits: string;
}

/**
 * The runs of a block's lines whose inline code Zulip reads apart, as its
 * block processors take a block, in their order: an indented start (code,
 * whose backticks open no span), a heading, a table (each row apart), a
 * rule. What is left is a paragraph, a list (each item apart) or a quote.
 * Each run is the lines from its first to before its second number.
 */
function inlineRuns(
  from: number,
  to: number,
  lines: string[],
  next: { heading: number[]; rule: number[] },
  table: (from: number, to: number) => boolean,
): [number, number][] {
  const runs: [number, number][] = [];
  const todo: [number, number][] = [[from, to]];
  while (todo.length > 0) {
    let [a, b] = todo.pop()!;
    while (a < b && CODE.test(lines[a]!)) a += 1;
    if (a >= b) continue;
    const heading = next.heading[a]!;
    if (heading < b) {
      runs.push([heading, heading + 1]);
      todo.push([a, heading], [heading + 1, b]);
      continue;
    }
    if (table(a, b)) {
      for (let i = a; i < b; i += 1) runs.push([i, i + 1]);
      continue;
    }
    const rule = next.rule[a]!;
    if (rule < b) {
      todo.push([a, rule], [rule + 1, b]);
      continue;
    }
    let start = a;
    for (let i = a + 1; i < b; i += 1) {
      if (LIST_ITEM.test(lines[i]!)) {
        runs.push([start, i]);
        start = i;
      }
    }
    runs.push([start, b]);
  }
  return runs;
}

/**
 * Each `[[msg:ID]]` that Zulip would read as Markdown: outside every code and
 * math fence (a quote, quoted or spoiler fence's inside counts as Markdown)
 * and outside inline code. Zulip has no backslash escape for a bracket, so a
 * backslash before a token changes nothing.
 */
function messageTokens(text: string): MessageToken[] {
  if (!text.includes('[[msg:')) return [];
  const raw = text.split('\n');
  const starts: number[] = [];
  let at = 0;
  for (const line of raw) {
    starts.push(at);
    at += line.length + 1;
  }
  const lines = raw.map((line) => expandTabs(line.replace(/\r$/, '')));
  const table = tableTest(lines);
  const next = { heading: nextMatching(lines, HEADING), rule: nextMatching(lines, RULE) };
  const markdownLine = new Array<boolean>(lines.length).fill(false);
  const code: [number, number][] = [];
  for (const block of markdownBlocks(lines)) {
    for (const i of block) markdownLine[i] = true;
    for (const [first, end] of inlineRuns(block[0]!, block.at(-1)! + 1, lines, next, table)) {
      const from = starts[first]!;
      const last = end - 1;
      const slice = text.slice(from, starts[last]! + raw[last]!.length);
      for (const m of slice.matchAll(BACKTICK)) {
        if (m[3] !== undefined) code.push([from + m.index, from + m.index + m[0].length]);
      }
    }
  }
  // Tokens come in order, and code spans don't overlap, so one pointer into
  // each finds a token's line and the first span that doesn't end before it.
  code.sort((x, y) => x[0] - y[0]);
  const tokens: MessageToken[] = [];
  let line = 0;
  let span = 0;
  for (const m of text.matchAll(TOKEN)) {
    const start = m.index;
    const end = start + m[0].length;
    while (line + 1 < starts.length && starts[line + 1]! <= start) line += 1;
    while (span < code.length && code[span]![1] <= start) span += 1;
    if (!markdownLine[line]) continue;
    if (span < code.length && code[span]![0] < end) continue;
    tokens.push({ start, end, digits: m[1]! });
  }
  return tokens;
}

// ── The link ──

const HASH_REPLACEMENTS = new Map([
  ['%', '.'],
  ['!', '.21'],
  ["'", '.27'],
  ['(', '.28'],
  [')', '.29'],
  ['*', '.2A'],
  ['.', '.2E'],
]);

function encodeHashComponent(str: string): string {
  return encodeURIComponent(str).replaceAll(/[%!'()*.]/g, (matched) => HASH_REPLACEMENTS.get(matched)!);
}

function streamSlug(streamId: number, name: string): string {
  const slug = `${streamId}-${name.replaceAll(' ', '-')}`;
  const encoded = encodeHashComponent(slug);
  if (encoded.length - slug.length > 10 || encoded.length > slug.length * 1.5) return String(streamId);
  return encoded;
}

const MARKDOWN_SYNTAX = /[`>*&[\]|]|(\$\$)/g;
const MARKDOWN_ESCAPES = new Map([
  ['`', '&#96;'],
  ['>', '&gt;'],
  ['*', '&#42;'],
  ['&', '&amp;'],
  ['$$', '&#36;&#36;'],
  ['[', '&#91;'],
  [']', '&#93;'],
  // Not in Zulip's client: a | would split a table row that the link sits in.
  ['|', '&#124;'],
]);

function escapeMarkdownSyntax(text: string): string {
  return text.replaceAll(MARKDOWN_SYNTAX, (matched) => MARKDOWN_ESCAPES.get(matched) ?? matched);
}

/** Zulip's empty-topic name, as its server writes it in a link (zerver/lib/topic_link_util.py). */
const EMPTY_TOPIC_NAME = 'general chat';

interface LinkedMessage {
  id: number;
  streamId: number;
  stream: string;
  topic: string;
}

/** The Markdown link Zulip's web client writes for a message in a stream. */
function messageLink(m: LinkedMessage): string {
  const label = `#${escapeMarkdownSyntax(m.stream)} > ${escapeMarkdownSyntax(m.topic === '' ? EMPTY_TOPIC_NAME : m.topic)} @ 💬`;
  const url = `#narrow/channel/${streamSlug(m.streamId, m.stream)}/topic/${encodeHashComponent(m.topic)}/near/${m.id}`;
  return `[${label}](${url})`;
}

// ── Who reads the message ──

/** Where a text goes: a stream, by id or by name where known, or a direct conversation. */
export type Audience = { kind: 'stream'; id?: number; name?: string } | { kind: 'direct' };

/** The stream of a send's `to`, read as Zulip reads it: JSON first, an integer an id and a string a name. */
export function audienceOf(destination: { type: unknown; to: unknown }): Audience {
  if (destination.type !== 'stream' && destination.type !== 'channel') return { kind: 'direct' };
  let to = destination.to;
  if (typeof to === 'string') {
    const raw = to;
    try {
      to = JSON.parse(raw);
    } catch {
      return { kind: 'stream', name: raw };
    }
  }
  if (Array.isArray(to)) to = to.length === 1 ? to[0] : undefined;
  if (typeof to === 'string') return { kind: 'stream', name: to };
  if (typeof to === 'number' && Number.isSafeInteger(to)) return { kind: 'stream', id: to };
  return { kind: 'stream' };
}

// ── The preparation ──

/** What happens to a token that can't become a link: a tool call refuses; a plain answer posts it as typed. */
export type Unlinkable = 'refuse' | 'as-typed';

/** One or more tokens can't become links; each reason names its token. */
export class MessageLinkRefused extends Error {
  constructor(readonly reasons: string[]) {
    super(reasons.join(' '));
    this.name = 'MessageLinkRefused';
  }
}

type Outcome = { link: string } | { reason: string };

/**
 * Puts a link in place of each `[[msg:ID]]` that Zulip reads as Markdown.
 * Each message is read once, and each stream's privacy once, at this call.
 * A token for a message the bot can't read, for a direct message, or for a
 * message in a private stream toward any other conversation can't become a
 * link: `refuse` throws MessageLinkRefused naming each, before anything is
 * sent, and `as-typed` leaves it. A text with no token reads nothing.
 */
export async function linkMessages(
  api: ZulipApi,
  text: string,
  audience: () => Promise<Audience>,
  unlinkable: Unlinkable,
): Promise<string> {
  const tokens = messageTokens(text);
  if (tokens.length === 0) return text;

  let destination: Promise<Audience> | undefined;
  const destinationOnce = () => (destination ??= audience());
  let destinationId: Promise<{ id?: number } | { refused: string }> | undefined;
  const destinationIdOnce = () =>
    (destinationId ??= destinationOnce().then(async (to) => {
      if (to.kind !== 'stream') return {};
      if (to.id !== undefined || to.name === undefined) return { id: to.id };
      const answer = await api.requestAnswer<{ stream_id?: unknown }>('GET', '/get_stream_id', { stream: to.name });
      if (isRefusal(answer)) return { refused: `Zulip refused the lookup of the destination stream ${to.name} (${answer.msg ?? answer.code ?? 'unknown error'})` };
      return typeof answer.stream_id === 'number' ? { id: answer.stream_id } : {};
    }));
  const privacy = new Map<number, Promise<{ inviteOnly: boolean } | { refused: string }>>();
  const privacyOnce = (streamId: number) => {
    let read = privacy.get(streamId);
    if (read === undefined) {
      read = api.requestAnswer<{ stream?: { invite_only?: unknown } }>('GET', `/streams/${streamId}`).then((answer) =>
        isRefusal(answer) ? { refused: answer.msg ?? answer.code ?? 'unknown error' } : { inviteOnly: answer.stream?.invite_only !== false },
      );
      privacy.set(streamId, read);
    }
    return read;
  };

  const decide = async (digits: string): Promise<Outcome> => {
    const id = Number(digits);
    if (!Number.isSafeInteger(id) || id <= 0) return { reason: `there is no message ${digits}` };
    const answer = await api.requestAnswer<{ message?: Record<string, unknown> }>('GET', `/messages/${id}`, {
      apply_markdown: false,
      allow_empty_topic_name: true,
    });
    if (isRefusal(answer)) return { reason: `Zulip refused to show message ${id} to this bot (${answer.msg ?? answer.code ?? 'unknown error'})` };
    const m = answer.message ?? {};
    if (m.type !== 'stream') return { reason: `message ${id} is a direct message, and only a stream message can be linked` };
    const { stream_id: streamId, display_recipient: stream, subject: topic } = m;
    if (typeof streamId !== 'number' || typeof stream !== 'string' || typeof topic !== 'string') {
      return { reason: `Zulip's answer for message ${id} names no stream and topic` };
    }
    const link = { link: messageLink({ id, streamId, stream, topic }) };
    const to = await destinationOnce();
    if (to.kind === 'stream' && to.id === streamId) return link;
    const read = await privacyOnce(streamId);
    if ('refused' in read) return { reason: `Zulip didn't say whether message ${id}'s stream is private (${read.refused})` };
    if (!read.inviteOnly) return link;
    const destinationId = await destinationIdOnce();
    if ('refused' in destinationId) return { reason: destinationId.refused };
    if (destinationId.id === streamId) return link;
    return { reason: `message ${id} is in a private stream, and a link would show that stream's and topic's names to this conversation's readers` };
  };

  const keyOf = (digits: string) => (Number.isSafeInteger(Number(digits)) ? Number(digits) : digits);
  const outcomes = new Map<number | string, { digits: string; outcome: Outcome | null }>();
  let answered = true;
  for (const token of tokens) {
    const key = keyOf(token.digits);
    if (outcomes.has(key)) continue;
    if (!answered) {
      outcomes.set(key, { digits: token.digits, outcome: null });
      continue;
    }
    try {
      outcomes.set(key, { digits: token.digits, outcome: await decide(token.digits) });
    } catch (err) {
      // A tool call says what failed, and its caller adds that nothing was
      // sent: the preparation only reads. A plain answer reads no further
      // once Zulip doesn't answer, and posts the rest of its tokens as typed.
      if (unlinkable === 'refuse') throw new MessageLinkRefused([`[[msg:${token.digits}]] can't become a link: ${String(err instanceof Error ? err.message : err).replace(/\.$/, '')}.`]);
      if (!(err instanceof ZulipNoAnswer)) throw err;
      outcomes.set(key, { digits: token.digits, outcome: null });
      answered = false;
    }
  }

  if (unlinkable === 'refuse') {
    const reasons: string[] = [];
    for (const { digits, outcome } of outcomes.values()) {
      if (outcome && 'reason' in outcome) reasons.push(`[[msg:${digits}]] can't become a link: ${outcome.reason}.`);
    }
    if (reasons.length > 0) throw new MessageLinkRefused(reasons);
  }

  let out = '';
  let from = 0;
  for (const token of tokens) {
    const outcome = outcomes.get(keyOf(token.digits))?.outcome;
    if (outcome && 'link' in outcome) {
      out += text.slice(from, token.start) + outcome.link;
      from = token.end;
    }
  }
  return out + text.slice(from);
}
