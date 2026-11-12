/*
 * isTable, splitRow and split are ported from Python-Markdown 3.10.3,
 * markdown/extensions/tables.py (TableProcessor.test, _split_row, _split),
 * the version Zulip 12.4 pins. Its licence:
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

const RE_CODE_PIPES = /(\\\\)|(\\`+)|(`+)|(\\\|)|(\|)/g;
const RE_END_BORDER = /(?<!\\)(?:\\\\)*\|$/;

function isTable(block: string[]): boolean {
  const rows = block.map((row) => row.replace(/^ +| +$/g, ''));
  if (rows.length < 2) return false;
  const header = rows[0]!;
  const border = header.startsWith('|') || RE_END_BORDER.test(header);
  const width = splitRow(header, border).length;
  let table = width > 1;
  if (!table && width === 1 && border) {
    for (const row of rows.slice(1)) {
      table = row.startsWith('|') || RE_END_BORDER.test(row);
      if (!table) break;
    }
  }
  if (!table) return false;
  const separator = splitRow(rows[1]!, border);
  return separator.length === width && /^[|:\- ]*$/.test(separator.join(''));
}

function splitRow(row: string, border: boolean): string[] {
  if (border) {
    if (row.startsWith('|')) row = row.slice(1);
    row = row.replace(RE_END_BORDER, '');
  }
  return split(row);
}

function split(row: string): string[] {
  const pipes: number[] = [];
  const tics: number[] = [];
  const ticPoints: [number, number, number][] = [];
  for (const m of row.matchAll(RE_CODE_PIPES)) {
    const at = m.index!;
    if (m[2] !== undefined) {
      tics.push(m[2].length - 1);
      ticPoints.push([at, at + m[2].length - 1, 1]);
    } else if (m[3] !== undefined) {
      tics.push(m[3].length);
      ticPoints.push([at, at + m[3].length - 1, 0]);
    } else if (m[5] !== undefined) {
      pipes.push(at);
    }
  }
  const regions: [number, number][] = [];
  for (let pos = 0; pos < tics.length; ) {
    const size = tics[pos]! - ticPoints[pos]![2];
    const index = size === 0 ? -1 : tics.slice(pos + 1).indexOf(size);
    if (index === -1) {
      pos += 1;
      continue;
    }
    regions.push([ticPoints[pos]![0], ticPoints[pos + index + 1]![1]]);
    pos += index + 2;
  }
  const cells: string[] = [];
  let start = 0;
  for (const pipe of pipes) {
    const inCode = regions.some(([from, to]) => from <= pipe && pipe <= to);
    if (inCode) continue;
    cells.push(row.slice(start, pipe));
    start = pipe + 1;
  }
  cells.push(row.slice(start));
  return cells;
}

/** Zulip 12.4's FENCE_RE (zerver/lib/markdown/fenced_code.py), with PLAYGROUND_LANGUAGE_REGEX in place. */
const FENCE = /^(~{3,}|`{3,})[ ]*(?:\{?\.?([a-zA-Z0-9_+-.\/#]+)[ ]*([^ ~`][^~`]*)?\}?)?$/;
const NESTING = new Set(['quote', 'quoted', 'spoiler']);
const LIST_OR_QUOTE = /^[ ]*(?:>|(?:[-*+]|\d+[.)])(?:[ ]|$))/;

function expandTabs(line: string): string {
  let out = '';
  for (const ch of line) out += ch === '\t' ? ' '.repeat(4 - (out.length % 4)) : ch;
  return out;
}

/**
 * The runs of lines that Zulip's parser reads as blocks: lines between blank
 * lines, outside code fences. Zulip reads the inside of a quote, quoted or
 * spoiler fence as Markdown, so its lines make blocks too. A fence's own
 * lines belong to no block.
 */
function blocks(lines: string[]): number[][] {
  const out: number[][] = [];
  let current: number[] = [];
  const end = () => {
    if (current.length > 0) out.push(current);
    current = [];
  };
  const stack: { fence: string; nests: boolean }[] = [];
  lines.forEach((line, i) => {
    const top = stack.at(-1);
    if (top !== undefined && line.trimEnd() === top.fence) {
      end();
      stack.pop();
      return;
    }
    if (top !== undefined && !top.nests) return;
    const m = FENCE.exec(line);
    if (m) {
      end();
      stack.push({ fence: m[1]!, nests: NESTING.has((m[2] ?? '').toLowerCase()) });
      return;
    }
    if (/^ *$/.test(line)) end();
    else current.push(i);
  });
  end();
  return out;
}

/** Zulip 12.4's HashHeaderProcessor.RE (zerver/lib/markdown/__init__.py), for one line. */
const HEADING = /^#{1,6}\s/;
/** Python-Markdown 3.10.3's HRProcessor.RE (markdown/blockprocessors.py). */
const RULE = /^[ ]{0,3}(?=((?:-+[ ]{0,2}){3,}|(?:_+[ ]{0,2}){3,}|(?:\*+[ ]{0,2}){3,}))\1[ ]*$/;
const CODE = /^ {4}/;

/**
 * Puts one blank line above the header row of each table that sits directly
 * under a line of text, where Zulip's parser shows no table, and changes
 * nothing else.
 */
export function blankLineAboveTables(text: string): string {
  const raw = text.split('\n');
  const lines = raw.map((line) => expandTabs(line.replace(/\r$/, '')));
  const above = new Set<number>();
  // How many tables Zulip shows for one block, taking it as its block
  // processors do, in their order: an indented start (code), a heading, a
  // table, a rule. What none of them takes holds no table.
  const tables = (rows: string[]): number => {
    if (rows.length === 0) return 0;
    if (CODE.test(rows[0]!)) {
      const rest = rows.findIndex((row) => !CODE.test(row));
      return rest === -1 ? 0 : tables(rows.slice(rest));
    }
    const split = rows.findIndex((row) => HEADING.test(row));
    if (split !== -1) return tables(rows.slice(0, split)) + tables(rows.slice(split + 1));
    if (isTable(rows)) return 1;
    const rule = rows.findIndex((row) => RULE.test(row));
    return rule === -1 ? 0 : tables(rows.slice(0, rule)) + tables(rows.slice(rule + 1));
  };
  // Whether a block's first line opens a table: the block is one, or what
  // comes before its first rule, which Zulip splits off when it isn't.
  const startsTable = (rows: string[]): boolean => {
    if (isTable(rows)) return true;
    const rule = rows.findIndex((row) => RULE.test(row));
    return rule > 0 && startsTable(rows.slice(0, rule));
  };
  // The runs of a block that Zulip takes as paragraphs, lists or quotes,
  // with no heading in the block: what no table takes, split at rules,
  // past any indented start.
  const paragraphs = (rows: string[], from: number): [number, number][] => {
    if (rows.length === 0) return [];
    if (CODE.test(rows[0]!)) {
      const rest = rows.findIndex((row) => !CODE.test(row));
      return rest === -1 ? [] : paragraphs(rows.slice(rest), from + rest);
    }
    if (isTable(rows)) return [];
    const rule = rows.findIndex((row) => RULE.test(row));
    if (rule === -1) return [[from, from + rows.length]];
    return [...paragraphs(rows.slice(0, rule), from), ...paragraphs(rows.slice(rule + 1), from + rule + 1)];
  };
  const walk = (block: number[]): void => {
    if (block.length === 0) return;
    const rows = block.map((i) => lines[i]!);
    const heading = rows.findIndex((row) => HEADING.test(row));
    if (heading !== -1) {
      walk(block.slice(0, heading));
      walk(block.slice(heading + 1));
      return;
    }
    // The blank line goes inside a paragraph, above a line that then opens
    // a table, and only where Zulip then shows more tables: that table
    // takes the rest of the block as rows, a rule and any table after that
    // included.
    const shown = tables(rows);
    for (const [from, to] of paragraphs(rows, 0)) {
      for (let at = from + 1; at < to; at += 1) {
        if (LIST_OR_QUOTE.test(rows[at - 1]!)) break;
        const rest = rows.slice(at);
        if (CODE.test(rows[at]!) || !startsTable(rest)) continue;
        if (tables(rows.slice(0, at)) + tables(rest) > shown) {
          above.add(block[at]!);
          walk(block.slice(0, at));
          walk(block.slice(at));
          return;
        }
      }
    }
  };
  for (const block of blocks(lines)) walk(block);
  if (above.size === 0) return text;
  return raw.flatMap((line, i) => (above.has(i) ? ['', line] : [line])).join('\n');
}
