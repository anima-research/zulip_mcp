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
 * lines, outside every fence. A fence's lines belong to no block.
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
    if (top !== undefined) {
      if (line.trimEnd() === top.fence) stack.pop();
      else if (top.nests) {
        const m = FENCE.exec(line);
        if (m) stack.push({ fence: m[1]!, nests: NESTING.has((m[2] ?? '').toLowerCase()) });
      }
      return;
    }
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

/**
 * Puts one blank line above the header row of each table that sits directly
 * under a line of text, where Zulip's parser shows no table, and changes
 * nothing else.
 */
export function blankLineAboveTables(text: string): string {
  const raw = text.split('\n');
  const lines = raw.map((line) => expandTabs(line.replace(/\r$/, '')));
  const above = new Set<number>();
  for (const block of blocks(lines)) {
    const rows = block.map((i) => lines[i]!);
    if (!/^ {4}/.test(rows[0]!) && isTable(rows)) continue;
    for (let at = 1; at < rows.length; at += 1) {
      if (LIST_OR_QUOTE.test(rows[at - 1]!)) break;
      if (/^ {4}/.test(rows[at]!)) continue;
      if (isTable(rows.slice(at))) {
        above.add(block[at]!);
        break;
      }
    }
  }
  if (above.size === 0) return text;
  return raw.flatMap((line, i) => (above.has(i) ? ['', line] : [line])).join('\n');
}
