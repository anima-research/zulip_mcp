/*
 * FENCE and the fence rule in blocks are ported from Zulip 12.4,
 * zerver/lib/markdown/fenced_code.py (FENCE_RE, generic_handler and the
 * handlers' handle_line), and HEADING from its HashHeaderProcessor.RE
 * (zerver/lib/markdown/__init__.py). Its notice:
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
 * isTable, splitRow and split are ported from Python-Markdown 3.10.3,
 * markdown/extensions/tables.py (TableProcessor.test, _split_row, _split),
 * the version Zulip 12.4 pins, and RULE from its HRProcessor.RE
 * (markdown/blockprocessors.py). Its licence:
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

/**
 * TableProcessor.test for the block of lines `from` to `to`, reading only
 * the lines it needs. It checks the delimiter row before a one-column
 * table's rows, which gives the same answer. `nextUnbordered` gives, for
 * each line, the next line at or after it with no pipe at either end.
 */
function isTable(lines: string[], from: number, to: number, nextUnbordered: number[]): boolean {
  if (to - from < 2) return false;
  const row = (i: number) => lines[i]!.replace(/^ +| +$/g, '');
  const header = row(from);
  const border = header.startsWith('|') || RE_END_BORDER.test(header);
  const width = splitRow(header, border).length;
  const separator = splitRow(row(from + 1), border);
  if (separator.length !== width || !/^[|:\- ]*$/.test(separator.join(''))) return false;
  if (width > 1) return true;
  return border && nextUnbordered[from + 1]! >= to;
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
  // Where each run length occurs, in order, so that the next run of a
  // length is found without searching the runs between.
  const at = new Map<number, number[]>();
  tics.forEach((size, i) => {
    const runs = at.get(size);
    if (runs) runs.push(i);
    else at.set(size, [i]);
  });
  const seen = new Map<number, number>();
  const regions: [number, number][] = [];
  for (let pos = 0; pos < tics.length; ) {
    const size = tics[pos]! - ticPoints[pos]![2];
    const runs = at.get(size) ?? [];
    let next = seen.get(size) ?? 0;
    while (next < runs.length && runs[next]! <= pos) next += 1;
    seen.set(size, next);
    if (size === 0 || next === runs.length) {
      pos += 1;
      continue;
    }
    const close = runs[next]!;
    regions.push([ticPoints[pos]![0], ticPoints[close]![1]]);
    pos = close + 1;
  }
  const cells: string[] = [];
  let start = 0;
  let region = 0;
  for (const pipe of pipes) {
    while (region < regions.length && regions[region]![1] < pipe) region += 1;
    if (region < regions.length && regions[region]![0] <= pipe) continue;
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
  // The next heading, the next rule and the next line with no pipe at
  // either end, at or after each line, so that no reading of a block
  // searches its lines again.
  const nextHeading = new Array<number>(lines.length + 1).fill(lines.length);
  const nextRule = new Array<number>(lines.length + 1).fill(lines.length);
  const nextUnbordered = new Array<number>(lines.length + 1).fill(lines.length);
  for (let i = lines.length - 1; i >= 0; i -= 1) {
    const row = lines[i]!.replace(/^ +| +$/g, '');
    nextHeading[i] = HEADING.test(lines[i]!) ? i : nextHeading[i + 1]!;
    nextRule[i] = RULE.test(lines[i]!) ? i : nextRule[i + 1]!;
    nextUnbordered[i] = row.startsWith('|') || RE_END_BORDER.test(row) ? nextUnbordered[i + 1]! : i;
  }
  const table = (from: number, to: number): boolean => isTable(lines, from, to, nextUnbordered);
  const pastCode = (from: number, to: number): number => {
    while (from < to && CODE.test(lines[from]!)) from += 1;
    return from;
  };
  // Zulip takes the lines from `from` to `to` of one block with the first of
  // its block processors that wants them: an indented start (code), and the
  // lines after it as a block of their own; a heading, which splits the
  // block; a table, which takes the whole block; a rule, which splits what
  // no table took. What none of them takes is a paragraph, a list or a quote.
  // Each reading below follows that order, with a list of the blocks still
  // to read in place of recursion, since a block can hold thousands of
  // headings or rules.

  // How many tables Zulip shows for those lines.
  const tables = (from: number, to: number): number => {
    let count = 0;
    const todo: [number, number][] = [[from, to]];
    while (todo.length > 0) {
      const [start, end] = todo.pop()!;
      const a = pastCode(start, end);
      if (a >= end) continue;
      const heading = nextHeading[a]!;
      if (heading < end) todo.push([a, heading], [heading + 1, end]);
      else if (table(a, end)) count += 1;
      else if (nextRule[a]! < end) todo.push([a, nextRule[a]!], [nextRule[a]! + 1, end]);
    }
    return count;
  };
  // The runs of those lines, with no heading among them, that Zulip takes as
  // paragraphs, lists or quotes, in order.
  const paragraphs = (from: number, to: number): [number, number][] => {
    const out: [number, number][] = [];
    const todo: [number, number][] = [[from, to]];
    while (todo.length > 0) {
      const [start, end] = todo.pop()!;
      const a = pastCode(start, end);
      if (a >= end || table(a, end)) continue;
      const rule = nextRule[a]!;
      if (rule < end) todo.push([rule + 1, end], [a, rule]);
      else out.push([a, end]);
    }
    return out;
  };
  // Whether the line `at` opens a table in a block that runs to `to`: the
  // block is one, or what comes before its first rule, which Zulip splits
  // off when it isn't.
  const startsTable = (at: number, to: number): boolean => {
    const rule = nextRule[at]!;
    return table(at, to) || (rule > at && rule < to && table(at, rule));
  };
  const above = new Set<number>();
  const todo: [number, number][] = blocks(lines).map((block) => [block[0]!, block.at(-1)! + 1]);
  while (todo.length > 0) {
    const [from, to] = todo.pop()!;
    const heading = nextHeading[from]!;
    if (heading < to) {
      todo.push([from, heading], [heading + 1, to]);
      continue;
    }
    // The blank line goes inside a paragraph, above a line that then opens
    // a table, and only where Zulip then shows more tables: that table
    // takes the rest of the block as rows, a rule and any table after that
    // included. The lines above it and the lines from it are then read as
    // blocks of their own.
    const runs = paragraphs(from, to);
    if (runs.length === 0) continue;
    const shown = tables(from, to);
    search: for (const [start, end] of runs) {
      for (let at = start + 1; at < end; at += 1) {
        if (LIST_OR_QUOTE.test(lines[at - 1]!)) break;
        if (CODE.test(lines[at]!) || !startsTable(at, to)) continue;
        if (tables(from, at) + tables(at, to) > shown) {
          above.add(at);
          todo.push([from, at], [at, to]);
          break search;
        }
      }
    }
  }
  if (above.size === 0) return text;
  return raw.flatMap((line, i) => (above.has(i) ? ['', line] : [line])).join('\n');
}
