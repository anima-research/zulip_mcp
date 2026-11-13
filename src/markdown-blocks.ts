/*
 * FENCE and the fence rule in markdownBlocks are ported from Zulip 12.4,
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
 * RULE is Python-Markdown 3.10.3's HRProcessor.RE (markdown/blockprocessors.py).
 * Its licence:
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

/** Zulip 12.4's FENCE_RE (zerver/lib/markdown/fenced_code.py), with PLAYGROUND_LANGUAGE_REGEX in place. */
const FENCE = /^(~{3,}|`{3,})[ ]*(?:\{?\.?([a-zA-Z0-9_+-.\/#]+)[ ]*([^ ~`][^~`]*)?\}?)?$/;
const NESTING = new Set(['quote', 'quoted', 'spoiler']);

/** Zulip 12.4's HashHeaderProcessor.RE (zerver/lib/markdown/__init__.py), for one line. */
export const HEADING = /^#{1,6}\s/;
/** Python-Markdown 3.10.3's HRProcessor.RE (markdown/blockprocessors.py). */
export const RULE = /^[ ]{0,3}(?=((?:-+[ ]{0,2}){3,}|(?:_+[ ]{0,2}){3,}|(?:\*+[ ]{0,2}){3,}))\1[ ]*$/;
export const CODE = /^ {4}/;

export function nextMatching(lines: string[], pattern: RegExp): number[] {
  const next = new Array<number>(lines.length + 1).fill(lines.length);
  for (let i = lines.length - 1; i >= 0; i -= 1) next[i] = pattern.test(lines[i]!) ? i : next[i + 1]!;
  return next;
}

export function expandTabs(line: string): string {
  let out = '';
  for (const ch of line) out += ch === '\t' ? ' '.repeat(4 - (out.length % 4)) : ch;
  return out;
}

/**
 * The runs of lines that Zulip's parser reads as Markdown blocks, by line
 * number: lines between blank lines and fence lines, outside every code and
 * math fence. The inside of a quote, quoted or spoiler fence is Markdown
 * too, apart from a fence nested in it. A fence's own lines, and a code or
 * math fence's inside, belong to no block.
 */
export function markdownBlocks(lines: string[]): number[][] {
  const out: number[][] = [];
  let current: number[] = [];
  const stack: { fence: string; nests: boolean }[] = [];
  const end = () => {
    if (current.length > 0) out.push(current);
    current = [];
  };
  const open = (m: RegExpExecArray) => {
    end();
    stack.push({ fence: m[1]!, nests: NESTING.has((m[2] ?? '').toLowerCase()) });
  };
  lines.forEach((line, i) => {
    const top = stack.at(-1);
    if (top !== undefined) {
      if (line.trimEnd() === top.fence) {
        end();
        stack.pop();
        return;
      }
      if (!top.nests) return;
    }
    const m = FENCE.exec(line);
    if (m) open(m);
    else if (/^ *$/.test(line)) end();
    else current.push(i);
  });
  end();
  return out;
}
