/** Escaped pipes and backslash pairs follow Python-Markdown TableProcessor._split.
 * Backticks outside complete fences are refused before this is used.
 */
function tablePipes(row: string): number[] {
  const pipes: number[] = [];
  for (let i = 0; i < row.length; i++) {
    if (row[i] === '\\') i++; // Escaped pipe or backslash pair.
    else if (row[i] === '|') pipes.push(i);
  }
  return pipes;
}

/** Pair exact delimiter runs on one row, never across rows or paragraphs. */
function balancedRow(row: string): boolean {
  const runs = row.match(/\*+|_+|~+|`+/g);
  if (!runs) return true;
  // Escaped delimiters and non-strikethrough tilde runs are ambiguous.
  if (row.includes('\\')) return false;
  const odd = new Set<string>();
  for (const run of runs) {
    if (run[0] === '~' && run !== '~~') return false;
    if (odd.has(run)) odd.delete(run);
    else odd.add(run);
  }
  return odd.size === 0;
}

/** Competing block syntax must not become a header/body just because it has pipes. */
function competingBlock(row: string): boolean {
  return /^(?:#{1,6}(?:[ \t]|$)|(?:[-+*]|\d+[.)])[ \t]+|>|`{3,}|~{3,})/.test(row) ||
    /^(?:[-*_][ \t]*){3,}$/.test(row);
}

// NormalizeWhitespace expands tabs and removes space-only lines before the
// BlockParser splits on literal "\n\n". Other whitespace is NOT a boundary.
const blankLine = (line: string): boolean => /^[ \t]*$/.test(line);

/** Add only missing boundary blanks in simple messages; otherwise byte identity. */
export function normalizeMarkdownTables(content: string): string {
  // Refuse unsupported whitespace/control bytes rather than model their
  // normalization (STX/ETX deletion, bare-CR newlines, Unicode non-boundaries).
  // ASCII tabs and LF/CRLF are supported; ordinary Unicode text remains safe.
  if (/[^\S \t\r\n]|[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f-\u009f]|\r(?!\n)/u.test(content)) return content;
  const lines = content.split(/(?<=\n)/);
  const text = lines.map(line => line.replace(/\r?\n$/, ''));
  const protectedLines: boolean[] = [];
  let fence: { char: string; length: number } | undefined;
  for (let i = 0; i < text.length; i++) {
    const line = text[i];
    const marker = line.match(/^([ \t]*)(`{3,}|~{3,})(.*)$/);
    if (fence) {
      protectedLines[i] = true;
      if (marker) {
        // Zulip closes only at column zero with the exact fence run.
        // Refuse other marker-like lines conservatively.
        if (marker[1] !== '' || marker[2][0] !== fence.char ||
            marker[2].length !== fence.length || !/^[ \t]*$/.test(marker[3])) return content;
        fence = undefined;
      }
    } else if (marker && marker[1] === '' &&
        /^ *(?:[A-Za-z][A-Za-z0-9]*)? *$/.test(marker[3])) {
      protectedLines[i] = true;
      fence = { char: marker[2][0], length: marker[2].length };
    } else {
      if (marker) return content; // Zulip only opens/closes fences at column zero.
      protectedLines[i] = false;
      if (/[`\[<]|\$\$/.test(line) || /^[ \t]*>/.test(line)) return content;
    }
  }
  if (fence) return content;
  const containerLine = (i: number): boolean => i >= 0 && i < text.length &&
    !protectedLines[i] && !blankLine(text[i]) &&
    /^(?:[ \t]+|(?:[-+*]|\d+[.)])[ \t]+)/.test(text[i]);

  const detectColumns = (i: number): number => {
    if (i + 1 >= text.length || protectedLines[i] || protectedLines[i + 1] || /^[ \t]/.test(text[i])) return 0;
    // Check separator characters before splitting/counting the header. Most
    // body lines cannot start another table; avoid reparsing them expensively.
    const separator = text[i + 1].replace(/^ +| +$/g, '');
    if (!separator.includes('|') || !/^[|: \-]*$/.test(separator)) return 0;
    const row = text[i].replace(/ +$/, '');
    const pipes = tablePipes(row);
    if (!pipes.length) return 0;
    const border = pipes[0] === 0 || pipes.at(-1) === row.length - 1;
    const count = (value: string, positions: number[]) => Math.max(1,
      positions.length + 1 - (border && value.startsWith('|') ? 1 : 0) -
      (border && positions.at(-1) === value.length - 1 ? 1 : 0));
    const columns = count(row, pipes);
    // TableProcessor.test checks a character set, not GFM's dash grammar.
    return count(separator, tablePipes(separator)) === columns ? columns : 0;
  };
  const columnCache = new Uint32Array(text.length);
  const headerColumns = (i: number): number => {
    if (columnCache[i]) return columnCache[i] - 1;
    const columns = detectColumns(i);
    columnCache[i] = columns + 1;
    return columns;
  };
  const blocks: { start: number; end: number }[] = [];
  const tableRows = new Uint8Array(text.length);
  for (let i = 0; i < text.length; i++) {
    const columns = headerColumns(i);
    if (!columns) continue;
    let uncertain = false;
    let end = i + 2;
    while (end < text.length && !protectedLines[end] && !blankLine(text[end]) && tablePipes(text[end]).length) {
      if (headerColumns(end)) break; // Adjacent tables need a boundary, too.
      // Python-Markdown requires a border on EVERY row of a one-column table.
      const row = text[end].replace(/ +$/, '');
      if (columns === 1 && !row.startsWith('|') && tablePipes(row).at(-1) !== row.length - 1) {
        uncertain = true;
        break;
      }
      end++;
    }
    // Refuse the WHOLE message when a candidate touches a list/indented
    // line (including its separator/body). No container parsing is attempted.
    for (let j = i - 1; j <= end; j++) if (containerLine(j)) return content;
    if (uncertain) continue;
    blocks.push({ start: i, end });
    for (let j = i; j < end; j++) {
      if (competingBlock(text[j]) || !balancedRow(text[j])) return content;
      tableRows[j] = 1;
    }
    i = end - 1;
  }
  if (!blocks.length) return content;
  // Classify each maximal nonblank run ONCE, not once per candidate. Remote
  // list/indent lines are just as unsafe as immediate neighbors (lazy lists).
  for (let i = 0; i < text.length;) {
    if (protectedLines[i] || blankLine(text[i])) { i++; continue; }
    let hasTable = false;
    let unsafe = false;
    do {
      if (tableRows[i]) hasTable = true;
      else if (containerLine(i) || /[*_~`\[\]()<>$\\|!&]/.test(text[i]) ||
          competingBlock(text[i]) || /[ \t]$/.test(text[i])) unsafe = true;
      i++;
    } while (i < text.length && !protectedLines[i] && !blankLine(text[i]));
    if (hasTable && unsafe) return content;
  }
  const insertBefore = new Uint8Array(text.length);
  for (const { start, end } of blocks) {
    if (start > 0 && !blankLine(text[start - 1])) insertBefore[start] = 1;
    if (end < text.length && !blankLine(text[end])) insertBefore[end] = 1;
  }
  return lines.map((line, i) => (insertBefore[i]
    ? (lines[i - 1].endsWith('\r\n') ? '\r\n' : '\n') : '') + line).join('');
}
