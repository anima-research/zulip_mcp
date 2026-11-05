/** Zulip inline parsing stops at ASCII blank paragraphs and complete fences. */
const table = 'a|b\n-|-\nc|d';
const report = `Results:\n${table}\nDone.`;
const normalized = `Results:\n\n${table}\n\nDone.`;
export const inlineTableCases: [string, string, string][] = [];
for (const syntax of [
  '`npm test`', '``literal ` tick``', '[docs](https://example.invalid/docs)',
  '[message](https://example.invalid/#narrow/near/123)',
  'Use `multi\nline` here', '[multi\nline](https://example.invalid)',
  '[label](https://example.invalid/\npath)', '[label][ref]\n\n[ref]: https://example.invalid',
  '`unclosed', '[unclosed', 'escaped \\`tick and \\[bracket',
]) {
  for (const gap of ['\n\n', '\n \t\n', '\n~~~text\nopaque\n~~~\n']) {
    for (const input of [`${syntax}${gap}${report}`, `${report}${gap}${syntax}`]) {
      for (const ending of ['\n', '\r\n']) {
        inlineTableCases.push(['separate inline paragraph ' + JSON.stringify([syntax, gap, ending]),
          input.replaceAll('\n', ending), input.replace(report, normalized).replaceAll('\n', ending)]);
      }
    }
  }
}
for (const [open, close] of [['`open', 'close`'], ['``open', 'close``'],
  ['[open', 'close](https://example.invalid)'], ['[open](', 'https://example.invalid)']]) {
  for (const gap of ['\n\n', '\n \t\n', '\n~~~\nopaque\n~~~\n']) {
    const input = `${open}${gap}${report}${gap}${close}`;
    inlineTableCases.push(['blank/fence interrupts span ' + JSON.stringify([open, gap]), input,
      input.replace(report, normalized)]);
  }
  for (const input of [`${open}\n${table}\n${close}`,
    `${open}\nintervening\n${table}\nDone.\n\n${close}`,
    `${open}\n\nResults:\n${table}\nintervening\n${close}`]) {
    inlineTableCases.push(['span touches table paragraph ' + open, input, input]);
  }
}
// Balanced inline syntax on table rows must remain refused too.
for (const row of ['`a`|b', '[a](https://example.invalid)|b', '[a]|b']) {
  for (const position of [0, 2]) {
    const rows = table.split('\n');
    rows[position] = row;
    const input = `Results:\n${rows.join('\n')}\nDone.`;
    inlineTableCases.push(['inline table row ' + row, input, input]);
  }
}
for (const syntax of ['$$math$$', '<html>', '> quote']) {
  const input = `${syntax}\n\n${report}`;
  inlineTableCases.push(['global refusal unchanged ' + syntax, input, input]);
}
