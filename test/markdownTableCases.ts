/** Shared helper and real send/edit HTTP regressions from both reviews. */
import { contextTableCases } from './markdownTableContextCases.ts';
const table = '| a | b |\n|-|-|\n| c | d |';
export const reviewTableCases: [string, string, string][] = [...contextTableCases];
for (const header of ['a|b', String.raw`a\|b`, '`a|b`']) {
  const input = `Intro\n${header}\n---\nAfter`;
  reviewTableCases.push([`setext, not a table: ${header}`, input, input]);
  const bare = `${header}\n---`;
  reviewTableCases.push([`bare setext: ${header}`, bare, bare]);
}
for (const separator of [':|:', '--- ---|---', '-|-', '::---|---', '---|']) {
  reviewTableCases.push([`Python-Markdown separator ${separator}`,
    `Intro\na|b\n${separator}\nAfter`, `Intro\n\na|b\n${separator}\n\nAfter`]);
}
for (const marker of ['- ````', '> ````', '    ````', '\t````', '````\u00a0', '````\v', '```', '~~~~', '```` trailing-info']) {
  const input = `\`\`\`\`\n${marker}\n\nIntro\na|b\n-|-\nAfter\n\`\`\`\``;
  reviewTableCases.push([`not a top-level fence closer: ${marker}`, input, input]);
}
for (const prefix of ['- item\n# Heading', '> quote\n# Heading', '- item\n***', '> quote\n---', '- item\n* * *', '> quote\n_ _ _', '- item\n', '> quote\n', '# Heading']) {
  reviewTableCases.push([`top-level block after container: ${prefix}`,
    `${prefix}\n${table}\nAfter`, (prefix.startsWith('>') || /[*_#]/.test(prefix)) ? `${prefix}\n${table}\nAfter` :
      `${prefix}${prefix.endsWith('\n') ? '\n' : '\n\n'}${table}\n\nAfter`]);
}
for (const prefix of ['> quote', '- item']) {
  const input = `${prefix}\n${table}\nAfter`;
  reviewTableCases.push(['lazy container continuation', input, input]);
}
const mixed = `Intro\r\n${table}\r\nAfter`;
reviewTableCases.push(['mixed endings', mixed, `Intro\r\n\r\n${table}\r\n\r\nAfter`]);
const fence = `\`\`\`\`\n\`\`\`\n${table}\nAfter\n\`\`\`\``;
reviewTableCases.push(['shorter fence', fence, fence]);
for (const [header, separator] of [
  [String.raw`a\|b`, '-|-'], ['`a|b`', '-|-'], ['a|b|c', '-|-'],
  ['a|`b|c`', '-|-'], [String.raw`a\\|b`, '-|-'],
  ['a|``b`|c``', '-|-'], ['a|`b|c', '-|-|-'],
  ['| a |', '|:|'], ['|a|', '|'], ['a|b', '|-|-|'],
]) {
  const input = `Intro\n${header}\n${separator}\nAfter`;
  const valid = [String.raw`a\\|b`, '| a |', '|a|'].includes(header);
  reviewTableCases.push([`escape/code/border columns: ${header}`, input,
    valid ? `Intro\n\n${header}\n${separator}\n\nAfter` : input]);
}

for (const input of [
  '- item\n  # Nested heading\n' + table + '\nAfter',
  '- item\n\n    continuation\n' + table + '\nAfter',
  '> ````\n````\n\nIntro\na|b\n-|-\nAfter\n> ````',
  'Intro\n|a|\n|-|\nb|c\nAfter',
]) reviewTableCases.push(['uncertain nested/one-column block', input, input]);
for (const close of ['````', ' `````', '   ````']) {
  const fenced = '- item\n````\n' + table + '\n' + close;
  reviewTableCases.push(['top-level closer after lazy container', fenced + '\n' + table + '\nAfter',
    close !== '````' ? fenced + '\n' + table + '\nAfter' :
      fenced + '\n\n' + table + '\n\nAfter']);
}

const resolved = 'a|http://127.0.0.1/#narrow/stream/1-a.7Cb/topic/x.7Cy/near/1\n-|-';
reviewTableCases.push(['resolved stream/topic pipes', `Intro\n${resolved}\nAfter`, `Intro\n\n${resolved}\n\nAfter`]);

for (const separator of ['-|-', ':|:', '--- ---|---']) {
  const input = `a|b\n${separator}`;
  reviewTableCases.push(['whole Python-Markdown table: ' + separator, input, input]);
}

for (const [name, input, expected] of reviewTableCases.slice()) {
  if (name === 'not a top-level fence closer: - ````' || name === 'setext, not a table: a|b' ||
      name === 'top-level block after container: - item\n# Heading') {
    reviewTableCases.push(['CRLF ' + name, input.replaceAll('\n', '\r\n'), expected.replaceAll('\n', '\r\n')]);
  }
}

/** Cautious whole-message gate: review 2 and complex-context refusals. */
const cautiousRefusals = [
  `A single $ and ] and inline > now refuse within PARA\n${table}\nAfter`,
  'prefix `\na|b\n-|-\nend`',
  '[label\na|b\n-|-\nend](https://example.invalid)',
  '````\n`````\nIntro\na|b\n-|-\nAfter\n````',
  '<pre>\na|b\n-|-\n</pre>',
  '$$\na|b\n-|-\n$$',
  ...['`literal`', '\\`', '[ordinary', String.raw`\[escaped`, '$$', '<', '  > quote'].map(prefix => `${prefix}\n\nIntro\n${table}\nAfter`),
  ...['- item', '+ item', '* item', '1. item', '2) item', '    indented', '\tindented', ' indented'].flatMap(line => [
    `${line}\n${table}\nAfter`, `Intro\n${table}\n${line}`,
    `Intro\n${table}\n${line} | row\nAfter`,
  ]),
  // A refusal protects all tables, not only the one touching the hazard.
  `Intro\n${table}\nAfter\n\n${table}\n- item`,
  `Intro\n${table}\nAfter\n\n~~~\nunclosed`,
  ...['~~~~', '~~~~~', '~~~ trailing', '```', '    ~~~'].map(close =>
    `Intro\n${table}\nAfter\n\n~~~\n${close}\n~~~`),
];
for (const input of cautiousRefusals.slice()) cautiousRefusals.push(input.replaceAll('\n', '\r\n'));
for (const input of cautiousRefusals) {
  const separateInline = ['`literal`', '\\`', '[ordinary', String.raw`\[escaped`].some(prefix => input.startsWith(prefix));
  const ending = input.includes('\r\n') ? '\r\n' : '\n';
  reviewTableCases.push([separateInline ? 'separate inline paragraph now eligible' : 'cautious gate byte identity', input,
    separateInline ? input.replace(`Intro${ending}`, `Intro${ending}${ending}`).replace(`${ending}After`, `${ending}${ending}After`) : input]);
}

const report = `# Report\n\nSummary\n${table}\nDone\n\n- Separate bullet\n\n\`\`\`text\n\` [ $$ < >\n${table}\n\`\`\``;
reviewTableCases.push(['typical agent report with separate bullets and fence', report,
  report.replace(`Summary\n${table}\nDone`, `Summary\n\n${table}\n\nDone`)]);

for (const prefix of ['- bullet\n\nParagraph',
  '~~~text\n~~\n~~~\n\nParagraph']) {
  const input = `${prefix}\n${table}\nAfter`;
  reviewTableCases.push(['simple report remains eligible', input, `${prefix}\n\n${table}\n\nAfter`]);
}
for (const char of ['`', '~']) for (const length of [3, 4, 7]) {
  const marker = char.repeat(length);
  const input = `Intro\n${table}\nAfter\n\n${marker}info\n[ $$ < > \`\n${marker} \t`;
  reviewTableCases.push(['exact-length complete fence remains eligible', input,
    input.replace(`Intro\n${table}\nAfter`, `Intro\n\n${table}\n\nAfter`)]);
}

// Zulip-oracle regressions: remote containers and newly active blocks are unsafe.
for (const prefix of ['1. item\ncontinuation', '+ item\ncontinuation', '- item\ncontinuation']) {
  for (const ending of ['\n', '\r\n']) {
    const input = `${prefix}\n${table}\nAfter`.replaceAll('\n', ending);
    reviewTableCases.push(['remote lazy list: preserve membership', input, input]);
  }
}
for (const suffix of ['Heading\n---', '# Heading', '---', 'Done.  \nMore']) {
  for (const prefix of ['Intro\n', '']) for (const ending of ['\n', '\r\n']) {
    const input = `${prefix}${table}\n${suffix}`.replaceAll('\n', ending);
    reviewTableCases.push(['Zulip: preserve non-table block/whitespace', input, input]);
  }
}
for (const ending of ['\n', '\r\n']) {
  const input = `Intro  \n${table}\nAfter`.replaceAll('\n', ending);
  reviewTableCases.push(['two-space boundary cautiously refuses', input, input]);
  const setext = `Intro\n${table}\nHeading\n===`.replaceAll('\n', ending);
  reviewTableCases.push(['Zulip disables setext equals headings', setext,
    `Intro\n\n${table}\n\nHeading\n===`.replaceAll('\n', ending)]);
}

for (const open of ['~~~!', '~~~text`', '~~~text~', '~~~ invalid~', '~~~\ttext']) {
  const input = `${open}\nopaque\n~~~\nIntro\na|b\n-|-\nAfter`;
  reviewTableCases.push(['invalid opener must not expose later code', input, input]);
}
