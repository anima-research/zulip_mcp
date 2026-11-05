import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeMarkdownTables } from '../src/markdown-tables.ts';
import { reviewTableCases } from './markdownTableCases.ts';

const table = '| Name | Value |\n|---|:--:|\n| a | b |';
const cases: [string, string, string][] = [
  ...reviewTableCases,
  ['experiment: missing before', `Intro\n${table}\n\nAfter`, `Intro\n\n${table}\n\nAfter`],
  ['experiment: missing after', `Intro\n\n${table}\nAfter`, `Intro\n\n${table}\n\nAfter`],
  ['experiment: correct on both sides', `Intro\n\n${table}\n\nAfter`, `Intro\n\n${table}\n\nAfter`],
  ['missing both', `Intro\n${table}\nAfter`, `Intro\n\n${table}\n\nAfter`],
  ['at start', `${table}\nAfter`, `${table}\n\nAfter`],
  ['at end', `Intro\n${table}`, `Intro\n\n${table}`],
  ['whole message', table, table],
  ['terminal newline', `Intro\n${table}\n`, `Intro\n\n${table}\n`],
  ['single column without delimiter pipes is not a table', 'Intro\n| a |\n:---:\nAfter', 'Intro\n| a |\n:---:\nAfter'],
  ['single column with optional trailing pipe', 'Intro\n| a |\n---|\nAfter', 'Intro\n\n| a |\n---|\n\nAfter'],
  ['no body rows', 'Intro\na|b\n---|---\nAfter', 'Intro\n\na|b\n---|---\n\nAfter'],
  ['adjacent tables', `${table}\n${table}`, `${table}\n\n${table}`],
  ['separated tables', `${table}\n\n${table}`, `${table}\n\n${table}`],
  ['adjacent list refuses', `${table}\n- next`, `${table}\n- next`],
  ['heading follows refuses', `${table}\n# Next`, `${table}\n# Next`],
  ['pipe prose elsewhere in paragraph refuses', `${table}\nAfter\nmore | prose`, `${table}\nAfter\nmore | prose`],
  ['blank ends block', `${table}\n\nmore | prose`, `${table}\n\nmore | prose`],
  ['existing whitespace blanks', `Intro\n \t\n${table}\n \nAfter`, `Intro\n \t\n${table}\n \nAfter`],
  ['inline code in cells refuses', 'Intro\n| `a | b` | c |\n|--|--|\n| d | e |\nAfter', 'Intro\n| `a | b` | c |\n|--|--|\n| d | e |\nAfter'],
  ['CRLF', `Intro\r\n${table.replaceAll('\n', '\r\n')}\r\nAfter\r\n`, `Intro\r\n\r\n${table.replaceAll('\n', '\r\n')}\r\n\r\nAfter\r\n`],
  ['mixed line endings use boundary ending', `Intro\r\n${table}\nAfter`, `Intro\r\n\r\n${table}\n\nAfter`],
];
for (const delimiter of ['---|---', '|---|:--:|', ' :-- | --: ', '| :--- | ---: |', '|-|-|']) {
  cases.push([`delimiter ${delimiter}`, `Intro\n|a|b|\n${delimiter}\nAfter`, delimiter.startsWith(' ') ? `Intro\n|a|b|\n${delimiter}\nAfter` : `Intro\n\n|a|b|\n${delimiter}\n\nAfter`]);
}

for (const content of [
  '', 'no table', '  unchanged \r\n\t\n💬\n', 'a | b\nnot a delimiter',
  'Intro\n---|---\nAfter', 'Intro\n|---|:--:|\nAfter',
  'a|b\n---|not---', 'a|b\n---||---',
  'a|b\n---|--- extra', 'no pipe header\n---',
  '`a|b`\nordinary prose', 'Intro\n`|---|---|`\nAfter',
  `> Intro\n${table.split('\n').map(line => `> ${line}`).join('\n')}\n> After`,
  `- Intro\n${table.split('\n').map(line => `  ${line}`).join('\n')}\n  After`,
  `1. Intro\n${table.split('\n').map(line => `   ${line}`).join('\n')}`,
  `- Intro\n${table}`, `> Intro\n${table}`,
  `Intro\n${table.split('\n').map(line => `    ${line}`).join('\n')}\nAfter`,
]) cases.push(['no-table/container byte parity', content, content]);

for (const [open, close] of [
  ['```', '```'], ['~~~ts', '~~~'], ['~~~~~~~~~~lang', '~~~~~~~~~~'], ['````markdown', '````'],
  ['~~~~~ language | options', '~~~~~~'], ['```spoiler details', '```'],
  ['  ```js', '  ```'], ['> ~~~md', '> ~~~'], ['- > ````md', '  > ````'],
]) {
  const fenced = `${open}\nIntro\n${table}\nAfter\n${close}`;
  cases.push(['fenced unchanged', fenced, fenced]);
  const crlf = fenced.replaceAll('\n', '\r\n');
  cases.push(['CRLF fenced unchanged', crlf, crlf]);
  const gap = /^[>-]/.test(open) ? '\n\n' : '\n';
  const refused = /^[ >-]/.test(open) || ['~~~~~ language | options', '```spoiler details'].includes(open);
  cases.push(['table after fence', `${fenced}${gap}${table}\nAfter`,
    refused ? `${fenced}${gap}${table}\nAfter` : `${fenced}\n\n${table}\n\nAfter`]);
}
for (const open of ['```js', '~~~', '`````md']) {
  const input = `Intro\n${open}\n${table}\nAfter`;
  cases.push(['unclosed fence', input, input]);
}
for (const close of ['```', '~~~~', '```` trailing-info', '```\n~~~']) {
  const input = `Intro\n\`\`\`\`md\n${close}\n${table}\nAfter`;
  cases.push(['short/mismatched/invalid closer does not close', input, input]);
}
const fencedPipe = `\`\`\`md\n${table}\n\`\`\``;
cases.push(['table followed by fenced code', `${table}\n${fencedPipe}`, `${table}\n\n${fencedPipe}`]);
for (const [name, input, expected] of cases) {
  test(name, () => {
    const actual = normalizeMarkdownTables(input);
    assert.equal(actual, expected);
    assert.equal(normalizeMarkdownTables(actual), actual, 'idempotent');
  });
}
