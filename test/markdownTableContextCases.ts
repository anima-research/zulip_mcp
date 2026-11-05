/** Paragraph-level safety cases, shared by helper, HTTP and rendering probes. */
export const contextTable = 'a|b\n-|-\nc|d';
export const contextTableCases: [string, string, string][] = [];
const refuse = (name: string, input: string) => contextTableCases.push([name, input, input]);
for (const token of ['*', '**', '_', '__', '***']) {
  const input = `${token}prefix\na|b\n-|-\nend${token}`;
  refuse('review 3 emphasis ' + token, input);
  refuse('review 3 emphasis CRLF ' + token, input.replaceAll('\n', '\r\n'));
}
for (const token of ['*', '**', '_', '__', '***', '~~', '`', '``', '[', '<', '$', '\\', '|', '!', '&', ']', '(', ')', '>']) {
  for (const gap of ['', 'Plain intervening line\n']) {
    for (const ending of ['\n', '\r\n']) {
      const input = `${token}prefix\n${gap}${contextTable}\n${gap}end${token}`.replaceAll('\n', ending);
      refuse(`paragraph delimiter ${JSON.stringify(token)} ${gap ? 'remote' : 'adjacent'} ${ending.length}`, input);
    }
  }
}
for (const token of ['*', '**', '_', '__', '***', '~~', '`', '``']) {
  for (const row of [0, 2]) {
    const rows = contextTable.split('\n');
    rows[row] = token + rows[row];
    refuse(`unbalanced table row ${token} ${row}`, `Results:\n${rows.join('\n')}\nDone.`);
    // Even total across rows is not sufficient: each line must balance.
    rows[2 - row] += token;
    refuse(`cross-row delimiter ${token} ${row}`, `Results:\n${rows.join('\n')}\nDone.`);
  }
}
for (const row of ['*a**|b*', '_a__|b_', '~a|b~', '~~~a|b~~~', String.raw`\*a|b*`]) {
  refuse('ambiguous delimiter runs', `Results:\n${row}\n-|-\nDone.`);
}
for (const token of ['*', '**', '_', '__', '***', '~~']) {
  const table = `${token}a${token}|b\n-|-\nc|${token}d${token}`;
  contextTableCases.push(['line-local balanced table delimiters ' + token,
    `Results:\n${table}\nDone.`, `Results:\n\n${table}\n\nDone.`]);
}
for (const prefix of ['Results:', 'Letters 123 . , ; ? \' " - / % # + = @', 'Status: all good.']) {
  const input = `${prefix}\n${contextTable}\nDone.`;
  contextTableCases.push(['plain agent report', input, `${prefix}\n\n${contextTable}\n\nDone.`]);
}
for (const token of ['*', '**', '_', '__', '***', '~~', '!', '&', '$', ']', '(', ')', '\\', '|']) {
  const input = `${token}separate paragraph${token}\n\nResults:\n${contextTable}\nDone.\n\n${token}another paragraph${token}`;
  contextTableCases.push(['paragraph-local, not message-wide ' + token, input,
    input.replace(`Results:\n${contextTable}\nDone.`, `Results:\n\n${contextTable}\n\nDone.`)]);
}
// A hazard anywhere in PARA refuses all candidates, including earlier paragraphs.
refuse('whole message refusal after earlier candidate', `Results:\n${contextTable}\nDone.\n\n*prefix\n${contextTable}\nend*`);
refuse('hazard beyond another candidate', `*prefix\n${contextTable}\n${contextTable}\nend*`);
refuse('pipe prose after table', `Results:\n${contextTable}\nAfter\nmore | prose`);

// Exact byte-preservation reproducers from reviews 1 and 2.
refuse("earlier review mismatch", "Intro\na|b\n---\nAfter");
refuse("earlier review escaped", "Intro\na\\|b\n---\nAfter");
refuse("earlier review code", "Intro\n`a|b`\n---\nAfter");
refuse("earlier review quote", "> quote\n| a | b |\n|-|-|\n| c | d |\nAfter");
refuse("earlier review fence", "````\n```\n| a | b |\n|-|-|\n| c | d |\nAfter\n````");
refuse("earlier review multiline-code", "prefix `\na|b\n-|-\nend`");
refuse("earlier review long-closer", "````\n`````\nIntro\na|b\n-|-\nAfter\n````");
refuse("earlier review html", "<pre>\na|b\n-|-\n</pre>");
refuse("earlier review math", "$$\na|b\n-|-\n$$");
refuse("earlier review frontmatter", "---\na|b\n---\n---");
refuse("earlier review link", "[label\na|b\n-|-\nend](https://example.invalid)");
refuse("earlier review unmatched-tick", "Intro\na|`b|c\n-|-|-\nAfter");
refuse("earlier review quoted", "> Intro\n> a|b\n> -|-\n> After");
refuse("earlier review list", "- Intro\n  a|b\n  -|-\n  After");
refuse("earlier review fence container", "````\n- ````\n\nIntro\na|b\n-|-\nAfter\n````");

// Review 5: block syntax on ANY candidate row, not only non-table prose.
for (const row of ['# a|b', '## a|b', '###### a|b', '- a|b', '+ a|b', '* a*|b',
  '1. a|b', '1) a|b', '> a|b', '~~~a|b', '```a|b', '- - -|b', '* * *|b', '_ _ _|b']) {
  for (const position of [0, 2]) {
    for (const ending of ['\n', '\r\n']) {
      const rows = contextTable.split('\n');
      rows[position] = row;
      refuse(`review 5 competing block ${position} ${JSON.stringify(row)} ${ending.length}`,
        `Prelude\n${rows.join('\n')}\nTail`.replaceAll('\n', ending));
    }
  }
}
// Unsupported whitespace/control anywhere refuses the whole message, even if
// blank-separated or in an otherwise eligible cell/fence. Unicode letters stay eligible.
for (const whitespace of ['\u00a0', '\u2003', '\f', '\v', '\u0085', '\u1680', '\u2028',
  '\u2029', '\u202f', '\u205f', '\u3000', '\ufeff', '\u0000', '\u0002', '\u0003', '\u001c', '\u007f']) {
  for (const input of [`${whitespace}\n${contextTable}\nTail`,
    `Prelude\n${contextTable}\n${whitespace}`, `Prelude\n${contextTable}${whitespace}\nTail`,
    `${whitespace}\n\nPrelude\n${contextTable}\nTail`]) {
    refuse(`review 5 unsupported whitespace/control ${JSON.stringify(whitespace)}`, input);
    refuse(`review 5 unsupported whitespace/control CRLF ${JSON.stringify(whitespace)}`, input.replaceAll('\n', '\r\n'));
  }
}
for (const input of [`Prelude\r${contextTable.replaceAll('\n', '\r')}\rTail`,
  `Prelude\n${contextTable}\n\r`, `Prelude\na|b\rX\n-|-\nc|d\nTail`]) {
  refuse('review 5 bare CR is unsupported', input);
}
