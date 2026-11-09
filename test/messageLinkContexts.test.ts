/** Conservative message-wide rendering policy; no external Markdown oracle. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveMessageLinks } from '../src/message-links.ts';
const token = '[[msg:101]]';
const url = 'https://example.invalid/#narrow/channel/7-Secret/topic/Plans/near/101';
const label = `[#Secret > Plans @ 💬](${url})`;
const session = {realm:'https://example.invalid',client:{messages:{async getById(){return {result:'success',message:{id:101,type:'stream',stream_id:7,display_recipient:'Secret',subject:'Plans'}};}}}} as any;
// earlier 28 regressions: each contains a backtick, fence or indented line, so all use bare URLs.
for (const [name, input] of [
  ["nested tilde code in backtick spoiler", "````spoiler title\n~~~python\nTOKEN\n~~~\n````"],
  ["quote inside spoiler then prose", "~~~~spoiler title\n```quote\nx\n```\n~~~~\nTOKEN"],
  ["code after nested quote before spoiler close", "~~~~spoiler title\n```quote\nx\n```\n~~~\nTOKEN\n~~~\n~~~~"],
  ["tilde quote with backtick code", "~~~quote\n````python\nTOKEN\n````\n~~~"],
  ["different length unmatched runs", "`` before TOKEN after ```"],
  ["unmatched long before short pair", "```x TOKEN `a`"],
  ["escaped closing tick", "`TOKEN\\`"],
  ["escaped run pair", "\\\\`TOKEN`"],
  ["table paired pipes", "a | b\n--- | ---\n`x|TOKEN` | y"],
  ["table unmatched row reset", "a | b\n--- | ---\n`x | y\nTOKEN | z`"],
  ["table unmatched cell reset", "a | b\n--- | ---\n`x | TOKEN`"],
  ["table leading escaped pipe", "a\\|b | c\n--- | ---\n`x | TOKEN`"],
  ["blank reset nested quote", "~~~~quote\n`x\n\nTOKEN\ny`\n~~~~"],
  ["blank reset CRLF", "`x\r\n\r\nTOKEN\r\ny`"],
  ["fenced quote brace alias", "```{quote}\nTOKEN\n```"],
  ["fenced quote dot alias", "~~~.quoted\nTOKEN\n~~~"],
  ["fenced spoiler brace alias", "~~~{spoiler} title\nTOKEN\n~~~"],
  ["invalid tilde header ticks", "~~~ bad`header\nTOKEN"],
  ["inline on fence-like line", "```a TOKEN b```"],
  ["prefixed paired triple inline", "> ```a TOKEN b```"],
  ["blockquote triple inline", "> ```\n> TOKEN\n> ```"],
  ["indented code", "    TOKEN"],
  ["list blocks reset", "- `one\n- TOKEN\n- three`"],
  ["heading blocks reset", "`one\n# TOKEN\nthree`"],
  ["quote blocks reset", "`one\n> TOKEN\nthree`"],
  ["partial maximal run fallback", "before ``x TOKEN `a`"],
  ["ordinary paragraph paired", "`one\nTOKEN\nthree`"],
  ["blank followed tilde fence", "`one\n\n~~~\nTOKEN\n~~~\nthree`"]
]) test(`message-wide fallback: ${name}`, async () => {
  const content = input.replace('TOKEN', token);
  assert.equal(await resolveMessageLinks(content, session, {allowPrivateReferences:true}), content.replace(token, url));
});
// original 25 regressions: each contains a backtick, fence or indented line, so all use bare URLs.
for (const [name, input] of [
  ["closed quote", "```quote\nhello\n```\nSee TOKEN"],
  ["closed spoiler", "```spoiler title\nhello\n```\nSee TOKEN"],
  ["stray tick", "` earlier TOKEN"],
  ["escaped tick", "\\` earlier TOKEN"],
  ["earlier paragraph", "` earlier\n\nTOKEN\nlater `"],
  ["blank line resets", "` earlier\n \t\nTOKEN\nlater `"],
  ["unmatched run before paired run", "` stray ``TOKEN``"],
  ["quoted alias", "```quoted\nTOKEN\n```"],
  ["mixed-case quote", "```Quote\nTOKEN\n```"],
  ["mixed-case spoiler", "```SpOiLeR title\nTOKEN\n```"],
  ["quoted closer consumed", "```QUOTED\nhello\n```\nTOKEN"],
  ["blockquote prefix", "> ~~~\n> TOKEN\n> ~~~"],
  ["list prefix", "- > ```\n  > TOKEN\n  > ```"],
  ["space indent", " ```\nTOKEN\n ```"],
  ["tab indent", "\t~~~\nTOKEN\n\t~~~"],
  ["longer backticks do not close", "```py\n````\nTOKEN\n```"],
  ["suffix does not close", "```py\n``` x\nTOKEN\n```"],
  ["shorter fence does not close", "````py\n```\nTOKEN\n````"],
  ["different marker does not close", "~~~py\n```\nTOKEN\n~~~"],
  ["rstrip closer", "```py\ncode\n``` \t\nTOKEN"],
  ["nested code in quote", "````quote\n```py\nTOKEN\n```\n````"],
  ["after nested quote", "````quote\n```spoiler\nhello\n```\n````\nTOKEN"],
  ["paired multiline ticks", "``a\nTOKEN\nb``"],
  ["shorter suffix pairs", "``a\nTOKEN\nb`"],
  ["paired escaped opener is prose", "\\`TOKEN`"]
]) test(`message-wide fallback: ${name}`, async () => {
  const content = input.replace('TOKEN', token);
  assert.equal(await resolveMessageLinks(content, session, {allowPrivateReferences:true}), content.replace(token, url));
});
test('table labels entity-escape pipes in both stream and topic',async()=>{
 const source={id:101,type:'stream',stream_id:7,display_recipient:'a|b',subject:'x|y'};
 const s={realm:session.realm,client:{messages:{async getById(){return {result:'success',message:source};}}}} as any;
 const result=await resolveMessageLinks('| [[msg:101]] | 2 |',s,{allowPrivateReferences:true});
 assert.match(result,/\[#a&#124;b > x&#124;y @ 💬\]/);assert.equal(result.split('|').length,4);
});


// Reviewer supplied classes/examples, not the unavailable full 18-input corpus.
// Three independent representatives per class; expected by policy, not by a parser.
const reviewerClasses = [
  ['list continuation after blank', '- lead\n\n    TOKEN'],
  ['ordered list continuation', '1. lead\n\n    TOKEN'],
  ['tab list continuation', '- lead\n\n\tTOKEN'],
  ['horizontal rule boundary', '`opening\n---\nTOKEN closing'],
  ['asterisk rule boundary', '`opening\n* * *\nTOKEN closing'],
  ['underscore rule boundary', '`opening\n___\nTOKEN closing'],
  ['false table', 'a | b\n- | -\n| `a | TOKEN |\n| z` | b |'],
  ['false table without outer pipes', 'a | b\n- | -\n`a | TOKEN\nz` | b'],
  ['false table short separator', 'a | b\n- | -\n| ``a | TOKEN |\n| z`` | b |'],
  ['CR-only paragraph', '`opening\r\rTOKEN closing`'],
  ['CR-only indented paragraph', 'lead\r\r    TOKEN'],
  ['CR-only rule boundary', '`opening\r---\rTOKEN closing'],
];
for (const [name, input] of reviewerClasses) test(`reviewer mismatch class: ${name}`, async () => {
  const content = input.replace('TOKEN', token);
  assert.equal(await resolveMessageLinks(content, session, {allowPrivateReferences:true}), content.replace(token, url));
});

// R3: indentation can occur at any depth in mixed list/quote prefixes.
const nestedIndentationCases = [
  ['reviewer quote in list', '- >     TOKEN'],
  ['list in quote', '> -     TOKEN'],
  ['nested quote in list', '- > >     TOKEN'],
  ['nested list in quote', '> - +     TOKEN'],
  ['asterisk list', '*     TOKEN'],
  ['plus list', '+     TOKEN'],
  ['ordered dot list', '12.     TOKEN'],
  ['ordered parenthesis list', '12)     TOKEN'],
  ['ordered list in quote', '> 12.     TOKEN'],
  ['quote in ordered list', '12) >     TOKEN'],
  ['mixed nesting', '1. > + > 2)     TOKEN'],
  ['indent before markers', '    - > TOKEN'],
  ['indent between markers', '-    > TOKEN'],
  ['indent before nested list', '>    1) TOKEN'],
  ['indent before first digit', '    12. TOKEN'],
  ['CR-only nested indentation', 'lead\r- >     TOKEN'],
  ['CRLF nested indentation', 'lead\r\n> 2)     TOKEN'],
  ['tab before markers', '\t- > TOKEN'],
  ['tab between markers', '- \t> TOKEN'],
  ['mixed tabs after markers', '> 12) \t  TOKEN'],
  ['mixed tabs before nested list', '> \t + TOKEN'],
  ['tab after non-marker text', 'prose\tTOKEN'],
  ['spaces after non-marker text then tab', 'prose    TOKEN\t'],
];
for (const [name, input] of nestedIndentationCases) test(`conservative indentation: ${name}`, async () => {
  const content = input.replace('TOKEN', token);
  // Exact reviewer shape, then unrelated prose tokens before and after it.
  for (const message of [content, `${token}\n\n${content}\n\n${token}`]) {
    assert.equal(await resolveMessageLinks(message, session, {allowPrivateReferences:true}), message.replaceAll(token, url));
  }
});

for (const input of [
  '`before\n\nTOKEN', 'TOKEN\n\n`after', "\\` earlier TOKEN",
  'TOKEN\n~~~', 'prose~~~TOKEN', '> ~~~\nTOKEN', 'TOKEN\n    unrelated', 'TOKEN\n\tunrelated',
  'TOKEN\n---', '* * *\nTOKEN', 'TOKEN\n___', 'TOKEN\n*\t*\t*',
  '> ---\nTOKEN', '>     code\nTOKEN', 'TOKEN\r\r---',
  '@_**A|1** [said](url):\n```quote\nhello\n```\nSee TOKEN',
  '~~~spoiler title\nTOKEN', '~~~quote\nTOKEN',
]) test(`all tokens fall back, including prose: ${JSON.stringify(input)}`, async () => {
  // The ambiguity may precede/follow either token; no per-token context decisions.
  const content = input.replace('TOKEN', `${token} + ${token}`);
  const links: any[] = [], replacements: string[] = [];
  const actual = await resolveMessageLinks(content, session, {allowPrivateReferences:true, links, replacements});
  assert.equal(actual, content.replaceAll(token, url));
  assert.deepEqual(links, [{token, url}, {token, url}]);
  assert.deepEqual(replacements, [url, url]);
  assert.ok(!actual.includes('[#Secret'), 'no literal Markdown label can leak into code');
});

for (const input of [
  'See TOKEN', '> quoted text\n\nSee TOKEN', '- lead\n- TOKEN',
  '# heading\nTOKEN', '| left | right |\n| --- | --- |\n| TOKEN | 2 |',
  'a ~ b ~~ c\nTOKEN', '   TOKEN', 'TOKEN\n--',
  '- >   TOKEN', '> -   TOKEN', '12) > +   TOKEN',
  'prose    TOKEN', // Four spaces after ordinary text are not indentation.
]) test(`unambiguous message retains labels: ${JSON.stringify(input)}`, async () => {
  const content = input.replace('TOKEN', `${token} + ${token}`);
  assert.equal(await resolveMessageLinks(content, session, {allowPrivateReferences:true}), content.replaceAll(token, label));
});

test('DM labels follow the same message-wide rule', async () => {
  const dmUrl = 'https://example.invalid/#narrow/dm/1,2-dm/near/101';
  const dm = {realm:session.realm, client:{messages:{async getById(){
    return {result:'success', message:{id:101, type:'private', display_recipient:[{id:1},{id:2}]}};
  }}}} as any;
  assert.equal(await resolveMessageLinks(token, dm, {allowPrivateReferences:true}), `[DM @ 💬](${dmUrl})`);
  for (const input of ['TOKEN\n\n~~~spoiler details\nTOKEN', ...nestedIndentationCases.map(([, input]) => input)]) {
    const content = `${token}\n\n${input.replaceAll('TOKEN', token)}`;
    assert.equal(await resolveMessageLinks(content, dm, {allowPrivateReferences:true}), content.replaceAll(token, dmUrl));
  }
});
