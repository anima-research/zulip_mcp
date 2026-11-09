/** Owner's conservative message-wide math/bracket/angle rule, not renderer parity. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { resolveMessageLinks } from '../src/message-links.ts';

const token = '[[msg:101]]';
const stream = {id:101, type:'stream', stream_id:7, display_recipient:'Secret', subject:'Plans'};
const dm = {id:101, type:'private', display_recipient:[{id:1}, {id:2}]};

// Exact 10 shapes (20 stream/DM cases) from review 4's render-failures.json.
const reviewFailures = [
  '$$[[msg:101]]$$',
  '$$ [[msg:101]] $$',
  '$$\\invalid{[[msg:101]]}$$',
  '$$\\text{[[msg:101]]}$$',
  'lead $$[[msg:101]]$$ tail',
  '> $$[[msg:101]]$$',
  '- $$[[msg:101]]$$',
  '[[[msg:101]]](https://example.invalid)',
  '[outer]([[msg:101]])',
  '[outer [[msg:101]]](https://example.invalid)',
];
// The two remaining review-evidence failures are this time shape in stream/DM.
const angleCases = [
  '<time:[[msg:101]]>',
  '<b>[[msg:101]]</b>',
  '<a href="[[msg:101]]">link</a>',
  '<[[msg:101]]>',
  '<https://example.invalid/[[msg:101]]>',
  // Unrelated, unmatched and escaped < still force message-wide fallback.
  '<time:2026-11-09T00:00:00Z>', '<b>unrelated</b>',
  '<https://example.invalid>', '<', '\\<', 'a < b',
];
const conservativeCases = [
  '$$\n[[msg:101]]\n$$', // Block math.
  '$[[msg:101]]$', // Prefer over-refusal even for single dollars.
  '$', '$$', '$$unmatched', '\\$', '\\$\\$',
  '$$unrelated math$$', '$unrelated math$',
  '[', ']', '\\[', '\\]', '[unrelated](https://example.invalid)',
  '[reference]: https://example.invalid',
  '[[msg:abc]]', '[[msg:-1]]', '[[MSG:101]]', // Not exact tokens.
  '[[msg:101]]]', '[[[msg:101]]', // Do not strip outer brackets with tokens.
];

for (const message of [stream, dm]) {
  const url = message.type === 'stream'
    ? 'https://example.invalid/#narrow/channel/7-Secret/topic/Plans/near/101'
    : 'https://example.invalid/#narrow/dm/1,2-dm/near/101';
  const label = `[${message.type === 'stream' ? '#Secret > Plans' : 'DM'} @ 💬](${url})`;
  const reads: number[] = [];
  const session = {realm:'https://example.invalid', client:{messages:{async getById({message_id}: {message_id:number}) {
    reads.push(message_id);
    return {result:'success', message};
  }}}} as any;

  for (const content of [...reviewFailures, ...angleCases, ...conservativeCases]) {
    test(`${message.type}: math/bracket/angle fallback ${JSON.stringify(content)}`, async () => {
      // Exact failing shape, then unrelated tokens on both sides: message-wide.
      for (const input of [content, `${token}\n\n${content}\n\n${token}`]) {
        if (!input.includes(token)) continue;
        const links: any[] = [], replacements: string[] = [];
        const before = reads.length;
        assert.equal(await resolveMessageLinks(input, session, {allowPrivateReferences:true, links, replacements}), input.replaceAll(token, url));
        const count = input.split(token).length - 1;
        assert.deepEqual(links, Array.from({length:count}, () => ({token, url})));
        assert.deepEqual(replacements, Array(count).fill(url));
        assert.deepEqual(reads.slice(before), [101], 'one lookup despite repeated tokens');
      }
    });
  }

  test(`${message.type}: exact/escaped token brackets alone still permit labels`, async () => {
    const before = reads.length;
    const input = `${token} \\[[msg:999]] [[msg:00101]] ${token}`;
    assert.equal(await resolveMessageLinks(input, session, {allowPrivateReferences:true}), `${label} [[msg:999]] ${label} ${label}`);
    assert.deepEqual(reads.slice(before), [101], 'escaped token is not looked up');
  });

  test(`${message.type}: angle fallback preserves escaped tokens without lookup`, async () => {
    const before = reads.length;
    const input = `${token} <time:\\[[msg:999]]> ${token}`;
    assert.equal(await resolveMessageLinks(input, session, {allowPrivateReferences:true}), `${url} <time:[[msg:999]]> ${url}`);
    assert.deepEqual(reads.slice(before), [101]);
  });
}
