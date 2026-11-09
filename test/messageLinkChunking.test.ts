/** Inline pipes and protected-span chunking; child timeout can interrupt a synchronous chunk-loop hang. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { resolveMessageLinks } from '../src/message-links.ts';

const url = 'https://example.invalid/#narrow/channel/12-general/topic/secret/near/101';
const session = {realm:'https://example.invalid', client:{messages:{async getById(){
  return {result:'success',message:{id:101,type:'stream',stream_id:12,display_recipient:'general',subject:'secret'}};
}}}} as any;
for (const content of ['`a|[[msg:101]]`', '`a|\n[[msg:101]]`', '``a|[[msg:101]]``']) {
  test(`R1-2 ordinary inline code retains pipes: ${JSON.stringify(content)}`, async () => {
    assert.equal(await resolveMessageLinks(content,session,{allowPrivateReferences:true}),content.replace('[[msg:101]]',url));
  });
}
test('R1-2 table backticks force bare URLs, matched or unmatched', async () => {
  const content='| a | b |\n| --- | --- |\n| `unclosed | [[msg:101]] |';
  assert.equal(await resolveMessageLinks(content,session,{allowPrivateReferences:true}),content.replace('[[msg:101]]',url));
  const paired='| a | b |\n| --- | --- |\n| `a|[[msg:101]]` | x |';
  assert.equal(await resolveMessageLinks(paired,session,{allowPrivateReferences:true}),paired.replace('[[msg:101]]',url));
});
test('R1-4 CRLF metadata and protected-span chunking finish within a bounded subprocess', () => {
  const script = `
    import assert from 'node:assert/strict';
    import {mkdtempSync,rmSync} from 'node:fs';
    import {tmpdir} from 'node:os';
    import {join} from 'node:path';
    import {chunkMessage} from ${JSON.stringify(new URL('../src/content.ts',import.meta.url).href)};
    import {ZulipToolRuntime} from ${JSON.stringify(new URL('../src/tool-runtime.ts',import.meta.url).href)};
    // Independent of label sanitization: a newline inside a protected span at 0.
    const span='[a\\r\\nb](https://example.invalid/x)';
    const chunks=chunkMessage(span+'x'.repeat(10000),10000,[span]);
    assert.equal(chunks.join(''),span+'x'.repeat(10000));
    assert.ok(chunks.every(c=>c.length>0 && c.length<=10000));
    assert.ok(chunks[0].startsWith(span));
    // Overlap order must not permit cuts inside either protected span.
    assert.throws(()=>chunkMessage('abcdefghiZ',6,['defghi','abcdef']),/expansion/);
  `;
  const production = `
    const dir=mkdtempSync(join(tmpdir(),'links-crlf-')); const sent=[];
    const client={messages:{async getById(){return {result:'success',message:{id:101,type:'stream',stream_id:12,display_recipient:'a\\r\\nb',subject:'c\\nd\\re'}};},async send(args){sent.push(args);return {result:'success',id:501};}}};
    try {
      const tools=new ZulipToolRuntime({client,realm:'https://example.invalid',selfUserId:1,authHeader:'test',sessionId:'crlf',maxMessageLength:10000},dir);
      await tools.handleToolCall('send_message',{type:'stream',to:'public',topic:'links',allow_private_references:true,content:'[[msg:101]]'+'x'.repeat(10000)});
      assert.equal(sent.length,2);
      assert.ok(sent.every(s=>s.content.length>0 && s.content.length<=10000));
      assert.ok(sent[0].content.startsWith('[#a b > c d e @ 💬]('));
      assert.ok(!/[\\r\\n]/.test(sent[0].content));
    } finally {rmSync(dir,{recursive:true,force:true});}
  `;
  const result=spawnSync(process.execPath,['--import','tsx','--input-type=module','-e',script+production],{timeout:4000,encoding:'utf8'});
  assert.equal(result.error,undefined,String(result.error));
  assert.equal(result.status,0,result.stderr);
});
