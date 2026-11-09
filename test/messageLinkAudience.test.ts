/** Audience and atomic-delivery regressions: real handlers, isolated clients, no external services. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';

const stream = (id=101, topic='secret') => ({id,type:'stream',stream_id:12,display_recipient:'private',subject:topic});
function fixture(t: any, options: {public?: boolean; limit?: number; error?: string} = {}) {
  const reads: any[] = [], sent: any[] = []; let uploads=0;
  const client = {config:{realm:'https://example.invalid'}, messages:{
    async getById(args: any) { reads.push(args); return options.error ? {result:'error',code:options.error,msg:'PRIVATE_BODY'} : {result:'success',message:args.message_id===102 ? {id:102,type:'private',display_recipient:[{id:1},{id:2}]} : stream(args.message_id)}; },
    async send(args: any) { sent.push(args); return {result:'success',id:501}; },
  }, async callEndpoint(path: string, _method?: string, args?: any) {if(path==='get_stream_id')return {result:'success',stream_id:args.stream==='private'?12:20} as any; return {result:'success',stream:{invite_only:!options.public,history_public_to_subscribers:false,is_web_public:false}};} };
  const dir=mkdtempSync(join(tmpdir(),'links-r1-')); t.after(()=>rmSync(dir,{recursive:true,force:true}));
  const tools=new ZulipToolRuntime({client,realm:'https://example.invalid',authHeader:'test',selfUserId:1,sessionId:'r1',maxMessageLength:options.limit} as any,dir,{uploader:{async upload(){uploads++;return {name:'x',path:'/x',url:'https://example.invalid/x'};}}});
  return {tools,client,reads,sent,uploads:()=>uploads};
}
const send = {type:'stream',to:'public',topic:'links',content:'[[msg:101]]'};
test('R1-1 private stream to public destination is refused with escape advice',async t=>{
 const f=fixture(t); await assert.rejects(f.tools.handleToolCall('send_message',send), /\[\[msg:101\]\].*\\\[\[msg:101\]\]/); assert.equal(f.sent.length,0);
});
test('R1-1 opt-in returns an auditable link and labelled fallback',async t=>{
 const f=fixture(t); const r=await f.tools.handleToolCall('send_message',{...send,allow_private_references:true});
 assert.deepEqual(r.links,[{token:'[[msg:101]]',url:'https://example.invalid/#narrow/channel/12-private/topic/secret/near/101'}]);
 assert.equal(f.sent[0].content,'[#private > secret @ 💬](https://example.invalid/#narrow/channel/12-private/topic/secret/near/101)');
});
test('R1-1 non-invite-only stream allowed regardless of web-public/history settings',async t=>{
 const f=fixture(t,{public:true}); await f.tools.handleToolCall('send_message',send); assert.equal(f.sent.length,1);
});
test('R1-1 same stream and topic allowed without stream metadata',async t=>{
 const f=fixture(t); f.client.callEndpoint=async()=>{throw Error('must not read metadata');}; await f.tools.handleToolCall('send_message',{...send,to:12,topic:'secret'}); assert.equal(f.sent.length,1);
});
test('R1-1 DM outside same DM refused; same DM allowed including self',async t=>{
 const f=fixture(t); await assert.rejects(f.tools.handleToolCall('send_message',{...send,content:'[[msg:102]]'}));
 await f.tools.handleToolCall('send_message',{type:'private',to:[2],content:'[[msg:102]]'}); assert.match(f.sent[0].content,/\[DM @ 💬\]\(.*#narrow\/dm\/1,2-dm\/near\/102\)/);
});
test('R1-2 one inline-code token forces bare URLs throughout the message',async t=>{
 const f=fixture(t); await f.tools.handleToolCall('send_message',{...send,allow_private_references:true,content:'`[[msg:101]]` [[msg:101]]'});
 const url = 'https://example.invalid/#narrow/channel/12-private/topic/secret/near/101';
 assert.equal(f.sent[0].content, `\`${url}\` ${url}`);
});
test('R1-3 send_dm resolves before uploads and fails closed',async t=>{
 const f=fixture(t); await assert.rejects(f.tools.handleToolCall('send_dm',{to:['2'],content:'[[msg:101]]',attachments:[{data:'eA==',name:'x'}]})); assert.equal(f.uploads(),0); assert.equal(f.sent.length,0);
 await f.tools.handleToolCall('send_dm',{to:['2'],content:'[[msg:102]]'}); assert.match(f.sent[0].content,/#narrow\/dm\//);
});
test('R1-3 publish resolves before uploads and chunks, returns links',async t=>{
 const f=fixture(t); const adapter=new ZulipAdapter(f.client,1,'test');
 await assert.rejects(adapter.publish('zulip:public',undefined,[{type:'text',text:'[[msg:101]]'}])); assert.equal(f.sent.length,0);
 const r=await adapter.publish('zulip:private',undefined,[{type:'text',text:'[[msg:101]]'}],{threadId:'secret'});
 assert.equal((r as any).links[0].token,'[[msg:101]]');
});
test('R1-4 send never cuts inside a resolved token',async t=>{
 const f=fixture(t,{public:true}); await f.tools.handleToolCall('send_message',{...send,content:'a'.repeat(9990)+'[[msg:101]]'+'b'.repeat(30)});
 assert.equal(f.sent[0].content,'a'.repeat(9990)); assert.match(f.sent[1].content,/^\[#private.*101\)b+$/);
});
test('R1-4 edit expansion exceeds realm limit before PATCH',async t=>{
 const f=fixture(t,{limit:70}); await assert.rejects(f.tools.handleToolCall('edit_message',{message_id:101,content:'x'.repeat(30)+'[[msg:101]]'}),/link expansion.*limit/i);
});
for(const code of ['BAD_REQUEST','RATE_LIMIT_HIT','INTERNAL_SERVER_ERROR']) test(`R1-5 ${code} consistently mapped for tokens and reply_to`,async t=>{
 const f=fixture(t,{error:code}); const pattern=code==='BAD_REQUEST'?/not found or not accessible/:new RegExp(`temporarily unavailable \\(${code}\\); retry`);
 await assert.rejects(f.tools.handleToolCall('send_message',send),pattern);
 await assert.rejects(f.tools.handleToolCall('send_message',{reply_to:101,content:'hi'}),pattern);
});
test('R1-6 reply checks precede all reads; token and reply share memo',async t=>{
 const f=fixture(t); await assert.rejects(f.tools.handleToolCall('send_message',{...send,reply_to:101}),/omit to/); assert.equal(f.reads.length,0);
 await f.tools.handleToolCall('send_message',{reply_to:101,content:'[[msg:101]]'}); assert.equal(f.reads.length,1);
});
test('R1-7 request literal empty topic names',async t=>{
 const f=fixture(t); await f.tools.handleToolCall('send_message',{...send,allow_private_references:true}); assert.equal(f.reads[0].allow_empty_topic_name,true);
});

test('R1-1 unknown metadata and stream-to-DM fail closed even for public streams',async t=>{
 const f=fixture(t,{public:true}); f.client.callEndpoint=async(path)=>path==='get_stream_id'?{result:'success',stream_id:20} as any:({result:'success',stream:{}} as any);
 await assert.rejects(f.tools.handleToolCall('send_message',send),/outside the allowed/);
 await assert.rejects(f.tools.handleToolCall('send_message',{type:'private',to:[2],content:'[[msg:101]]'}),/outside the allowed/);
 assert.equal(f.sent.length,0);
});
test('R1-1 quote cannot launder a private reference; opt-in must be boolean true',async t=>{
 const f=fixture(t); await assert.rejects(f.tools.handleToolCall('send_message',{...send,allow_private_references:'true',content:'```quote\n[[msg:101]]\n```'}),/outside the allowed/); assert.equal(f.sent.length,0);
});
test('R1-3 send_dm bad lookup happens before recipient directory lookup',async t=>{
 const f=fixture(t,{error:'BAD_REQUEST'});
 await assert.rejects(f.tools.handleToolCall('send_dm',{to:['Unknown Person'],content:'[[msg:101]]'}),/Cannot resolve \[\[msg:101\]\]: message not found/); assert.equal(f.sent.length,0);
});
test('R1-3 publish bad reference is atomic even with image uploads',async t=>{
 const f=fixture(t); let uploads=0;
 const adapter=new ZulipAdapter(f.client,1,'test',{uploader:{async upload(){uploads++;throw Error('must not upload');}},uploadPolicy:{} as any});
 await assert.rejects(adapter.publish('zulip:public',undefined,[{type:'text',text:'a'.repeat(20000)+'[[msg:101]]'},{type:'image',data:'eA==',mimeType:'image/png'}])); assert.equal(uploads,0); assert.equal(f.sent.length,0);
});
test('R1-4 over-limit single link fails before uploads/sends',async t=>{
 const f=fixture(t,{limit:70}); await assert.rejects(f.tools.handleToolCall('send_message',{...send,allow_private_references:true,attachments:[{data:'eA==',name:'x'}]}),/expansion.*limit/i); assert.equal(f.uploads(),0); assert.equal(f.sent.length,0);
});
test('R1-4 successful edit expansion is checked and returned before PATCH response',async t=>{
 const f=fixture(t,{limit:500}); let body='';
 t.mock.method(globalThis,'fetch',async (_url:any, options:any)=>{body=options.body.get('content');return new Response(JSON.stringify({result:'success'}));});
 const result=await f.tools.handleToolCall('edit_message',{message_id:101,content:'[[msg:101]]'});
 assert.match(body,/^\[#private/); assert.equal(result.links[0].token,'[[msg:101]]'); assert.equal(f.reads.length,1);
});
test('R1-5 transport failure advises retry without raw body or URL',async t=>{
 const f=fixture(t); f.client.messages.getById=async()=>{throw Error('PRIVATE_BODY https://secret.invalid');};
 for(const args of [send,{reply_to:101,content:'hi'}]) await assert.rejects(f.tools.handleToolCall('send_message',args),error=>{
  assert.match(String(error),/temporarily unavailable \(TRANSPORT_ERROR\); retry/); assert.doesNotMatch(String(error),/PRIVATE_BODY|secret.invalid/); return true;
 });
});

test('edit length counts Unicode code points, not UTF-16 units',async t=>{
 const f=fixture(t,{limit:4});let patches=0;
 t.mock.method(globalThis,'fetch',async(_url:any,options:any)=>{patches++;assert.equal(options.body.get('content'),'😀'.repeat(4));return new Response(JSON.stringify({result:'success'}));});
 await f.tools.handleToolCall('edit_message',{message_id:101,content:'😀'.repeat(4)});
 assert.equal(patches,1);assert.equal(f.reads.length,0);
 await assert.rejects(f.tools.handleToolCall('edit_message',{message_id:101,content:'😀'.repeat(5)}),error=>{
  assert.match(String(error),/content exceeds.*limit/);assert.doesNotMatch(String(error),/expansion/);return true;
 });assert.equal(patches,1);
});
test('escaped tokens do not blame expansion for an over-limit edit',async t=>{
 const f=fixture(t,{limit:4});
 await assert.rejects(f.tools.handleToolCall('edit_message',{message_id:101,content:'\\[[msg:101]]'}),error=>{
  assert.doesNotMatch(String(error),/expansion/);return true;
 });assert.equal(f.reads.length,0);
});
