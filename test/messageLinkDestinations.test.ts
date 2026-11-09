/** Authorization and sending must share the same resolved destination. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
function fixture(t:any, source:any={id:101,type:'stream',stream_id:7,display_recipient:'Secret',subject:'Plans'}) {
 const sent:any[]=[], names:string[]=[], users:string[]=[];
 const client={config:{realm:'https://example.invalid'},messages:{async getById(){return {result:'success',message:source};},async send(a:any){sent.push(a);return {result:'success',id:501};}},async callEndpoint(path:string,_method:string,args:any){
  if(path==='get_stream_id'){names.push(args.stream);return {result:'success',stream_id:args.stream.toLowerCase()==='secret'?7:args.stream==='12'?7:12};}
  return {result:'success',stream:{invite_only:true}};
 }};
 const dir=mkdtempSync(join(tmpdir(),'link-dest-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));
 const tools=new ZulipToolRuntime({client,realm:'https://example.invalid',selfUserId:1,authHeader:'test',sessionId:'dest'} as any,dir);
 t.mock.method(tools,'resolveUserId',async(q:string)=>{users.push(q);return {'alice@example.com':2,'bob@example.com':3}[q.toLowerCase()]??Number(q);});
 return {tools,client,sent,names,users};
}
test('same DM: comma-separated emails resolve once and send numeric ids',async t=>{
 const f=fixture(t,{id:101,type:'private',display_recipient:[{id:3},{id:1},{id:2}]});
 await f.tools.handleToolCall('send_message',{type:'private',to:'alice@example.com,bob@example.com',content:'[[msg:101]]'});
 assert.deepEqual(f.users,['alice@example.com','bob@example.com']);assert.deepEqual(f.sent[0].to,[2,3]);
});
for(const to of ['secret','SECRET','7',7,'"Secret"','["secret"]']) test(`same stream canonical form ${JSON.stringify(to)}`,async t=>{
 const f=fixture(t);await f.tools.handleToolCall('send_message',{type:'stream',to,topic:'plans',content:'[[msg:101]]'});
 assert.equal(f.sent[0].to,7);assert.equal(f.names.length,typeof to==='number'||to==='7'?0:1);
});
test('digits-only stream name cannot authorize a different numeric stream id',async t=>{
 const f=fixture(t,{id:101,type:'stream',stream_id:7,display_recipient:'12',subject:'Plans'});
 await assert.rejects(f.tools.handleToolCall('send_message',{type:'stream',to:'12',topic:'Plans',content:'[[msg:101]]'}),/outside the allowed/);assert.equal(f.sent.length,0);
 await f.tools.handleToolCall('send_message',{type:'stream',to:'"12"',topic:'Plans',content:'[[msg:101]]'});
 assert.equal(f.sent[0].to,7);assert.deepEqual(f.names,['12']);
});
test('publish uses canonical stream id and gives actionable refusal',async t=>{
 const f=fixture(t);const a=new ZulipAdapter(f.client,1,'test');
 await a.publish('zulip:secret',undefined,[{type:'text',text:'[[msg:101]]'}],{threadId:'plans'});assert.equal(f.sent[0].to,7);
 await assert.rejects(a.publish('zulip:elsewhere',undefined,[{type:'text',text:'[[msg:101]]'}]),/use send_message with allow_private_references: true/);assert.equal(f.sent.length,1);
});
