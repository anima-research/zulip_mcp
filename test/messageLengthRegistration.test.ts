/** Realm message limits come from /register and reach tools and publishes. */
import test from 'node:test';
import assert from 'node:assert/strict';
import { ZulipEventLoop } from '../src/platforms/zulip-events.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { updateRealmMessageLength } from '../src/zulip-client.ts';
for(const limit of [70,20000,0,-1,NaN,undefined]) test(`registration validates realm max_message_length ${limit}`,async()=>{
 const seen:number[]=[];
 const loop=new ZulipEventLoop({onRealmMessageLength:n=>seen.push(n)});
 await loop.start({queues:{async register(params){
  assert.deepEqual(params.fetch_event_types,['realm']);
  assert.deepEqual(params.event_types,['message','reaction','update_message','delete_message']);
  return {result:'success',queue_id:'q1',last_event_id:-1,max_message_length:limit};
 }},events:{async retrieve(){loop.stop();return {events:[]};}}},()=>{});
 assert.deepEqual(seen,typeof limit==='number'&&limit>0?[limit]:[]);
});
for(const override of [undefined,'100','0','invalid']) test(`registered limit reaches publishes and tool session; override ${override}`,async t=>{
 const session={maxMessageLength:10000,client:null as any,realm:'https://example.invalid',authHeader:'test',sessionId:'registered',selfUserId:1};const sent:string[]=[];
 let registered!:()=>void;
 const polled=new Promise<void>(resolve=>{registered=resolve;});
 const client={queues:{async register(){return {result:'success',queue_id:'q1',last_event_id:-1,max_message_length:70};}},
  events:{async retrieve(){adapter.stopEvents();registered();return {events:[]};}},
  messages:{async send(args:any){sent.push(args.content);return {result:'success',id:501};}}};
 session.client=client;
 const dir=mkdtempSync(join(tmpdir(),'registered-limit-'));t.after(()=>rmSync(dir,{recursive:true,force:true}));
 const tools=new ZulipToolRuntime(session,dir);
 const adapter=new ZulipAdapter(client,1,'limit',{maxMessageLength:session.maxMessageLength,
  onRealmMessageLength:limit=>updateRealmMessageLength(session,limit,{ZULIP_MAX_MESSAGE_LENGTH:override})});
 adapter.startEvents(()=>{});await polled;
 const expected=override==='100'?100:70;assert.equal(session.maxMessageLength,expected);
 await adapter.publish('zulip:general',undefined,[{type:'text',text:'x'.repeat(150)}]);
 assert.ok(sent.every(s=>s.length<=expected));assert.equal(sent.join(''),'x'.repeat(150));
 sent.length=0;
 await tools.handleToolCall('send_message',{type:'stream',to:'general',topic:'plans',content:'x'.repeat(150)});
 assert.ok(sent.every(s=>s.length<=expected));assert.equal(sent.join(''),'x'.repeat(150));
 await assert.rejects(tools.handleToolCall('edit_message',{message_id:101,content:'x'.repeat(expected+1)}),/content exceeds.*limit/);
});
