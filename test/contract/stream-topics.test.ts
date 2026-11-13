import { after, before, test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { ZulipToolRuntime } from '../../src/tool-runtime.ts';
import { initializeZulipClient } from '../../src/zulip-client.ts';
import { contractEnv } from './env.ts';
import { newStream } from './zulip.ts';

const env = contractEnv();
let tools: ZulipToolRuntime;
let stateDir: string;

before(async () => {
  const session = await initializeZulipClient(process.env);
  stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-topics-'));
  tools = new ZulipToolRuntime(session, stateDir);
});

after(() => rmSync(stateDir, { recursive: true, force: true }));

test('get_stream_topics by name with a limit lists the newest topics first, as Zulip orders them, and counts them all', async () => {
  const stream = await newStream(env, 'topics');
  for (const topic of ['one', 'two', 'three', 'four']) {
    await tools.handleToolCall('send_message', { type: 'stream', to: stream, topic, content: `in ${topic}` });
  }
  const result = (await tools.handleToolCall('get_stream_topics', { stream, limit: 2, verbose: true })) as {
    topic_count: number;
    formatted_list: string;
    raw_data: { topics: { name: string }[] };
  };
  const all = result.raw_data.topics.map((t) => t.name);
  assert.deepEqual(all.slice(0, 4), ['four', 'three', 'two', 'one']);
  assert.equal(result.topic_count, all.length);
  assert.deepEqual([...result.formatted_list.matchAll(/\*\*([^*]+)\*\* \(latest/g)].map((m) => m[1]), ['four', 'three']);
  assert.match(result.formatted_list, new RegExp(`^📑 \\*\\*${all.length} Topics, the newest 2 shown\\*\\*`));
});
