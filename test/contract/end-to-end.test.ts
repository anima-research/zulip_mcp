import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  McplConnection,
  method,
  type ChannelDescriptor,
  type ChannelsIncomingParams,
  type ChannelsRegisterParams,
  type IncomingChannelMessage,
  type PushEventParams,
} from '@animalabs/mcpl-core';
import { contractEnv } from './env.ts';
import { apis, newStream, until } from './zulip.ts';

const env = contractEnv();
const { user, bot } = apis(env);
const BIN = join(import.meta.dirname, '../../build/index.js');

const GRANT = [
  'tools',
  'pushEvents',
  'channels.acknowledge',
  'channels.streaming',
  'channels.register',
  'channels.lifecycle',
  'channels.publish',
  'channels.incoming',
  'channels.typing',
  'contextHooks.beforeInference.inject.beforeUser',
];

interface Served {
  host: McplConnection;
  incoming: IncomingChannelMessage[];
  pushed: PushEventParams[];
  stop(): Promise<void>;
}

async function serve(): Promise<Served> {
  const stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-e2e-'));
  const child = spawn(process.execPath, [BIN, '--stdio'], {
    env: { ...process.env, ZULIP_STATE_DIR: stateDir },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const exited = new Promise<number | null>((resolve) => child.once('exit', (code) => resolve(code)));
  const host = McplConnection.fromStreams(child.stdout, child.stdin);
  const incoming: IncomingChannelMessage[] = [];
  const pushed: PushEventParams[] = [];
  host.on('request', (req) => {
    if (req.method === method.CHANNELS_REGISTER || req.method === method.CHANNELS_CHANGED) {
      const p = req.params as ChannelsRegisterParams & { added?: ChannelDescriptor[] };
      host.sendResponse(req.id, { results: (p.channels ?? p.added ?? []).map((c) => ({ id: c.id, accepted: true })) });
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      incoming.push(...p.messages);
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: true })) });
    } else if (req.method === method.PUSH_EVENT) {
      pushed.push(req.params as PushEventParams);
      host.sendResponse(req.id, { accepted: true });
    } else {
      host.sendError(req.id, -32601, `unexpected ${req.method}`);
    }
  });
  const stop = async () => {
    child.stdin.end();
    const code = await Promise.race([exited, new Promise<'timeout'>((r) => setTimeout(() => r('timeout'), 10_000))]);
    if (code === 'timeout') {
      child.kill('SIGKILL');
      await exited;
    }
    host.close();
    rmSync(stateDir, { recursive: true, force: true });
  };
  try {
    await host.sendRequest(method.INITIALIZE, {
      protocolVersion: '2025-03-26',
      capabilities: { experimental: { mcpl: { version: '0.5', channels: true } } },
      clientInfo: { name: 'contract-host', version: '0' },
    });
    host.sendNotification('notifications/initialized');
    await host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
  } catch (err) {
    await stop();
    throw err;
  }
  return { host, incoming, pushed, stop };
}

async function registered(host: McplConnection, channelId: string): Promise<void> {
  await until(async () => {
    const listed = (await host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    return listed.channels.some((c) => c.id === channelId) ? true : undefined;
  }, `${channelId} to be registered`);
}

test('end to end: a second user\'s message reaches the host, and the host\'s plain answer lands in its topic', async () => {
  const stream = await newStream(env, 'e2e');
  const topic = 'the question';
  const botId = await bot.myId();
  const { host, incoming, stop } = await serve();
  try {
    const channelId = `zulip:${stream}`;
    await registered(host, channelId);
    await host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {} });

    const question = await user.ok('POST', 'messages', { type: 'stream', to: stream, topic, content: 'is anyone there?' });
    const arrived = await until(
      () => incoming.find((m) => m.messageId === String(question.id)),
      `message ${question.id} at the host`,
    );
    assert.equal(arrived.channelId, channelId);
    assert.equal(arrived.threadId, topic);
    assert.match((arrived.content[0] as { text: string }).text, /is anyone there\?/);

    const published = (await host.sendRequest(method.CHANNELS_PUBLISH, {
      conversationId: 'contract',
      channelId,
      content: [{ type: 'text', text: 'here, answering' }],
    })) as { delivered: boolean; messageId?: string };
    assert.equal(published.delivered, true);
    const answer = await user.message(published.messageId!);
    assert.equal(answer.sender_id, botId);
    assert.equal(answer.display_recipient, stream);
    assert.equal(answer.subject, topic, `the answer landed in topic ${JSON.stringify(answer.subject)}, not the question's`);
    assert.equal(answer.content, 'here, answering');
  } finally {
    await stop();
  }
});

test('end to end: on a closed channel, the host\'s plain answer to a pushed mention lands in the topic it was renamed to', async () => {
  const stream = await newStream(env, 'e2e-moved');
  const botId = await bot.myId();
  const botName = (await bot.ok('GET', 'users/me')).full_name as string;
  const { host, pushed, stop } = await serve();
  try {
    const channelId = `zulip:${stream}`;
    await registered(host, channelId);

    const question = await user.ok('POST', 'messages', { type: 'stream', to: stream, topic: 'before', content: `@**${botName}** is anyone there?` });
    await until(() => pushed.find((p) => p.origin.messageId === String(question.id)), `the mention ${question.id} pushed to the host`);
    await user.ok('PATCH', `messages/${question.id}`, { topic: 'after', propagate_mode: 'change_all' });
    assert.equal((await user.message(question.id)).subject, 'after', 'Zulip renamed the topic');
    await until(() => pushed.find((p) => String(p.origin.messageId).startsWith('move:')), 'the move pushed to the host');

    const published = (await host.sendRequest(method.CHANNELS_PUBLISH, {
      conversationId: 'contract',
      channelId,
      content: [{ type: 'text', text: 'here, after the rename' }],
    })) as { delivered: boolean; messageId?: string };
    assert.equal(published.delivered, true);
    const answer = await user.message(published.messageId!);
    assert.equal(answer.sender_id, botId);
    assert.equal(answer.subject, 'after', `the answer landed in topic ${JSON.stringify(answer.subject)}, not the one its question was renamed to`);
  } finally {
    await stop();
  }
});
