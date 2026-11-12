import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  McplConnection,
  method,
  type ChannelDescriptor,
  type ChannelsIncomingParams,
  type ChannelsRegisterParams,
  type IncomingChannelMessage,
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

test('end to end: a second user\'s message reaches the host, and the host\'s plain answer lands in its topic', async () => {
  assert.ok(existsSync(BIN), `${BIN} is missing; npm run build makes it`);
  const stream = await newStream(env, 'e2e');
  const topic = 'the question';
  const botId = await bot.myId();
  const stateDir = mkdtempSync(join(tmpdir(), 'zulip-contract-e2e-'));
  const child = spawn(process.execPath, [BIN, '--stdio'], {
    env: { ...process.env, ZULIP_STATE_DIR: stateDir },
    stdio: ['pipe', 'pipe', 'inherit'],
  });
  const exited = new Promise<number | null>((resolve) => child.once('exit', (code) => resolve(code)));
  const host = McplConnection.fromStreams(child.stdout, child.stdin);
  const incoming: IncomingChannelMessage[] = [];
  host.on('request', (req) => {
    if (req.method === method.CHANNELS_REGISTER || req.method === method.CHANNELS_CHANGED) {
      const p = req.params as ChannelsRegisterParams & { added?: ChannelDescriptor[] };
      host.sendResponse(req.id, { results: (p.channels ?? p.added ?? []).map((c) => ({ id: c.id, accepted: true })) });
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      incoming.push(...p.messages);
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: true })) });
    } else if (req.method === method.PUSH_EVENT) {
      host.sendResponse(req.id, { accepted: true });
    } else {
      host.sendError(req.id, -32601, `unexpected ${req.method}`);
    }
  });

  try {
    await host.sendRequest(method.INITIALIZE, {
      protocolVersion: '2025-03-26',
      capabilities: { experimental: { mcpl: { version: '0.5', channels: true } } },
      clientInfo: { name: 'contract-host', version: '0' },
    });
    host.sendNotification('notifications/initialized');
    await host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });

    const channelId = `zulip:${stream}`;
    await until(async () => {
      const listed = (await host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
      return listed.channels.some((c) => c.id === channelId) ? true : undefined;
    }, `${channelId} to be registered`);
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
    child.stdin.end();
    const code = await Promise.race([exited, new Promise<'timeout'>((r) => setTimeout(() => r('timeout'), 10_000))]);
    if (code === 'timeout') child.kill();
    host.close();
    rmSync(stateDir, { recursive: true, force: true });
  }
});
