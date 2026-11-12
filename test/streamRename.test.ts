/**
 * A stream renamed, archived or reused, end to end: an in-process host on the
 * real server, the real Zulip adapter and tool runtime, and a stand-in realm
 * whose event queue hears each change (test/zulip-realm.ts).
 *
 * Run: node --import tsx --test test/streamRename.test.ts
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
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
import { ZulipMcplServer } from '../src/server.ts';
import { ZulipAdapter } from '../src/platforms/zulip.ts';
import { ZulipToolRuntime } from '../src/tool-runtime.ts';
import { FiltersPlane } from '../src/filters.ts';
import { realm, SELF, type Realm } from './zulip-realm.ts';

const directoryModule = await import('../src/stream-directory.ts').catch(() => null);
const clientModule = (await import('../src/zulip-client.ts')) as Record<string, unknown>;
type Subscribe = (api: Realm['api'], directory: unknown, setting: string | undefined) => Promise<void>;
const subscribeConfigured = clientModule.subscribeConfigured as Subscribe | undefined;

const report = console.error;
/** Every run's log lines, printed when a scenario fails. */
let runLogs: string[] = [];

const GRANT = [
  'tools', 'pushEvents', 'channels.acknowledge', 'channels.streaming', 'channels.register', 'channels.lifecycle',
  'channels.publish', 'channels.incoming', 'channels.typing', 'contextHooks.beforeInference.inject.beforeUser',
];

interface Run {
  server: ZulipMcplServer;
  host: McplConnection;
  incoming: IncomingChannelMessage[];
  pushed: PushEventParams[];
  changed: { removed?: string[]; added?: ChannelDescriptor[] }[];
  logs: string[];
  /** Answer the `channels/changed` requests held so far. */
  answerChanged(): void;
  /** Answer the held `channels/changed` requests, and every later one at once. */
  releaseChanged(): void;
  open(channelId: string, history?: Record<string, unknown>): Promise<{ channel?: ChannelDescriptor; history?: IncomingChannelMessage[] }>;
  tool(name: string, args: Record<string, unknown>): Promise<{ text: string; isError: boolean }>;
  close(): Promise<void>;
}

async function until(predicate: () => boolean, what: string, ms = 3000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

/** `ZULIP_SUBSCRIBE`, as src/index.ts hands it on at a start. */
function wireSubscribe(r: Realm, directory: unknown, setting: string | undefined): void {
  if (subscribeConfigured && directory) void subscribeConfigured(r.api, directory, setting);
}

async function start(r: Realm, dir: string, opts: { mcpl?: boolean; subscribe?: string; holdChanged?: boolean; backscroll?: Record<string, number> } = {}): Promise<Run> {
  const mcpl = opts.mcpl ?? true;
  const logs: string[] = [];
  runLogs.push(`--- run ${opts.mcpl === false ? 'without MCPL' : 'with MCPL'}`);
  const consoleError = console.error;
  console.error = (...args: unknown[]) => { const line = args.map(String).join(' '); logs.push(line); runLogs.push(line); };
  const filters = new FiltersPlane(join(dir, 'filters.json'));
  filters.start();
  const directory = directoryModule ? new directoryModule.StreamDirectory() : undefined;
  wireSubscribe(r, directory, opts.subscribe);
  const backscrollLimits = new Map(Object.entries(opts.backscroll ?? {}));
  const adapter = new ZulipAdapter(r.api, SELF, 's', { directory, filters, backscrollLimits, dmDiscoveryLimit: 0, formatTime: () => 'T' } as ConstructorParameters<typeof ZulipAdapter>[3]);
  const tools = new ZulipToolRuntime({ api: r.api, selfUserId: SELF, realm: '', authHeader: '', sessionId: 's' }, dir, { formatTime: () => 'T' });
  const server = new ZulipMcplServer(adapter, tools, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: true,
    batchWindowMs: 5,
    contextHistorySize: 3,
    stateDir: dir,
    sessionId: 's',
    catchupLimit: 100,
    formatTime: () => 'T',
    announceTimeoutMs: 300,
    filters,
    directory,
  } as ConstructorParameters<typeof ZulipMcplServer>[2]);

  const toServer = new PassThrough();
  const toHost = new PassThrough();
  const host = McplConnection.fromStreams(toHost, toServer);
  const incoming: IncomingChannelMessage[] = [];
  const pushed: PushEventParams[] = [];
  const changed: Run['changed'] = [];
  const registered: ChannelDescriptor[] = [];
  const held: (() => void)[] = [];
  let holding = opts.holdChanged ?? false;
  host.on('request', (req) => {
    if (req.method === method.CHANNELS_REGISTER) {
      const p = req.params as ChannelsRegisterParams;
      registered.push(...p.channels);
      host.sendResponse(req.id, { results: p.channels.map((c) => ({ id: c.id, accepted: true })) });
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      incoming.push(...p.messages);
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: true })) });
    } else if (req.method === method.PUSH_EVENT) {
      pushed.push(req.params as PushEventParams);
      host.sendResponse(req.id, { accepted: true });
    } else if (req.method === method.CHANNELS_CHANGED) {
      const p = req.params as Run['changed'][number];
      changed.push(p);
      const answer = () => { try { host.sendResponse(req.id, { results: (p.added ?? []).map((c) => ({ id: c.id, accepted: true })) }); } catch { /* closed */ } };
      if (holding) held.push(answer); else answer();
    } else {
      host.sendError(req.id, -32601, `unexpected ${req.method}`);
    }
  });
  const served = server.serve(McplConnection.fromStreams(toServer, toHost));
  await host.sendRequest(method.INITIALIZE, {
    protocolVersion: '2025-03-26',
    capabilities: mcpl ? { experimental: { mcpl: { version: '0.5', channels: true } } } : {},
    clientInfo: { name: 'test-host', version: '0' },
  });
  host.sendNotification('notifications/initialized');
  if (mcpl) {
    await host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
    await until(() => registered.length > 0 && server.isLive, 'registration and live delivery');
  }
  return {
    server, host, incoming, pushed, changed, logs,
    answerChanged() { for (const answer of held.splice(0)) answer(); },
    releaseChanged() { holding = false; for (const answer of held.splice(0)) answer(); },
    async open(channelId, history) {
      return (await host.sendRequest(method.CHANNELS_OPEN, { channelId, type: 'zulip', address: {}, ...(history ? { history } : {}) })) as { channel?: ChannelDescriptor; history?: IncomingChannelMessage[] };
    },
    async tool(name, args) {
      const result = (await host.sendRequest('tools/call', { name, arguments: args })) as { content: { text: string }[]; isError?: boolean };
      return { text: result.content.map((c) => c.text).join('\n'), isError: result.isError === true };
    },
    async close() {
      try {
        toServer.end();
        await served;
        await server.shutdown();
        host.close();
      } finally {
        filters.stop();
        console.error = consoleError;
      }
    },
  };
}

function writeFilters(dir: string, filters: Record<string, unknown>): void {
  writeFileSync(join(dir, 'filters.json'), JSON.stringify(filters));
}

function readJson(dir: string, file: string): Record<string, unknown> | null {
  const path = join(dir, file);
  return existsSync(path) ? JSON.parse(readFileSync(path, 'utf-8')) as Record<string, unknown> : null;
}

async function untilAsync(predicate: () => Promise<boolean>, what: string, ms = 3000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 10));
  }
}

/** What `channel_missed` counts for a channel; null when it tracks none. */
async function missedOn(run: Run, channelId: string): Promise<number | null> {
  const answer = await run.tool('channel_missed', { channel: channelId });
  if (answer.isError) return null;
  const tally = JSON.parse(answer.text) as { tracked: boolean; missedMessages?: number };
  return tally.tracked ? tally.missedMessages ?? 0 : null;
}

const textOf = (m: { content: unknown[] }) => (m.content as { type: string; text?: string }[]).map((c) => c.text ?? '').join('\n');

/** A run against its own state directory, closed and removed whatever happens. */
function scenario(name: string, body: (dir: string) => Promise<void>): void {
  test(name, async () => {
    const dir = mkdtempSync(join(tmpdir(), 'zulip-rename-'));
    runLogs = [];
    try {
      await body(dir);
    } catch (error) {
      report(runLogs.join('\n'));
      throw error;
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
  });
}

/** Rename stream 17 from #general to #lobby while the host holds zulip:general open, and let the host take it. */
async function renameWhileOpen(r: Realm, run: Run): Promise<void> {
  await run.open('zulip:general');
  r.rename(17, 'lobby');
  await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the host told of the rename');
}

// ── 1, 2, 4, 5, 10, 11, 12: a rename, an archive, a reused name, in one run ──

scenario('1. a stream renamed while its channel is open: one line on the agent\'s channel, and one channels/changed', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    await renameWhileOpen(r, run);
    await until(() => run.incoming.some((m) => m.channelId === 'zulip:general' && /zulip:lobby/.test(textOf(m))), 'the line to the agent');
    const lines = run.incoming.filter((m) => m.channelId === 'zulip:general' && /zulip:lobby/.test(textOf(m)));
    assert.equal(lines.length, 1);
    assert.match(textOf(lines[0]), /zulip:general/);
    const renames = run.changed.filter((c) => c.removed?.includes('zulip:general'));
    assert.equal(renames.length, 1);
    assert.deepEqual(renames[0].removed, ['zulip:general']);
    assert.deepEqual(renames[0].added?.map((c) => c.id), ['zulip:lobby']);
  } finally { await run.close(); }
});

scenario('2. after the rename, a message is missed for the new id, and its open with history since last seen returns it and nothing older', async (dir) => {
  const r = realm({ 17: 'general' });
  r.post(17, 'before the run');
  const run = await start(r, dir);
  try {
    await run.open('zulip:general');
    const seen = r.post(17, 'seen while open');
    await until(() => run.incoming.some((m) => m.messageId === String(seen)), 'delivery on the open channel');
    r.rename(17, 'lobby');
    await untilAsync(async () => (await missedOn(run, 'zulip:lobby')) === 0, 'the host\'s answer taken: zulip:lobby counts from the watermark');
    const missed = r.post(17, 'missed after the rename');
    await untilAsync(async () => (await missedOn(run, 'zulip:lobby')) === 1, 'the message counted as missed for zulip:lobby');
    const opened = await run.open('zulip:lobby', { limit: 50, sinceLastSeen: true });
    assert.deepEqual(opened.history?.map((m) => m.messageId), [String(missed)]);
  } finally { await run.close(); }
});

const origin = (p: PushEventParams) => p.origin as { messageId: string; mcplChannelId: string };
const payloadText = (p: PushEventParams) => ((p.payload as { content?: { text?: string }[] }).content ?? []).map((c) => c.text ?? '').join('\n');
const named = (run: Run, id: number) => [...run.incoming.map((m) => m.messageId), ...run.pushed.map((p) => origin(p).messageId)].includes(String(id))
  || run.pushed.some((p) => payloadText(p).includes(`id=${id}]`));

/** The host's answer to a rename of a channel it had open has landed: the new id counts what it misses. */
async function tookRename(run: Run, newId = 'zulip:lobby'): Promise<void> {
  await untilAsync(async () => (await missedOn(run, newId)) !== null, `the host's answer taken for ${newId}`);
}

function publish(run: Run, channelId: string, text: string): Promise<unknown> {
  return run.host.sendRequest(method.CHANNELS_PUBLISH, { conversationId: '', channelId, content: [{ type: 'text', text }] });
}

scenario('3. a muted stream renamed stays muted, and the filters file holds the new name', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  writeFilters(dir, { mutedStreams: ['general'] });
  const run = await start(r, dir);
  try {
    r.rename(17, 'lobby');
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["lobby"]', 'the filters file rewritten');
    const muted = r.post(17, '@bot here', { mentioned: true });
    const control = r.post(18, '@bot there', { mentioned: true });
    await until(() => named(run, control), 'the control mention');
    assert.equal(named(run, muted), false);
  } finally { await run.close(); }
});

scenario('3. an allowlisted stream renamed stays admitted, and the filters file holds the new name', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  writeFilters(dir, { streams: ['general', 'random'] });
  const run = await start(r, dir);
  try {
    r.rename(17, 'lobby');
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.streams) === '["lobby","random"]', 'the filters file rewritten');
    const admitted = r.post(17, '@bot here', { mentioned: true });
    await until(() => named(run, admitted), 'the renamed stream\'s mention');
  } finally { await run.close(); }
});

scenario('3. a stream whose reactions are visible, renamed, keeps them visible: the filters file holds the new channel id', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  writeFilters(dir, { reactionChannels: ['zulip:general', 'zulip:random'] });
  const run = await start(r, dir);
  try {
    r.rename(17, 'lobby');
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.reactionChannels) === '["zulip:lobby","zulip:random"]', 'the filters file rewritten');
  } finally { await run.close(); }
});

scenario('4. an open or a publish of the old id is refused with the new id in its words, and no stream is created', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    await renameWhileOpen(r, run);
    await tookRename(run);
    await assert.rejects(run.open('zulip:general'), /zulip:lobby/);
    await assert.rejects(publish(run, 'zulip:general', 'hello?'), /zulip:lobby/);
    assert.deepEqual(r.created, []);
    assert.equal(r.sends.length, 0);
  } finally { await run.close(); }
});

scenario('5. a new stream that takes the old name arrives under the old id, with none of the renamed stream\'s state', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    await renameWhileOpen(r, run);
    await tookRename(run);
    const reused = r.create('general');
    const first = r.post(reused, '@bot new here', { mentioned: true });
    await until(() => run.pushed.some((p) => origin(p).messageId === String(first)), 'its first message');
    assert.equal(origin(run.pushed.find((p) => origin(p).messageId === String(first))!).mcplChannelId, 'zulip:general');
    assert.equal(await missedOn(run, 'zulip:general'), null, 'the renamed stream\'s tally stays with zulip:lobby');
    assert.equal(await missedOn(run, 'zulip:lobby'), 0);
  } finally { await run.close(); }
});

scenario('6. renamed while the server was down: the catch-up starts at the carried watermark, and the mute still holds', async (dir) => {
  const r = realm({ 17: 'general', 18: 'ops' });
  writeFilters(dir, { mutedStreams: ['ops'] });
  let run = await start(r, dir);
  let seen = 0;
  try {
    await run.open('zulip:general');
    seen = r.post(17, 'seen');
    await until(() => named(run, seen), 'delivery before the stop');
  } finally { await run.close(); }
  r.rename(17, 'lobby');
  r.rename(18, 'ops2');
  const missed = r.post(17, '@bot while you were down', { mentioned: true });
  const muted = r.post(18, '@bot muted', { mentioned: true });
  const readsBefore = r.reads.length;
  run = await start(r, dir);
  try {
    await until(() => named(run, missed), 'the catch-up of the missed mention');
    const catchUp = r.reads.slice(readsBefore).find((q) => JSON.stringify(q.narrow).includes('17'));
    assert.ok(catchUp, 'stream 17 is read by its id');
    assert.equal(Number(catchUp!.anchor), seen, 'from the carried watermark');
    assert.equal(named(run, muted), false);
    assert.deepEqual(readJson(dir, 'filters.json')?.mutedStreams, ['ops2']);
  } finally { await run.close(); }
});

scenario('7. a state file as main writes it carries by name; an entry whose name no stream has is dropped with one log line', async (dir) => {
  const r = realm({ 17: 'general' });
  const seen = r.post(17, 'old news');
  writeFileSync(join(dir, 's.delivery.json'), JSON.stringify({ watermarks: { 'zulip:general': seen, 'zulip:gone': 5 }, missed: {}, lastOpen: ['zulip:general'] }));
  const missed = r.post(17, '@bot new', { mentioned: true });
  const run = await start(r, dir);
  try {
    await until(() => named(run, missed), 'the catch-up from the carried watermark');
    assert.equal(named(run, seen), false);
    assert.equal(run.logs.filter((l) => l.includes('zulip:gone')).length, 1);
  } finally { await run.close(); }
});

scenario('8. the state file this release writes keeps main\'s keys for watermarks, missed and lastOpen', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir);
  try {
    await run.open('zulip:general');
    await run.open('zulip:random');
    const first = r.post(17, 'first');
    const other = r.post(18, 'other');
    await until(() => named(run, first) && named(run, other), 'delivery');
    await run.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    const ambient = r.post(17, 'after the close');
    await untilAsync(async () => (await missedOn(run, 'zulip:general')) === 1, `${ambient} tallied`);
  } finally { await run.close(); }
  const state = readJson(dir, 's.delivery.json')!;
  assert.deepEqual(Object.keys(state.watermarks as object).sort(), ['zulip:general', 'zulip:random']);
  assert.deepEqual(Object.keys(state.missed as object), ['zulip:general']);
  assert.deepEqual(state.lastOpen, ['zulip:random']);
});

scenario('9. a stream named like a direct-message id is not served, and zulip:dm:42 publishes to user 42', async (dir) => {
  const r = realm({ 17: 'general', 42: 'dm:42' });
  const run = await start(r, dir);
  try {
    const listed = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    assert.equal(listed.channels.some((c) => (c.address as { stream_id?: unknown } | undefined)?.stream_id === 42), false);
    const hidden = r.post(42, '@bot hidden', { mentioned: true });
    const control = r.post(17, '@bot here', { mentioned: true });
    await until(() => named(run, control), 'the control mention');
    assert.equal(named(run, hidden), false);
    await publish(run, 'zulip:dm:42', 'hello');
    const sent = r.sends[r.sends.length - 1];
    assert.notEqual(sent.type, 'stream');
    assert.deepEqual(sent.to, [42]);
  } finally { await run.close(); }
});

scenario('10. a stream archived while its channel is open: a line to the agent, and the host gets the id removed', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir);
  try {
    await run.open('zulip:general');
    r.archive(17);
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the removal');
    assert.deepEqual(run.changed.find((c) => c.removed?.includes('zulip:general'))!.added ?? [], []);
    await until(() => run.incoming.some((m) => m.channelId === 'zulip:general' && /archived/.test(textOf(m))), 'the line to the agent');
  } finally { await run.close(); }
});

scenario('11. until the host answers the rename, an addressed message goes out under the old id, nothing goes out under the new one, and a new stream under the old name waits', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the held announcement');
    const addressed = r.post(17, '@bot meanwhile', { mentioned: true });
    await until(() => run.incoming.some((m) => m.messageId === String(addressed)), 'the addressed message');
    assert.equal(run.incoming.find((m) => m.messageId === String(addressed))!.channelId, 'zulip:general');
    await run.tool('refresh_channels', {});
    const ids = [
      ...run.incoming.map((m) => m.channelId),
      ...run.pushed.map((p) => origin(p).mcplChannelId),
      ...run.changed.flatMap((c) => (c.added ?? []).map((d) => d.id)),
    ];
    assert.equal(ids.filter((id) => id === 'zulip:lobby').length, 1, 'only the held announcement names zulip:lobby');
    const reused = r.create('general');
    const intruder = r.post(reused, '@bot from the new #general', { mentioned: true });
    const control = r.post(17, '@bot control', { mentioned: true });
    await until(() => named(run, control), 'the control mention');
    assert.equal(named(run, intruder), false, 'a new #general sends nothing under the id the host holds for stream 17');
    const listed = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    assert.equal((listed.channels.find((c) => c.id === 'zulip:general')?.address as { stream_id?: number }).stream_id, 17, 'zulip:general stays stream 17\'s');
    run.answerChanged();
  } finally { await run.close(); }
});

scenario('12. a stream list read after a rename that still has the old name leaves the new one', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    await renameWhileOpen(r, run);
    await tookRename(run);
    r.staleList = { 17: 'general' };
    await run.tool('refresh_channels', {});
    assert.equal(r.staleList, null, 'the stale list was read');
    await assert.rejects(run.open('zulip:general'), /zulip:lobby/);
    assert.equal((await run.open('zulip:lobby')).channel?.id, 'zulip:lobby');
  } finally { await run.close(); }
});

/** A run with MCPL that saves stream 17's name as #general, then stops. */
async function savedAsGeneral(r: Realm, dir: string): Promise<void> {
  const run = await start(r, dir);
  try {
    await run.open('zulip:general');
  } finally { await run.close(); }
}

scenario('13. ZULIP_SUBSCRIBE with a former name creates no stream and writes one log line', async (dir) => {
  const r = realm({ 17: 'general' });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  const subscribedBefore = r.subscribed.length;
  const run = await start(r, dir, { subscribe: 'general' });
  try {
    await until(() => run.logs.some((l) => l.includes('ZULIP_SUBSCRIBE') && l.includes('#general')), 'the log line');
    assert.equal(run.logs.filter((l) => l.includes('ZULIP_SUBSCRIBE')).length, 1);
    assert.deepEqual(r.created, []);
    assert.deepEqual(r.subscribed.slice(subscribedBefore), []);
  } finally { await run.close(); }
});

scenario('14. ZULIP_SUBSCRIBE with a current name subscribes the bot after a start', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir, { subscribe: 'random' });
  try {
    await until(() => r.subscribed.includes('random'), 'the subscription');
    assert.deepEqual(r.created, []);
  } finally { await run.close(); }
});

scenario('15. a stream open at last sight and renamed while the server was down gets one push from the new channel; the next start sends none', async (dir) => {
  const r = realm({ 17: 'general' });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  const told = (run: Run) => run.pushed.filter((p) => /zulip:general/.test(payloadText(p)) && /zulip:lobby/.test(payloadText(p)));
  let run = await start(r, dir);
  try {
    await until(() => told(run).length > 0, 'the push');
    assert.equal(told(run).length, 1);
    assert.equal(origin(told(run)[0]).mcplChannelId, 'zulip:lobby');
  } finally { await run.close(); }
  run = await start(r, dir);
  try {
    const control = r.post(17, '@bot control', { mentioned: true });
    await until(() => named(run, control), 'the control mention');
    assert.equal(told(run).length, 0);
  } finally { await run.close(); }
});

scenario('16. a publish and a read to a name no stream has go to Zulip by that name, and the caller gets Zulip\'s refusal', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    const published = await publish(run, 'zulip:nowhere', 'hello?').then((v) => JSON.stringify(v), (e: Error) => e.message);
    assert.match(published, /does not exist/);
    assert.equal(r.sends[r.sends.length - 1].to, 'nowhere');
    const read = await run.tool('fetch_history', { channel: 'nowhere' });
    assert.equal(read.isError, true);
    assert.match(read.text, /unknown channel/);
    assert.match(JSON.stringify(r.reads[r.reads.length - 1].narrow), /nowhere/);
  } finally { await run.close(); }
});

scenario('17. a client without MCPL: ZULIP_SUBSCRIBE subscribes a current name, holds back a former name where a copy was saved, and subscribes by the name where none was', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  let run = await start(r, dir, { mcpl: false, subscribe: 'random' });
  try {
    await until(() => r.subscribed.includes('random'), 'the current name subscribed');
  } finally { await run.close(); }
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  run = await start(r, dir, { mcpl: false, subscribe: 'general' });
  try {
    await until(() => run.logs.some((l) => l.includes('ZULIP_SUBSCRIBE') && l.includes('#general')), 'the log line');
    assert.deepEqual(r.created, []);
  } finally { await run.close(); }
  const fresh = mkdtempSync(join(tmpdir(), 'zulip-rename-nocopy-'));
  try {
    run = await start(r, fresh, { mcpl: false, subscribe: 'general' });
    try {
      await until(() => r.created.includes('general'), 'a subscribe by the name, as main does');
    } finally { await run.close(); }
  } finally { rmSync(fresh, { recursive: true, force: true }); }
});

scenario('18. a run without MCPL after a rename leaves the filters file and the copy on disk as they were, and its tools use Zulip\'s own names', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { mutedStreams: ['general'] });
  const id = r.post(17, 'hello');
  await savedAsGeneral(r, dir);
  const filtersBefore = readFileSync(join(dir, 'filters.json'), 'utf-8');
  const stateBefore = readFileSync(join(dir, 's.delivery.json'), 'utf-8');
  r.rename(17, 'lobby');
  const run = await start(r, dir, { mcpl: false });
  try {
    const around = JSON.parse((await run.tool('fetch_around', { message_id: id })).text) as { channel: string; channelId: string; formatted_history: string };
    assert.equal(around.channel, 'lobby');
    assert.equal(around.channelId, 'zulip:lobby');
    assert.match(around.formatted_history, /#lobby/);
    await run.tool('fetch_history', { channel: 'general' });
    assert.deepEqual(r.reads[r.reads.length - 1].narrow, [['stream', 'general']]);
  } finally { await run.close(); }
  assert.equal(readFileSync(join(dir, 'filters.json'), 'utf-8'), filtersBefore);
  assert.equal(readFileSync(join(dir, 's.delivery.json'), 'utf-8'), stateBefore);
});

scenario('19. a client without MCPL between the rename and the next host leaves the push and the filters to that host', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { mutedStreams: ['general'] });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  let run = await start(r, dir, { mcpl: false });
  await run.close();
  run = await start(r, dir);
  try {
    await until(() => run.pushed.some((p) => /zulip:general/.test(payloadText(p)) && /zulip:lobby/.test(payloadText(p))), 'the push');
    assert.deepEqual(readJson(dir, 'filters.json')?.mutedStreams, ['lobby']);
  } finally { await run.close(); }
});

scenario('20. a filters entry that matches no stream is named in the log when the directory is first filled', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { mutedStreams: ['nowhere'] });
  const run = await start(r, dir);
  try {
    await until(() => run.logs.some((l) => l.includes('mutedStreams') && l.includes('#nowhere')), 'the log line');
  } finally { await run.close(); }
});

scenario('21. a ZULIP_BACKSCROLL_CHANNELS entry under a former name is named in the log', async (dir) => {
  const r = realm({ 17: 'general' });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  const run = await start(r, dir, { backscroll: { general: 5 } });
  try {
    await until(() => run.logs.some((l) => l.includes('ZULIP_BACKSCROLL_CHANNELS') && l.includes('#general')), 'the log line');
  } finally { await run.close(); }
});

/** Where a stream's mention reached the host: the channel id it came on, or null. */
function arrivedOn(run: Run, id: number): string | null {
  const incoming = run.incoming.find((m) => m.messageId === String(id));
  if (incoming) return incoming.channelId;
  const pushed = run.pushed.find((p) => origin(p).messageId === String(id));
  return pushed ? origin(pushed).mcplChannelId : null;
}

scenario('22. two streams swap names while the host holds its answers: it never holds one id for two streams, and each stream ends on its own new id', async (dir) => {
  const r = realm({ 17: 'a', 18: 'b' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:a');
    await run.open('zulip:b');
    r.rename(17, 'tmp');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:a')), 'the first announcement');
    r.rename(18, 'a');
    r.rename(17, 'b');
    await new Promise((res) => setTimeout(res, 800));
    run.releaseChanged();
    await run.tool('refresh_channels', {});
    await new Promise((res) => setTimeout(res, 800));
    const held = new Map<string, number | undefined>([['zulip:a', 17], ['zulip:b', 18]]);
    for (const c of run.changed) {
      for (const id of c.removed ?? []) held.delete(id);
      for (const d of c.added ?? []) {
        const stream = (d.address as { stream_id?: number }).stream_id;
        assert.ok(!held.has(d.id) || held.get(d.id) === stream, `${d.id} added for stream ${stream} while the host holds it for stream ${held.get(d.id)}`);
        held.set(d.id, stream);
      }
    }
    const listed = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    assert.deepEqual(listed.channels.map((c) => [c.id, (c.address as { stream_id?: number }).stream_id]).sort(), [['zulip:a', 18], ['zulip:b', 17]]);
    const lineOn = (held: string, fresh: string) => run.incoming.some((m) => m.channelId === held && textOf(m).includes(`Its channel is now ${fresh}`));
    assert.ok(lineOn('zulip:a', 'zulip:b'), 'stream 17\'s open channel was told of zulip:b');
    assert.ok(lineOn('zulip:b', 'zulip:a'), 'stream 18\'s open channel was told of zulip:a');
    const to17 = r.post(17, '@bot to 17', { mentioned: true });
    const to18 = r.post(18, '@bot to 18', { mentioned: true });
    await until(() => arrivedOn(run, to17) !== null && arrivedOn(run, to18) !== null, 'both mentions');
    assert.equal(arrivedOn(run, to17), 'zulip:b');
    assert.equal(arrivedOn(run, to18), 'zulip:a');
  } finally { run.answerChanged(); await run.close(); }
});

scenario('23. two streams swap names while the server is down: each keeps its own filters entry, and each gets its push', async (dir) => {
  const r = realm({ 17: 'a', 18: 'b' });
  writeFilters(dir, { mutedStreams: ['b'] });
  let run = await start(r, dir);
  try {
    await run.open('zulip:a');
    await run.open('zulip:b');
  } finally { await run.close(); }
  r.rename(17, 'tmp');
  r.rename(18, 'a');
  r.rename(17, 'b');
  run = await start(r, dir);
  try {
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["a"]', 'the mute on stream 18 under its new name');
    await until(() => run.pushed.some((p) => origin(p).mcplChannelId === 'zulip:a') && run.pushed.some((p) => origin(p).mcplChannelId === 'zulip:b'), 'a push from each new channel');
    const pushText = (channelId: string) => run.pushed.filter((p) => origin(p).mcplChannelId === channelId).map((p) => JSON.stringify(p)).join('\n');
    assert.match(pushText('zulip:b'), /zulip:a now belongs to another stream, which took the name #a\./, 'stream 17\'s push says its old id is another stream\'s now');
    assert.match(pushText('zulip:a'), /zulip:b now belongs to another stream, which took the name #b\./, 'stream 18\'s push says its old id is another stream\'s now');
    assert.doesNotMatch(pushText('zulip:a') + pushText('zulip:b'), /is gone/);
    const to17 = r.post(17, '@bot to 17', { mentioned: true });
    const to18 = r.post(18, '@bot to 18', { mentioned: true });
    await until(() => arrivedOn(run, to17) !== null, 'the mention in 17');
    assert.equal(arrivedOn(run, to17), 'zulip:b');
    assert.equal(named(run, to18), false, 'stream 18 is still muted');
  } finally { await run.close(); }
});

scenario('24. renamed while the server was down, beside a new stream that took the old name: the rename is seen, the mute follows it, and the push goes', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { mutedStreams: ['general'] });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  const reused = r.create('general');
  const run = await start(r, dir);
  try {
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["lobby"]', 'the mute under the new name');
    await until(() => run.logs.some((l) => l.includes('Zulip stream 17 was renamed #general -> #lobby')), 'the rename in the log');
    const fromNew = r.post(reused, '@bot from the new #general', { mentioned: true });
    await until(() => arrivedOn(run, fromNew) !== null, 'the new #general\'s mention');
    assert.equal(arrivedOn(run, fromNew), 'zulip:general');
  } finally { await run.close(); }
});

/**
 * What a host holds after it applies each `channels/changed` in order, from
 * `initial`: each channel id and its stream. A removal comes before an
 * addition, and an added id replaces what the host held under it. `skip`
 * leaves out one change, as a host that never received it.
 */
function hostView(run: Run, initial: Record<string, number>, skip?: number): Record<string, number> {
  const held = new Map(Object.entries(initial));
  run.changed.forEach((c, i) => {
    if (i === skip) return;
    for (const id of c.removed ?? []) held.delete(id);
    for (const d of c.added ?? []) held.set(d.id, (d.address as { stream_id: number }).stream_id);
  });
  return Object.fromEntries([...held].sort(([x], [y]) => (x < y ? -1 : 1)));
}

/** What `channels/list` answers: each id and its stream. */
async function listed(run: Run): Promise<Record<string, number>> {
  const answer = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
  return Object.fromEntries(answer.channels.map((c) => [c.id, (c.address as { stream_id: number }).stream_id] as const).sort(([x], [y]) => (x < y ? -1 : 1)));
}

/** Wait until `read` gives `expected`; past the bound, fail on what it gives then. */
async function settles<T>(read: () => T | Promise<T>, expected: T, what: string, ms = 3000): Promise<void> {
  const deadline = Date.now() + ms;
  for (;;) {
    const actual = await read();
    try {
      assert.deepEqual(actual, expected, what);
      return;
    } catch (error) {
      if (Date.now() > deadline) throw error;
    }
    await new Promise((r) => setTimeout(r, 10));
  }
}

/** Rename stream 17 from #a to #tmp, and let the host's answer to that announcement miss its bound. Returns the announcement's index in `changed`. */
async function unansweredRename(r: Realm, run: Run): Promise<number> {
  const before = run.changed.length;
  r.rename(17, 'tmp');
  await until(() => run.logs.some((l) => l.includes('got no answer')), 'the announcement of #tmp left unanswered');
  const first = run.changed.findIndex((c, i) => i >= before && (c.added ?? []).some((d) => d.id === 'zulip:tmp'));
  assert.ok(first >= 0, 'the host was offered zulip:tmp');
  run.releaseChanged();
  return first;
}

scenario('25. two open streams swap names while the event queue is lost: the registration\'s list shows it, each id ends on its own stream, and each open channel is told that another stream took its old name', async (dir) => {
  const r = realm({ 17: 'a', 18: 'b', 19: 'other' });
  const run = await start(r, dir);
  try {
    await run.open('zulip:a');
    await run.open('zulip:b');
    r.gap(() => {
      r.rename(17, 'tmp');
      r.rename(18, 'a');
      r.rename(17, 'b');
    });
    const after = { 'zulip:a': 18, 'zulip:b': 17, 'zulip:other': 19 };
    await settles(() => hostView(run, { 'zulip:a': 17, 'zulip:b': 18, 'zulip:other': 19 }), after, 'what the host holds');
    assert.deepEqual(await listed(run), after);
    const line = (channelId: string) => run.incoming.filter((m) => m.channelId === channelId && textOf(m).includes('was renamed')).map(textOf).join('\n');
    assert.match(line('zulip:a'), /Its channel is now zulip:b, and zulip:a now belongs to another stream, which took the name #a\./);
    assert.match(line('zulip:b'), /Its channel is now zulip:a, and zulip:b now belongs to another stream, which took the name #b\./);
    const to17 = r.post(17, '@bot to 17', { mentioned: true });
    const to18 = r.post(18, '@bot to 18', { mentioned: true });
    await until(() => arrivedOn(run, to17) !== null && arrivedOn(run, to18) !== null, 'both mentions');
    assert.equal(arrivedOn(run, to17), 'zulip:b');
    assert.equal(arrivedOn(run, to18), 'zulip:a');
  } finally { await run.close(); }
});

scenario('26. a rename whose announcement gets no answer, then a second rename: the host ends holding the newest id alone, whether or not it applied the first', async (dir) => {
  const r = realm({ 17: 'a' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:a');
    const first = await unansweredRename(r, run);
    r.rename(17, 'b');
    await settles(() => hostView(run, { 'zulip:a': 17 }), { 'zulip:b': 17 }, 'a host that applied the unanswered announcement');
    assert.deepEqual(hostView(run, { 'zulip:a': 17 }, first), { 'zulip:b': 17 }, 'a host that never received it');
    assert.deepEqual(await listed(run), { 'zulip:b': 17 });
    const to17 = r.post(17, '@bot to 17', { mentioned: true });
    await until(() => arrivedOn(run, to17) !== null, 'the mention');
    assert.equal(arrivedOn(run, to17), 'zulip:b');
  } finally { run.answerChanged(); await run.close(); }
});

scenario('27. a rename whose announcement gets no answer, then a rename back: the host is told the old id again and ends holding it alone, whether or not it applied the first, and the agent gets no line for it', async (dir) => {
  const r = realm({ 17: 'a' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:a');
    const first = await unansweredRename(r, run);
    r.rename(17, 'a');
    await until(() => run.changed.slice(first + 1).some((c) => (c.added ?? []).some((d) => d.id === 'zulip:a')), 'the host told zulip:a again');
    await settles(() => hostView(run, { 'zulip:a': 17 }), { 'zulip:a': 17 }, 'a host that applied the unanswered announcement');
    assert.deepEqual(hostView(run, { 'zulip:a': 17 }, first), { 'zulip:a': 17 }, 'a host that never received it');
    assert.deepEqual(await listed(run), { 'zulip:a': 17 });
    assert.equal(run.incoming.some((m) => textOf(m).includes('was renamed #a.')), false, 'no line for an id that did not change');
    const to17 = r.post(17, '@bot to 17', { mentioned: true });
    await until(() => arrivedOn(run, to17) !== null, 'the mention');
    assert.equal(arrivedOn(run, to17), 'zulip:a');
  } finally { run.answerChanged(); await run.close(); }
});
