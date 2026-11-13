import test from 'node:test';
import assert from 'node:assert/strict';
import { PassThrough } from 'node:stream';
import { existsSync, mkdtempSync, readFileSync, rmSync, utimesSync, writeFileSync } from 'node:fs';
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
import { streamKey } from '../src/channel-key.ts';
import { realm, SELF, type Realm } from './zulip-realm.ts';

const directoryModule = await import('../src/stream-directory.ts').catch(() => null);
const clientModule = (await import('../src/zulip-client.ts')) as Record<string, unknown>;
type Subscribe = (api: Realm['api'], directory: unknown, setting: string | undefined) => Promise<void>;
const subscribeConfigured = clientModule.subscribeConfigured as Subscribe | undefined;

const report = console.error;
let runLogs: string[] = [];

const GRANT = [
  'tools', 'pushEvents', 'channels.acknowledge', 'channels.streaming', 'channels.register', 'channels.lifecycle',
  'channels.publish', 'channels.incoming', 'channels.typing', 'contextHooks.beforeInference.inject.beforeUser',
];

interface Run {
  server: ZulipMcplServer;
  filters: FiltersPlane;
  host: McplConnection;
  serverSide: McplConnection;
  incoming: IncomingChannelMessage[];
  pushed: PushEventParams[];
  changed: { removed?: string[]; added?: ChannelDescriptor[] }[];
  registered: ChannelDescriptor[];
  logs: string[];
  subscribing: Promise<void>;
  directory: { isFilled: boolean; nameOf(streamId: number): string | undefined } | undefined;
  answerChanged(): void;
  releaseChanged(): void;
  grant(): Promise<void>;
  open(channelId: string, history?: Record<string, unknown>): Promise<{ channel?: ChannelDescriptor; history?: IncomingChannelMessage[] }>;
  tool(name: string, args: Record<string, unknown>): Promise<{ text: string; isError: boolean }>;
  close(): Promise<void>;
  end(): Promise<void>;
}

async function until(predicate: () => boolean, what: string, ms = 3000): Promise<void> {
  const deadline = Date.now() + ms;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((r) => setTimeout(r, 5));
  }
}

function wireSubscribe(r: Realm, directory: unknown, setting: string | undefined): Promise<void> {
  return subscribeConfigured && directory ? subscribeConfigured(r.api, directory, setting) : Promise.resolve();
}

interface StartOptions {
  mcpl?: boolean;
  subscribe?: string;
  holdChanged?: boolean;
  announceTimeoutMs?: number;
  backscroll?: Record<string, number>;
  reuse?: Run;
  beforePushAnswer?: (push: PushEventParams, host: McplConnection) => Promise<void>;
  /** Send the host's policy now but don't wait for live delivery. */
  noWait?: boolean;
  /** Hold the host's policy until `grant()`. */
  grantLater?: boolean;
  /** Record channels/register but never answer it. */
  holdRegister?: boolean;
  beforeRegisterAnswer?: () => Promise<void>;
  shutdownGraceMs?: number;
}

async function start(r: Realm, dir: string, opts: StartOptions = {}): Promise<Run> {
  const mcpl = opts.mcpl ?? true;
  const logs: string[] = [];
  runLogs.push(`--- run ${opts.mcpl === false ? 'without MCPL' : 'with MCPL'}`);
  const consoleError = console.error;
  console.error = (...args: unknown[]) => { const line = args.map(String).join(' '); logs.push(line); runLogs.push(line); };
  const filters = opts.reuse?.filters ?? new FiltersPlane(join(dir, 'filters.json'));
  if (!opts.reuse) filters.start();
  const directory = opts.reuse ? opts.reuse.directory : directoryModule ? new directoryModule.StreamDirectory() : undefined;
  const subscribing = opts.reuse ? Promise.resolve() : wireSubscribe(r, directory, opts.subscribe);
  const backscrollLimits = new Map(Object.entries(opts.backscroll ?? {}));
  const adapter = new ZulipAdapter(r.api, { selfUserId: SELF, sessionId: 's' }, { directory, filters, backscrollLimits, dmDiscoveryLimit: 0, formatTime: () => 'T' } as ConstructorParameters<typeof ZulipAdapter>[3]);
  const tools = new ZulipToolRuntime({ api: r.api, selfUserId: SELF, realm: '', authHeader: '', sessionId: 's' }, dir, { formatTime: () => 'T' });
  const server = opts.reuse?.server ?? new ZulipMcplServer(adapter, tools, {
    serverInfo: { name: 'zulip-mcp-test', version: '0.0.0' },
    mcplEnabled: true,
    batchWindowMs: 5,
    contextHistorySize: 3,
    stateDir: dir,
    sessionId: 's',
    catchupLimit: 100,
    formatTime: () => 'T',
    announceTimeoutMs: opts.announceTimeoutMs ?? 300,
    shutdownGraceMs: opts.shutdownGraceMs,
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
      if (!opts.holdRegister) {
        void Promise.resolve(opts.beforeRegisterAnswer?.()).then(() => {
          try { host.sendResponse(req.id, { results: p.channels.map((c) => ({ id: c.id, accepted: true })) }); } catch { /* closed */ }
        });
      }
    } else if (req.method === method.CHANNELS_INCOMING) {
      const p = req.params as ChannelsIncomingParams;
      incoming.push(...p.messages);
      host.sendResponse(req.id, { results: p.messages.map((m) => ({ messageId: m.messageId, accepted: true })) });
    } else if (req.method === method.PUSH_EVENT) {
      pushed.push(req.params as PushEventParams);
      void Promise.resolve(opts.beforePushAnswer?.(req.params as PushEventParams, host)).then(() => host.sendResponse(req.id, { accepted: true }));
    } else if (req.method === method.CHANNELS_CHANGED) {
      const p = req.params as Run['changed'][number];
      changed.push(p);
      const answer = () => { try { host.sendResponse(req.id, { results: (p.added ?? []).map((c) => ({ id: c.id, accepted: true })) }); } catch { /* closed */ } };
      if (holding) held.push(answer); else answer();
    } else {
      host.sendError(req.id, -32601, `unexpected ${req.method}`);
    }
  });
  const serverSide = McplConnection.fromStreams(toServer, toHost);
  const served = server.serve(serverSide);
  await host.sendRequest(method.INITIALIZE, {
    protocolVersion: '2025-03-26',
    capabilities: mcpl ? { experimental: { mcpl: { version: '0.5', channels: true } } } : {},
    clientInfo: { name: 'test-host', version: '0' },
  });
  host.sendNotification('notifications/initialized');
  const grant = async () => {
    await host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
    await until(() => registered.length > 0, 'registration');
    await server.quiet();
  };
  if (mcpl && !opts.grantLater) {
    if (opts.noWait) await host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
    else await grant();
  }
  return {
    server, filters, host, serverSide, incoming, pushed, changed, registered, logs, subscribing, directory, grant,
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
    async end() {
      try {
        toServer.end();
        await served;
        host.close();
      } finally {
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

async function missedOn(run: Run, channelId: string): Promise<number | null> {
  const answer = await run.tool('channel_missed', { channel: channelId });
  if (answer.isError) return null;
  const tally = JSON.parse(answer.text) as { tracked: boolean; missedMessages?: number };
  return tally.tracked ? tally.missedMessages ?? 0 : null;
}

const textOf = (m: { content: unknown[] }) => (m.content as { type: string; text?: string }[]).map((c) => c.text ?? '').join('\n');

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

async function renameWhileOpen(r: Realm, run: Run): Promise<void> {
  await run.open('zulip:general');
  r.rename(17, 'lobby');
  await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the host told of the rename');
}

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
const deliveriesSettled = (run: Run) => run.server.quiet();

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
    await deliveriesSettled(run);
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
  const reactionOn = (messageId: number) => run.pushed.find((p) => payloadText(p).includes(`[reaction] Ann reacted :thumbs_up: on message ${messageId}`));
  try {
    const before = r.post(17, 'before the rename');
    r.react(before, 'thumbs_up', '1f44d');
    await until(() => reactionOn(before) !== undefined, 'the reaction before the rename');
    assert.equal(origin(reactionOn(before)!).mcplChannelId, 'zulip:general');
    r.rename(17, 'lobby');
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.reactionChannels) === '["zulip:lobby","zulip:random"]', 'the filters file rewritten');
    const after = r.post(17, 'after the rename');
    r.react(after, 'thumbs_up', '1f44d');
    await until(() => reactionOn(after) !== undefined, 'the reaction after the rename');
    assert.equal(origin(reactionOn(after)!).mcplChannelId, 'zulip:lobby');
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
    await run.open('zulip:general');
    const seen = r.post(17, 'seen while open');
    await until(() => run.incoming.some((m) => m.messageId === String(seen)), 'delivery on the open channel');
    assert.equal((await run.tool('mute_channel', { channel: 'zulip:general' })).isError, false);
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["general"]', 'the mute on stream 17');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the host told of the rename');
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["lobby"]', 'the mute under the new name');
    await tookRename(run);
    const reused = r.create('general');
    await until(() => run.directory?.nameOf(reused) === 'general', 'the new #general in the directory');
    assert.equal(run.server.delivery.watermark(streamKey(reused)), undefined, 'the new #general has no watermark of stream 17\'s');
    assert.equal(run.server.delivery.watermark(streamKey(17)), seen, 'stream 17 keeps its own');
    const first = r.post(reused, '@bot new here', { mentioned: true });
    await until(() => run.pushed.some((p) => origin(p).messageId === String(first)), 'its first message, not muted');
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
    await deliveriesSettled(run);
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

scenario('11. until the host answers the rename, nothing of the stream goes out, nothing goes out under the new id, and a new stream under the old name waits; after the answer, the stream\'s message comes from the new id', async (dir) => {
  const r = realm({ 17: 'general', 18: 'dev' });
  const run = await start(r, dir, { holdChanged: true, announceTimeoutMs: 60_000 });
  const gaveUp = () => run.logs.some((l) => l.includes('zulip:general') && l.includes('got no answer'));
  try {
    await run.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the held announcement');
    const addressed = r.post(17, '@bot meanwhile', { mentioned: true });
    await run.tool('refresh_channels', {});
    const ids = [
      ...run.incoming.map((m) => m.channelId),
      ...run.pushed.map((p) => origin(p).mcplChannelId),
      ...run.changed.flatMap((c) => (c.added ?? []).map((d) => d.id)),
    ];
    assert.equal(ids.filter((id) => id === 'zulip:lobby').length, 1, 'only the held announcement names zulip:lobby');
    const reused = r.create('general');
    const intruder = r.post(reused, '@bot from the new #general', { mentioned: true });
    const control = r.post(18, '@bot control', { mentioned: true });
    await until(() => named(run, control), 'the control mention');
    assert.equal(gaveUp(), false, 'the announcement is still in flight when the new #general\'s message is handled');
    assert.equal(named(run, addressed), false, 'nothing of the renamed stream goes while the host has the announcement');
    assert.equal(named(run, intruder), false, 'a new #general sends nothing under the id the host holds for stream 17');
    const listed = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    assert.equal((listed.channels.find((c) => c.id === 'zulip:general')?.address as { stream_id?: number }).stream_id, 17, 'zulip:general stays stream 17\'s');
    run.answerChanged();
    await until(() => arrivedOn(run, addressed) !== null, 'the renamed stream\'s message after the answer');
    assert.equal(arrivedOn(run, addressed), 'zulip:lobby', 'from the new id, as a push while the host hasn\'t opened it');
    assert.equal(run.pushed.filter((p) => origin(p).messageId === String(addressed)).length, 1);
  } finally { run.releaseChanged(); await run.close(); }
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
    await run.subscribing;
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
    await run.subscribing;
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
    await until(() => run.directory?.isFilled === true && run.directory.nameOf(17) === 'lobby', 'the stream list read, with the rename');
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
  try {
    await until(() => run.directory?.isFilled === true && run.directory.nameOf(17) === 'lobby', 'the stream list read, with the rename');
  } finally { await run.close(); }
  assert.deepEqual(readJson(dir, 'filters.json')?.mutedStreams, ['general'], 'the run without MCPL left the filters file as it was');
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
    await until(() => run.directory?.nameOf(18) === 'a' && run.directory.nameOf(17) === 'b', 'both renames in the directory');
    await until(() => run.logs.some((l) => l.includes('changing zulip:a') && l.includes('got no answer')), 'the first announcement given up while the host holds it');
    run.releaseChanged();
    await run.tool('refresh_channels', {});
    const settledOn = async () => {
      const { channels } = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
      const a = channels.find((c) => c.id === 'zulip:a'), b = channels.find((c) => c.id === 'zulip:b');
      return (a?.address as { stream_id?: number } | undefined)?.stream_id === 18 && (b?.address as { stream_id?: number } | undefined)?.stream_id === 17;
    };
    const lineOn = (held: string, fresh: string) => run.incoming.some((m) => m.channelId === held && textOf(m).includes(`Its channel is now ${fresh}`));
    await untilAsync(async () => lineOn('zulip:a', 'zulip:b') && lineOn('zulip:b', 'zulip:a') && await settledOn(), 'each open channel told, and the host holding each stream on its new id');
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
    const after = r.post(17, '@bot after 18\'s', { mentioned: true });
    await until(() => arrivedOn(run, to17) !== null && arrivedOn(run, after) !== null, 'the mentions in 17, the second posted after 18\'s');
    await deliveriesSettled(run);
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
    await until(() => run.pushed.some((p) => origin(p).mcplChannelId === 'zulip:lobby'), 'the push from the new channel');
    const pushText = run.pushed.filter((p) => origin(p).mcplChannelId === 'zulip:lobby').map((p) => JSON.stringify(p)).join('\n');
    assert.match(pushText, /zulip:general now belongs to another stream, which took the name #general\./);
    const fromNew = r.post(reused, '@bot from the new #general', { mentioned: true });
    await until(() => arrivedOn(run, fromNew) !== null, 'the new #general\'s mention');
    assert.equal(arrivedOn(run, fromNew), 'zulip:general');
  } finally { await run.close(); }
});

function hostView(run: Run, initial: Record<string, number>, skip?: number): Record<string, number> {
  const held = new Map(Object.entries(initial));
  run.changed.forEach((c, i) => {
    if (i === skip) return;
    for (const id of c.removed ?? []) held.delete(id);
    for (const d of c.added ?? []) held.set(d.id, (d.address as { stream_id: number }).stream_id);
  });
  return Object.fromEntries([...held].sort(([x], [y]) => (x < y ? -1 : 1)));
}

async function listed(run: Run): Promise<Record<string, number>> {
  const answer = (await run.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
  return Object.fromEntries(answer.channels.map((c) => [c.id, (c.address as { stream_id: number }).stream_id] as const).sort(([x], [y]) => (x < y ? -1 : 1)));
}

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

scenario('28. a state file as main writes it: a mention in a closed stream while the server was down comes as a catch-up', async (dir) => {
  const r = realm({ 17: 'general' });
  const seen = r.post(17, 'old news');
  writeFileSync(join(dir, 's.delivery.json'), JSON.stringify({ watermarks: { 'zulip:general': seen }, missed: {}, lastOpen: [] }));
  const missed = r.post(17, '@bot while you were away', { mentioned: true });
  const run = await start(r, dir);
  try {
    await until(() => named(run, missed), 'the catch-up from the carried watermark');
    assert.equal(named(run, seen), false);
  } finally { await run.close(); }
});

scenario('29. a rename the host doesn\'t answer, offered again at a refresh: the agent gets its line once', async (dir) => {
  const r = realm({ 17: 'a' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:a');
    const before = run.changed.length;
    r.rename(17, 'b');
    await until(() => run.logs.some((l) => l.includes('got no answer')), 'the announcement of #b left unanswered');
    await run.tool('refresh_channels', {});
    await until(() => run.changed.slice(before).filter((c) => (c.added ?? []).some((d) => d.id === 'zulip:b')).length >= 2, 'the rename offered again');
    assert.equal(run.incoming.filter((m) => m.channelId === 'zulip:a' && textOf(m).includes('was renamed #b.')).length, 1);
  } finally { run.releaseChanged(); await run.close(); }
});

scenario('30. a client without MCPL whose read of the stream list is refused: ZULIP_SUBSCRIBE still subscribes', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  r.listRefusals = 1;
  const run = await start(r, dir, { mcpl: false, subscribe: 'random' });
  try {
    await until(() => r.subscribed.includes('random'), 'the subscribe');
  } finally { await run.close(); }
});

scenario('31. a stream open at last sight and renamed while the server was down: the push, then its whole backlog from the new channel, then it counts as closed', async (dir) => {
  const r = realm({ 17: 'general' });
  let run = await start(r, dir);
  try {
    await run.open('zulip:general');
    const seen = r.post(17, 'seen');
    await until(() => named(run, seen), 'delivery before the stop');
  } finally { await run.close(); }
  r.rename(17, 'lobby');
  const ambient = r.post(17, 'ambient while you were down');
  run = await start(r, dir);
  try {
    await until(() => run.pushed.some((p) => payloadText(p).includes(`id=${ambient}]`)), 'the backlog, with the ambient message');
    const fromLobby = run.pushed.filter((p) => origin(p).mcplChannelId === 'zulip:lobby');
    assert.match(payloadText(fromLobby[0]), /was renamed #lobby since its channel was last open\. Its channel is now zulip:lobby/, 'the push that says so comes first');
    assert.equal(fromLobby.length, 2);
    assert.ok(fromLobby[1].tags?.includes('zulip:missed'), 'the backlog comes as a catch-up block, which the README\'s wake policy wakes on');
    await untilAsync(async () => (await missedOn(run, 'zulip:lobby')) === 0, 'zulip:lobby counted as closed, from after its backlog');
  } finally { await run.close(); }
});

scenario('32. renamed while the server was down, and the host opens the new channel while the push waits for its answer: the stream stays open at last sight', async (dir) => {
  const r = realm({ 17: 'general' });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  let opened = false;
  const run = await start(r, dir, {
    async beforePushAnswer(push, host) {
      if (opened || !/was renamed #lobby/.test(payloadText(push))) return;
      opened = true;
      await host.sendRequest(method.CHANNELS_OPEN, { channelId: 'zulip:lobby', type: 'zulip', address: {} });
    },
  });
  try {
    await until(() => opened, 'the host\'s open of zulip:lobby');
  } finally { await run.close(); }
  assert.deepEqual(readJson(dir, 's.delivery.json')?.lastOpen, ['zulip:lobby']);
});

async function startInOrder(r: Realm, dir: string, first: 'list' | 'register', opts: StartOptions = {}): Promise<Run> {
  const before = r.registrations;
  if (first === 'list') {
    const release = r.hold('POST /register');
    const run = await start(r, dir, opts);
    release();
    await until(() => r.registrations > before, 'the queue\'s registration');
    return run;
  }
  const release = r.hold('GET /streams');
  const starting = start(r, dir, opts);
  await until(() => r.registrations > before, 'the queue\'s registration');
  release();
  return starting;
}

const toldRename = (run: Run) => run.pushed.filter((p) => /zulip:general/.test(payloadText(p)) && /zulip:lobby/.test(payloadText(p)));

for (const first of ['list', 'register'] as const) {
  const order = first === 'list' ? 'the stream list answered first' : 'the queue\'s registration answered first';

  scenario(`33. renamed while the server was down, ${order}: one push from the new channel, and the mute follows its stream`, async (dir) => {
    const r = realm({ 17: 'general', 18: 'ops' });
    writeFilters(dir, { mutedStreams: ['ops'] });
    await savedAsGeneral(r, dir);
    r.rename(17, 'lobby');
    r.rename(18, 'ops2');
    const run = await startInOrder(r, dir, first);
    try {
      await until(() => toldRename(run).length > 0, 'the push');
      assert.equal(toldRename(run).length, 1);
      assert.equal(origin(toldRename(run)[0]).mcplChannelId, 'zulip:lobby');
      await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["ops2"]', 'the mute under the new name');
    } finally { await run.close(); }
  });

  scenario(`34. one process, a second host after a rename while no host was connected, ${order}: one push from the new channel, as after a restart, and the mute follows its stream`, async (dir) => {
    const r = realm({ 17: 'general', 18: 'ops' });
    writeFilters(dir, { mutedStreams: ['ops'] });
    const before = await start(r, dir);
    try {
      await before.open('zulip:general');
    } finally { await before.end(); }
    r.rename(17, 'lobby');
    r.rename(18, 'ops2');
    const run = await startInOrder(r, dir, first, { reuse: before });
    try {
      await until(() => toldRename(run).length > 0, 'the push');
      assert.equal(toldRename(run).length, 1);
      await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["ops2"]', 'the mute under the new name');
    } finally { await run.close(); }
  });
}

scenario('35. one process: a client without MCPL between a rename and the next host leaves the push and the filters to that host', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { mutedStreams: ['general'] });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  const plain = await start(r, dir, { mcpl: false });
  try {
    await until(() => plain.directory?.isFilled === true && plain.directory.nameOf(17) === 'lobby', 'the stream list read, with the rename');
  } finally { await plain.end(); }
  assert.deepEqual(readJson(dir, 'filters.json')?.mutedStreams, ['general'], 'the client without MCPL left the filters file as it was');
  const run = await start(r, dir, { reuse: plain });
  try {
    await until(() => toldRename(run).length > 0, 'the push');
    await until(() => JSON.stringify(readJson(dir, 'filters.json')?.mutedStreams) === '["lobby"]', 'the mute under the new name');
  } finally { await run.close(); }
});

scenario('36. with MCPL, listen, unlisten and send_message by a former name are refused with the new name, and nothing goes to Zulip; by a current name a send goes by the stream\'s id', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir);
  try {
    await renameWhileOpen(r, run);
    await tookRename(run);
    const callsBefore = r.calls.length;
    const listened = await run.tool('listen', { channels: ['random', 'general'] });
    assert.match(listened.text, /#general was renamed #lobby/);
    const unlistened = await run.tool('unlisten', { channels: ['general'] });
    assert.match(unlistened.text, /#general was renamed #lobby/);
    const sent = await run.tool('send_message', { type: 'stream', to: 'general', topic: 't', content: 'hello?' });
    assert.equal(sent.isError, true);
    assert.match(sent.text, /#general was renamed #lobby/);
    assert.deepEqual(r.calls.slice(callsBefore).filter((c) => c.path === '/users/me/subscriptions' || c.path === '/messages'), [], 'nothing went to Zulip');
    assert.deepEqual(r.created, []);
    const sendsBefore = r.sends.length;
    const current = await run.tool('send_message', { type: 'stream', to: 'lobby', topic: 't', content: 'hello' });
    assert.equal(current.isError, false, current.text);
    assert.equal(r.sends[sendsBefore]?.to, 17);
  } finally { await run.close(); }
});

scenario('37. a rename the host doesn\'t answer is offered again at a listen', async (dir) => {
  const r = realm({ 17: 'a', 18: 'other' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:a');
    const before = run.changed.length;
    r.rename(17, 'b');
    await until(() => run.logs.some((l) => l.includes('got no answer')), 'the announcement of #b left unanswered');
    await run.tool('listen', { channels: ['other'] });
    await until(() => run.changed.slice(before).filter((c) => (c.added ?? []).some((d) => d.id === 'zulip:b')).length >= 2, 'the rename offered again');
  } finally { run.releaseChanged(); await run.close(); }
});

scenario('38. a ZULIP_BACKSCROLL_CHANNELS entry whose stream is renamed while the server runs is named in the log', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { backscroll: { general: 50 } });
  try {
    r.rename(17, 'lobby');
    await until(() => run.logs.some((l) => l.includes('ZULIP_BACKSCROLL_CHANNELS names #general') && l.includes('#lobby')), 'the log line');
  } finally { await run.close(); }
});

scenario('39. one process, two clients without MCPL, and a rename between them: the second reads its own stream list', async (dir) => {
  const r = realm({ 17: 'general' });
  const tallied = await start(r, dir);
  try {
    await tallied.open('zulip:general');
    await tallied.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:general' });
    await untilAsync(async () => (await missedOn(tallied, 'zulip:general')) === 0, 'stream 17 tallied');
  } finally { await tallied.close(); }
  const first = await start(r, dir, { mcpl: false });
  try {
    await until(() => first.directory?.isFilled === true, 'the first client\'s read of the stream list');
  } finally { await first.end(); }
  r.rename(17, 'lobby');
  const second = await start(r, dir, { mcpl: false, reuse: first });
  try {
    await until(() => second.directory?.isFilled === true && second.directory.nameOf(17) === 'lobby', 'the second client\'s read of the stream list');
    assert.equal(await missedOn(second, 'zulip:lobby'), 0);
  } finally { await second.close(); }
});

scenario('40. a removal the host doesn\'t answer, offered again at a refresh: the agent gets its line once', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir, { holdChanged: true });
  try {
    await run.open('zulip:general');
    const before = run.changed.length;
    r.archive(17);
    await until(() => run.logs.some((l) => l.includes('got no answer')), 'the removal left unanswered');
    await run.tool('refresh_channels', {});
    await until(() => run.changed.slice(before).filter((c) => (c.removed ?? []).includes('zulip:general')).length >= 2, 'the removal offered again');
    assert.equal(run.incoming.filter((m) => m.channelId === 'zulip:general' && /archived/.test(textOf(m))).length, 1);
  } finally { run.releaseChanged(); await run.close(); }
});

scenario('41. a stream removed, seen again, and removed again: the agent gets a line each time', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir);
  const lines = () => run.incoming.filter((m) => m.channelId === 'zulip:general' && /archived/.test(textOf(m))).length;
  const answered = () => run.logs.filter((l) => l.includes('stream 17 is gone: the host no longer holds zulip:general')).length;
  try {
    await run.open('zulip:general');
    r.archive(17);
    await until(() => lines() === 1 && answered() === 1, 'the first line, and the host\'s answer to the first removal');
    r.regain(17, 'general');
    await run.tool('refresh_channels', {});
    await run.open('zulip:general');
    r.archive(17);
    await until(() => lines() === 2 && answered() === 2, 'the second line, and the host\'s answer to the second removal');
  } finally { await run.close(); }
});

scenario('42. a reactionChannels entry that matches no stream is named in the log when the directory is first filled, and a DM\'s entry is not', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { reactionChannels: ['zulip:nowhere', 'zulip:dm:42', 'zulip:general'] });
  const run = await start(r, dir);
  try {
    await until(() => run.logs.some((l) => l.includes('reactionChannels') && l.includes('#nowhere')), 'the log line');
    assert.equal(run.logs.filter((l) => l.includes('reactionChannels') && /matches no stream/.test(l)).length, 1);
  } finally { await run.close(); }
});

scenario('43. with MCPL, the tools that write the filters refuse a former name with its new name and write nothing; a current name and a name no stream has are written', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const run = await start(r, dir);
  try {
    await renameWhileOpen(r, run);
    await tookRename(run);
    const before = readJson(dir, 'filters.json');
    for (const [tool, args] of [
      ['mute_channel', { channel: 'general' }],
      ['unmute_channel', { channel: '#general' }],
      ['filters_update', { addStreams: ['random', 'general'] }],
      ['filters_update', { removeStreams: ['zulip:general'] }],
      ['set_reaction_visibility', { channel: 'zulip:general', visible: true }],
    ] as const) {
      const answer = await run.tool(tool, args);
      assert.equal(answer.isError, true, `${tool} ${JSON.stringify(args)}`);
      assert.match(answer.text, /#general was renamed #lobby/);
    }
    assert.deepEqual(readJson(dir, 'filters.json'), before, 'nothing was written');
    assert.equal((await run.tool('mute_channel', { channel: 'lobby' })).isError, false);
    assert.equal((await run.tool('mute_channel', { channel: 'nowhere' })).isError, false);
    assert.deepEqual((readJson(dir, 'filters.json') as { mutedStreams?: string[] }).mutedStreams?.sort(), ['lobby', 'nowhere']);
  } finally { await run.close(); }
});

scenario('44. without MCPL, mute_channel by a former name that the state file knows is written as given, as on main', async (dir) => {
  const r = realm({ 17: 'general' });
  await savedAsGeneral(r, dir);
  r.rename(17, 'lobby');
  const run = await start(r, dir, { mcpl: false });
  try {
    await until(() => run.directory?.isFilled === true, 'the start\'s read of the stream list');
    const answer = await run.tool('mute_channel', { channel: 'general' });
    assert.equal(answer.isError, false, answer.text);
    assert.deepEqual((readJson(dir, 'filters.json') as { mutedStreams?: string[] }).mutedStreams, ['general']);
  } finally { await run.close(); }
});

scenario('45. one process: a host that leaves while a rename waits for its answer ends that pass as it goes, and the next host is offered the new id', async (dir) => {
  const r = realm({ 17: 'general' });
  const first = await start(r, dir, { holdChanged: true });
  try {
    await first.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => first.changed.some((c) => c.removed?.includes('zulip:general')), 'the rename offered to the first host');
  } finally { await first.end(); }
  assert.ok(first.logs.some((l) => l.includes('zulip:general') && l.includes('got no answer')), 'the pass ended before the connection was done');
  assert.equal((first.server as unknown as { reconciling: boolean }).reconciling, false);
  const second = await start(r, dir, { reuse: first });
  try {
    const listed = (await second.host.sendRequest(method.CHANNELS_LIST)) as { channels: ChannelDescriptor[] };
    assert.ok(listed.channels.some((c) => c.id === 'zulip:lobby'));
    assert.ok(!listed.channels.some((c) => c.id === 'zulip:general'));
  } finally { await second.close(); }
});

scenario('46. one process: a host that leaves while its start reads the stream list leaves the next host\'s start to that host: no live delivery before its policy, then its own catch-up', async (dir) => {
  const r = realm({ 17: 'general' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
    const seen = r.post(17, 'seen while open');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)), 'delivery on the open channel');
  } finally { await first.end(); }
  const away = r.post(17, 'written while no host was connected', { mentioned: true });
  const lists = () => r.calls.filter((c) => c.method === 'GET' && c.path === '/streams').length;
  const listsBefore = lists();
  const release = r.hold('GET /streams');
  const second = await start(r, dir, { reuse: first, noWait: true });
  await until(() => lists() > listsBefore, 'the second host\'s start reading the stream list');
  await second.end();
  const third = await start(r, dir, { reuse: first, grantLater: true });
  try {
    release();
    await until(() => third.registered.length > 0, 'the second host\'s start, resumed, reaching the third host with its registration');
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.deepEqual([...third.pushed.map(payloadText), ...third.incoming.map((m) => m.messageId)], [], 'nothing reaches the third host before its policy');
    await third.grant();
    await until(() => named(third, away), 'the third host\'s catch-up of the message written while no host was connected');
  } finally { await third.close(); }
});

scenario('47. one process: a host that leaves while its catch-up reads leaves the next host\'s start to that host: no live delivery before its policy, then its own catch-up', async (dir) => {
  const r = realm({ 17: 'general' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
    const seen = r.post(17, 'seen while open');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)), 'delivery on the open channel');
  } finally { await first.end(); }
  const away = r.post(17, 'written while no host was connected', { mentioned: true });
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  const second = await start(r, dir, { reuse: first, noWait: true });
  await until(() => r.reads.length > readsBefore, 'the second host\'s catch-up reading');
  await second.end();
  const third = await start(r, dir, { reuse: first, grantLater: true });
  try {
    release();
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.deepEqual([...third.pushed.map(payloadText), ...third.incoming.map((m) => m.messageId)], [], 'nothing reaches the third host before its policy');
    await third.grant();
    await until(() => named(third, away), 'the third host\'s catch-up of the message written while no host was connected');
  } finally { await third.close(); }
});

scenario('48. a stream renamed and renamed back while the connected host holds no channel for it gets no push', async (dir) => {
  const r = realm({ 17: 'general' });
  await savedAsGeneral(r, dir);
  const registrations = r.registrations;
  const run = await start(r, dir, { grantLater: true });
  try {
    await until(() => r.registrations > registrations && run.directory?.isFilled === true, 'the queue registered, before the host\'s policy');
    r.rename(17, 'lobby');
    r.rename(17, 'general');
    await until(() => run.logs.some((l) => l.includes('#lobby -> #general')), 'the rename back seen');
    await run.grant();
    await deliveriesSettled(run);
    assert.deepEqual(run.pushed.filter((p) => /was renamed/.test(payloadText(p))).map(payloadText), []);
  } finally { await run.close(); }
});

scenario('49. a stream outside the allowlist gets no push after a rename, nor at a later start where the allowlist holds it', async (dir) => {
  const r = realm({ 17: 'general', 18: 'other' });
  await savedAsGeneral(r, dir);
  writeFilters(dir, { streams: ['other'] });
  r.rename(17, 'lobby');
  const first = await start(r, dir);
  try {
    await until(() => first.logs.some((l) => l.includes('#general -> #lobby')), 'the rename found');
    await deliveriesSettled(first);
    assert.deepEqual(first.pushed.filter((p) => /was renamed/.test(payloadText(p))).map(payloadText), [], 'no push for a stream outside the allowlist');
  } finally { await first.end(); }
  writeFilters(dir, {});
  // Move the mtime forward, as test/filters.test.ts does: the plane sees a
  // change by mtime, and two writes within one timestamp step share it.
  utimesSync(join(dir, 'filters.json'), new Date(Date.now() + 5000), new Date(Date.now() + 5000));
  first.filters.tick();
  assert.equal(first.filters.streamAllowed('lobby'), true, 'the allowlist widened');
  const second = await start(r, dir, { reuse: first });
  try {
    await until(() => second.registered.some((c) => c.id === 'zulip:lobby'), 'the stream registered at the start where the allowlist holds it');
    await deliveriesSettled(second);
    assert.deepEqual(second.pushed.filter((p) => /was renamed/.test(payloadText(p))).map(payloadText), [], 'no push at a later start either');
  } finally { await second.close(); }
});

scenario('50. one process: a host that leaves while its catch-up reads, and a next host whose policy comes before that read answers, gets each stream\'s catch-up once', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
    await first.open('zulip:random');
    const seen = r.post(17, 'seen while open');
    const seenRandom = r.post(18, 'seen while open in random');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)) && first.incoming.some((m) => m.messageId === String(seenRandom)), 'delivery on both open channels');
  } finally { await first.end(); }
  const away = r.post(17, 'written while no host was connected', { mentioned: true });
  const awayRandom = r.post(18, 'written in random while no host was connected', { mentioned: true });
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  const second = await start(r, dir, { reuse: first, noWait: true });
  await until(() => r.reads.length > readsBefore, 'the second host\'s catch-up reading');
  await second.end();
  const readsHeld = r.reads.length;
  const third = await start(r, dir, { reuse: first, grantLater: true });
  try {
    await third.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
    await until(() => r.reads.length > readsHeld, 'the third host\'s own catch-up reading');
    release();
    await until(() => named(third, away) && named(third, awayRandom), 'the third host\'s catch-up of both streams');
    await deliveriesSettled(third);
    for (const id of [away, awayRandom]) {
      const catchUps = third.pushed.filter((p) => payloadText(p).includes(`id=${id}]`));
      assert.equal(catchUps.length, 1, `one catch-up of message ${id}, not ${catchUps.map((p) => p.eventId).join(', ')}`);
    }
  } finally { await third.close(); }
});

scenario('51. one process: a host that leaves while its channels/register waits leaves the rename\'s push to the next host', async (dir) => {
  const r = realm({ 17: 'general' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
  } finally { await first.end(); }
  r.rename(17, 'lobby');
  const second = await start(r, dir, { reuse: first, noWait: true, holdRegister: true });
  await until(() => second.registered.length > 0, 'the second host asked to register, with no answer');
  await second.end();
  const third = await start(r, dir, { reuse: first });
  try {
    await until(() => third.pushed.some((p) => /was renamed #lobby/.test(payloadText(p))), 'the rename\'s push at the next start');
  } finally { await third.close(); }
});

const streamOfRead = (read: Record<string, unknown>) => ((read.narrow as unknown[]) ?? [])
  .map((n) => (Array.isArray(n) ? n : [(n as { operator: string }).operator, (n as { operand: unknown }).operand]))
  .find(([operator]) => operator === 'channel' || operator === 'stream')?.[1];

scenario('52. one process: a sweep whose host leaves during its first stream\'s read reads no other stream', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
    await first.open('zulip:random');
    const seen = r.post(17, 'seen while open');
    const seenRandom = r.post(18, 'seen while open in random');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)) && first.incoming.some((m) => m.messageId === String(seenRandom)), 'delivery on both open channels');
  } finally { await first.end(); }
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  const second = await start(r, dir, { reuse: first, noWait: true });
  await until(() => r.reads.length > readsBefore, 'the second host\'s catch-up reading its first stream');
  const readsHeld = r.reads.length;
  await second.end();
  release();
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.deepEqual(r.reads.slice(readsHeld).map(streamOfRead), [], 'no read after the host left');
});

scenario('53. one process: a sweep whose host leaves during its last stream\'s read doesn\'t go on to the missed tallies', async (dir) => {
  const r = realm({ 17: 'general', 19: 'dev' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
    const seen = r.post(17, 'seen while open');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)), 'delivery on the open channel');
    await first.open('zulip:dev');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:dev' });
    r.post(19, 'ambient in dev while it is closed');
    await untilAsync(async () => (await missedOn(first, 'zulip:dev')) === 1, 'dev tallied');
  } finally { await first.end(); }
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  const second = await start(r, dir, { reuse: first, noWait: true });
  await until(() => r.reads.length > readsBefore, 'the second host\'s catch-up reading general');
  const readsHeld = r.reads.length;
  assert.deepEqual(r.reads.slice(readsBefore).map(streamOfRead), [17], 'the sweep\'s only stream is general');
  await second.end();
  release();
  await new Promise((resolve) => setTimeout(resolve, 200));
  assert.deepEqual(r.reads.slice(readsHeld).map(streamOfRead), [], 'no read of dev\'s tally after the host left');
});

scenario('54. one process: a host that leaves while its start\'s rename pushes wait for their answers was offered each push once, and the next host isn\'t offered them again', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:general');
    await first.open('zulip:random');
  } finally { await first.end(); }
  r.rename(17, 'lobby');
  r.rename(18, 'lounge');
  const renamePush = (p: PushEventParams) => /was renamed/.test(payloadText(p));
  const second = await start(r, dir, { reuse: first, noWait: true, beforePushAnswer: (p) => (renamePush(p) ? new Promise<void>(() => {}) : Promise.resolve()) });
  await until(() => second.pushed.filter(renamePush).length === 2, 'both renames\' pushes, which get no answer');
  assert.deepEqual(second.pushed.filter(renamePush).map((p) => origin(p).mcplChannelId).sort(), ['zulip:lobby', 'zulip:lounge']);
  await second.open('zulip:lobby');
  await second.end();
  const third = await start(r, dir, { reuse: first });
  try {
    await deliveriesSettled(third);
    assert.deepEqual(third.pushed.filter(renamePush).map((p) => origin(p).mcplChannelId), [], 'no push again, zulip:lobby open at last sight included');
  } finally { await third.close(); }
});

scenario('55. one process: a backfill of missed tallies whose host leaves during its read leaves the count to the next host', async (dir) => {
  const r = realm({ 19: 'dev' });
  const first = await start(r, dir);
  try {
    await first.open('zulip:dev');
    await first.host.sendRequest(method.CHANNELS_CLOSE, { channelId: 'zulip:dev' });
    r.post(19, 'ambient in dev while it is closed');
    await untilAsync(async () => (await missedOn(first, 'zulip:dev')) === 1, 'dev tallied');
  } finally { await first.end(); }
  r.post(19, 'ambient in dev while no host was connected');
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  const second = await start(r, dir, { reuse: first, noWait: true });
  await until(() => r.reads.length > readsBefore, 'the second host\'s backfill reading dev');
  const readsHeld = r.reads.length;
  assert.deepEqual(r.reads.slice(readsBefore).map(streamOfRead), [19], 'the sweep\'s only read is dev\'s backfill');
  await second.end();
  const third = await start(r, dir, { reuse: first, grantLater: true });
  try {
    await third.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
    await until(() => r.reads.length > readsHeld, 'the third host\'s backfill reading dev');
    release();
    await third.server.quiet();
    assert.equal(await missedOn(third, 'zulip:dev'), 2, 'one ambient message counted live and one backfilled, each once');
  } finally { await third.close(); }
});

scenario('56. one process: a host that leaves while a rename\'s push waits behind its open on the stream\'s lane leaves the push to the next host', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const first = await start(r, dir, { shutdownGraceMs: 20 });
  try {
    await first.open('zulip:general');
    await first.open('zulip:random');
    const seen = r.post(18, 'seen while open in random');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)), 'delivery on random');
  } finally { await first.end(); }
  r.rename(17, 'lobby');
  let answerRegister!: () => void;
  const registerAnswered = new Promise<void>((resolve) => { answerRegister = resolve; });
  const second = await start(r, dir, { reuse: first, noWait: true, beforeRegisterAnswer: () => registerAnswered });
  await until(() => second.registered.length > 0, 'the second host asked to register');
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  try {
    second.open('zulip:lobby', { limit: 50, sinceLastSeen: true }).catch(() => {});
    await until(() => r.reads.slice(readsBefore).some((read) => streamOfRead(read) === 17), 'the open of zulip:lobby reading its page');
    answerRegister();
    await until(() => r.reads.slice(readsBefore).some((read) => streamOfRead(read) === 18), 'the lanes open: random\'s catch-up reading');
    assert.deepEqual(second.pushed.filter((p) => /was renamed/.test(payloadText(p))).map(payloadText), [], 'the push waits behind the open');
    await second.end();
  } finally { release(); }
  const third = await start(r, dir, { reuse: first });
  try {
    await until(() => third.pushed.some((p) => /was renamed #lobby/.test(payloadText(p))), 'the rename\'s push at the next start');
  } finally { await third.close(); }
});

scenario('57. a rename whose announcement gets no answer: the stream\'s message waits for the bound, then goes under the old id', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { holdChanged: true, announceTimeoutMs: 200 });
  const gaveUp = () => run.logs.some((l) => l.includes('zulip:general') && l.includes('got no answer'));
  try {
    await run.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the held announcement');
    const addressed = r.post(17, '@bot meanwhile', { mentioned: true });
    let gaveUpBefore: boolean | null = null;
    const push = run.incoming.push.bind(run.incoming);
    run.incoming.push = (...messages) => {
      if (gaveUpBefore === null && messages.some((m) => m.messageId === String(addressed))) gaveUpBefore = gaveUp();
      return push(...messages);
    };
    await until(() => arrivedOn(run, addressed) !== null, 'the message after the bound');
    assert.equal(gaveUpBefore, true, 'it arrived only once the announcement was given up');
    assert.equal(arrivedOn(run, addressed), 'zulip:general');
  } finally { run.releaseChanged(); await run.close(); }
});

scenario('58. a quote lookup held on one stream: a mention on another stream reaches the host meanwhile', async (dir) => {
  const r = realm({ 17: 'general', 18: 'dev' });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    const quoting = r.post(17, `@_**User|${SELF}** [said](/#narrow/channel/17-general/near/999):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`);
    await until(() => r.calls.some((c) => c.method === 'GET' && c.id === 999), 'the quote lookup');
    const mention = r.post(18, '@bot meanwhile', { mentioned: true });
    await until(() => named(run, mention), 'the other stream\'s mention');
    assert.equal(run.logs.some((l) => l.includes('could not resolve message 999')), false, 'the mention came while the lookup was still out');
    assert.equal(named(run, quoting), false);
    release();
    await run.server.quiet();
  } finally { release(); await run.close(); }
});

scenario('59. two messages without stream_id of a stream the directory doesn\'t hold, the second after an event taught the directory its name: the first reaches the host first, and neither is lost', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    r.streams.set(30, 'north');
    const first = r.post(30, '@bot one', { mentioned: true, noStreamId: true });
    r.regain(30, 'north');
    const second = r.post(30, '@bot two', { mentioned: true, noStreamId: true });
    await until(() => named(run, first) && named(run, second), 'both messages');
    const order = run.pushed.map((p) => origin(p).messageId).filter((id) => id === String(first) || id === String(second));
    assert.deepEqual(order, [String(first), String(second)]);
  } finally { await run.close(); }
});

scenario("60. a reaction to a new stream's first message, on a channel set to show reactions, reaches the host after its message: it arrived before the directory held the stream's name", async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { reactionChannels: ['zulip:north'] });
  const run = await start(r, dir);
  try {
    r.streams.set(30, 'north');
    const first = r.post(30, '@bot one', { mentioned: true });
    r.react(first, 'thumbs_up', '1f44d');
    const reaction = () => run.pushed.findIndex((p) => payloadText(p).includes(`[reaction] Ann reacted :thumbs_up: on message ${first}`));
    await until(() => reaction() >= 0, 'the reaction');
    const message = run.pushed.findIndex((p) => origin(p).messageId === String(first));
    assert.ok(message >= 0 && message < reaction(), 'the message reaches the host before its reaction');
    assert.equal(origin(run.pushed[reaction()]).mcplChannelId, 'zulip:north');
  } finally { await run.close(); }
});

scenario("64. a message's move to another stream waits in its busy lane, and a reaction to the message arrives meanwhile: the reaction's line goes on the new stream's channel", async (dir) => {
  const r = realm({ 17: 'general', 18: 'dev' });
  writeFilters(dir, { reactionChannels: ['zulip:general', 'zulip:dev'] });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    const moved = r.post(17, 'to be moved');
    r.post(17, `@_**User|${SELF}** [said](/#narrow/channel/17-general/near/999):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`);
    await until(() => r.calls.some((c) => c.method === 'GET' && c.id === 999), "general's lane busy with a quote lookup");
    r.move(moved, 18);
    r.react(moved, 'thumbs_up', '1f44d');
    const marker = r.post(18, '@bot after the reaction', { mentioned: true });
    await until(() => named(run, marker), 'an event read after the reaction');
    release();
    const reaction = () => run.pushed.find((p) => payloadText(p).includes(`[reaction] Ann reacted :thumbs_up: on message ${moved}`));
    await until(() => reaction() !== undefined, 'the reaction');
    assert.equal(origin(reaction()!).mcplChannelId, 'zulip:dev');
    await run.server.quiet();
  } finally { release(); await run.close(); }
});

scenario('61. one process: a host that leaves while a request is still served, and while a rename\'s push waits behind its open on the stream\'s lane, leaves the push to the next host', async (dir) => {
  const r = realm({ 17: 'general', 18: 'random' });
  const first = await start(r, dir, { shutdownGraceMs: 20 });
  try {
    await first.open('zulip:general');
    await first.open('zulip:random');
    const seen = r.post(18, 'seen while open in random');
    await until(() => first.incoming.some((m) => m.messageId === String(seen)), 'delivery on random');
  } finally { await first.end(); }
  r.rename(17, 'lobby');
  let answerRegister!: () => void;
  const registerAnswered = new Promise<void>((resolve) => { answerRegister = resolve; });
  const second = await start(r, dir, { reuse: first, noWait: true, beforeRegisterAnswer: () => registerAnswered });
  await until(() => second.registered.length > 0, 'the second host asked to register');
  const readsBefore = r.reads.length;
  const release = r.hold('GET /messages');
  let releaseList = () => {};
  let leaving: Promise<void> = Promise.resolve();
  try {
    second.open('zulip:lobby', { limit: 50, sinceLastSeen: true }).catch(() => {});
    await until(() => r.reads.slice(readsBefore).some((read) => streamOfRead(read) === 17), 'the open of zulip:lobby reading its page');
    answerRegister();
    await until(() => r.reads.slice(readsBefore).some((read) => streamOfRead(read) === 18), 'the lanes open: random\'s catch-up reading');
    const lists = () => r.calls.filter((c) => c.method === 'GET' && c.path === '/streams').length;
    const listsBefore = lists();
    releaseList = r.hold('GET /streams');
    second.tool('refresh_channels', {}).catch(() => {});
    await until(() => lists() > listsBefore, 'refresh_channels reading the stream list, on the request chain');
    const sawClose = new Promise((resolve) => second.serverSide.once('close', resolve));
    leaving = second.end();
    await sawClose;
    await new Promise((resolve) => setImmediate(resolve));
    const laneOf = (streamId: number) => (second.server as unknown as { lanes: { lane(key: ReturnType<typeof streamKey>): { isQuiet: boolean } } }).lanes.lane(streamKey(streamId));
    assert.equal(laneOf(17).isQuiet, false, 'the open still runs on its lane, held at its page read');
    release();
    await until(() => laneOf(17).isQuiet && laneOf(18).isQuiet, 'the open, resumed, and each job its lanes would run after it, done');
  } finally {
    release();
    releaseList();
  }
  await leaving;
  const third = await start(r, dir, { reuse: first });
  try {
    await until(() => third.pushed.some((p) => /was renamed #lobby/.test(payloadText(p))), 'the rename\'s push at the next start');
  } finally { await third.close(); }
});

scenario('62. quiet() waits while a rename\'s channels/changed is with the host, and settles once the host answers', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { holdChanged: true, announceTimeoutMs: 60_000 });
  try {
    await run.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the held announcement');
    let settled = false;
    void run.server.quiet().then(() => { settled = true; });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(settled, false, 'quiet() waits for the announcement\'s answer');
    run.answerChanged();
    await until(() => settled, 'quiet() once the host answers');
  } finally { run.releaseChanged(); await run.close(); }
});

scenario('63. a read of the stream list held for one stream\'s message: a mention on another stream reaches the host meanwhile', async (dir) => {
  const r = realm({ 17: 'general', 18: 'dev' });
  const run = await start(r, dir);
  const lists = () => r.calls.filter((c) => c.method === 'GET' && c.path === '/streams').length;
  const before = lists();
  const release = r.hold('GET /streams');
  try {
    r.streams.set(30, 'north');
    const unlisted = r.post(30, '@bot north', { mentioned: true });
    await until(() => lists() > before, 'the read of the stream list');
    const mention = r.post(18, '@bot meanwhile', { mentioned: true });
    await until(() => named(run, mention), 'the other stream\'s mention');
    assert.equal(named(run, unlisted), false, 'the mention came while the read was still out');
    release();
    await until(() => named(run, unlisted), 'the mention on the stream the read found');
  } finally { release(); await run.close(); }
});

const quoting = `@_**User|${SELF}** [said](/#narrow/channel/17-general/near/999):\n\n\`\`\`quote\noriginal\n\`\`\`\nreply`;

async function busyGeneral(r: Realm): Promise<void> {
  r.post(17, quoting);
  await until(() => r.calls.some((c) => c.method === 'GET' && c.id === 999), "general's lane busy with a quote lookup");
}

const pushesAbout = (run: Run, kind: string, id: number) => run.pushed.filter((p) => origin(p).messageId.startsWith(`${kind}:${id}:`));

scenario('65. a mention that waits behind a quote lookup and is deleted reaches the host as nothing: no push, and no line', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    await busyGeneral(r);
    const gone = r.post(17, '@bot soon gone', { mentioned: true });
    r.remove([gone]);
    const after = r.post(17, '@bot after it', { mentioned: true });
    release();
    await until(() => named(run, after), 'the mention after it');
    await run.server.quiet();
    assert.equal(named(run, gone), false);
    assert.deepEqual(pushesAbout(run, 'delete', gone), []);
  } finally { release(); await run.close(); }
});

scenario('66. on a closed channel, an ambient message that waits and is edited into a mention is pushed once, as a mention, with its new text and "(edited)"', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    await busyGeneral(r);
    const plain = r.post(17, 'plain words');
    r.edit(plain, '@bot now a mention', { mentioned: true });
    const after = r.post(17, '@bot after it', { mentioned: true });
    release();
    await until(() => named(run, after), 'the mention after it');
    await run.server.quiet();
    const pushes = run.pushed.filter((p) => origin(p).messageId === String(plain));
    assert.equal(pushes.length, 1);
    assert.match(payloadText(pushes[0]), /now a mention \(edited\)/);
    assert.ok(pushes[0].tags?.includes('chat:mention'));
    assert.deepEqual(pushesAbout(run, 'edit', plain), []);
  } finally { release(); await run.close(); }
});

scenario("67. an edit that arrives once its message's job has started follows it as a line: the message comes with its first text", async (dir) => {
  const r = realm({ 17: 'general', 18: 'dev' });
  const run = await start(r, dir);
  const lists = () => r.calls.filter((c) => c.method === 'GET' && c.path === '/streams').length;
  const before = lists();
  const release = r.hold('GET /streams');
  try {
    r.streams.set(30, 'north');
    const first = r.post(30, '@bot first words', { mentioned: true });
    await until(() => lists() > before, "the read of the stream list, in the message's job");
    r.edit(first, '@bot second words', { mentioned: true });
    const marker = r.post(18, '@bot meanwhile', { mentioned: true });
    await until(() => named(run, marker), 'an event read after the edit');
    release();
    await until(() => pushesAbout(run, 'edit', first).length === 1, "the edit's line");
    const at = run.pushed.findIndex((p) => origin(p).messageId === String(first));
    assert.ok(at >= 0 && at < run.pushed.indexOf(pushesAbout(run, 'edit', first)[0]));
    assert.match(payloadText(run.pushed[at]), /first words$/);
  } finally { release(); await run.close(); }
});

scenario('68. a deletion that names a message the host has and a message that waits: the waiting one never comes, and the line names the other alone', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    const had = r.post(17, '@bot first', { mentioned: true });
    await until(() => named(run, had), 'the first mention');
    await busyGeneral(r);
    const waiting = r.post(17, '@bot second', { mentioned: true });
    r.remove([had, waiting]);
    const after = r.post(17, '@bot after it', { mentioned: true });
    release();
    await until(() => named(run, after), 'the mention after it');
    await run.server.quiet();
    assert.equal(named(run, waiting), false);
    const lines = run.pushed.filter((p) => origin(p).messageId.startsWith('delete:'));
    assert.equal(lines.length, 1);
    assert.equal(origin(lines[0]).messageId.split(':')[1], String(had));
    assert.doesNotMatch(payloadText(lines[0]), /2 messages/);
  } finally { release(); await run.close(); }
});

scenario('69. one event that edits a waiting message and moves it to another topic: the message comes with its first text, and the line follows it', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    await busyGeneral(r);
    const moved = r.post(17, '@bot first words', { mentioned: true });
    r.edit(moved, '@bot second words', { mentioned: true, topic: 'elsewhere' });
    const after = r.post(17, '@bot after it', { mentioned: true });
    release();
    await until(() => named(run, after), 'the mention after it');
    await run.server.quiet();
    const at = run.pushed.findIndex((p) => origin(p).messageId === String(moved));
    assert.match(payloadText(run.pushed[at]), /first words$/);
    const line = pushesAbout(run, 'edit', moved);
    assert.equal(line.length, 1);
    assert.ok(run.pushed.indexOf(line[0]) > at);
  } finally { release(); await run.close(); }
});

scenario("70. an edit applied to a waiting message reaches the cache: a reaction after the message was handed over quotes the new text", async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { reactionChannels: ['zulip:general'] });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    await busyGeneral(r);
    const edited = r.post(17, '@bot first words', { mentioned: true });
    r.edit(edited, '@bot second words', { mentioned: true });
    release();
    await until(() => named(run, edited), 'the edited mention');
    r.react(edited, 'thumbs_up', '1f44d');
    const reaction = () => run.pushed.find((p) => payloadText(p).includes(`[reaction] Ann reacted :thumbs_up: on message ${edited}`));
    await until(() => reaction() !== undefined, 'the reaction');
    assert.match(payloadText(reaction()!), /second words/);
    assert.doesNotMatch(payloadText(reaction()!), /first words/);
  } finally { release(); await run.close(); }
});

scenario('71. under a disabled zulip.messaging, an edit runs after an older edit of the same message that waits in its lane: a reaction after the enable quotes the newer text', async (dir) => {
  const r = realm({ 17: 'general' });
  writeFilters(dir, { reactionChannels: ['zulip:general'] });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  const polls = () => r.calls.filter((c) => c.method === 'GET' && c.path === '/events').length;
  try {
    const had = r.post(17, '@bot first words', { mentioned: true });
    await until(() => named(run, had), 'the mention');
    await busyGeneral(r);
    let read = polls();
    r.edit(had, '@bot second words', { mentioned: true });
    await until(() => polls() > read, 'the older edit read');
    run.host.sendNotification(method.FEATURE_SETS_UPDATE, { disabled: ['zulip.messaging'] });
    await run.host.sendRequest('ping');
    read = polls();
    r.edit(had, '@bot third words', { mentioned: true });
    await until(() => polls() > read, 'the newer edit read');
    release();
    await run.host.sendRequest(method.FEATURE_SETS_UPDATE, { effectiveCapabilities: GRANT });
    await run.server.quiet();
    r.react(had, 'thumbs_up', '1f44d');
    const reaction = () => run.pushed.find((p) => payloadText(p).includes(`[reaction] Ann reacted :thumbs_up: on message ${had}`));
    await until(() => reaction() !== undefined, 'the reaction');
    assert.match(payloadText(reaction()!), /third words/);
    assert.equal(pushesAbout(run, 'edit', had).length, 1, "only the older edit's line, from before the disable");
  } finally { release(); await run.close(); }
});

scenario('72. once the host has answered a rename, an acknowledge under the old id, or under an id of no known format, adds its reaction, and a publish under the old id is still refused', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  try {
    const posted = r.post(17, 'to acknowledge');
    await renameWhileOpen(r, run);
    await tookRename(run);
    const ack = await run.host.sendRequest(method.CHANNELS_ACKNOWLEDGE, { channelId: 'zulip:general', messageId: String(posted), value: 'eyes' });
    assert.deepEqual(ack, { acknowledged: true, representation: ':eyes:' });
    assert.deepEqual(r.calls.filter((c) => c.path.endsWith('/reactions')).map((c) => [c.method, c.path, c.params.emoji_name]), [['POST', `/messages/${posted}/reactions`, 'eyes']]);
    const unformed = await run.host.sendRequest(method.CHANNELS_ACKNOWLEDGE, { channelId: 'nonsense-general', messageId: String(posted), value: 'tada' });
    assert.deepEqual(unformed, { acknowledged: true, representation: ':tada:' }, 'an id of no known format adds the reaction too');
    assert.deepEqual(r.calls.filter((c) => c.path.endsWith('/reactions')).map((c) => [c.method, c.path, c.params.emoji_name]), [
      ['POST', `/messages/${posted}/reactions`, 'eyes'],
      ['POST', `/messages/${posted}/reactions`, 'tada'],
    ]);
    await assert.rejects(publish(run, 'zulip:general', 'hello?'), /zulip:lobby/);
  } finally { await run.close(); }
});

scenario('73. a host that leaves before it answers the push about a rename leaves the stream open at last sight: the next host gets its ambient messages in the backscroll', async (dir) => {
  const r = realm({ 17: 'general' });
  const before = await start(r, dir);
  let left: Run | undefined;
  try {
    await before.open('zulip:general');
    const seen = r.post(17, 'seen while open');
    await until(() => before.incoming.some((m) => m.messageId === String(seen)), 'delivery on the open channel');
  } finally { await before.end(); }
  r.rename(17, 'lobby');
  try {
    left = await start(r, dir, { reuse: before, noWait: true, beforePushAnswer: () => new Promise<void>(() => {}) });
    await until(() => toldRename(left!).length > 0, 'the push');
  } finally { await left?.end(); }
  r.post(17, 'ambient after the rename');
  const next = await start(r, dir, { reuse: left, noWait: true });
  try {
    await until(() => next.pushed.some((p) => payloadText(p).includes('ambient after the rename')), "the ambient message, in an open channel's backscroll");
  } finally { await next.close(); }
});

scenario("74. a deletion of a new stream's first message that comes while the message waits for the stream's announcement follows the message as a line", async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { holdChanged: true, announceTimeoutMs: 60_000 });
  const polls = () => r.calls.filter((c) => c.method === 'GET' && c.path === '/events').length;
  try {
    const fresh = r.create('fresh');
    const first = r.post(fresh, '@bot hello', { mentioned: true });
    await until(() => run.changed.some((c) => c.added?.some((d) => d.id === 'zulip:fresh')), 'the held announcement');
    const read = polls();
    r.remove([first]);
    await until(() => polls() > read, 'the deletion read');
    run.releaseChanged();
    await until(() => named(run, first), 'the message');
    await run.server.quiet();
    assert.equal(pushesAbout(run, 'delete', first).length, 1);
  } finally { run.releaseChanged(); await run.close(); }
});

scenario('77. a move of a waiting message and a message the host has, whose waiting message is then deleted, reaches the host as a line about the other message', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir);
  const release = r.hold('GET /messages/:id');
  try {
    const had = r.post(17, '@bot first', { mentioned: true });
    await until(() => named(run, had), 'the first mention');
    await busyGeneral(r);
    const waiting = r.post(17, '@bot second', { mentioned: true });
    r.moveTopic([waiting, had], 'elsewhere');
    r.remove([waiting]);
    const after = r.post(17, '@bot after it', { mentioned: true });
    release();
    await until(() => named(run, after), 'the mention after it');
    await run.server.quiet();
    assert.equal(named(run, waiting), false);
    assert.equal(pushesAbout(run, 'move', had).length, 1);
  } finally { release(); await run.close(); }
});

scenario('75. while the host holds a renamed stream\'s old id, the history tools give a new stream under the old name no channel id', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { holdChanged: true, announceTimeoutMs: 60_000 });
  try {
    await run.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the held announcement');
    const reused = r.create('general');
    const intruder = r.post(reused, 'from the new #general');
    await run.tool('refresh_channels', {});
    const history = JSON.parse((await run.tool('fetch_history', { channel: 'general' })).text) as { channel: string; channelId: string | null };
    assert.equal(history.channel, 'general');
    assert.equal(history.channelId, null, 'fetch_history names the new #general by no id while the host holds zulip:general for stream 17');
    const around = JSON.parse((await run.tool('fetch_around', { message_id: intruder })).text) as { channelId: string | null };
    assert.equal(around.channelId, null, 'fetch_around names it by no id either');
    const renamed = JSON.parse((await run.tool('fetch_history', { channel: 'lobby' })).text) as { channelId: string | null };
    assert.equal(renamed.channelId, 'zulip:general', 'the renamed stream keeps the id the host holds');
    run.answerChanged();
  } finally { await run.close(); }
});

scenario('76. while the host holds a renamed stream\'s old id, a reply to a message in a new stream under the old name gives a receipt with no channel id', async (dir) => {
  const r = realm({ 17: 'general' });
  const run = await start(r, dir, { holdChanged: true, announceTimeoutMs: 60_000 });
  try {
    await run.open('zulip:general');
    r.rename(17, 'lobby');
    await until(() => run.changed.some((c) => c.removed?.includes('zulip:general')), 'the held announcement');
    const reused = r.create('general');
    const intruder = r.post(reused, 'from the new #general');
    const inRenamed = r.post(17, 'in the renamed stream');
    await run.tool('refresh_channels', {});
    const reply = await run.tool('send_message', { reply_to: intruder, content: 'answer in the new #general' });
    assert.equal(reply.isError, false, reply.text);
    const receipt = JSON.parse(reply.text) as { channelId: string | null; channel: string };
    assert.equal(receipt.channel, 'general');
    assert.equal(receipt.channelId, null, 'the receipt names the new #general by no id while the host holds zulip:general for stream 17');
    const control = JSON.parse((await run.tool('send_message', { reply_to: inRenamed, content: 'answer in lobby' })).text) as { channelId: string | null; channel: string };
    assert.equal(control.channel, 'lobby');
    assert.equal(control.channelId, 'zulip:general', 'a reply in the renamed stream names the id the host holds for it');
    run.answerChanged();
  } finally { await run.close(); }
});
