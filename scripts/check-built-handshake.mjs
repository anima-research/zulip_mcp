// Run after npm run build. No live Zulip or credentials required.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { McplConnection } from '@animalabs/mcpl-core';

const api = createServer((req, res) => {
  res.setHeader('content-type', 'application/json');
  res.end(JSON.stringify({ result: 'success', user_id: 1 }));
});
api.listen(0, '127.0.0.1');
await once(api, 'listening');
const dir = mkdtempSync(join(tmpdir(), 'handshake-'));
try {
  for (const mode of ['compat', 'strict']) for (const mcpl of [false, true]) {
    const child = spawn(process.execPath, [fileURLToPath(new URL('../build/index.js', import.meta.url))], {
      cwd: dir,
      env: {
        PATH: process.env.PATH, HOME: dir,
        ZULIP_REALM: `http://127.0.0.1:${api.address().port}`,
        ZULIP_EMAIL: 'test@example.invalid', ZULIP_API_KEY: 'not-a-secret',
        ZULIP_STATE_DIR: dir, ZULIP_ID_MODE: mode, ZULIP_REALM_TAG: 'alpha',
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let diagnostics = '';
    child.stderr.on('data', b => { diagnostics += b; });
    // close follows exit and draining stderr, so failures retain all diagnostics.
    const exited = once(child, 'close');
    const conn = McplConnection.fromStreams(child.stdout, child.stdin);
    const timer = setTimeout(() => child.kill('SIGKILL'), 15000);
    let failure;
    let status;
    try {
      const result = await conn.sendRequest('initialize', {
        protocolVersion: '2025-03-26',
        capabilities: mcpl ? { experimental: { mcpl: { version: '0.5', channels: true } } } : {},
        clientInfo: { name: 'built-smoke', version: '0' },
      });
      const manifest = result.capabilities?.experimental?.mcpl;
      if (mcpl) {
        assert.equal(manifest?.version, '0.5');
        for (const capability of ['incoming', 'publish', 'acknowledge', 'lifecycle']) {
          assert.equal(manifest.channels?.[capability], true, capability);
        }
      } else assert.equal(manifest, undefined);
      assert.deepEqual(result.serverInfo, {
        name: 'zulip-mcp-server',
        version: JSON.parse(readFileSync(new URL('../package.json', import.meta.url))).version,
        _meta: { 'zulip_mcp/messageIds': { version: 1, mode, realmTag: 'alpha' } },
      });
    } catch (error) {
      failure = error;
    } finally {
      child.stdin.end();
      status = await exited;
      clearTimeout(timer);
      conn.close();
    }
    if (!failure && (status[0] !== 0 || status[1] !== null)) {
      failure = new Error(`Child exited with code ${status[0]}, signal ${status[1]}`);
    }
    if (failure) throw new Error(`${failure.message}\n${diagnostics}`, { cause: failure });
    assert.equal(diagnostics.match(/message IDs:/g)?.length, 1, 'exactly one message-ID startup announcement');
    assert.equal(diagnostics.split(`message IDs: ${mode}, tag "alpha"`).length - 1, 1, 'startup names the explicit mode and tag once');
  }
  console.log('Built MCP/MCPL handshake matches package version in compat and strict.');
} finally {
  api.closeAllConnections();
  api.close();
  rmSync(dir, { recursive: true, force: true });
}
