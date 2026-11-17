// Run after npm run build. No live Zulip or credentials required.
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
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
    const child = spawn(process.execPath, [new URL('../build/index.js', import.meta.url).pathname], {
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
    const exited = once(child, 'exit');
    const conn = McplConnection.fromStreams(child.stdout, child.stdin);
    const timer = setTimeout(() => child.kill('SIGKILL'), 15000);
    try {
      const result = await conn.sendRequest('initialize', {
        protocolVersion: '2025-03-26',
        capabilities: mcpl ? { experimental: { mcpl: { version: '0.5', channels: true } } } : {},
        clientInfo: { name: 'built-smoke', version: '0' },
      });
      assert.deepEqual(result.serverInfo, {
        name: 'zulip-mcp-server',
        version: JSON.parse(readFileSync(new URL('../package.json', import.meta.url))).version,
        _meta: { 'zulip_mcp/messageIds': { version: 1, mode, realmTag: 'alpha' } },
      });
    } catch (error) {
      throw new Error(`${error.message}\n${diagnostics}`, { cause: error });
    } finally {
      child.stdin.end();
      await exited;
      clearTimeout(timer);
      conn.close();
    }
  }
  console.log('Built MCP/MCPL handshake matches package version in compat and strict.');
} finally {
  api.closeAllConnections();
  api.close();
  rmSync(dir, { recursive: true, force: true });
}
