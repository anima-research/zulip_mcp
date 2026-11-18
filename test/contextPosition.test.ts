import test from 'node:test';
import assert from 'node:assert/strict';
import { isCapabilityPath } from '@animalabs/mcpl-core';
import { contextPositionFromEnv } from '../src/index.ts';
import { buildFeatureSets, buildServerCapabilities } from '../src/feature-sets.ts';

function withEnv(raw: string | undefined, run: (lines: string[]) => void): void {
  const saved = process.env.MCPL_CONTEXT_POSITION;
  const lines: string[] = [];
  const original = console.error;
  console.error = (line: string) => lines.push(line);
  try {
    if (raw === undefined) delete process.env.MCPL_CONTEXT_POSITION;
    else process.env.MCPL_CONTEXT_POSITION = raw;
    run(lines);
  } finally {
    console.error = original;
    if (saved === undefined) delete process.env.MCPL_CONTEXT_POSITION;
    else process.env.MCPL_CONTEXT_POSITION = saved;
  }
}

test('MCPL_CONTEXT_POSITION unset or empty leaves the injection at beforeUser, with no line', () => {
  for (const raw of [undefined, '']) {
    withEnv(raw, (lines) => {
      assert.equal(contextPositionFromEnv(), 'beforeUser');
      assert.deepEqual(lines, []);
    });
  }
});

test('MCPL_CONTEXT_POSITION names a position in any letter case, with white space around it allowed', () => {
  for (const [raw, position] of [['afterUser', 'afterUser'], [' afteruser ', 'afterUser'], ['AFTERUSER', 'afterUser'], ['beforeUser', 'beforeUser'], ['beforeuser', 'beforeUser']] as const) {
    withEnv(raw, (lines) => {
      assert.equal(contextPositionFromEnv(), position, raw);
      assert.deepEqual(lines, [], raw);
    });
  }
});

test('an MCPL_CONTEXT_POSITION that names neither position is ignored at start, with a line saying so', () => {
  for (const raw of ['system', 'after', 'true']) {
    withEnv(raw, (lines) => {
      assert.equal(contextPositionFromEnv(), 'beforeUser', raw);
      assert.deepEqual(lines, [`[zulip-mcp] ignoring MCPL_CONTEXT_POSITION="${raw}" (not "beforeUser" or "afterUser"); using beforeUser`]);
    });
  }
});

test('the manifest declares the context injection at beforeUser unless told otherwise', () => {
  for (const options of [{ typing: true }, { typing: true, contextPosition: 'beforeUser' as const }]) {
    const caps = buildServerCapabilities(options);
    const hooks = caps.contextHooks as { beforeInference: { inject: unknown } };
    assert.deepEqual(hooks.beforeInference.inject, { system: false, beforeUser: true, afterUser: false });
    assert.deepEqual(buildFeatureSets(options)['zulip.context'].uses, ['contextHooks.beforeInference.inject.beforeUser']);
  }
});

test('with afterUser, the manifest declares afterUser alone, and zulip.context uses that path alone', () => {
  const options = { typing: true, contextPosition: 'afterUser' as const };
  const caps = buildServerCapabilities(options);
  const hooks = caps.contextHooks as { beforeInference: { observe: boolean; inject: unknown } };
  assert.equal(hooks.beforeInference.observe, false);
  assert.deepEqual(hooks.beforeInference.inject, { system: false, beforeUser: false, afterUser: true });

  const uses = buildFeatureSets(options)['zulip.context'].uses;
  assert.deepEqual(uses, ['contextHooks.beforeInference.inject.afterUser']);
  assert.ok(isCapabilityPath(uses[0]));
  assert.deepEqual(
    (caps.featureSets as Record<string, { uses: string[] }>)['zulip.context'].uses,
    uses,
    'the manifest carries the same declaration',
  );
});
