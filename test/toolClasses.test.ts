import test from 'node:test';
import assert from 'node:assert/strict';
import { toolDefinitions } from '../src/tools.ts';
import { TOOL_CLASSES, TOOL_CLASS_VOCABULARY, UNCLASSED, withToolClasses } from '../src/tool-classes.ts';

const RFC_008_VOCABULARY = ['comms', 'memory', 'notes', 'files', 'shell', 'web', 'computer', 'media', 'body', 'control'];

const ALL_TOOLS = toolDefinitions.map((t) => t.name);

test('every tool the server can list is in TOOL_CLASSES or UNCLASSED', () => {
  const missing = ALL_TOOLS.filter((name) => !Object.hasOwn(TOOL_CLASSES, name) && !UNCLASSED.has(name));
  assert.deepEqual(missing, [], 'class new tools in src/tool-classes.ts (or list them in UNCLASSED)');
});

test('the class map names only real tools, and none is both classed and unclassed', () => {
  const names = new Set(ALL_TOOLS);
  assert.deepEqual(Object.keys(TOOL_CLASSES).filter((n) => !names.has(n)), []);
  assert.deepEqual([...UNCLASSED].filter((n) => !names.has(n)), []);
  assert.deepEqual([...UNCLASSED].filter((n) => Object.hasOwn(TOOL_CLASSES, n)), []);
});

test('every class is from the RFC-008 vocabulary', () => {
  assert.deepEqual([...TOOL_CLASS_VOCABULARY], RFC_008_VOCABULARY);
  for (const [name, classes] of Object.entries(TOOL_CLASSES)) {
    assert.ok(classes.length > 0, `${name}: empty class list (use UNCLASSED instead)`);
    assert.equal(new Set(classes).size, classes.length, `${name}: duplicate class`);
    for (const c of classes) assert.ok(RFC_008_VOCABULARY.includes(c), `${name}: unknown class ${c}`);
  }
});

test('the class merges into existing _meta without touching the definitions', () => {
  const defs = [
    { name: 'send_dm', _meta: { 'example.com/x': 1 } },
    { name: 'not_a_tool', _meta: { 'example.com/y': 2 } },
  ];
  const before = structuredClone(defs);
  const out = withToolClasses(defs);
  assert.deepEqual(out[0]._meta, { 'example.com/x': 1, 'mcpl/class': ['comms', 'files'] });
  assert.equal(out[1], defs[1], 'an unclassed tool passes through unchanged, with no class key');
  assert.deepEqual(defs, before);
});
