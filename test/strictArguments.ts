/** Check strict callers against the flat type/enum/minimum constraints we publish.
 * The server transports inputSchema unchanged; it has no generic schema validator.
 */
import assert from 'node:assert/strict';
import { toolDefinitions } from '../src/tools.ts';

export function assertStrictArguments(name: string, args: Record<string, unknown>) {
  const properties = toolDefinitions.find(t => t.name === name)!.inputSchema.properties as Record<string, any>;
  assert.deepEqual(Object.keys(args).sort(), Object.keys(properties).sort(), 'strict callers include every property');
  for (const [key, schema] of Object.entries(properties)) {
    const value = args[key];
    const types = Array.isArray(schema.type) ? schema.type : [schema.type];
    const matches = types.some((type: string) => type === 'null' ? value === null
      : type === 'integer' ? Number.isInteger(value)
      : type === 'number' ? typeof value === 'number' && Number.isFinite(value)
      : type === 'array' ? Array.isArray(value) : typeof value === type);
    assert.ok(matches, `${name}.${key}: ${JSON.stringify(value)} must match ${JSON.stringify(types)}`);
    if (schema.enum) assert.ok(schema.enum.includes(value), `${name}.${key}: value must be in enum`);
    if (typeof value === 'number' && schema.minimum !== undefined) assert.ok(value >= schema.minimum, `${name}.${key}: minimum`);
  }
}
