/**
 * SPEC 0.5 §6.6: a handler that cannot answer must return a JSON-RPC error
 * object with a documented code, never silence.
 */

import { ERR_CAPABILITY_DENIED } from '@animalabs/mcpl-core';

export class McplRpcError extends Error {
  constructor(
    readonly code: number,
    message: string,
    readonly data?: unknown,
  ) {
    super(message);
    this.name = 'McplRpcError';
  }
}

export function capabilityDenied(capability: string): McplRpcError {
  return new McplRpcError(ERR_CAPABILITY_DENIED, `Capability denied: ${capability}`, { capability });
}
