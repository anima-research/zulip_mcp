/**
 * Context Provider — Handles context/beforeInference.
 *
 * Injects recent message history from open channels into the inference
 * context. History fetching/formatting is delegated to the owning
 * PlatformAdapter per channel.
 */

import type {
  ContextBeforeInferenceParams,
  ContextBeforeInferenceResult,
  ContextInjection,
} from '@animalabs/mcpl-core';
import type { ChannelKey } from './channel-key.js';
import type { ChannelManager } from './channels.js';
import type { CapabilityGrant } from './grant.js';

const DEFAULT_HISTORY_SIZE = 20;

/** SPEC §6.2 — the capability path for each injection position. */
const POSITION_CAPABILITY: Record<ContextInjection['position'], string> = {
  system: 'contextHooks.beforeInference.inject.system',
  beforeUser: 'contextHooks.beforeInference.inject.beforeUser',
  afterUser: 'contextHooks.beforeInference.inject.afterUser',
};

export interface ContextProviderOptions {
  /** Open channels for which nothing is injected (a muted stream). */
  excludeChannel?: (key: ChannelKey) => boolean;
}

export class ContextProvider {
  private historySize: number;
  private readonly excludeChannel: (key: ChannelKey) => boolean;

  constructor(
    private channelManager: Pick<ChannelManager, 'getOpenChannels' | 'keyOfHeld' | 'adapterFor' | 'getChannel' | 'firstAdapterType'>,
    private grant: CapabilityGrant,
    historySize?: number,
    options: ContextProviderOptions = {},
  ) {
    this.historySize = historySize ?? DEFAULT_HISTORY_SIZE;
    this.excludeChannel = options.excludeChannel ?? (() => false);
  }

  /**
   * Handle context/beforeInference — return context injections for open channels.
   *
   * `params` is deliberately unread. This server injects history and never
   * needs the user's text, so it does not declare
   * `contextHooks.beforeInference.observe` and a conforming host sends
   * `userMessage: null` regardless (§10.1). Injections are filtered by
   * position against the grant before returning: the host authorizes each one
   * independently at response-receipt (§5.4, §10.8), and a server that must
   * respect a reduction immediately (§6.7) should not be offering them.
   */
  async handleBeforeInference(_params: ContextBeforeInferenceParams): Promise<ContextBeforeInferenceResult> {
    const injections: ContextInjection[] = [];
    const contributingTypes = new Set<string>();
    const openChannels = this.channelManager.getOpenChannels();

    for (const channelId of openChannels) {
      const key = this.channelManager.keyOfHeld(channelId);
      if (key === null || this.excludeChannel(key)) continue;
      const adapter = this.channelManager.adapterFor(channelId);
      if (!adapter) continue;
      // §6.7: a server must immediately respect a reduction. The response
      // claims `{type}.context` (§6.5); if the host disabled it, or the grant
      // no longer covers what it declares, this server does not contribute
      // under it. That is derivation, not authorization — the host authorizes
      // each injection again at response-receipt (§5.4).
      if (!this.grant.isFeatureSetActive(`${adapter.type}.context`)) continue;
      try {
        const fetched = await adapter.fetchContext(
          key,
          this.channelManager.getChannel(channelId),
          this.historySize,
        );
        if (fetched) {
          const injection: ContextInjection = { ...fetched, namespace: channelId };
          if (!this.grant.has(POSITION_CAPABILITY[injection.position])) {
            console.error(
              `Dropping ${injection.position} injection for ${channelId}: ` +
                `${POSITION_CAPABILITY[injection.position]} not granted`,
            );
            continue;
          }
          injections.push(injection);
          contributingTypes.add(adapter.type);
        }
      } catch (error) {
        console.error(`Failed to get context for channel ${channelId}:`, error);
      }
    }

    // Tag with the contributing platform's context feature set. When multiple
    // (or no) platforms contributed, fall back to the first registered one.
    const type = contributingTypes.size === 1
      ? contributingTypes.values().next().value
      : this.channelManager.firstAdapterType();

    return {
      featureSet: `${type}.context`,
      contextInjections: injections,
    };
  }
}
