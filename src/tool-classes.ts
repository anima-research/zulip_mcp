/**
 * Tool classes: MCPL RFC-008
 * (https://github.com/anima-research/mcpl/blob/main/RFC-008-tool-classes.md).
 *
 * Hosts use `_meta["mcpl/class"]` as a policy key. They never share `comms`
 * arguments, so a messaging tool classed without `comms` could expose a
 * private conversation. Hosts apply the strictest of a tool's classes, so the
 * send tools are also `files` (their attachments may be local paths), and
 * treat a tool with no class most restrictively, so leaving one out
 * (UNCLASSED) is always safe; mislabelling one is not.
 */

/** The RFC-008 vocabulary. */
export const TOOL_CLASS_VOCABULARY = [
  'comms',
  'memory',
  'notes',
  'files',
  'shell',
  'web',
  'computer',
  'media',
  'body',
  'control',
] as const;

export type ToolClass = (typeof TOOL_CLASS_VOCABULARY)[number];

export const TOOL_CLASSES: Record<string, readonly ToolClass[]> = {
  // Sends that can carry files.
  send_message: ['comms', 'files'],
  send_dm: ['comms', 'files'],
  upload_file: ['comms', 'files'],
  // Messages, and the people, streams and emoji around them.
  edit_message: ['comms'],
  delete_message: ['comms'],
  add_reaction: ['comms'],
  remove_reaction: ['comms'],
  get_channel_history: ['comms'],
  get_unread_messages: ['comms'],
  fetch_history: ['comms'],
  fetch_around: ['comms'],
  fetch_attachment: ['comms'],
  list_users: ['comms'],
  find_user: ['comms'],
  get_stream_topics: ['comms'],
  list_streams: ['comms'],
  list_emojis: ['comms'],
  // This server's own delivery state and the bot's own profile.
  get_user_profile: ['control'],
  start_monitoring: ['control'],
  stop_monitoring: ['control'],
  get_monitored_channels: ['control'],
  listen: ['control'],
  unlisten: ['control'],
  filters_get: ['control'],
  filters_update: ['control'],
  mute_channel: ['control'],
  unmute_channel: ['control'],
  refresh_channels: ['control'],
  set_reaction_visibility: ['control'],
  channel_missed: ['control'],
};

/** Tools deliberately left without a class. None today. */
export const UNCLASSED: ReadonlySet<string> = new Set<string>();

/** The tools with their class added to `_meta`, keeping any other `_meta`
 *  keys. Unclassed tools are returned as they are. */
export function withToolClasses<T extends { name: string; _meta?: Record<string, unknown> }>(
  tools: readonly T[],
): T[] {
  return tools.map((tool) => {
    if (!Object.hasOwn(TOOL_CLASSES, tool.name)) return tool;
    return { ...tool, _meta: { ...tool._meta, 'mcpl/class': [...TOOL_CLASSES[tool.name]] } };
  });
}
