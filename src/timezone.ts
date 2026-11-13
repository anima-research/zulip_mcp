export function isValidTimeZone(zone: string): boolean {
  try {
    new Intl.DateTimeFormat('en-US', { timeZone: zone }).format(new Date(0));
    return true;
  } catch {
    return false;
  }
}

/** Never throws: a typo'd AGENT_TIMEZONE must not kill the stdio child before
 *  the MCPL handshake, since a host respawns a dead child only when it is
 *  configured to reconnect. */
export function resolveAgentTimeZone(configured = process.env.AGENT_TIMEZONE): string {
  const fallback = Intl.DateTimeFormat().resolvedOptions().timeZone || 'UTC';
  const zone = configured?.trim();
  if (!zone) return fallback;
  if (!isValidTimeZone(zone)) {
    console.error(`[timezone] Invalid AGENT_TIMEZONE ${JSON.stringify(zone)} — falling back to system time zone ${JSON.stringify(fallback)}`);
    return fallback;
  }
  return zone;
}

export type TimestampStyle = 'full' | 'compact' | 'time' | 'none';

const TIMESTAMP_STYLES: ReadonlySet<string> = new Set(['full', 'compact', 'time', 'none']);

/** Never throws, for the reason resolveAgentTimeZone gives. */
export function resolveTimestampStyle(configured = process.env.AGENT_TIMESTAMP_STYLE): TimestampStyle {
  const style = configured?.trim().toLowerCase();
  if (!style) return 'full';
  if (TIMESTAMP_STYLES.has(style)) return style as TimestampStyle;
  console.error(
    `[timezone] Invalid AGENT_TIMESTAMP_STYLE ${JSON.stringify(configured)} — ` +
    `falling back to "full" (valid: full, compact, time, none)`,
  );
  return 'full';
}

export function formatAgentDateTime(
  value: Date | number,
  timeZone = resolveAgentTimeZone(),
  style: TimestampStyle = 'full',
  options: { zoneName?: boolean } = {},
): string {
  if (style === 'none') return '';
  const date = value instanceof Date ? value : new Date(value);
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
    hourCycle: 'h23', timeZoneName: 'longOffset',
  }).formatToParts(date);
  const get = (type: Intl.DateTimeFormatPartTypes) => parts.find((part) => part.type === type)?.value ?? '';
  if (style === 'time') return `${get('hour')}:${get('minute')}`;
  if (style === 'compact') return `${get('year')}-${get('month')}-${get('day')} ${get('hour')}:${get('minute')}`;
  const rawOffset = get('timeZoneName');
  const offset = rawOffset === 'GMT' ? '+00:00' : rawOffset.replace('GMT', '');
  const stamp = `${get('year')}-${get('month')}-${get('day')}T${get('hour')}:${get('minute')}:${get('second')}${offset}`;
  return options.zoneName === false ? stamp : `${stamp} [${timeZone}]`;
}

export function agentLineTimeFormatter(env: NodeJS.ProcessEnv = process.env): (d: Date) => string {
  const zone = resolveAgentTimeZone(env.AGENT_TIMEZONE);
  const style = resolveTimestampStyle(env.AGENT_TIMESTAMP_STYLE);
  return (d) => (Number.isNaN(d.getTime()) ? '' : formatAgentDateTime(d, zone, style, { zoneName: false }));
}
