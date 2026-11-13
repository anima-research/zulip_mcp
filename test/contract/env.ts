export interface ContractEnv {
  realm: string;
  bot: { email: string; apiKey: string };
  user: { email: string; apiKey: string };
}

const REQUIRED = ['ZULIP_REALM', 'ZULIP_EMAIL', 'ZULIP_API_KEY', 'ZULIP_CONTRACT_USER_EMAIL', 'ZULIP_CONTRACT_USER_API_KEY'] as const;

export function contractEnv(env: NodeJS.ProcessEnv = process.env): ContractEnv {
  const missing = REQUIRED.filter((name) => !env[name]);
  if (missing.length > 0) {
    throw new Error(
      `the contract suite needs a Zulip realm and two accounts; set ${missing.join(', ')} (scripts/contract-zulip.sh prints them)`,
    );
  }
  for (const name of Object.keys(env)) {
    if (/^(ZULIP_|MCPL_|AGENT_)/.test(name) && !(REQUIRED as readonly string[]).includes(name)) delete env[name];
  }
  return {
    realm: env.ZULIP_REALM!.replace(/\/+$/, ''),
    bot: { email: env.ZULIP_EMAIL!, apiKey: env.ZULIP_API_KEY! },
    user: { email: env.ZULIP_CONTRACT_USER_EMAIL!, apiKey: env.ZULIP_CONTRACT_USER_API_KEY! },
  };
}
