import { contractEnv } from './env.ts';

try {
  contractEnv();
} catch (err) {
  console.error((err as Error).message);
  process.exit(1);
}
