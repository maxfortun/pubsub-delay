import { createBroker } from './broker/index.js';
import { loadConfig } from './config.js';
import { Router } from './router.js';
import { BucketAdvisory } from './bucket-advisory.js';
import { WorkerPool } from './worker-pool.js';

async function main() {
  const config = loadConfig();
  const broker = createBroker(config.broker);

  await broker.connect();
  console.log(`Connected to ${config.broker.type} broker`);

  const advisory = new BucketAdvisory(broker, config);
  await advisory.start();

  const pool = new WorkerPool(broker, config, advisory);
  await pool.start();

  const router = new Router(broker, config, advisory);

  const shutdown = async () => {
    console.log('\nShutting down...');
    await router.stop();
    await pool.stop();
    await advisory.stop();
    await broker.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  await router.start();
}

main().catch((error) => {
  console.error('Fatal error:', error);
  process.exit(1);
});
