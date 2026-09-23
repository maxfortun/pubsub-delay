import { createBroker } from './broker/index.js';
import { loadConfig } from './config.js';
import { Router } from './router.js';
import { BucketAdvisory } from './bucket-advisory.js';
import { Scheduler } from './scheduler.js';

async function initializeTopics(broker: ReturnType<typeof createBroker>, config: ReturnType<typeof loadConfig>) {
  const admin = broker.admin();
  const requiredTopics = [config.ingestTopic, config.advisoryTopic];

  // Add pre-created bucket topics
  for (const duration of config.precreateBuckets) {
    const bucketTopic = `${config.ingestTopic}${config.bucketSeparator}${duration}`;
    requiredTopics.push(bucketTopic);
  }

  for (const topic of requiredTopics) {
    await createTopicWithRetry(admin, topic);
  }
}

async function createTopicWithRetry(admin: ReturnType<ReturnType<typeof createBroker>['admin']>, topic: string, retries = 5) {
  for (let i = 0; i < retries; i++) {
    try {
      const exists = await admin.topicExists(topic);
      if (!exists) {
        console.log(`Creating topic: ${topic}`);
        await admin.createTopic(topic);
      }
      return;
    } catch (error) {
      if (i < retries - 1) {
        const delay = 1000 * (i + 1);
        console.log(`Retry creating topic ${topic} in ${delay}ms (attempt ${i + 1}/${retries})`);
        await new Promise((r) => setTimeout(r, delay));
      } else {
        throw error;
      }
    }
  }
}

async function main() {
  const config = loadConfig();
  const broker = createBroker(config.broker);

  await broker.connect();
  console.log(`Connected to ${config.broker.type} broker`);

  await initializeTopics(broker, config);
  console.log('Required topics initialized');

  const advisory = new BucketAdvisory(broker, config);
  await advisory.start();

  const scheduler = new Scheduler(broker, config, advisory);
  await scheduler.start();

  const router = new Router(broker, config, advisory);

  const shutdown = async () => {
    console.log('\nShutting down...');
    await router.stop();
    await scheduler.stop();
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
