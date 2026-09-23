import { Kafka, Admin, Producer, Consumer, EachMessagePayload } from 'kafkajs';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const DURATION_MINUTES = parseInt(process.env.STRESS_DURATION_MINUTES || '60', 10);
const MESSAGES_PER_SECOND = parseInt(process.env.MESSAGES_PER_SECOND || '100', 10);
const REPORT_INTERVAL_SECONDS = parseInt(process.env.REPORT_INTERVAL_SECONDS || '30', 10);

const BP_INGEST = process.env.BP_INGEST_TOPIC || 'bp-ingest';
const BP_OUTPUT = process.env.BP_OUTPUT_TOPIC || 'bp-output';
const TW_INGEST = process.env.TW_INGEST_TOPIC || 'tw-ingest';
const TW_OUTPUT = process.env.TW_OUTPUT_TOPIC || 'tw-output';

interface Stats {
  sent: number;
  received: number;
  totalError: number;
  maxError: number;
  minError: number;
  errors: number[];  // Last 1000 for percentile calculation
}

function createStats(): Stats {
  return { sent: 0, received: 0, totalError: 0, maxError: 0, minError: Infinity, errors: [] };
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = Math.ceil(p / 100 * sorted.length) - 1;
  return sorted[Math.max(0, idx)];
}

async function main() {
  console.log(`\n=== Stress Test Configuration ===`);
  console.log(`Duration: ${DURATION_MINUTES} minutes`);
  console.log(`Target rate: ${MESSAGES_PER_SECOND} msg/sec per strategy`);
  console.log(`Report interval: ${REPORT_INTERVAL_SECONDS} seconds`);
  console.log(`================================\n`);

  const kafka = new Kafka({ clientId: 'stress-test', brokers: KAFKA_BROKERS });
  const admin = kafka.admin();
  await admin.connect();

  // Create output topics
  try {
    await admin.createTopics({
      topics: [
        { topic: BP_OUTPUT, numPartitions: 1 },
        { topic: TW_OUTPUT, numPartitions: 1 },
      ],
    });
  } catch { /* ignore */ }

  const producer = kafka.producer();
  await producer.connect();

  // Stats per strategy
  const stats: Record<string, Stats> = {
    BoundedPool: createStats(),
    TimeWheel: createStats(),
  };

  // Track sent messages for timing calculation
  const pending = new Map<string, { sentAt: number; delayMs: number; strategy: string }>();

  // Set up consumers
  const bpConsumer = kafka.consumer({ groupId: 'stress-bp-' + Date.now() });
  await bpConsumer.connect();
  await bpConsumer.subscribe({ topic: BP_OUTPUT, fromBeginning: false });

  const twConsumer = kafka.consumer({ groupId: 'stress-tw-' + Date.now() });
  await twConsumer.connect();
  await twConsumer.subscribe({ topic: TW_OUTPUT, fromBeginning: false });

  const handleMessage = (strategy: string) => async ({ message }: EachMessagePayload) => {
    const testId = message.headers?.['TEST_ID']?.toString();
    if (!testId) return;

    const info = pending.get(testId);
    if (!info) return;

    pending.delete(testId);
    const receivedAt = Date.now();
    const actualDelay = receivedAt - info.sentAt;
    const error = Math.abs(actualDelay - info.delayMs);

    const s = stats[strategy];
    s.received++;
    s.totalError += error;
    s.maxError = Math.max(s.maxError, error);
    s.minError = Math.min(s.minError, error);
    s.errors.push(error);
    if (s.errors.length > 1000) s.errors.shift();
  };

  await bpConsumer.run({ eachMessage: handleMessage('BoundedPool') });
  await twConsumer.run({ eachMessage: handleMessage('TimeWheel') });

  // Reporting interval
  const reportInterval = setInterval(() => {
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    console.log(`\n--- Report at ${elapsed}s ---`);

    for (const [name, s] of Object.entries(stats)) {
      const avgError = s.received > 0 ? (s.totalError / s.received).toFixed(1) : 'N/A';
      const p50 = percentile(s.errors, 50).toFixed(0);
      const p95 = percentile(s.errors, 95).toFixed(0);
      const p99 = percentile(s.errors, 99).toFixed(0);
      const lossRate = s.sent > 0 ? ((1 - s.received / s.sent) * 100).toFixed(2) : '0';

      console.log(`${name}:`);
      console.log(`  sent=${s.sent} received=${s.received} loss=${lossRate}%`);
      console.log(`  avg=${avgError}ms p50=${p50}ms p95=${p95}ms p99=${p99}ms max=${s.maxError}ms`);
    }
    console.log(`  pending=${pending.size}`);
  }, REPORT_INTERVAL_SECONDS * 1000);

  // Message sender
  const delayOptions = [1, 2, 3, 4, 5]; // 1-5 second delays
  const intervalMs = 1000 / MESSAGES_PER_SECOND;
  let msgCounter = 0;
  const startTime = Date.now();
  const endTime = startTime + DURATION_MINUTES * 60 * 1000;

  console.log('Starting stress test...\n');

  const sendLoop = async () => {
    while (Date.now() < endTime) {
      const batchStart = Date.now();

      // Send a batch of messages
      for (let i = 0; i < MESSAGES_PER_SECOND && Date.now() < endTime; i++) {
        const id = `msg-${++msgCounter}`;
        const delaySec = delayOptions[msgCounter % delayOptions.length];
        const duration = `PT${delaySec}S`;
        const delayMs = delaySec * 1000;
        const sentAt = Date.now();

        // Send to both strategies
        const bpId = `bp-${id}`;
        const twId = `tw-${id}`;

        pending.set(bpId, { sentAt, delayMs, strategy: 'BoundedPool' });
        pending.set(twId, { sentAt, delayMs, strategy: 'TimeWheel' });

        await Promise.all([
          producer.send({
            topic: BP_INGEST,
            messages: [{
              key: bpId,
              value: Buffer.from(`stress-${id}`),
              headers: {
                DELAY_DURATION: duration,
                DELAY_DESTINATION: BP_OUTPUT,
                DELAY_ENQUEUED_AT: sentAt.toString(),
                TEST_ID: bpId,
              },
            }],
          }),
          producer.send({
            topic: TW_INGEST,
            messages: [{
              key: twId,
              value: Buffer.from(`stress-${id}`),
              headers: {
                DELAY_DURATION: duration,
                DELAY_DESTINATION: TW_OUTPUT,
                DELAY_ENQUEUED_AT: sentAt.toString(),
                TEST_ID: twId,
              },
            }],
          }),
        ]);

        stats.BoundedPool.sent++;
        stats.TimeWheel.sent++;
      }

      // Wait for next second
      const elapsed = Date.now() - batchStart;
      if (elapsed < 1000) {
        await new Promise(r => setTimeout(r, 1000 - elapsed));
      }
    }
  };

  await sendLoop();

  // Wait for remaining messages (max delay + buffer)
  console.log('\nWaiting for remaining messages to be delivered...');
  await new Promise(r => setTimeout(r, 10000));

  clearInterval(reportInterval);

  // Final report
  console.log(`\n========== FINAL RESULTS ==========`);
  console.log(`Duration: ${DURATION_MINUTES} minutes`);
  console.log(`Target rate: ${MESSAGES_PER_SECOND} msg/sec per strategy`);
  console.log(`Total messages: ${stats.BoundedPool.sent} per strategy\n`);

  for (const [name, s] of Object.entries(stats)) {
    const avgError = s.received > 0 ? (s.totalError / s.received).toFixed(1) : 'N/A';
    const p50 = percentile(s.errors, 50).toFixed(0);
    const p95 = percentile(s.errors, 95).toFixed(0);
    const p99 = percentile(s.errors, 99).toFixed(0);
    const lossRate = s.sent > 0 ? ((1 - s.received / s.sent) * 100).toFixed(4) : '0';

    console.log(`${name}:`);
    console.log(`  Sent:     ${s.sent}`);
    console.log(`  Received: ${s.received}`);
    console.log(`  Loss:     ${lossRate}%`);
    console.log(`  Avg:      ${avgError}ms`);
    console.log(`  P50:      ${p50}ms`);
    console.log(`  P95:      ${p95}ms`);
    console.log(`  P99:      ${p99}ms`);
    console.log(`  Max:      ${s.maxError}ms`);
    console.log(`  Min:      ${s.minError === Infinity ? 'N/A' : s.minError}ms`);
    console.log();
  }
  console.log(`===================================\n`);

  // Cleanup
  await bpConsumer.disconnect();
  await twConsumer.disconnect();
  await producer.disconnect();
  await admin.disconnect();

  // Exit with error if significant message loss
  const bpLoss = 1 - stats.BoundedPool.received / stats.BoundedPool.sent;
  const twLoss = 1 - stats.TimeWheel.received / stats.TimeWheel.sent;

  if (bpLoss > 0.01 || twLoss > 0.01) {
    console.error('ERROR: Message loss exceeded 1%');
    process.exit(1);
  }

  console.log('Stress test completed successfully!');
}

main().catch((err) => {
  console.error('Stress test failed:', err);
  process.exit(1);
});
