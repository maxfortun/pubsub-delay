import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { Kafka, Admin, Producer, Consumer, EachMessagePayload } from 'kafkajs';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');

const BP_INGEST = process.env.BP_INGEST_TOPIC || 'bp-ingest';
const BP_OUTPUT = process.env.BP_OUTPUT_TOPIC || 'bp-output';
const TW_INGEST = process.env.TW_INGEST_TOPIC || 'tw-ingest';
const TW_OUTPUT = process.env.TW_OUTPUT_TOPIC || 'tw-output';

interface ReceivedMessage {
  key: string | undefined;
  receivedAt: number;
  headers: Record<string, string>;
  strategy: string;
}

interface SentMessage {
  id: string;
  delayMs: number;
  sentAt: number;
  strategy: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

describe('Strategy Comparison Tests', { timeout: 60000 }, () => {
  let kafka: Kafka;
  let admin: Admin;
  let producer: Producer;
  let bpConsumer: Consumer;
  let twConsumer: Consumer;
  const receivedMessages: ReceivedMessage[] = [];

  before(async () => {
    kafka = new Kafka({
      clientId: 'compare-test',
      brokers: KAFKA_BROKERS,
    });

    admin = kafka.admin();
    await admin.connect();

    // Create output topics
    try {
      await admin.createTopics({
        topics: [
          { topic: BP_OUTPUT, numPartitions: 1 },
          { topic: TW_OUTPUT, numPartitions: 1 },
        ],
      });
    } catch {
      // Topics may already exist
    }

    producer = kafka.producer();
    await producer.connect();

    // Set up consumers for both outputs
    bpConsumer = kafka.consumer({ groupId: 'compare-test-bp' });
    await bpConsumer.connect();
    await bpConsumer.subscribe({ topic: BP_OUTPUT, fromBeginning: true });

    twConsumer = kafka.consumer({ groupId: 'compare-test-tw' });
    await twConsumer.connect();
    await twConsumer.subscribe({ topic: TW_OUTPUT, fromBeginning: true });

    await bpConsumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        receivedMessages.push({
          key: message.key?.toString(),
          receivedAt: Date.now(),
          headers: Object.fromEntries(
            Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString() || ''])
          ),
          strategy: 'BoundedPool',
        });
      },
    });

    await twConsumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        receivedMessages.push({
          key: message.key?.toString(),
          receivedAt: Date.now(),
          headers: Object.fromEntries(
            Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString() || ''])
          ),
          strategy: 'TimeWheel',
        });
      },
    });

    console.log('Both services ready');
  });

  after(async () => {
    await bpConsumer.disconnect();
    await twConsumer.disconnect();
    await producer.disconnect();
    await admin.disconnect();
  });

  it('should compare timing accuracy between strategies', async () => {
    receivedMessages.length = 0;
    const sentMessages: SentMessage[] = [];
    const toleranceMs = 150; // Allow 150ms variance

    // Send identical messages to both strategies (10 messages each for better stats)
    const delays = ['PT1S', 'PT2S', 'PT3S', 'PT1S', 'PT2S', 'PT3S', 'PT1S', 'PT2S', 'PT3S', 'PT1S'];
    const batchId = Date.now();

    for (let i = 0; i < delays.length; i++) {
      const duration = delays[i];
      const delayMs = parseIsoDuration(duration);
      const sentAt = Date.now();

      // Send to BoundedPool - use ENQUEUED_AT header for precise measurement
      const bpId = `bp-${batchId}-${i}`;
      const bpEnqueuedAt = Date.now();
      await producer.send({
        topic: BP_INGEST,
        messages: [{
          key: bpId,
          value: Buffer.from(`BP test ${i}`),
          headers: {
            DELAY_DURATION: duration,
            DELAY_DESTINATION: BP_OUTPUT,
            DELAY_ENQUEUED_AT: bpEnqueuedAt.toString(),
            TEST_ID: bpId,
          },
        }],
      });
      sentMessages.push({ id: bpId, delayMs, sentAt: bpEnqueuedAt, strategy: 'BoundedPool' });

      // Send to TimeWheel
      const twId = `tw-${batchId}-${i}`;
      const twEnqueuedAt = Date.now();
      await producer.send({
        topic: TW_INGEST,
        messages: [{
          key: twId,
          value: Buffer.from(`TW test ${i}`),
          headers: {
            DELAY_DURATION: duration,
            DELAY_DESTINATION: TW_OUTPUT,
            DELAY_ENQUEUED_AT: twEnqueuedAt.toString(),
            TEST_ID: twId,
          },
        }],
      });
      sentMessages.push({ id: twId, delayMs, sentAt: twEnqueuedAt, strategy: 'TimeWheel' });
    }

    console.log(`Sent ${sentMessages.length} messages (${delays.length} per strategy)`);

    // Wait for all messages (max delay + buffer for processing)
    const maxDelayMs = Math.max(...sentMessages.map((m) => m.delayMs));
    await delay(maxDelayMs + 5000);

    // Analyze results per strategy
    const results: Record<string, { avgError: number; maxError: number; count: number }> = {};

    for (const strategy of ['BoundedPool', 'TimeWheel']) {
      const sent = sentMessages.filter((m) => m.strategy === strategy);
      const received = receivedMessages.filter((m) => m.strategy === strategy);

      const errors: number[] = [];
      for (const s of sent) {
        const r = received.find((m) => m.headers['TEST_ID'] === s.id);
        if (r) {
          const actualDelay = r.receivedAt - s.sentAt;
          const error = Math.abs(actualDelay - s.delayMs);
          errors.push(error);
        }
      }

      results[strategy] = {
        avgError: errors.length > 0 ? errors.reduce((a, b) => a + b, 0) / errors.length : 0,
        maxError: errors.length > 0 ? Math.max(...errors) : 0,
        count: received.length,
      };
    }

    console.log('\n=== Strategy Comparison ===');
    console.log('BoundedPool:', results['BoundedPool']);
    console.log('TimeWheel:', results['TimeWheel']);
    console.log('===========================\n');

    // Both should receive all messages
    assert.strictEqual(results['BoundedPool'].count, delays.length, 'BoundedPool should receive all messages');
    assert.strictEqual(results['TimeWheel'].count, delays.length, 'TimeWheel should receive all messages');

    // Both should be within tolerance
    assert.ok(results['BoundedPool'].maxError <= toleranceMs, `BoundedPool max error ${results['BoundedPool'].maxError}ms exceeds tolerance`);
    assert.ok(results['TimeWheel'].maxError <= toleranceMs, `TimeWheel max error ${results['TimeWheel'].maxError}ms exceeds tolerance`);
  });

  it('should compare throughput under load', async () => {
    receivedMessages.length = 0;
    const messageCount = 50;
    const sentMessages: SentMessage[] = [];
    const batchId = Date.now();

    console.log(`Sending ${messageCount} messages per strategy...`);
    const sendStart = Date.now();

    // Send messages to both strategies in parallel
    for (let i = 0; i < messageCount; i++) {
      const delaySec = (i % 3) + 1; // 1-3 seconds
      const duration = `PT${delaySec}S`;
      const delayMs = delaySec * 1000;
      const sentAt = Date.now();

      const bpId = `bp-load-${batchId}-${i}`;
      const twId = `tw-load-${batchId}-${i}`;
      const enqueuedAt = Date.now();

      await Promise.all([
        producer.send({
          topic: BP_INGEST,
          messages: [{
            key: bpId,
            value: Buffer.from(`BP load ${i}`),
            headers: {
              DELAY_DURATION: duration,
              DELAY_DESTINATION: BP_OUTPUT,
              DELAY_ENQUEUED_AT: enqueuedAt.toString(),
              TEST_ID: bpId,
            },
          }],
        }),
        producer.send({
          topic: TW_INGEST,
          messages: [{
            key: twId,
            value: Buffer.from(`TW load ${i}`),
            headers: {
              DELAY_DURATION: duration,
              DELAY_DESTINATION: TW_OUTPUT,
              DELAY_ENQUEUED_AT: enqueuedAt.toString(),
              TEST_ID: twId,
            },
          }],
        }),
      ]);

      sentMessages.push({ id: bpId, delayMs, sentAt: enqueuedAt, strategy: 'BoundedPool' });
      sentMessages.push({ id: twId, delayMs, sentAt: enqueuedAt, strategy: 'TimeWheel' });
    }

    const sendEnd = Date.now();
    console.log(`Sent all messages in ${sendEnd - sendStart}ms`);

    // Wait for all messages (max 3s delay + buffer)
    await delay(6000);

    // Count received per strategy
    const bpReceived = receivedMessages.filter((m) => m.strategy === 'BoundedPool' && m.headers['TEST_ID']?.startsWith(`bp-load-${batchId}`));
    const twReceived = receivedMessages.filter((m) => m.strategy === 'TimeWheel' && m.headers['TEST_ID']?.startsWith(`tw-load-${batchId}`));

    console.log('\n=== Load Test Results ===');
    console.log(`BoundedPool: ${bpReceived.length}/${messageCount} messages`);
    console.log(`TimeWheel: ${twReceived.length}/${messageCount} messages`);
    console.log('=========================\n');

    assert.strictEqual(bpReceived.length, messageCount, `BoundedPool lost ${messageCount - bpReceived.length} messages`);
    assert.strictEqual(twReceived.length, messageCount, `TimeWheel lost ${messageCount - twReceived.length} messages`);
  });
});

function parseIsoDuration(duration: string): number {
  const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
  if (!match) throw new Error(`Invalid duration: ${duration}`);

  const hours = parseInt(match[1] || '0', 10);
  const minutes = parseInt(match[2] || '0', 10);
  const seconds = parseInt(match[3] || '0', 10);

  return (hours * 3600 + minutes * 60 + seconds) * 1000;
}
