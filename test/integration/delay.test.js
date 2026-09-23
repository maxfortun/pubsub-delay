import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { Kafka } from 'kafkajs';
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const INGEST_TOPIC = process.env.INGEST_TOPIC || 'delay-ingest';
const DESTINATION_TOPIC = process.env.DESTINATION_TOPIC || 'delay-output';
const BUCKET_SEPARATOR = '-';
function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
async function waitForKafka(admin, maxRetries = 30) {
    for (let i = 0; i < maxRetries; i++) {
        try {
            await admin.listTopics();
            console.log('Kafka is ready');
            return;
        }
        catch {
            console.log(`Waiting for Kafka... (${i + 1}/${maxRetries})`);
            await delay(1000);
        }
    }
    throw new Error('Kafka not ready after max retries');
}
async function waitForTopic(admin, topic, maxRetries = 30) {
    for (let i = 0; i < maxRetries; i++) {
        const topics = await admin.listTopics();
        if (topics.includes(topic)) {
            console.log(`Topic ${topic} exists`);
            return;
        }
        await delay(1000);
    }
    throw new Error(`Topic ${topic} not created after max retries`);
}
async function waitForTopicDeleted(admin, topic, maxRetries = 60) {
    for (let i = 0; i < maxRetries; i++) {
        const topics = await admin.listTopics();
        if (!topics.includes(topic)) {
            console.log(`Topic ${topic} deleted`);
            return;
        }
        await delay(1000);
    }
    throw new Error(`Topic ${topic} not deleted after max retries`);
}
describe('PubSub Delay Integration Tests', { timeout: 120000 }, () => {
    let kafka;
    let admin;
    let producer;
    let consumer;
    const receivedMessages = [];
    before(async () => {
        kafka = new Kafka({
            clientId: 'integration-test',
            brokers: KAFKA_BROKERS,
        });
        admin = kafka.admin();
        await admin.connect();
        await waitForKafka(admin);
        // Create destination topic
        try {
            await admin.createTopics({
                topics: [{ topic: DESTINATION_TOPIC, numPartitions: 1 }],
            });
        }
        catch {
            // Topic may already exist
        }
        producer = kafka.producer();
        await producer.connect();
        consumer = kafka.consumer({ groupId: 'integration-test-consumer' });
        await consumer.connect();
        await consumer.subscribe({ topic: DESTINATION_TOPIC, fromBeginning: true });
        await consumer.run({
            eachMessage: async ({ message }) => {
                receivedMessages.push({
                    key: message.key?.toString(),
                    body: message.value?.toString() || '',
                    receivedAt: Date.now(),
                    headers: Object.fromEntries(Object.entries(message.headers || {}).map(([k, v]) => [k, v?.toString() || ''])),
                });
            },
        });
        // Wait for pubsub-delay service to be ready
        console.log('Waiting for pubsub-delay service...');
        await delay(5000);
    });
    after(async () => {
        await consumer.disconnect();
        await producer.disconnect();
        await admin.disconnect();
    });
    it('should delay messages by the specified duration', async () => {
        const sentMessages = [];
        const toleranceMs = 500; // Allow 500ms variance
        // Send messages with different delays
        const delays = ['PT2S', 'PT3S', 'PT1S'];
        for (let i = 0; i < delays.length; i++) {
            const id = `msg-${i}-${Date.now()}`;
            const delayDuration = delays[i];
            const delayMs = parseIsoDuration(delayDuration);
            const sentAt = Date.now();
            await producer.send({
                topic: INGEST_TOPIC,
                messages: [
                    {
                        key: id,
                        value: Buffer.from(`Test message ${i}`),
                        headers: {
                            DELAY_DURATION: delayDuration,
                            DELAY_DESTINATION: DESTINATION_TOPIC,
                            TEST_ID: id,
                        },
                    },
                ],
            });
            sentMessages.push({
                id,
                delayDuration,
                delayMs,
                sentAt,
                expectedDeliverAt: sentAt + delayMs,
            });
            console.log(`Sent message ${id} with delay ${delayDuration}`);
        }
        // Wait for all messages to be delivered (max delay + buffer)
        const maxDelayMs = Math.max(...sentMessages.map((m) => m.delayMs));
        console.log(`Waiting ${maxDelayMs + 5000}ms for messages to be delivered...`);
        await delay(maxDelayMs + 5000);
        // Verify all messages received
        assert.strictEqual(receivedMessages.length, sentMessages.length, `Expected ${sentMessages.length} messages, got ${receivedMessages.length}`);
        // Verify timing of each message
        for (const sent of sentMessages) {
            const received = receivedMessages.find((r) => r.headers['TEST_ID'] === sent.id);
            assert.ok(received, `Message ${sent.id} not received`);
            const actualDelay = received.receivedAt - sent.sentAt;
            const expectedDelay = sent.delayMs;
            const difference = Math.abs(actualDelay - expectedDelay);
            console.log(`Message ${sent.id}: expected ${expectedDelay}ms, actual ${actualDelay}ms, diff ${difference}ms`);
            assert.ok(difference <= toleranceMs, `Message ${sent.id} delay difference ${difference}ms exceeds tolerance ${toleranceMs}ms`);
        }
    });
    it('should deliver messages in order of their deliver_at time', async () => {
        receivedMessages.length = 0; // Clear previous messages
        const sentMessages = [];
        // Send in reverse order: long delay first, short delay last
        // They should be delivered in order: short first, long last
        const delays = [
            { duration: 'PT4S', order: 2 },
            { duration: 'PT2S', order: 1 },
            { duration: 'PT6S', order: 3 },
        ];
        const batchId = Date.now();
        for (let i = 0; i < delays.length; i++) {
            const id = `order-${batchId}-${i}`;
            const { duration, order } = delays[i];
            const delayMs = parseIsoDuration(duration);
            const sentAt = Date.now();
            await producer.send({
                topic: INGEST_TOPIC,
                messages: [
                    {
                        key: id,
                        value: Buffer.from(`Order test ${order}`),
                        headers: {
                            DELAY_DURATION: duration,
                            DELAY_DESTINATION: DESTINATION_TOPIC,
                            TEST_ID: id,
                            EXPECTED_ORDER: order.toString(),
                        },
                    },
                ],
            });
            sentMessages.push({
                id,
                delayDuration: duration,
                delayMs,
                sentAt,
                expectedDeliverAt: sentAt + delayMs,
            });
        }
        // Wait for all messages
        await delay(10000);
        // Verify order
        const orderReceived = receivedMessages
            .filter((m) => m.headers['TEST_ID']?.startsWith(`order-${batchId}`))
            .map((m) => parseInt(m.headers['EXPECTED_ORDER'], 10));
        console.log('Expected order: [1, 2, 3]');
        console.log('Received order:', orderReceived);
        assert.deepStrictEqual(orderReceived, [1, 2, 3], 'Messages not delivered in expected order');
    });
    it('should create bucket topics dynamically', async () => {
        const uniqueDuration = 'PT7S';
        const expectedBucketTopic = `${INGEST_TOPIC}${BUCKET_SEPARATOR}${uniqueDuration}`;
        // Verify bucket doesn't exist yet
        let topics = await admin.listTopics();
        const existedBefore = topics.includes(expectedBucketTopic);
        console.log(`Bucket topic ${expectedBucketTopic} exists before: ${existedBefore}`);
        // Send a message with unique delay
        await producer.send({
            topic: INGEST_TOPIC,
            messages: [
                {
                    value: Buffer.from('Bucket test'),
                    headers: {
                        DELAY_DURATION: uniqueDuration,
                        DELAY_DESTINATION: DESTINATION_TOPIC,
                    },
                },
            ],
        });
        // Wait for bucket to be created
        await waitForTopic(admin, expectedBucketTopic);
        topics = await admin.listTopics();
        assert.ok(topics.includes(expectedBucketTopic), `Bucket topic ${expectedBucketTopic} should exist`);
    });
    it('should delete idle bucket topics after timeout', async () => {
        const uniqueDuration = 'PT1S';
        const bucketTopic = `${INGEST_TOPIC}${BUCKET_SEPARATOR}${uniqueDuration}`;
        // Send a message to create the bucket
        await producer.send({
            topic: INGEST_TOPIC,
            messages: [
                {
                    value: Buffer.from('Idle test'),
                    headers: {
                        DELAY_DURATION: uniqueDuration,
                        DELAY_DESTINATION: DESTINATION_TOPIC,
                    },
                },
            ],
        });
        // Wait for bucket to be created
        await waitForTopic(admin, bucketTopic);
        // Wait for message to be delivered (1s delay + buffer)
        await delay(3000);
        // Wait for idle timeout (30s in test config) + cleanup interval (5s)
        console.log('Waiting for idle timeout and cleanup...');
        await waitForTopicDeleted(admin, bucketTopic, 60);
        const topics = await admin.listTopics();
        assert.ok(!topics.includes(bucketTopic), `Bucket topic ${bucketTopic} should be deleted after idle timeout`);
    });
    it('should not lose messages under load', async () => {
        receivedMessages.length = 0;
        const messageCount = 50;
        const sentIds = new Set();
        const batchId = Date.now();
        console.log(`Sending ${messageCount} messages...`);
        // Send many messages with varied delays
        for (let i = 0; i < messageCount; i++) {
            const id = `load-${batchId}-${i}`;
            const delaySec = (i % 5) + 1; // 1-5 seconds
            const duration = `PT${delaySec}S`;
            await producer.send({
                topic: INGEST_TOPIC,
                messages: [
                    {
                        key: id,
                        value: Buffer.from(`Load test message ${i}`),
                        headers: {
                            DELAY_DURATION: duration,
                            DELAY_DESTINATION: DESTINATION_TOPIC,
                            TEST_ID: id,
                        },
                    },
                ],
            });
            sentIds.add(id);
        }
        // Wait for all messages to be delivered (max 5s delay + buffer)
        console.log('Waiting for all messages to be delivered...');
        await delay(15000);
        // Verify all messages received
        const receivedIds = new Set(receivedMessages
            .filter((m) => m.headers['TEST_ID']?.startsWith(`load-${batchId}`))
            .map((m) => m.headers['TEST_ID']));
        const missing = [...sentIds].filter((id) => !receivedIds.has(id));
        const extra = [...receivedIds].filter((id) => !sentIds.has(id));
        console.log(`Sent: ${sentIds.size}, Received: ${receivedIds.size}`);
        if (missing.length > 0)
            console.log('Missing:', missing);
        if (extra.length > 0)
            console.log('Extra:', extra);
        assert.strictEqual(missing.length, 0, `Lost ${missing.length} messages: ${missing.join(', ')}`);
        assert.strictEqual(receivedIds.size, sentIds.size, 'Message count mismatch');
    });
});
function parseIsoDuration(duration) {
    const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?/);
    if (!match)
        throw new Error(`Invalid duration: ${duration}`);
    const hours = parseInt(match[1] || '0', 10);
    const minutes = parseInt(match[2] || '0', 10);
    const seconds = parseInt(match[3] || '0', 10);
    return (hours * 3600 + minutes * 60 + seconds) * 1000;
}
