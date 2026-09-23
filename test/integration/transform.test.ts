import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import { createServer, Server } from 'http';
import { gzipSync, gunzipSync } from 'zlib';
import { Kafka, Admin, Producer, Consumer } from 'kafkajs';

// A second pubsub-delay instance runs with TRANSFORM_PLUGIN=http pointed at this mock plugin
const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const INGEST_TOPIC = process.env.XFORM_INGEST_TOPIC || 'xform-ingest';
const DESTINATION_TOPIC = process.env.XFORM_DESTINATION_TOPIC || 'xform-output';
const MOCK_PORT = parseInt(process.env.TRANSFORM_MOCK_PORT || '18081', 10);
const VALID_TOKEN = process.env.TRANSFORM_MOCK_TOKEN || 'Bearer valid-token';

interface Received {
  headers: Record<string, string>;
  body: Buffer;
}

function delay(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function until(cond: () => boolean, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond() && Date.now() < deadline) await delay(100);
}

const toHeaders = (h: Record<string, Buffer | string | undefined> | undefined) =>
  Object.fromEntries(Object.entries(h || {}).map(([k, v]) => [k, v?.toString() ?? '']));

// Pre: auth check (401 unless the token is valid), strip Authorization, gzip the body.
// Post: gunzip the body. Fails the first post call per TEST_ID with 503 when FAIL_ONCE is set.
function startMockPlugin(calls: { stage: string; testId: string }[]): Promise<Server> {
  const failedOnce = new Set<string>();
  const server = createServer((req, res) => {
    let data = '';
    req.on('data', (c) => (data += c));
    req.on('end', () => {
      const msg = JSON.parse(data);
      const testId = msg.headers.TEST_ID ?? '';
      calls.push({ stage: msg.stage, testId });
      const body = Buffer.from(msg.body, 'base64');

      if (req.url === '/pre') {
        if (msg.headers.Authorization !== VALID_TOKEN) {
          res.writeHead(401).end('invalid or missing Authorization');
          return;
        }
        const { Authorization: _, ...headers } = msg.headers;
        res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({
          headers: { ...headers, X_PRE_TRANSFORMED: 'gzip' },
          body: gzipSync(body).toString('base64'),
        }));
        return;
      }

      if (msg.headers.FAIL_ONCE && !failedOnce.has(testId)) {
        failedOnce.add(testId);
        res.writeHead(503).end('try later');
        return;
      }
      res.writeHead(200, { 'content-type': 'application/json' }).end(JSON.stringify({
        headers: { ...msg.headers, X_POST_TRANSFORMED: 'gunzip' },
        body: gunzipSync(body).toString('base64'),
      }));
    });
  });
  return new Promise((r) => server.listen(MOCK_PORT, () => r(server)));
}

describe('Transform plugin (http) end to end', { timeout: 120000 }, () => {
  let kafka: Kafka;
  let admin: Admin;
  let producer: Producer;
  let consumer: Consumer;
  let bucketConsumer: Consumer;
  let server: Server;
  const calls: { stage: string; testId: string }[] = [];
  const received = new Map<string, Received>();
  const inBucket = new Map<string, Received>();

  const send = (testId: string, headers: Record<string, string>, body = `payload ${testId}`) =>
    producer.send({
      topic: INGEST_TOPIC,
      messages: [{
        key: testId,
        value: Buffer.from(body),
        headers: { DELAY_DURATION: 'PT2S', DELAY_DESTINATION: DESTINATION_TOPIC, TEST_ID: testId, ...headers },
      }],
    });

  before(async () => {
    server = await startMockPlugin(calls);
    kafka = new Kafka({ clientId: 'transform-test', brokers: KAFKA_BROKERS });
    admin = kafka.admin();
    await admin.connect();
    try {
      await admin.createTopics({ topics: [{ topic: DESTINATION_TOPIC, numPartitions: 1 }] });
    } catch { /* exists */ }

    producer = kafka.producer();
    await producer.connect();

    consumer = kafka.consumer({ groupId: `transform-test-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic: DESTINATION_TOPIC, fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        const headers = toHeaders(message.headers as never);
        received.set(headers.TEST_ID, { headers, body: message.value ?? Buffer.alloc(0) });
      },
    });

    // Read the bucket directly to check what the broker actually stores between the transforms
    bucketConsumer = kafka.consumer({ groupId: `transform-bucket-${Date.now()}` });
    await bucketConsumer.connect();
    await bucketConsumer.subscribe({ topic: `${INGEST_TOPIC}-PT2S`, fromBeginning: true });
    await bucketConsumer.run({
      eachMessage: async ({ message }) => {
        const headers = toHeaders(message.headers as never);
        inBucket.set(headers.TEST_ID, { headers, body: message.value ?? Buffer.alloc(0) });
      },
    });
  });

  after(async () => {
    await consumer.disconnect();
    await bucketConsumer.disconnect();
    await producer.disconnect();
    await admin.disconnect();
    await new Promise((r) => server.close(r));
  });

  it('applies pre and post transforms: auth header stripped, body compressed in the bucket, restored on delivery', async () => {
    const id = `xform-ok-${Date.now()}`;
    const original = 'hello '.repeat(50);
    const sentAt = Date.now();
    await send(id, { Authorization: VALID_TOKEN }, original);

    await until(() => received.has(id) && inBucket.has(id), 15000);
    const out = received.get(id);
    assert.ok(out, 'message not delivered');
    assert.ok(Date.now() - sentAt >= 2000, 'delivered before its delay');

    assert.strictEqual(out.body.toString(), original);
    assert.strictEqual(out.headers.X_PRE_TRANSFORMED, 'gzip');
    assert.strictEqual(out.headers.X_POST_TRANSFORMED, 'gunzip');
    assert.strictEqual(out.headers.Authorization, undefined, 'Authorization must not be forwarded');
    assert.strictEqual(out.headers.DELAY_ENQUEUED_AT, undefined);

    const stored = inBucket.get(id);
    assert.ok(stored, 'message not found in bucket');
    assert.strictEqual(stored.headers.Authorization, undefined, 'Authorization must not be stored');
    assert.strictEqual(gunzipSync(stored.body).toString(), original, 'bucket should hold the gzipped body');
    assert.ok(stored.body.length < Buffer.byteLength(original));

    assert.deepStrictEqual(calls.filter((c) => c.testId === id).map((c) => c.stage), ['pre', 'post']);
  });

  it('drops messages that fail the pre-transform auth check', async () => {
    const missing = `xform-noauth-${Date.now()}`;
    const bad = `xform-badauth-${Date.now()}`;
    const good = `xform-after-${Date.now()}`;
    await send(missing, {});
    await send(bad, { Authorization: 'Bearer forged' });
    // A valid message sent afterwards proves the router moved past the rejected ones
    await send(good, { Authorization: VALID_TOKEN });

    await until(() => received.has(good), 15000);
    assert.ok(received.has(good), 'valid message after rejected ones not delivered');
    await delay(2000);
    assert.ok(!received.has(missing), 'message without Authorization was delivered');
    assert.ok(!received.has(bad), 'message with a forged token was delivered');
    assert.ok(!inBucket.has(missing) && !inBucket.has(bad), 'rejected messages must not reach a bucket');
    assert.strictEqual(calls.filter((c) => c.testId === missing && c.stage === 'post').length, 0);
  });

  it('retries a post-transform failure and delivers exactly once', async () => {
    const id = `xform-retry-${Date.now()}`;
    await send(id, { Authorization: VALID_TOKEN, FAIL_ONCE: '1' });

    await until(() => received.has(id), 20000);
    assert.ok(received.has(id), 'message not delivered after a transient post-transform failure');
    assert.strictEqual(received.get(id)!.headers.X_POST_TRANSFORMED, 'gunzip');
    assert.ok(calls.filter((c) => c.testId === id && c.stage === 'post').length >= 2, 'post-transform was not retried');
  });
});
