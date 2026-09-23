import { describe, it, before, after } from 'node:test';
import assert from 'node:assert';
import stompit from 'stompit';

const HOST = process.env.ACTIVEMQ_HOST || 'localhost';
const PORT = parseInt(process.env.ACTIVEMQ_PORT || '61613', 10);
const LOGIN = process.env.ACTIVEMQ_LOGIN || 'admin';
const PASSCODE = process.env.ACTIVEMQ_PASSCODE || 'admin';
const JOLOKIA_URL = process.env.ACTIVEMQ_JOLOKIA_URL || 'http://localhost:8161/api/jolokia';
const INGEST_TOPIC = process.env.INGEST_TOPIC || 'amq-ingest';
const DESTINATION_TOPIC = process.env.DESTINATION_TOPIC || 'amq-output';
const GROUP_DESTINATION_TOPIC = process.env.GROUP_DESTINATION_TOPIC || 'amq-grouped-output';
const BUCKET_SEPARATOR = '-';

interface ReceivedMessage {
  body: string;
  receivedAt: number;
  headers: Record<string, string>;
  consumer: string;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function connect(): Promise<stompit.Client> {
  const manager = new stompit.ConnectFailover([
    { host: HOST, port: PORT, connectHeaders: { host: '/', login: LOGIN, passcode: PASSCODE, 'heart-beat': '5000,5000' } },
  ]);
  return new Promise((resolve, reject) => manager.connect((error, client) => (error ? reject(error) : resolve(client))));
}

function send(client: stompit.Client, topic: string, headers: Record<string, string>, body: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const frame = client.send({ destination: `/queue/${topic}`, persistent: 'true', ...headers }, { onReceipt: resolve, onError: reject });
    frame.write(Buffer.from(body));
    frame.end();
  });
}

// Collects every message on a queue into `sink`, acking each one
function consume(client: stompit.Client, topic: string, sink: ReceivedMessage[], consumer: string): stompit.Client.Subscription {
  return client.subscribe({ destination: `/queue/${topic}`, ack: 'client-individual' }, (error, message) => {
    if (error) {
      console.error(`Test consumer error on ${topic}:`, error);
      return;
    }
    const chunks: Buffer[] = [];
    message.on('data', (c: Buffer) => chunks.push(c));
    message.on('end', () => {
      sink.push({
        body: Buffer.concat(chunks).toString(),
        receivedAt: Date.now(),
        headers: Object.fromEntries(Object.entries(message.headers).map(([k, v]) => [k, String(v)])),
        consumer,
      });
      client.ack(message);
    });
  });
}

async function jolokia(body: object): Promise<{ status: number; value?: unknown }> {
  const res = await fetch(JOLOKIA_URL, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Origin: 'http://localhost',
      Authorization: `Basic ${Buffer.from(`${LOGIN}:${PASSCODE}`).toString('base64')}`,
    },
    body: JSON.stringify(body),
  });
  return (await res.json()) as { status: number; value?: unknown };
}

async function listQueues(): Promise<string[]> {
  const reply = await jolokia({ type: 'search', mbean: 'org.apache.activemq:type=Broker,brokerName=*,destinationType=Queue,destinationName=*' });
  return ((reply.value as string[]) ?? []).map((n) => n.match(/destinationName=([^,]+)/)?.[1] ?? '');
}

async function waitFor(what: string, check: () => boolean | Promise<boolean>, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await delay(250);
  }
  throw new Error(`Timed out waiting for ${what}`);
}

describe('PubSub Delay on ActiveMQ', { timeout: 180000 }, () => {
  let producer: stompit.Client;
  const consumers: stompit.Client[] = [];
  const received: ReceivedMessage[] = [];
  const grouped: ReceivedMessage[] = [];
  const runId = Date.now().toString(36);

  const byTestId = (prefix: string, from = received) => from.filter((m) => m.headers['TEST_ID']?.startsWith(prefix));

  before(async () => {
    producer = await connect();
    const c = await connect();
    consumers.push(c);
    consume(c, DESTINATION_TOPIC, received, 'main');
    // Two competing consumers on one queue: message groups must pin each group to one of them
    for (const name of ['group-a', 'group-b']) {
      const gc = await connect();
      consumers.push(gc);
      consume(gc, GROUP_DESTINATION_TOPIC, grouped, name);
    }
  });

  after(async () => {
    for (const c of [producer, ...consumers]) c.disconnect();
  });

  it('should delay messages by the specified duration and keep custom headers', async () => {
    const cases = [
      { duration: 'PT1S', ms: 1000 },
      { duration: 'PT2S', ms: 2000 },
      { duration: 'PT5S', ms: 5000 },
    ];
    const sent = cases.map((c, i) => ({ ...c, id: `timing-${runId}-${i}`, sentAt: 0 }));
    for (const s of sent) {
      s.sentAt = Date.now();
      await send(producer, INGEST_TOPIC, {
        DELAY_DURATION: s.duration,
        DELAY_DESTINATION: DESTINATION_TOPIC,
        TEST_ID: s.id,
        X_TRACE: `trace-${s.id}`,
      }, `body-${s.id}`);
    }

    await waitFor('timing messages', () => byTestId(`timing-${runId}`).length === sent.length, 20000);

    for (const s of sent) {
      const r = byTestId(s.id)[0];
      const actual = r.receivedAt - s.sentAt;
      console.log(`${s.duration}: expected ${s.ms}ms, actual ${actual}ms`);
      assert.ok(actual >= s.ms - 100, `${s.id} delivered early: ${actual}ms < ${s.ms}ms`);
      assert.ok(actual < s.ms + 2000, `${s.id} delivered late: ${actual}ms`);
      assert.strictEqual(r.body, `body-${s.id}`);
      assert.strictEqual(r.headers['X_TRACE'], `trace-${s.id}`, 'custom header not preserved');
      assert.strictEqual(r.headers['DELAY_ENQUEUED_AT'], undefined, 'internal header leaked');
    }
  });

  it('should deliver each message exactly once, without looping back through the bucket', async () => {
    const id = `once-${runId}`;
    await send(producer, INGEST_TOPIC, { DELAY_DURATION: 'PT1S', DELAY_DESTINATION: DESTINATION_TOPIC, TEST_ID: id }, 'once');
    await waitFor('single message', () => byTestId(id).length === 1, 10000);
    // A leaked STOMP destination header would re-send the message to its bucket or ingest queue
    await delay(3000);
    assert.strictEqual(byTestId(id).length, 1, 'message delivered more than once');
  });

  it('should deliver messages in order of their deliverAt time', async () => {
    const prefix = `order-${runId}`;
    // Sent in reverse of the expected delivery order, across three buckets
    for (const [duration, order] of [['PT3S', 3], ['PT2S', 2], ['PT1S', 1]] as const) {
      await send(producer, INGEST_TOPIC, {
        DELAY_DURATION: duration,
        DELAY_DESTINATION: DESTINATION_TOPIC,
        TEST_ID: `${prefix}-${order}`,
        EXPECTED_ORDER: String(order),
      }, 'order');
    }
    await waitFor('ordered messages', () => byTestId(prefix).length === 3, 15000);
    const order = byTestId(prefix).map((m) => parseInt(m.headers['EXPECTED_ORDER'], 10));
    assert.deepStrictEqual(order, [1, 2, 3]);
  });

  it('should create bucket queues dynamically', async () => {
    const id = `dynamic-${runId}`;
    const bucket = `${INGEST_TOPIC}${BUCKET_SEPARATOR}PT9S`;
    await send(producer, INGEST_TOPIC, { DELAY_DURATION: 'PT9S', DELAY_DESTINATION: DESTINATION_TOPIC, TEST_ID: id }, 'dynamic');
    await waitFor(`bucket ${bucket}`, async () => (await listQueues()).includes(bucket), 10000);
    await waitFor('dynamic message', () => byTestId(id).length === 1, 20000);
  });

  it('should preserve JMSXGroupID end to end', async () => {
    const id = `groupid-${runId}`;
    await send(producer, INGEST_TOPIC, {
      DELAY_DURATION: 'PT1S',
      DELAY_DESTINATION: DESTINATION_TOPIC,
      TEST_ID: id,
      JMSXGroupID: `order-${runId}`,
    }, 'grouped');
    await waitFor('grouped message', () => byTestId(id).length === 1, 10000);
    assert.strictEqual(byTestId(id)[0].headers['JMSXGroupID'], `order-${runId}`);
  });

  it('should keep each JMSXGroupID on one destination consumer, in order', async () => {
    const prefix = `affinity-${runId}`;
    const groups = ['g1', 'g2', 'g3', 'g4'].map((g) => `${prefix}-${g}`);
    const perGroup = 10;
    // Interleave groups and buckets, so affinity and order survive the delay service
    for (let seq = 0; seq < perGroup; seq++) {
      for (const group of groups) {
        await send(producer, INGEST_TOPIC, {
          DELAY_DURATION: 'PT2S',
          DELAY_DESTINATION: GROUP_DESTINATION_TOPIC,
          TEST_ID: `${group}-${seq}`,
          SEQ: String(seq),
          JMSXGroupID: group,
        }, `${group}-${seq}`);
      }
    }

    await waitFor('grouped messages', () => byTestId(prefix, grouped).length === groups.length * perGroup, 30000);

    const consumersUsed = new Set<string>();
    for (const group of groups) {
      const msgs = grouped.filter((m) => m.headers['JMSXGroupID'] === group);
      assert.strictEqual(msgs.length, perGroup, `${group}: expected ${perGroup} messages`);
      const owners = new Set(msgs.map((m) => m.consumer));
      assert.strictEqual(owners.size, 1, `${group} split across consumers: ${[...owners].join(', ')}`);
      consumersUsed.add(msgs[0].consumer);
      assert.deepStrictEqual(msgs.map((m) => parseInt(m.headers['SEQ'], 10)), [...Array(perGroup).keys()], `${group} out of order`);
    }
    console.log(`Groups spread over consumers: ${[...consumersUsed].join(', ')}`);
  });

  it('should not lose or duplicate messages under load', async () => {
    const prefix = `load-${runId}`;
    const durations = ['PT1S', 'PT2S', 'PT3S', 'PT4S', 'PT5S'];
    const count = 300;
    const ids: string[] = [];
    // More in flight than TIMEOUT_POOL_SIZE, so buckets are paused, nacked and evicted
    for (let i = 0; i < count; i++) {
      const id = `${prefix}-${i}`;
      ids.push(id);
      await send(producer, INGEST_TOPIC, {
        DELAY_DURATION: durations[i % durations.length],
        DELAY_DESTINATION: DESTINATION_TOPIC,
        TEST_ID: id,
      }, id);
    }

    await waitFor('load messages', () => byTestId(prefix).length >= count, 60000);
    await delay(2000);
    const got = byTestId(prefix).map((m) => m.headers['TEST_ID']);
    const missing = ids.filter((id) => !got.includes(id));
    assert.strictEqual(missing.length, 0, `Lost ${missing.length}: ${missing.slice(0, 10).join(', ')}`);
    assert.strictEqual(got.length, new Set(got).size, `Duplicates: ${got.length - new Set(got).size}`);
  });

  it('should delete idle bucket queues after timeout', async () => {
    const id = `idle-${runId}`;
    const bucket = `${INGEST_TOPIC}${BUCKET_SEPARATOR}PT11S`;
    await send(producer, INGEST_TOPIC, { DELAY_DURATION: 'PT11S', DELAY_DESTINATION: DESTINATION_TOPIC, TEST_ID: id }, 'idle');
    await waitFor(`bucket ${bucket}`, async () => (await listQueues()).includes(bucket), 10000);
    await waitFor('idle message', () => byTestId(id).length === 1, 20000);
    await waitFor(`bucket ${bucket} deletion`, async () => !(await listQueues()).includes(bucket), 60000);
  });
});
