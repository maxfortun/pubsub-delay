import { describe, it } from 'node:test';
import assert from 'node:assert';
import { Router } from '../../src/router.js';
import { BucketAdvisory } from '../../src/bucket-advisory.js';
import { Broker, Consumer, Message, MessageEnvelope, Producer } from '../../src/broker/types.js';
import { Transform, TransformContext, TransformResult } from '../../src/transform/index.js';
import { loadConfig } from '../../src/config.js';

// Minimal in-memory broker: one queue of envelopes, records acks, nacks and sends
class FakeBroker {
  queue: MessageEnvelope[] = [];
  acked: MessageEnvelope[] = [];
  nacked: MessageEnvelope[] = [];
  sent: { topic: string; message: Message }[] = [];
  private waiting: ((e: MessageEnvelope) => void) | null = null;

  push(message: Message): void {
    const env: MessageEnvelope = { topic: 'delay-ingest', partition: 0, offset: String(this.queue.length), message };
    this.deliver(env);
  }

  private deliver(env: MessageEnvelope): void {
    if (this.waiting) {
      const w = this.waiting;
      this.waiting = null;
      w(env);
    } else {
      this.queue.push(env);
    }
  }

  broker(): Broker {
    const consumer: Consumer = {
      subscribe: async () => {},
      receive: () => (this.queue.length ? Promise.resolve(this.queue.shift()!) : new Promise((r) => (this.waiting = r))),
      ack: async (e) => void this.acked.push(e),
      // Like a Kafka seek: the same message comes back
      nack: async (e) => {
        this.nacked.push(e);
        this.deliver(e);
      },
      pause: () => {},
      resume: () => {},
      close: async () => {},
    };
    const producer: Producer = {
      send: async (topic, message) => void this.sent.push({ topic, message }),
      close: async () => {},
    };
    return {
      connect: async () => {},
      disconnect: async () => {},
      createConsumer: async () => consumer,
      createProducer: async () => producer,
      admin: () => { throw new Error('not used'); },
    };
  }
}

class ScriptedTransform implements Transform {
  readonly name = 'scripted';
  calls: TransformContext[] = [];
  constructor(private script: (msg: Message, call: number) => TransformResult) {}
  async apply(msg: Message, ctx: TransformContext): Promise<TransformResult> {
    this.calls.push(ctx);
    return this.script(msg, this.calls.length);
  }
}

const fakeAdvisory = { registerBucket: async () => {} } as unknown as BucketAdvisory;

function config() {
  const c = loadConfig();
  c.transform.retryBackoffMs = 1;
  c.routerRetryBackoffMs = 1;
  return c;
}

const msg = (headers: Record<string, string>): Message => ({
  key: 'k',
  headers: { delay_duration: 'PT5S', delay_destination: 'out', ...headers },
  body: Buffer.from('payload'),
});

async function until(cond: () => boolean, timeoutMs = 2000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error('condition not met');
    await new Promise((r) => setTimeout(r, 5));
  }
}

async function run(transform: Transform, messages: Message[], done: (b: FakeBroker) => boolean, advisory = fakeAdvisory) {
  const fake = new FakeBroker();
  const router = new Router(fake.broker(), config(), advisory, transform);
  const started = router.start();
  messages.forEach((m) => fake.push(m));
  await until(() => done(fake));
  await router.stop();
  void started;
  return fake;
}

describe('Router pre-transform', () => {
  it('routes the transformed message to the bucket and acks', async () => {
    const t = new ScriptedTransform((m) => ({
      action: 'forward',
      message: { ...m, headers: { ...m.headers, x_pre: '1' }, body: Buffer.from('changed') },
    }));
    const fake = await run(t, [msg({})], (b) => b.acked.length === 1);
    assert.strictEqual(t.calls[0].stage, 'pre');
    assert.strictEqual(t.calls[0].topic, 'delay-ingest');
    assert.strictEqual(fake.sent.length, 1);
    assert.strictEqual(fake.sent[0].topic, 'delay-ingest-PT5S');
    assert.strictEqual(fake.sent[0].message.headers.x_pre, '1');
    assert.strictEqual(fake.sent[0].message.body.toString(), 'changed');
    assert.ok(fake.sent[0].message.headers.delay_enqueued_at);
  });

  it('routes by the headers the transform returns', async () => {
    const t = new ScriptedTransform((m) => ({
      action: 'forward',
      message: { ...m, headers: { ...m.headers, delay_duration: 'PT1M' } },
    }));
    const fake = await run(t, [msg({})], (b) => b.acked.length === 1);
    assert.strictEqual(fake.sent[0].topic, 'delay-ingest-PT1M');
  });

  it('matches service headers case-insensitively', async () => {
    const t = new ScriptedTransform((m) => ({ action: 'forward', message: m }));
    const legacy: Message = {
      key: 'k',
      headers: { DELAY_DURATION: 'PT10S', DELAY_DESTINATION: 'out', DELAY_ENQUEUED_AT: '1' },
      body: Buffer.from('payload'),
    };
    const fake = await run(t, [legacy], (b) => b.acked.length === 1);
    assert.strictEqual(fake.sent.length, 1);
    assert.strictEqual(fake.sent[0].topic, 'delay-ingest-PT10S');
    const headers = fake.sent[0].message.headers;
    assert.strictEqual(headers.DELAY_ENQUEUED_AT, undefined, 'stale enqueued-at not replaced');
    assert.ok(Number(headers.delay_enqueued_at) > 1);
  });

  it('acks and drops a rejected message without writing to a bucket', async () => {
    const t = new ScriptedTransform(() => ({ action: 'reject', reason: 'bad token' }));
    const fake = await run(t, [msg({})], (b) => b.acked.length === 1);
    assert.strictEqual(fake.sent.length, 0);
    assert.strictEqual(fake.nacked.length, 0);
  });

  it('nacks on a transform error and retries the same message', async () => {
    const t = new ScriptedTransform((m, call) => {
      if (call < 3) throw new Error('plugin down');
      return { action: 'forward', message: m };
    });
    const fake = await run(t, [msg({ test_id: 'retry' })], (b) => b.acked.length === 1);
    assert.strictEqual(fake.nacked.length, 2);
    assert.strictEqual(fake.sent.length, 1);
    assert.strictEqual(fake.sent[0].message.headers.test_id, 'retry');
  });
});

describe('Router failures', () => {
  const passthrough = () => new ScriptedTransform((m) => ({ action: 'forward', message: m }));

  it('nacks and retries when the broker fails, instead of skipping the message', async () => {
    let calls = 0;
    const flaky = {
      registerBucket: async () => {
        if (++calls === 1) throw new Error('topic is marked for deletion');
      },
    } as unknown as BucketAdvisory;
    const fake = await run(passthrough(), [msg({})], (b) => b.acked.length === 1, flaky);
    assert.strictEqual(fake.nacked.length, 1);
    assert.strictEqual(fake.sent.length, 1);
  });

  it('skips a message with an invalid duration without retrying', async () => {
    const fake = await run(passthrough(), [msg({ delay_duration: 'soon' })], (b) => b.acked.length === 1);
    assert.strictEqual(fake.sent.length, 0);
    assert.strictEqual(fake.nacked.length, 0);
  });
});
