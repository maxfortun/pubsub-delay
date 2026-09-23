import { Broker, Consumer, Producer, Message, MessageEnvelope } from './broker/types.js';
import { DelayServiceConfig, bucketTopicToDuration } from './config.js';
import { parseDurationToMs } from './duration.js';
import { BucketAdvisory } from './bucket-advisory.js';

interface PendingMessage {
  envelope: MessageEnvelope;
  deliverAt: number;
  timeoutHandle: NodeJS.Timeout | null;
}

export class Scheduler {
  private broker: Broker;
  private config: DelayServiceConfig;
  private advisory: BucketAdvisory;
  private consumer: Consumer | null = null;
  private producer: Producer | null = null;
  private running = false;

  private timeoutPool: Map<string, PendingMessage> = new Map();
  private poolIdCounter = 0;

  constructor(broker: Broker, config: DelayServiceConfig, advisory: BucketAdvisory) {
    this.broker = broker;
    this.config = config;
    this.advisory = advisory;
  }

  async start(): Promise<void> {
    this.producer = await this.broker.createProducer();
    this.consumer = await this.broker.createConsumer({
      groupId: `${this.config.consumerGroupPrefix}-scheduler`,
      instanceId: this.config.instanceId ? `${this.config.instanceId}-scheduler` : undefined,
    });

    const buckets = this.advisory.getBuckets();
    const topics = buckets.map((b) => b.topic);
    if (topics.length > 0) {
      await this.consumer.subscribe(topics);
    }

    this.advisory.on('bucket:added', async (bucket) => {
      if (this.consumer) {
        await this.consumer.subscribe([bucket.topic]);
        console.log(`Scheduler subscribed to new bucket: ${bucket.topic}`);
      }
    });

    this.running = true;
    console.log(`Scheduler started with timeout pool size ${this.config.timeoutPoolSize}`);

    this.peekLoop();
  }

  async stop(): Promise<void> {
    this.running = false;

    for (const [_id, pending] of this.timeoutPool) {
      if (pending.timeoutHandle) {
        clearTimeout(pending.timeoutHandle);
      }
    }
    this.timeoutPool.clear();

    if (this.consumer) await this.consumer.close();
    if (this.producer) await this.producer.close();
  }

  private async peekLoop(): Promise<void> {
    while (this.running && this.consumer) {
      try {
        const envelope = await this.consumer.receive();
        await this.handleMessage(envelope);
      } catch (error) {
        if (this.running) {
          console.error('Scheduler peek error:', error);
        }
      }
    }
  }

  private async handleMessage(envelope: MessageEnvelope): Promise<void> {
    const { topic, message } = envelope;

    const isoDuration = bucketTopicToDuration(
      this.config.ingestTopic,
      this.config.bucketSeparator,
      topic
    );
    if (!isoDuration) {
      console.warn(`Scheduler: cannot determine duration from topic ${topic}`);
      await this.consumer!.ack(envelope);
      return;
    }

    const destination = message.headers[this.config.destinationHeader];
    if (!destination) {
      console.warn('Scheduler: message missing destination, skipping');
      await this.consumer!.ack(envelope);
      return;
    }

    const enqueuedAtStr = message.headers[this.config.enqueuedAtHeader];
    const enqueuedAt = enqueuedAtStr ? parseInt(enqueuedAtStr, 10) : Date.now();
    const delayMs = parseDurationToMs(isoDuration);
    const deliverAt = enqueuedAt + delayMs;

    const pending: PendingMessage = {
      envelope,
      deliverAt,
      timeoutHandle: null,
    };

    if (this.timeoutPool.size < this.config.timeoutPoolSize) {
      this.addToPool(pending);
    } else {
      const evictCandidate = this.findLongestWaiting();
      if (evictCandidate && pending.deliverAt < evictCandidate.pending.deliverAt) {
        await this.evictWithSeek(evictCandidate);
        this.addToPool(pending);
      } else {
        await this.nack(envelope);
      }
    }
  }

  private addToPool(pending: PendingMessage): void {
    const id = `pending-${++this.poolIdCounter}`;
    const now = Date.now();
    const delayMs = Math.max(0, pending.deliverAt - now);

    pending.timeoutHandle = setTimeout(() => {
      this.onTimeout(id);
    }, delayMs);

    this.timeoutPool.set(id, pending);
    console.log(`Scheduler: queued for ${new Date(pending.deliverAt).toISOString()} (pool: ${this.timeoutPool.size})`);
  }

  private findLongestWaiting(): { id: string; pending: PendingMessage } | null {
    let longest: { id: string; pending: PendingMessage } | null = null;

    for (const [id, pending] of this.timeoutPool) {
      if (!longest || pending.deliverAt > longest.pending.deliverAt) {
        longest = { id, pending };
      }
    }

    return longest;
  }

  private async evictWithSeek(entry: { id: string; pending: PendingMessage }): Promise<void> {
    const { id, pending } = entry;

    if (pending.timeoutHandle) {
      clearTimeout(pending.timeoutHandle);
    }
    this.timeoutPool.delete(id);

    await this.nack(pending.envelope);
    console.log(`Scheduler: evicted and nack'd message, will be redelivered`);
  }

  private async nack(envelope: MessageEnvelope): Promise<void> {
    await this.consumer!.nack(envelope);
  }

  private async onTimeout(id: string): Promise<void> {
    const pending = this.timeoutPool.get(id);
    if (!pending) return;

    this.timeoutPool.delete(id);

    const { envelope } = pending;
    const { message } = envelope;
    const destination = message.headers[this.config.destinationHeader];

    if (!destination) {
      console.warn('Scheduler: message missing destination on delivery');
      return;
    }

    const { [this.config.enqueuedAtHeader]: _, ...forwardHeaders } = message.headers;
    const forwardMessage: Message = {
      key: message.key,
      headers: forwardHeaders,
      body: message.body,
    };

    try {
      await this.producer!.send(destination, forwardMessage);
      await this.consumer!.ack(envelope);
      this.advisory.updateActivity(envelope.topic);
      console.log(`Scheduler: delivered to ${destination} (pool: ${this.timeoutPool.size})`);
    } catch (error) {
      console.error('Scheduler: delivery failed, will retry', error);
      await this.nack(envelope);
    }
  }
}
