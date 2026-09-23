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

  // Active timeout pool - messages with running timers
  private timeoutPool: Map<string, PendingMessage> = new Map();
  private poolIdCounter = 0;

  // Cache for peeked messages - keyed by bucket topic
  // Stores computed deliverAt and resume timer handle
  private bucketCache: Map<string, {
    deliverAt: number;
    destination: string;
    resumeTimer: NodeJS.Timeout;
  }> = new Map();

  private subscribedTopics = new Set<string>();
  private pendingTopics = new Set<string>();
  private restartGraceTimeout: NodeJS.Timeout | null = null;
  private restartGracePeriodMs = 5000;
  private isRestarting = false;

  constructor(broker: Broker, config: DelayServiceConfig, advisory: BucketAdvisory) {
    this.broker = broker;
    this.config = config;
    this.advisory = advisory;
  }

  async start(): Promise<void> {
    this.producer = await this.broker.createProducer();

    const buckets = this.advisory.getBuckets();
    const topics = buckets.map((b) => b.topic);

    await this.startConsumer(topics);

    this.advisory.on('bucket:added', (bucket) => {
      this.onBucketAdded(bucket.topic);
    });

    this.running = true;
    console.log(`Scheduler started with timeout pool size ${this.config.timeoutPoolSize}`);

    this.peekLoop();
  }

  private async startConsumer(topics: string[]): Promise<void> {
    this.consumer = await this.broker.createConsumer({
      groupId: `${this.config.consumerGroupPrefix}-scheduler`,
      instanceId: this.config.instanceId ? `${this.config.instanceId}-scheduler` : undefined,
    });

    if (topics.length > 0) {
      await this.consumer.subscribe(topics);
      topics.forEach((t) => this.subscribedTopics.add(t));
      console.log(`Scheduler subscribed to ${topics.length} topics`);
    }
  }

  private onBucketAdded(topic: string): void {
    if (this.subscribedTopics.has(topic)) return;

    this.pendingTopics.add(topic);
    console.log(`Scheduler: new bucket detected: ${topic}, scheduling restart`);

    if (this.restartGraceTimeout) {
      clearTimeout(this.restartGraceTimeout);
    }

    this.restartGraceTimeout = setTimeout(() => {
      this.restartConsumer();
    }, this.restartGracePeriodMs);
  }

  private async restartConsumer(): Promise<void> {
    if (this.isRestarting || this.pendingTopics.size === 0) return;

    this.isRestarting = true;
    const newTopics = Array.from(this.pendingTopics);
    this.pendingTopics.clear();

    console.log(`Scheduler: restarting consumer to add ${newTopics.length} new topics`);

    try {
      if (this.consumer) {
        await this.consumer.close();
      }

      const allTopics = [...this.subscribedTopics, ...newTopics];
      await this.startConsumer(allTopics);

      console.log(`Scheduler: consumer restarted with ${allTopics.length} topics`);
    } catch (error) {
      console.error('Scheduler: failed to restart consumer', error);
      newTopics.forEach((t) => this.pendingTopics.add(t));
    } finally {
      this.isRestarting = false;
    }
  }

  async stop(): Promise<void> {
    this.running = false;

    if (this.restartGraceTimeout) {
      clearTimeout(this.restartGraceTimeout);
    }

    for (const [_id, pending] of this.timeoutPool) {
      if (pending.timeoutHandle) {
        clearTimeout(pending.timeoutHandle);
      }
    }
    this.timeoutPool.clear();
    for (const cached of this.bucketCache.values()) {
      clearTimeout(cached.resumeTimer);
    }
    this.bucketCache.clear();

    if (this.consumer) await this.consumer.close();
    if (this.producer) await this.producer.close();
  }

  private async peekLoop(): Promise<void> {
    while (this.running) {
      if (!this.consumer || this.isRestarting) {
        await new Promise((r) => setTimeout(r, 100));
        continue;
      }

      try {
        const envelope = await this.consumer.receive();
        await this.handleMessage(envelope);
      } catch (error) {
        if (this.running && !this.isRestarting) {
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

    // Check bucket cache for pre-computed deliverAt (this is a resumed bucket)
    let deliverAt: number;
    let destination: string;
    const cached = this.bucketCache.get(topic);

    if (cached) {
      // Bucket was paused and just resumed - use cached data
      deliverAt = cached.deliverAt;
      destination = cached.destination;
      this.bucketCache.delete(topic);
    } else {
      destination = message.headers[this.config.destinationHeader];
      if (!destination) {
        console.warn('Scheduler: message missing destination, skipping');
        await this.consumer!.ack(envelope);
        return;
      }

      const enqueuedAtStr = message.headers[this.config.enqueuedAtHeader];
      const enqueuedAt = enqueuedAtStr ? parseInt(enqueuedAtStr, 10) : Date.now();
      const delayMs = parseDurationToMs(isoDuration);
      deliverAt = enqueuedAt + delayMs;
    }

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
        // New message is sooner - evict oldest, add new to pool
        await this.evictPauseAndNack(evictCandidate);
        this.addToPool(pending);
      } else {
        // New message is later - cache, pause bucket, nack
        this.cacheAndPause(topic, deliverAt, destination);
        await this.consumer!.nack(envelope);
        console.log(`Scheduler: paused bucket ${topic} until ${new Date(deliverAt).toISOString()} (pool: ${this.timeoutPool.size}, paused: ${this.bucketCache.size})`);
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
    console.log(`Scheduler: queued for ${new Date(pending.deliverAt).toISOString()} (pool: ${this.timeoutPool.size}, paused: ${this.bucketCache.size})`);
  }

  private cacheAndPause(topic: string, deliverAt: number, destination: string): void {
    const now = Date.now();
    const resumeInMs = Math.max(0, deliverAt - now);

    const resumeTimer = setTimeout(() => {
      this.onBucketResume(topic);
    }, resumeInMs);

    this.bucketCache.set(topic, { deliverAt, destination, resumeTimer });
    this.consumer!.pause([topic]);
  }

  private onBucketResume(topic: string): void {
    const cached = this.bucketCache.get(topic);
    if (!cached) return;

    // Don't delete from cache yet - handleMessage will use the cached data
    this.consumer!.resume([topic]);
    console.log(`Scheduler: resumed bucket ${topic}`);
  }

  private async evictPauseAndNack(entry: { id: string; pending: PendingMessage }): Promise<void> {
    const { id, pending } = entry;
    const topic = pending.envelope.topic;

    if (pending.timeoutHandle) {
      clearTimeout(pending.timeoutHandle);
    }
    pending.timeoutHandle = null;
    this.timeoutPool.delete(id);

    // Cache, pause bucket, and nack
    const destination = pending.envelope.message.headers[this.config.destinationHeader];
    this.cacheAndPause(topic, pending.deliverAt, destination);
    await this.consumer!.nack(pending.envelope);
    console.log(`Scheduler: evicted, paused bucket ${topic} until ${new Date(pending.deliverAt).toISOString()}`);
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

  private async onTimeout(id: string): Promise<void> {
    const pending = this.timeoutPool.get(id);
    if (!pending) return;

    this.timeoutPool.delete(id);

    const { envelope } = pending;
    const { message } = envelope;
    const destination = message.headers[this.config.destinationHeader];

    if (!destination) {
      console.warn('Scheduler: message missing destination on delivery');
      await this.consumer!.ack(envelope);
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
      console.log(`Scheduler: delivered to ${destination} (pool: ${this.timeoutPool.size}, paused: ${this.bucketCache.size})`);
    } catch (error) {
      console.error('Scheduler: delivery failed', error);
      // On failure, nack to retry later
      await this.consumer!.nack(envelope);
    }
  }
}
