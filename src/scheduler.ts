import { Broker, Consumer, Producer, Message, MessageEnvelope } from './broker/types.js';
import { DelayServiceConfig, bucketTopicToDuration } from './config.js';
import { parseDurationToMs } from './duration.js';
import { BucketAdvisory } from './bucket-advisory.js';
import { createStrategy, SchedulerStrategy } from './strategy/index.js';
import { deliveryLateness, messagesTotal, setSchedulerStatsSource } from './metrics.js';
import { Transform, applyTransform } from './transform/index.js';

export class Scheduler {
  private broker: Broker;
  private config: DelayServiceConfig;
  private advisory: BucketAdvisory;
  private consumer: Consumer | null = null;
  private producer: Producer | null = null;
  private strategy: SchedulerStrategy;
  private transform: Transform;
  private running = false;

  private subscribedTopics = new Set<string>();
  private pendingTopics = new Set<string>();
  private removedTopics = new Set<string>();
  private restartGraceTimeout: NodeJS.Timeout | null = null;
  private restartGracePeriodMs: number;
  private isRestarting = false;
  private restartDone: Promise<void> = Promise.resolve();
  private restartQueued = false;
  private restartQueuedAfterGrace = false;

  constructor(broker: Broker, config: DelayServiceConfig, advisory: BucketAdvisory, transform: Transform) {
    this.broker = broker;
    this.config = config;
    this.advisory = advisory;
    this.transform = transform;
    this.restartGracePeriodMs = config.consumerRestartGraceMs;
    this.strategy = createStrategy(config.strategyType, config.strategyConfig);
  }

  async start(): Promise<void> {
    this.producer = await this.broker.createProducer();

    const buckets = this.advisory.getBuckets();
    const topics = buckets.map((b) => b.topic);

    await this.startConsumer(topics);

    const strategyName = this.strategy.name;
    this.wirePauseControl();

    this.strategy.setDeliveryHandler(async (envelope, destination, deliverAt) => {
      const { message } = envelope;
      const { [this.config.enqueuedAtHeader]: _, ...forwardHeaders } = message.headers;
      const forwardMessage: Message = {
        key: message.key,
        headers: forwardHeaders,
        body: message.body,
      };

      let result;
      try {
        result = await applyTransform(this.transform, forwardMessage, { stage: 'post', topic: envelope.topic, destination });
      } catch (error) {
        // Throwing makes the strategy nack, so the message is re-read and retried
        await new Promise((r) => setTimeout(r, this.config.transform.retryBackoffMs));
        throw error;
      }
      if (result.action === 'reject') {
        console.warn(`Scheduler: message rejected: ${result.reason}`);
        this.advisory.updateActivity(envelope.topic);
        return;
      }

      await this.producer!.send(destination, result.message);
      deliveryLateness.observe({ strategy: strategyName }, Math.max(0, Date.now() - deliverAt));
      messagesTotal.inc({ strategy: strategyName, event: 'delivered' });
      this.advisory.updateActivity(envelope.topic);
    });

    this.strategy.setAckHandler(
      (envelope) => this.consumer!.ack(envelope),
      (envelope) => {
        messagesTotal.inc({ strategy: strategyName, event: 'nacked' });
        return this.consumer!.nack(envelope);
      }
    );

    setSchedulerStatsSource(() => ({ strategy: strategyName, ...this.strategy.getStats() }));

    this.advisory.on('bucket:added', (bucket) => {
      this.onBucketAdded(bucket.topic);
    });

    this.advisory.on('bucket:removed', (topic) => {
      this.onBucketRemoved(topic);
    });

    this.running = true;
    console.log(`Scheduler started with strategy: ${this.strategy.name}`);

    this.peekLoop();
  }

  private wirePauseControl(): void {
    const strategyName = this.strategy.name;
    this.strategy.setPauseControl(
      (t) => {
        messagesTotal.inc({ strategy: strategyName, event: 'paused' }, t.length);
        this.consumer?.pause(t);
      },
      (t) => this.consumer?.resume(t)
    );
  }

  private async startConsumer(topics: string[]): Promise<void> {
    this.consumer = await this.broker.createConsumer({
      groupId: `${this.config.consumerGroupPrefix}-scheduler`,
      instanceId: this.config.instanceId ? `${this.config.instanceId}-scheduler` : undefined,
      // A freshly (re)created bucket may already hold messages before we subscribe
      fromBeginning: true,
      // A resumed bucket is only fetched once the in-flight long-poll returns;
      // keep that short so paused buckets fire on time when traffic is quiet
      maxWaitMs: this.config.schedulerFetchMaxWaitMs,
    });

    if (topics.length > 0) {
      await this.consumer.subscribe(topics);
      topics.forEach((t) => this.subscribedTopics.add(t));
      console.log(`Scheduler subscribed to ${topics.length} topics`);
    }
  }

  private onBucketAdded(topic: string): void {
    // Re-added before a queued removal ran: keep the existing subscription
    if (this.removedTopics.delete(topic) && this.subscribedTopics.has(topic)) return;
    if (this.subscribedTopics.has(topic)) return;

    this.pendingTopics.add(topic);
    console.log(`Scheduler: new bucket detected: ${topic}, scheduling restart`);
    this.scheduleRestart(this.restartGracePeriodMs);
  }

  private onBucketRemoved(topic: string): void {
    this.pendingTopics.delete(topic);
    // Mid-restart the topic may be about to be subscribed again, so queue the removal anyway
    if (!this.subscribedTopics.has(topic) && !this.isRestarting) return;

    this.removedTopics.add(topic);
    console.log(`Scheduler: bucket removed: ${topic}, scheduling restart`);
    // Unsubscribe promptly; the topic is being deleted
    this.scheduleRestart(0);
  }

  // The grace window starts at the first change and is not extended by later ones,
  // so a trickle of new buckets cannot postpone the restart indefinitely
  private scheduleRestart(delayMs: number): void {
    if (this.restartGraceTimeout && delayMs > 0) return;
    if (this.restartGraceTimeout) clearTimeout(this.restartGraceTimeout);

    this.restartGraceTimeout = setTimeout(() => {
      this.restartGraceTimeout = null;
      this.restartConsumer();
    }, delayMs);
  }

  private async restartConsumer(): Promise<void> {
    if (this.isRestarting) {
      this.restartQueued = true;
      return;
    }
    if (this.pendingTopics.size === 0 && this.removedTopics.size === 0) return;

    this.isRestarting = true;
    let signalDone!: () => void;
    this.restartDone = new Promise((r) => (signalDone = r));
    const newTopics = Array.from(this.pendingTopics);
    const removed = Array.from(this.removedTopics);
    this.pendingTopics.clear();
    this.removedTopics.clear();
    removed.forEach((t) => this.subscribedTopics.delete(t));
    // Drop anything the advisory no longer knows, e.g. removed while a restart was queued
    const wanted = [...new Set([...this.subscribedTopics, ...newTopics])].filter((t) => this.advisory.getBucket(t));

    console.log(`Scheduler: restarting consumer (+${newTopics.length} / -${removed.length} topics)`);

    try {
      if (this.consumer) {
        await this.consumer.close();
        this.consumer = null;
      }

      // Subscribing to a topic that is missing (not yet created, or deleted by cleanup
      // mid-restart) can stall the group join, so only subscribe to what exists now
      const existing = new Set(await this.broker.admin().listTopics());
      const allTopics = wanted.filter((t) => existing.has(t));
      const missing = wanted.filter((t) => !existing.has(t));
      this.subscribedTopics.clear();

      await this.withTimeout(this.startConsumer(allTopics), this.config.consumerStartTimeoutMs, 'consumer start');
      this.wirePauseControl();
      console.log(`Scheduler: consumer restarted with ${allTopics.length} topics`);

      if (missing.length > 0) {
        console.log(`Scheduler: ${missing.length} bucket topics not found yet, retrying: ${missing.join(', ')}`);
        missing.forEach((t) => this.pendingTopics.add(t));
        this.restartQueuedAfterGrace = true;
      }
    } catch (error) {
      console.error('Scheduler: failed to restart consumer, retrying', error);
      if (this.consumer) {
        await this.consumer.close().catch(() => {});
        this.consumer = null;
      }
      wanted.forEach((t) => this.pendingTopics.add(t));
      this.subscribedTopics.clear();
      this.restartQueuedAfterGrace = true;
    } finally {
      this.isRestarting = false;
      signalDone();
      if (this.restartQueued) {
        this.restartQueued = false;
        this.restartConsumer();
      } else if (this.restartQueuedAfterGrace) {
        this.scheduleRestart(this.restartGracePeriodMs);
      }
      this.restartQueuedAfterGrace = false;
    }
  }

  private withTimeout<T>(promise: Promise<T>, ms: number, what: string): Promise<T> {
    let timer: NodeJS.Timeout;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
  }

  async stop(): Promise<void> {
    this.running = false;

    if (this.restartGraceTimeout) {
      clearTimeout(this.restartGraceTimeout);
    }

    this.strategy.stop();

    if (this.consumer) await this.consumer.close();
    if (this.producer) await this.producer.close();
  }

  private async peekLoop(): Promise<void> {
    while (this.running) {
      if (this.isRestarting) {
        await this.restartDone;
        continue;
      }
      if (!this.consumer) {
        // Between a failed restart and its retry; wait for the next restart to finish
        await new Promise((r) => setTimeout(r, this.config.schedulerFetchMaxWaitMs));
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

    messagesTotal.inc({ strategy: this.strategy.name, event: 'received' });
    await this.strategy.onMessage(envelope, deliverAt, destination);
  }
}
