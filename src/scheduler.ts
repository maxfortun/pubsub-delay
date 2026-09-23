import { Broker, Consumer, Producer, Message, MessageEnvelope } from './broker/types.js';
import { DelayServiceConfig, bucketTopicToDuration } from './config.js';
import { parseDurationToMs } from './duration.js';
import { BucketAdvisory } from './bucket-advisory.js';
import { createStrategy, SchedulerStrategy } from './strategy/index.js';
import { deliveryLateness, messagesTotal, setSchedulerStatsSource } from './metrics.js';

export class Scheduler {
  private broker: Broker;
  private config: DelayServiceConfig;
  private advisory: BucketAdvisory;
  private consumer: Consumer | null = null;
  private producer: Producer | null = null;
  private strategy: SchedulerStrategy;
  private running = false;

  private subscribedTopics = new Set<string>();
  private pendingTopics = new Set<string>();
  private removedTopics = new Set<string>();
  private restartGraceTimeout: NodeJS.Timeout | null = null;
  private restartGracePeriodMs = 5000;
  private isRestarting = false;
  private restartQueued = false;

  constructor(broker: Broker, config: DelayServiceConfig, advisory: BucketAdvisory) {
    this.broker = broker;
    this.config = config;
    this.advisory = advisory;
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
      await this.producer!.send(destination, forwardMessage);
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
    });

    if (topics.length > 0) {
      await this.consumer.subscribe(topics);
      topics.forEach((t) => this.subscribedTopics.add(t));
      console.log(`Scheduler subscribed to ${topics.length} topics`);
    }
  }

  private onBucketAdded(topic: string): void {
    if (this.subscribedTopics.has(topic)) return;

    this.removedTopics.delete(topic);
    this.pendingTopics.add(topic);
    console.log(`Scheduler: new bucket detected: ${topic}, scheduling restart`);
    this.scheduleRestart(this.restartGracePeriodMs);
  }

  private onBucketRemoved(topic: string): void {
    this.pendingTopics.delete(topic);
    if (!this.subscribedTopics.has(topic)) return;

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
    const newTopics = Array.from(this.pendingTopics);
    const removed = Array.from(this.removedTopics);
    this.pendingTopics.clear();
    this.removedTopics.clear();
    removed.forEach((t) => this.subscribedTopics.delete(t));

    console.log(`Scheduler: restarting consumer (+${newTopics.length} / -${removed.length} topics)`);

    try {
      if (this.consumer) {
        await this.consumer.close();
      }

      const allTopics = [...this.subscribedTopics, ...newTopics];
      await this.startConsumer(allTopics);

      this.wirePauseControl();

      console.log(`Scheduler: consumer restarted with ${allTopics.length} topics`);
    } catch (error) {
      console.error('Scheduler: failed to restart consumer', error);
      newTopics.forEach((t) => this.pendingTopics.add(t));
      removed.forEach((t) => this.removedTopics.add(t));
    } finally {
      this.isRestarting = false;
      if (this.restartQueued) {
        this.restartQueued = false;
        this.restartConsumer();
      }
    }
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
