import { Broker, Consumer, Producer, Message, MessageEnvelope } from './broker/types.js';
import { DelayServiceConfig, bucketTopicToDuration } from './config.js';
import { parseDurationToMs } from './duration.js';
import { BucketAdvisory } from './bucket-advisory.js';
import { createStrategy, SchedulerStrategy } from './strategy/index.js';

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
  private restartGraceTimeout: NodeJS.Timeout | null = null;
  private restartGracePeriodMs = 5000;
  private isRestarting = false;

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

    // Wire up strategy callbacks
    this.strategy.setPauseControl(
      (t) => this.consumer?.pause(t),
      (t) => this.consumer?.resume(t)
    );

    this.strategy.setDeliveryHandler(async (envelope, destination) => {
      const { message } = envelope;
      const { [this.config.enqueuedAtHeader]: _, ...forwardHeaders } = message.headers;
      const forwardMessage: Message = {
        key: message.key,
        headers: forwardHeaders,
        body: message.body,
      };
      await this.producer!.send(destination, forwardMessage);
      this.advisory.updateActivity(envelope.topic);
    });

    this.strategy.setAckHandler(
      (envelope) => this.consumer!.ack(envelope),
      (envelope) => this.consumer!.nack(envelope)
    );

    this.advisory.on('bucket:added', (bucket) => {
      this.onBucketAdded(bucket.topic);
    });

    this.running = true;
    console.log(`Scheduler started with strategy: ${this.strategy.name}`);

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

      // Re-wire pause control after consumer restart
      this.strategy.setPauseControl(
        (t) => this.consumer?.pause(t),
        (t) => this.consumer?.resume(t)
      );

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

    await this.strategy.onMessage(envelope, deliverAt, destination);
  }

  getStats() {
    return this.strategy.getStats();
  }
}
