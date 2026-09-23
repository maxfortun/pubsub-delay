import { Broker, Consumer, Producer, Message } from './broker/types.js';
import { DelayServiceConfig, bucketTopicToDuration } from './config.js';
import { delay, parseDurationToMs } from './duration.js';
import { BucketAdvisory } from './bucket-advisory.js';

export class Worker {
  readonly id: string;
  private broker: Broker;
  private config: DelayServiceConfig;
  private advisory: BucketAdvisory;
  private consumer: Consumer | null = null;
  private producer: Producer | null = null;
  private running = false;
  private subscribedTopics = new Set<string>();

  constructor(id: string, broker: Broker, config: DelayServiceConfig, advisory: BucketAdvisory) {
    this.id = id;
    this.broker = broker;
    this.config = config;
    this.advisory = advisory;
  }

  async start(): Promise<void> {
    this.producer = await this.broker.createProducer();
    this.consumer = await this.broker.createConsumer(`${this.config.consumerGroupPrefix}-worker`);

    const buckets = this.advisory.getBuckets();
    const topics = buckets.map((b) => b.topic);
    if (topics.length > 0) {
      await this.consumer.subscribe(topics);
      topics.forEach((t) => this.subscribedTopics.add(t));
    }

    this.advisory.on('bucket:added', (bucket) => {
      this.onBucketAdded(bucket.topic);
    });

    this.running = true;
    console.log(`Worker ${this.id} started, subscribed to ${topics.length} buckets`);

    this.consumeLoop();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.consumer) await this.consumer.close();
    if (this.producer) await this.producer.close();
  }

  private async onBucketAdded(topic: string): Promise<void> {
    if (this.subscribedTopics.has(topic)) return;

    this.subscribedTopics.add(topic);
    if (this.consumer) {
      await this.consumer.subscribe([topic]);
      console.log(`Worker ${this.id} subscribed to new bucket: ${topic}`);
    }
  }

  private async consumeLoop(): Promise<void> {
    while (this.running && this.consumer) {
      try {
        const { topic, message } = await this.consumer.receive();
        await this.processMessage(topic, message);
        await this.consumer.commit();
      } catch (error) {
        if (this.running) {
          console.error(`Worker ${this.id} error:`, error);
        }
      }
    }
  }

  private async processMessage(topic: string, message: Message): Promise<void> {
    const destination = message.headers[this.config.destinationHeader];
    if (!destination) {
      console.warn(`Worker ${this.id}: message missing destination, skipping`);
      return;
    }

    const isoDuration = bucketTopicToDuration(
      this.config.ingestTopic,
      this.config.bucketSeparator,
      topic
    );
    if (!isoDuration) {
      console.warn(`Worker ${this.id}: cannot determine duration from topic ${topic}`);
      return;
    }

    const delayMs = parseDurationToMs(isoDuration);
    console.log(`Worker ${this.id}: waiting ${isoDuration}`);
    await delay(delayMs);

    this.advisory.updateActivity(topic);

    await this.producer!.send(destination, message);
    console.log(`Worker ${this.id}: delivered to ${destination}`);
  }
}
