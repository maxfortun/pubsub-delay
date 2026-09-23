import { Broker, Consumer, Producer, Message } from './broker/types.js';
import { DelayServiceConfig, durationToBucketTopic } from './config.js';
import { parseDurationToMs } from './duration.js';
import { BucketAdvisory } from './bucket-advisory.js';

export class Router {
  private broker: Broker;
  private config: DelayServiceConfig;
  private bucketAdvisory: BucketAdvisory;
  private consumer: Consumer | null = null;
  private producer: Producer | null = null;
  private running = false;

  constructor(broker: Broker, config: DelayServiceConfig, bucketAdvisory: BucketAdvisory) {
    this.broker = broker;
    this.config = config;
    this.bucketAdvisory = bucketAdvisory;
  }

  async start(): Promise<void> {
    this.consumer = await this.broker.createConsumer({
      groupId: `${this.config.consumerGroupPrefix}-router`,
      instanceId: this.config.instanceId ? `${this.config.instanceId}-router` : undefined,
    });
    this.producer = await this.broker.createProducer();
    await this.consumer.subscribe([this.config.ingestTopic]);

    this.running = true;
    console.log(`Router started, listening on ${this.config.ingestTopic}`);

    while (this.running) {
      try {
        const envelope = await this.consumer.receive();
        await this.routeMessage(envelope.message);
        await this.consumer.ack(envelope);
      } catch (error) {
        console.error('Router error:', error);
      }
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.consumer) await this.consumer.close();
    if (this.producer) await this.producer.close();
  }

  private async routeMessage(message: Message): Promise<void> {
    const isoDuration = message.headers[this.config.delayDurationHeader];
    const destination = message.headers[this.config.destinationHeader];

    if (!isoDuration) {
      console.warn('Message missing DELAY_DURATION header, skipping');
      return;
    }

    if (!destination) {
      console.warn('Message missing DELAY_DESTINATION header, skipping');
      return;
    }

    const delayMs = parseDurationToMs(isoDuration);
    const bucketTopic = durationToBucketTopic(
      this.config.ingestTopic,
      this.config.bucketSeparator,
      isoDuration
    );

    await this.bucketAdvisory.registerBucket(bucketTopic, isoDuration, delayMs);

    const stampedMessage: Message = {
      key: message.key,
      headers: {
        ...message.headers,
        [this.config.enqueuedAtHeader]: Date.now().toString(),
      },
      body: message.body,
    };

    await this.producer!.send(bucketTopic, stampedMessage);
    console.log(`Routed message to ${bucketTopic}`);
  }
}
