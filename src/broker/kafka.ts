import {
  Kafka,
  Consumer as KafkaConsumer,
  Producer as KafkaProducer,
  Admin as KafkaAdmin,
  EachMessagePayload,
} from 'kafkajs';
import { Broker, Consumer, Producer, Message, BrokerConfig, TopicAdmin } from './types.js';

class KafkaConsumerAdapter implements Consumer {
  private consumer: KafkaConsumer;
  private messageQueue: { topic: string; message: Message }[] = [];
  private resolveWaiting: ((msg: { topic: string; message: Message }) => void) | null = null;
  private currentOffset: string | null = null;
  private currentTopic: string | null = null;
  private currentPartition: number | null = null;

  constructor(consumer: KafkaConsumer) {
    this.consumer = consumer;
  }

  async subscribe(topics: string[]): Promise<void> {
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
    }
    await this.consumer.run({
      autoCommit: false,
      eachMessage: async (payload: EachMessagePayload) => {
        const msg: Message = {
          key: payload.message.key?.toString(),
          headers: Object.fromEntries(
            Object.entries(payload.message.headers || {}).map(([k, v]) => [k, v?.toString() || ''])
          ),
          body: payload.message.value || Buffer.alloc(0),
        };
        this.currentOffset = payload.message.offset;
        this.currentTopic = payload.topic;
        this.currentPartition = payload.partition;

        const item = { topic: payload.topic, message: msg };
        if (this.resolveWaiting) {
          const resolve = this.resolveWaiting;
          this.resolveWaiting = null;
          resolve(item);
        } else {
          this.messageQueue.push(item);
        }
      },
    });
  }

  async receive(): Promise<{ topic: string; message: Message }> {
    if (this.messageQueue.length > 0) {
      return this.messageQueue.shift()!;
    }
    return new Promise((resolve) => {
      this.resolveWaiting = resolve;
    });
  }

  async commit(): Promise<void> {
    if (this.currentTopic && this.currentPartition !== null && this.currentOffset) {
      await this.consumer.commitOffsets([
        {
          topic: this.currentTopic,
          partition: this.currentPartition,
          offset: (BigInt(this.currentOffset) + 1n).toString(),
        },
      ]);
    }
  }

  async close(): Promise<void> {
    await this.consumer.disconnect();
  }
}

class KafkaProducerAdapter implements Producer {
  private producer: KafkaProducer;

  constructor(producer: KafkaProducer) {
    this.producer = producer;
  }

  async send(topic: string, message: Message): Promise<void> {
    await this.producer.send({
      topic,
      messages: [
        {
          key: message.key,
          value: message.body,
          headers: message.headers,
        },
      ],
    });
  }

  async close(): Promise<void> {
    await this.producer.disconnect();
  }
}

class KafkaTopicAdmin implements TopicAdmin {
  private admin: KafkaAdmin;

  constructor(admin: KafkaAdmin) {
    this.admin = admin;
  }

  async listTopics(): Promise<string[]> {
    return this.admin.listTopics();
  }

  async createTopic(topic: string): Promise<void> {
    await this.admin.createTopics({
      topics: [{ topic, numPartitions: 1 }],
    });
  }

  async deleteTopic(topic: string): Promise<void> {
    await this.admin.deleteTopics({ topics: [topic] });
  }

  async getConsumerLag(topic: string, groupId: string): Promise<number> {
    const offsets = await this.admin.fetchTopicOffsets(topic);
    const groupOffsets = await this.admin.fetchOffsets({ groupId, topics: [topic] });

    let totalLag = 0;
    for (const partition of offsets) {
      const groupPartition = groupOffsets.find(
        (g) => g.topic === topic
      )?.partitions.find((p) => p.partition === partition.partition);

      const currentOffset = BigInt(groupPartition?.offset || '0');
      const highWatermark = BigInt(partition.high);
      totalLag += Number(highWatermark - currentOffset);
    }
    return totalLag;
  }

  async topicExists(topic: string): Promise<boolean> {
    const topics = await this.listTopics();
    return topics.includes(topic);
  }
}

export class KafkaBroker implements Broker {
  private kafka: Kafka;
  private config: BrokerConfig['kafka'];
  private adminClient: KafkaAdmin | null = null;

  constructor(config: BrokerConfig['kafka']) {
    if (!config) throw new Error('Kafka config required');
    this.config = config;
    this.kafka = new Kafka({
      clientId: config.clientId,
      brokers: config.brokers,
    });
  }

  async connect(): Promise<void> {
    this.adminClient = this.kafka.admin();
    await this.adminClient.connect();
  }

  async disconnect(): Promise<void> {
    if (this.adminClient) {
      await this.adminClient.disconnect();
    }
  }

  async createConsumer(groupId: string): Promise<Consumer> {
    const consumer = this.kafka.consumer({ groupId });
    await consumer.connect();
    return new KafkaConsumerAdapter(consumer);
  }

  async createProducer(): Promise<Producer> {
    const producer = this.kafka.producer();
    await producer.connect();
    return new KafkaProducerAdapter(producer);
  }

  admin(): TopicAdmin {
    if (!this.adminClient) {
      throw new Error('Broker not connected');
    }
    return new KafkaTopicAdmin(this.adminClient);
  }
}
