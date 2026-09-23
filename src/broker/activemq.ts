import stompit from 'stompit';
import { Broker, Consumer, Producer, Message, MessageEnvelope, BrokerConfig, TopicAdmin, ConsumerOptions } from './types.js';

class ActiveMQConsumer implements Consumer {
  private client: stompit.Client;
  private subscriptions: stompit.Client.Subscription[] = [];
  private currentMessage: stompit.Client.Message | null = null;
  private messageQueue: MessageEnvelope[] = [];
  private resolveWaiting: ((env: MessageEnvelope) => void) | null = null;
  private messageIdCounter = 0;

  constructor(client: stompit.Client) {
    this.client = client;
  }

  async subscribe(topics: string[]): Promise<void> {
    for (const topic of topics) {
      this.subscribeToTopic(topic);
    }
  }

  private subscribeToTopic(topic: string): void {
    const headers: stompit.Client.SubscribeHeaders = {
      destination: `/queue/${topic}`,
      ack: 'client-individual',
    };

    const subscription = this.client.subscribe(headers, (error, message) => {
      if (error) {
        console.error(`ActiveMQ subscription error for ${topic}:`, error);
        return;
      }

      this.currentMessage = message;
      let body = Buffer.alloc(0);

      message.on('data', (chunk: Buffer) => {
        body = Buffer.concat([body, chunk]);
      });

      message.on('end', () => {
        const msg: Message = {
          key: message.headers['JMSXGroupID'] as string | undefined,
          headers: Object.fromEntries(
            Object.entries(message.headers).map(([k, v]) => [k, String(v)])
          ),
          body,
        };

        const envelope: MessageEnvelope = {
          topic,
          partition: 0,
          offset: String(++this.messageIdCounter),
          message: msg,
        };

        if (this.resolveWaiting) {
          const resolve = this.resolveWaiting;
          this.resolveWaiting = null;
          resolve(envelope);
        } else {
          this.messageQueue.push(envelope);
        }
      });
    });

    this.subscriptions.push(subscription);
  }

  async receive(): Promise<MessageEnvelope> {
    if (this.messageQueue.length > 0) {
      return this.messageQueue.shift()!;
    }
    return new Promise((resolve) => {
      this.resolveWaiting = resolve;
    });
  }

  async ack(_envelope: MessageEnvelope): Promise<void> {
    if (this.currentMessage) {
      this.client.ack(this.currentMessage);
      this.currentMessage = null;
    }
  }

  async nack(_envelope: MessageEnvelope): Promise<void> {
    // Don't ack - message will be redelivered on session end/timeout
    // For immediate redelivery, would need STOMP NACK frame extension
    this.currentMessage = null;
  }

  async close(): Promise<void> {
    for (const sub of this.subscriptions) {
      sub.unsubscribe();
    }
    this.client.disconnect();
  }
}

class ActiveMQProducer implements Producer {
  private client: stompit.Client;

  constructor(client: stompit.Client) {
    this.client = client;
  }

  async send(topic: string, message: Message): Promise<void> {
    return new Promise((resolve, _reject) => {
      const headers: stompit.Client.SendHeaders = {
        destination: `/queue/${topic}`,
        'content-type': 'application/octet-stream',
        ...message.headers,
      };

      if (message.key) {
        headers['JMSXGroupID'] = message.key;
      }

      const frame = this.client.send(headers);
      frame.write(message.body);
      frame.end();
      resolve();
    });
  }

  async close(): Promise<void> {
    this.client.disconnect();
  }
}

class ActiveMQTopicAdmin implements TopicAdmin {
  async listTopics(): Promise<string[]> {
    console.warn('ActiveMQ listTopics not fully implemented - requires JMX or web console API');
    return [];
  }

  async createTopic(_topic: string): Promise<void> {
    // ActiveMQ auto-creates queues on first use
  }

  async deleteTopic(_topic: string): Promise<void> {
    console.warn('ActiveMQ deleteTopic requires JMX or web console API');
  }

  async getConsumerLag(_topic: string, _groupId: string): Promise<number> {
    console.warn('ActiveMQ getConsumerLag requires JMX or web console API');
    return 0;
  }

  async topicExists(_topic: string): Promise<boolean> {
    return true;
  }
}

export class ActiveMQBroker implements Broker {
  private config: BrokerConfig['activemq'];
  private connectionManager: stompit.ConnectFailover | null = null;

  constructor(config: BrokerConfig['activemq']) {
    if (!config) throw new Error('ActiveMQ config required');
    this.config = config;
  }

  async connect(): Promise<void> {
    const servers = [
      {
        host: this.config!.host,
        port: this.config!.port,
        connectHeaders: {
          host: '/',
          login: this.config!.login || 'admin',
          passcode: this.config!.passcode || 'admin',
          'heart-beat': '5000,5000',
        },
      },
    ];

    this.connectionManager = new stompit.ConnectFailover(servers);
  }

  async disconnect(): Promise<void> {}

  private getClient(): Promise<stompit.Client> {
    return new Promise((resolve, reject) => {
      if (!this.connectionManager) {
        reject(new Error('Not connected'));
        return;
      }
      this.connectionManager.connect((error, client) => {
        if (error) reject(error);
        else resolve(client);
      });
    });
  }

  async createConsumer(_options: ConsumerOptions): Promise<Consumer> {
    const client = await this.getClient();
    return new ActiveMQConsumer(client);
  }

  async createProducer(): Promise<Producer> {
    const client = await this.getClient();
    return new ActiveMQProducer(client);
  }

  admin(): TopicAdmin {
    return new ActiveMQTopicAdmin();
  }
}
