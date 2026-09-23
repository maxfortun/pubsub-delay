import stompit from 'stompit';
import { Broker, Consumer, Producer, Message, MessageEnvelope, BrokerConfig, TopicAdmin, ConsumerOptions } from './types.js';

type ActiveMQConfig = NonNullable<BrokerConfig['activemq']>;
type JolokiaConfig = NonNullable<ActiveMQConfig['jolokia']>;
type Connect = () => Promise<stompit.Client>;

// JMSXGroupID is ActiveMQ's message group: every message of a group goes to the same
// consumer, in order. It is the closest equivalent of a Kafka message key.
const GROUP_HEADER = 'JMSXGroupID';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function destinationFor(config: ActiveMQConfig, name: string): string {
  return config.broadcastTopics.includes(name) ? `/topic/${name}` : `/queue/${name}`;
}

// Resolves once the broker confirms the frame, so a send or ack is never assumed to have landed
function withReceipt(send: (options: stompit.Client.SendOptions) => void): Promise<void> {
  return new Promise((resolve, reject) => send({ onReceipt: () => resolve(), onError: reject }));
}

class ActiveMQConsumer implements Consumer {
  private client: stompit.Client | null = null;
  private generation = 0;
  private topics = new Set<string>();
  private subscriptions = new Map<string, stompit.Client.Subscription>();
  // Delivered by the broker, not yet acked; keyed by envelope offset
  private unacked = new Map<string, { client: stompit.Client; topic: string; message: stompit.Client.Message }>();
  // Received and not yet handed out, in arrival order; nacked messages go back to the front
  private queue: MessageEnvelope[] = [];
  private pausedTopics = new Set<string>();
  private waiter: { resolve: (env: MessageEnvelope) => void; reject: (err: Error) => void } | null = null;
  private messageIdCounter = 0;
  private closed = false;

  constructor(private connect: Connect, private config: ActiveMQConfig) {}

  async start(): Promise<void> {
    this.attach(await this.connect());
  }

  private attach(client: stompit.Client): void {
    const generation = ++this.generation;
    this.client = client;
    // Every send or ack awaiting its receipt listens for 'error'
    client.setMaxListeners(0);
    client.on('error', (error: Error) => this.onConnectionLost(generation, error));
    for (const topic of this.topics) this.subscribeToTopic(topic);
  }

  private onConnectionLost(generation: number, error: Error): void {
    if (this.closed || generation !== this.generation) return;
    console.error(`ActiveMQ consumer connection lost, reconnecting: ${error.message}`);
    this.client = null;
    this.subscriptions.clear();
    // The broker redelivers everything unacked on the lost connection
    this.queue = [];
    this.unacked.clear();
    this.reconnect();
  }

  private async reconnect(): Promise<void> {
    while (!this.closed) {
      await sleep(this.config.reconnectDelayMs);
      try {
        this.attach(await this.connect());
        console.log('ActiveMQ consumer reconnected');
        return;
      } catch (error) {
        console.error('ActiveMQ consumer reconnect failed', error);
      }
    }
  }

  async subscribe(topics: string[]): Promise<void> {
    await this.updateSubscription(topics, []);
  }

  async updateSubscription(add: string[], remove: string[]): Promise<void> {
    for (const topic of remove) {
      this.topics.delete(topic);
      this.pausedTopics.delete(topic);
      this.subscriptions.get(topic)?.unsubscribe();
      this.subscriptions.delete(topic);
      // Not acked, so the broker hands them to another subscriber; a late ack is a no-op
      this.queue = this.queue.filter((env) => env.topic !== topic);
      for (const [offset, entry] of this.unacked) if (entry.topic === topic) this.unacked.delete(offset);
    }
    for (const topic of add) {
      if (this.topics.has(topic)) continue;
      this.topics.add(topic);
      if (this.client) this.subscribeToTopic(topic);
    }
  }

  private subscribeToTopic(topic: string): void {
    const client = this.client!;
    const headers: stompit.Client.SubscribeHeaders = {
      destination: destinationFor(this.config, topic),
      ack: 'client-individual',
      'activemq.prefetchSize': String(this.config.prefetchSize),
    };

    const subscription = client.subscribe(headers, (error, message) => {
      if (error) {
        console.error(`ActiveMQ subscription error for ${topic}:`, error);
        return;
      }

      const chunks: Buffer[] = [];
      message.on('data', (chunk: Buffer) => chunks.push(chunk));
      message.on('end', () => {
        if (client !== this.client || !this.topics.has(topic)) return;
        const envelope: MessageEnvelope = {
          topic,
          partition: 0,
          offset: String(++this.messageIdCounter),
          message: this.toMessage(message, Buffer.concat(chunks)),
        };
        this.unacked.set(envelope.offset, { client, topic, message });
        this.queue.push(envelope);
        this.wake();
      });
    });

    this.subscriptions.set(topic, subscription);
  }

  private toMessage(frame: stompit.Client.Message, body: Buffer): Message {
    const headers: Record<string, string> = {};
    for (const [k, v] of Object.entries(frame.headers)) {
      if (!this.config.stripHeaders.includes(k)) headers[k] = String(v);
    }
    const group = frame.headers[GROUP_HEADER];
    return { key: group === undefined ? undefined : String(group), headers, body };
  }

  private takeReady(): MessageEnvelope | undefined {
    const i = this.queue.findIndex((env) => !this.pausedTopics.has(env.topic));
    return i < 0 ? undefined : this.queue.splice(i, 1)[0];
  }

  private wake(): void {
    if (!this.waiter) return;
    const envelope = this.takeReady();
    if (!envelope) return;
    const { resolve } = this.waiter;
    this.waiter = null;
    resolve(envelope);
  }

  async receive(): Promise<MessageEnvelope> {
    if (this.closed) throw new Error('Consumer closed');
    const envelope = this.takeReady();
    if (envelope) return envelope;
    return new Promise((resolve, reject) => {
      this.waiter = { resolve, reject };
    });
  }

  pause(topics: string[]): void {
    for (const topic of topics) this.pausedTopics.add(topic);
  }

  resume(topics: string[]): void {
    for (const topic of topics) this.pausedTopics.delete(topic);
    this.wake();
  }

  async ack(envelope: MessageEnvelope): Promise<void> {
    const entry = this.unacked.get(envelope.offset);
    // Unknown: from a lost connection, and already being redelivered
    if (!entry || entry.client !== this.client) return;
    this.unacked.delete(envelope.offset);
    await withReceipt((options) => entry.client.ack(entry.message, {}, options));
  }

  // A nack is "do not ack": the message stays unacked and is handed out again locally,
  // ahead of anything received after it from the same destination
  async nack(envelope: MessageEnvelope): Promise<void> {
    const entry = this.unacked.get(envelope.offset);
    if (!entry || entry.client !== this.client || !this.topics.has(envelope.topic)) return;
    this.queue.unshift(envelope);
    this.wake();
  }

  async close(): Promise<void> {
    this.closed = true;
    if (this.waiter) {
      const { reject } = this.waiter;
      this.waiter = null;
      reject(new Error('Consumer closed'));
    }
    const client = this.client;
    this.client = null;
    // Disconnecting releases every unacked message back to the broker
    client?.disconnect();
  }
}

class ActiveMQProducer implements Producer {
  private client: Promise<stompit.Client> | null = null;

  constructor(private connect: Connect, private config: ActiveMQConfig) {}

  private getClient(): Promise<stompit.Client> {
    if (!this.client) {
      const client = this.connect();
      this.client = client;
      client.then(
        (c) => c.setMaxListeners(0).on('error', (error: Error) => {
          console.error(`ActiveMQ producer connection lost: ${error.message}`);
          if (this.client === client) this.client = null;
        }),
        () => {
          if (this.client === client) this.client = null;
        }
      );
    }
    return this.client;
  }

  async send(topic: string, message: Message): Promise<void> {
    const client = await this.getClient();
    const headers: stompit.Client.SendHeaders = {
      'content-type': 'application/octet-stream',
      persistent: 'true',
      ...message.headers,
      // Set last so a header carried over from the source can never redirect the message
      destination: destinationFor(this.config, topic),
    };
    if (message.key !== undefined) headers[GROUP_HEADER] = message.key;

    await withReceipt((options) => {
      const frame = client.send(headers, options);
      frame.write(message.body);
      frame.end();
    });
  }

  async close(): Promise<void> {
    const client = this.client;
    this.client = null;
    if (client) (await client.catch(() => null))?.disconnect();
  }
}

interface JolokiaResponse {
  status: number;
  value?: unknown;
  error?: string;
  error_type?: string;
}

// Admin through the broker's Jolokia (JMX over HTTP) endpoint, served by the web console
class JolokiaTopicAdmin implements TopicAdmin {
  private brokerName: Promise<string> | null = null;

  constructor(private config: JolokiaConfig, private broadcastTopics: string[]) {}

  private async request(body: object): Promise<JolokiaResponse> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      // Jolokia rejects requests without an allowed Origin
      Origin: this.config.origin,
    };
    if (this.config.login) {
      headers.Authorization = `Basic ${Buffer.from(`${this.config.login}:${this.config.password ?? ''}`).toString('base64')}`;
    }
    const res = await fetch(this.config.url, {
      method: 'POST',
      headers,
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(this.config.timeoutMs),
    });
    if (!res.ok) throw new Error(`Jolokia HTTP ${res.status}`);
    return (await res.json()) as JolokiaResponse;
  }

  private async call(body: object): Promise<unknown> {
    const reply = await this.request(body);
    if (reply.status !== 200) throw new Error(`Jolokia ${reply.status}: ${reply.error}`);
    return reply.value;
  }

  private getBrokerName(): Promise<string> {
    if (this.config.brokerName) return Promise.resolve(this.config.brokerName);
    if (!this.brokerName) {
      this.brokerName = (async () => {
        const names = (await this.call({ type: 'search', mbean: 'org.apache.activemq:type=Broker,brokerName=*' })) as string[];
        const name = names[0]?.match(/brokerName=([^,]+)/)?.[1];
        if (!name) throw new Error('Jolokia: no ActiveMQ broker found');
        return name;
      })();
      this.brokerName.catch(() => (this.brokerName = null));
    }
    return this.brokerName;
  }

  private async brokerMBean(): Promise<string> {
    return `org.apache.activemq:type=Broker,brokerName=${await this.getBrokerName()}`;
  }

  async listTopics(): Promise<string[]> {
    const names = (await this.call({
      type: 'search',
      mbean: `${await this.brokerMBean()},destinationType=Queue,destinationName=*`,
    })) as string[];
    return names.map((n) => n.match(/destinationName=([^,]+)/)?.[1]).filter((n): n is string => !!n);
  }

  async createTopic(topic: string): Promise<void> {
    // Broadcast destinations are JMS topics, created by their first subscriber
    if (this.broadcastTopics.includes(topic)) return;
    await this.call({ type: 'exec', mbean: await this.brokerMBean(), operation: 'addQueue(java.lang.String)', arguments: [topic] });
  }

  async deleteTopic(topic: string): Promise<void> {
    await this.call({ type: 'exec', mbean: await this.brokerMBean(), operation: 'removeQueue(java.lang.String)', arguments: [topic] });
  }

  // A queue has one backlog shared by all its consumers: messages not yet acked
  async getConsumerLag(topic: string, _groupId: string): Promise<number> {
    const reply = await this.request({
      type: 'read',
      mbean: `${await this.brokerMBean()},destinationType=Queue,destinationName=${topic}`,
      attribute: 'QueueSize',
    });
    if (reply.status === 404) return 0;
    if (reply.status !== 200) throw new Error(`Jolokia ${reply.status}: ${reply.error}`);
    return Number(reply.value);
  }

  async topicExists(topic: string): Promise<boolean> {
    if (this.broadcastTopics.includes(topic)) return true;
    return (await this.listTopics()).includes(topic);
  }
}

// Without Jolokia the broker cannot be inspected. Queues are created on first use, and
// buckets are never considered drained, so none is ever deleted.
class StompOnlyTopicAdmin implements TopicAdmin {
  async listTopics(): Promise<string[]> {
    return [];
  }

  async createTopic(_topic: string): Promise<void> {}

  async deleteTopic(_topic: string): Promise<void> {}

  async getConsumerLag(_topic: string, _groupId: string): Promise<number> {
    return Number.POSITIVE_INFINITY;
  }

  async topicExists(_topic: string): Promise<boolean> {
    return true;
  }
}

export class ActiveMQBroker implements Broker {
  private config: ActiveMQConfig;
  private connectionManager: stompit.ConnectFailover | null = null;
  private topicAdmin: TopicAdmin;

  constructor(config: BrokerConfig['activemq']) {
    if (!config) throw new Error('ActiveMQ config required');
    this.config = config;
    if (config.jolokia) {
      this.topicAdmin = new JolokiaTopicAdmin(config.jolokia, config.broadcastTopics);
    } else {
      console.warn('ActiveMQ: ACTIVEMQ_JOLOKIA_URL not set; bucket discovery and idle cleanup are disabled');
      this.topicAdmin = new StompOnlyTopicAdmin();
    }
  }

  async connect(): Promise<void> {
    const servers = [
      {
        host: this.config.host,
        port: this.config.port,
        connectHeaders: {
          host: '/',
          login: this.config.login || 'admin',
          passcode: this.config.passcode || 'admin',
          'heart-beat': '5000,5000',
        },
      },
    ];

    this.connectionManager = new stompit.ConnectFailover(servers);
    this.connectionManager.on('error', (error: Error) => {
      console.error(`ActiveMQ connection error: ${error.message}`);
    });
  }

  async disconnect(): Promise<void> {}

  // ConnectFailover retries until a server accepts, with its own backoff
  private getClient = (): Promise<stompit.Client> => {
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
  };

  async createConsumer(_options: ConsumerOptions): Promise<Consumer> {
    const consumer = new ActiveMQConsumer(this.getClient, this.config);
    await consumer.start();
    return consumer;
  }

  async createProducer(): Promise<Producer> {
    return new ActiveMQProducer(this.getClient, this.config);
  }

  admin(): TopicAdmin {
    return this.topicAdmin;
  }
}
