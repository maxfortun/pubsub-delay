export interface Message {
  key?: string;
  headers: Record<string, string>;
  body: Buffer;
}

export interface MessageEnvelope {
  topic: string;
  partition: number;
  offset: string;
  message: Message;
}

export interface Consumer {
  subscribe(topics: string[]): Promise<void>;
  receive(): Promise<MessageEnvelope>;
  ack(envelope: MessageEnvelope): Promise<void>;
  nack(envelope: MessageEnvelope): Promise<void>;
  pause(topics: string[]): void;
  resume(topics: string[]): void;
  // Brokers that can add and drop subscriptions in place implement this, so a bucket
  // change does not close the consumer and hand its in-flight messages to someone else
  updateSubscription?(add: string[], remove: string[]): Promise<void>;
  close(): Promise<void>;
}

export interface Producer {
  send(topic: string, message: Message): Promise<void>;
  close(): Promise<void>;
}

export interface TopicAdmin {
  listTopics(): Promise<string[]>;
  createTopic(topic: string): Promise<void>;
  deleteTopic(topic: string): Promise<void>;
  getConsumerLag(topic: string, groupId: string): Promise<number>;
  topicExists(topic: string): Promise<boolean>;
}

export interface ConsumerOptions {
  groupId: string;
  instanceId?: string;
  // Start from the earliest offset when the group has no committed offset
  fromBeginning?: boolean;
  // Upper bound on how long a fetch may block waiting for data
  maxWaitMs?: number;
}

export interface Broker {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  createConsumer(options: ConsumerOptions): Promise<Consumer>;
  createProducer(): Promise<Producer>;
  admin(): TopicAdmin;
}

export interface BrokerConfig {
  type: 'kafka' | 'activemq';
  kafka?: {
    brokers: string[];
    clientId: string;
  };
  activemq?: {
    host: string;
    port: number;
    login?: string;
    passcode?: string;
    // Messages the broker may push to a subscription before they are acked
    prefetchSize: number;
    reconnectDelayMs: number;
    // Frame headers set by the broker per delivery; not carried over when a message is forwarded
    stripHeaders: string[];
    // Destinations delivered to every subscriber (/topic/) instead of one (/queue/)
    broadcastTopics: string[];
    // Jolokia (web console) is used for admin: listing, sizing and deleting queues
    jolokia?: {
      url: string;
      login?: string;
      password?: string;
      origin: string;
      brokerName?: string;
      timeoutMs: number;
    };
  };
}
