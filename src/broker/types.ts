export interface Message {
  key?: string;
  headers: Record<string, string>;
  body: Buffer;
}

export interface Consumer {
  subscribe(topics: string[]): Promise<void>;
  receive(): Promise<{ topic: string; message: Message }>;
  commit(): Promise<void>;
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

export interface Broker {
  connect(): Promise<void>;
  disconnect(): Promise<void>;
  createConsumer(groupId: string): Promise<Consumer>;
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
  };
}
