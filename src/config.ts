import { BrokerConfig } from './broker/types.js';

export interface DelayServiceConfig {
  broker: BrokerConfig;
  ingestTopic: string;
  advisoryTopic: string;
  bucketSeparator: string;
  destinationHeader: string;
  delayDurationHeader: string;
  enqueuedAtHeader: string;
  consumerGroupPrefix: string;
  instanceId: string | null;
  timeoutPoolSize: number;
  advisorySyncIntervalMs: number;
  bucketIdleTimeoutMs: number;
  cleanupIntervalMs: number;
}

export function loadConfig(): DelayServiceConfig {
  const brokerType = (process.env.BROKER_TYPE || 'kafka') as 'kafka' | 'activemq';
  const ingestTopic = process.env.INGEST_TOPIC || 'delay-ingest';
  const separator = process.env.BUCKET_SEPARATOR || '-';

  const broker: BrokerConfig = {
    type: brokerType,
    kafka: brokerType === 'kafka' ? {
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
      clientId: process.env.KAFKA_CLIENT_ID || 'pubsub-delay',
    } : undefined,
    activemq: brokerType === 'activemq' ? {
      host: process.env.ACTIVEMQ_HOST || 'localhost',
      port: parseInt(process.env.ACTIVEMQ_PORT || '61613', 10),
      login: process.env.ACTIVEMQ_LOGIN,
      passcode: process.env.ACTIVEMQ_PASSCODE,
    } : undefined,
  };

  return {
    broker,
    ingestTopic,
    advisoryTopic: `${ingestTopic}${separator}advisory`,
    bucketSeparator: separator,
    destinationHeader: process.env.DESTINATION_HEADER || 'DELAY_DESTINATION',
    delayDurationHeader: process.env.DELAY_DURATION_HEADER || 'DELAY_DURATION',
    enqueuedAtHeader: process.env.ENQUEUED_AT_HEADER || 'ENQUEUED_AT',
    consumerGroupPrefix: process.env.CONSUMER_GROUP_PREFIX || 'pubsub-delay',
    instanceId: process.env.HOSTNAME || process.env.INSTANCE_ID || null,
    timeoutPoolSize: parseInt(process.env.TIMEOUT_POOL_SIZE || '100', 10),
    advisorySyncIntervalMs: parseInt(process.env.ADVISORY_SYNC_INTERVAL_MS || '10000', 10),
    bucketIdleTimeoutMs: parseInt(process.env.BUCKET_IDLE_TIMEOUT_MS || '3600000', 10), // 1 hour default
    cleanupIntervalMs: parseInt(process.env.CLEANUP_INTERVAL_MS || '60000', 10), // 1 minute default
  };
}

export function durationToBucketTopic(ingestTopic: string, separator: string, isoDuration: string): string {
  return `${ingestTopic}${separator}${isoDuration}`;
}

export function bucketTopicToDuration(ingestTopic: string, separator: string, topic: string): string | null {
  const prefix = `${ingestTopic}${separator}`;
  if (!topic.startsWith(prefix)) {
    return null;
  }
  const suffix = topic.slice(prefix.length);
  if (suffix === 'advisory' || !suffix.startsWith('PT')) {
    return null;
  }
  return suffix;
}

export function isBucketTopic(ingestTopic: string, separator: string, topic: string): boolean {
  return bucketTopicToDuration(ingestTopic, separator, topic) !== null;
}
