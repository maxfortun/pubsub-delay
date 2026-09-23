import { BrokerConfig } from './broker/types.js';
import { StrategyType, StrategyConfig } from './strategy/index.js';

export interface DelayServiceConfig {
  broker: BrokerConfig;
  ingestTopic: string;
  advisoryTopic: string;
  bucketSeparator: string;
  headerPrefix: string;
  destinationHeader: string;
  delayDurationHeader: string;
  enqueuedAtHeader: string;
  consumerGroupPrefix: string;
  instanceId: string | null;
  timeoutPoolSize: number;
  advisorySyncIntervalMs: number;
  bucketIdleTimeoutMs: number;
  cleanupIntervalMs: number;
  precreateBuckets: string[];
  schedulerFetchMaxWaitMs: number;
  consumerRestartGraceMs: number;
  topicCreateRetries: number;
  topicCreateRetryBaseMs: number;
  strategyType: StrategyType;
  strategyConfig: StrategyConfig;
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

  const headerPrefix = process.env.HEADER_PREFIX || 'DELAY_';
  const strategyType = (process.env.SCHEDULER_STRATEGY || 'bounded-pool') as StrategyType;
  const poolSize = parseInt(process.env.TIMEOUT_POOL_SIZE || '100', 10);

  return {
    broker,
    ingestTopic,
    advisoryTopic: `${ingestTopic}${separator}advisory`,
    bucketSeparator: separator,
    headerPrefix,
    destinationHeader: process.env.DESTINATION_HEADER || `${headerPrefix}DESTINATION`,
    delayDurationHeader: process.env.DELAY_DURATION_HEADER || `${headerPrefix}DURATION`,
    enqueuedAtHeader: process.env.ENQUEUED_AT_HEADER || `${headerPrefix}ENQUEUED_AT`,
    consumerGroupPrefix: process.env.CONSUMER_GROUP_PREFIX || 'pubsub-delay',
    instanceId: process.env.HOSTNAME || process.env.INSTANCE_ID || null,
    timeoutPoolSize: poolSize,
    advisorySyncIntervalMs: parseInt(process.env.ADVISORY_SYNC_INTERVAL_MS || '10000', 10),
    bucketIdleTimeoutMs: parseInt(process.env.BUCKET_IDLE_TIMEOUT_MS || '3600000', 10),
    cleanupIntervalMs: parseInt(process.env.CLEANUP_INTERVAL_MS || '60000', 10),
    precreateBuckets: (process.env.PRECREATE_BUCKETS || '').split(',').filter(Boolean),
    schedulerFetchMaxWaitMs: parseInt(process.env.SCHEDULER_FETCH_MAX_WAIT_MS || '100', 10),
    consumerRestartGraceMs: parseInt(process.env.CONSUMER_RESTART_GRACE_MS || '5000', 10),
    topicCreateRetries: parseInt(process.env.TOPIC_CREATE_RETRIES || '5', 10),
    topicCreateRetryBaseMs: parseInt(process.env.TOPIC_CREATE_RETRY_BASE_MS || '1000', 10),
    strategyType,
    strategyConfig: {
      poolSize,
      wheelResolutionMs: parseInt(process.env.WHEEL_RESOLUTION_MS || '100', 10),
      wheelSlots: parseInt(process.env.WHEEL_SLOTS || '600', 10),
      resumeLeadMs: parseInt(process.env.BUCKET_RESUME_LEAD_MS || '0', 10),
    },
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
