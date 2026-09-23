import { BrokerConfig } from './broker/types.js';
import { StrategyType, StrategyConfig } from './strategy/index.js';
import { TransformConfig, TransformPlugin } from './transform/types.js';

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
  bucketDeleteGraceMs: number;
  precreateBuckets: string[];
  schedulerFetchMaxWaitMs: number;
  consumerRestartGraceMs: number;
  consumerStartTimeoutMs: number;
  routerRetryBackoffMs: number;
  topicCreateRetries: number;
  topicCreateRetryBaseMs: number;
  strategyType: StrategyType;
  strategyConfig: StrategyConfig;
  transform: TransformConfig;
}

// STOMP/JMS headers the broker sets on each delivery
const DEFAULT_ACTIVEMQ_STRIP_HEADERS = [
  'destination', 'message-id', 'subscription', 'ack', 'receipt', 'content-length',
  'expires', 'priority', 'timestamp', 'persistent', 'redelivered', 'original-destination',
  'JMSXGroupID', 'JMSXGroupSeq', 'JMSXGroupFirstForConsumer', 'JMSXDeliveryCount', 'JMSXUserID',
].join(',');

export function loadConfig(): DelayServiceConfig {
  const brokerType = (process.env.BROKER_TYPE || 'kafka') as 'kafka' | 'activemq';
  const ingestTopic = process.env.INGEST_TOPIC || 'delay-ingest';
  const separator = process.env.BUCKET_SEPARATOR || '-';

  const advisoryTopic = `${ingestTopic}${separator}advisory`;
  const jolokiaUrl = process.env.ACTIVEMQ_JOLOKIA_URL;
  const activemqLogin = process.env.ACTIVEMQ_LOGIN || 'admin';
  const activemqPasscode = process.env.ACTIVEMQ_PASSCODE || 'admin';

  const headerPrefix = process.env.HEADER_PREFIX || 'DELAY_';

  const broker: BrokerConfig = {
    type: brokerType,
    kafka: brokerType === 'kafka' ? {
      brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
      clientId: process.env.KAFKA_CLIENT_ID || 'pubsub-delay',
    } : undefined,
    activemq: brokerType === 'activemq' ? {
      host: process.env.ACTIVEMQ_HOST || 'localhost',
      port: parseInt(process.env.ACTIVEMQ_PORT || '61613', 10),
      login: activemqLogin,
      passcode: activemqPasscode,
      keyHeader: process.env.KEY_HEADER || `${headerPrefix}KEY`,
      prefetchSize: parseInt(process.env.ACTIVEMQ_PREFETCH || '100', 10),
      reconnectDelayMs: parseInt(process.env.ACTIVEMQ_RECONNECT_DELAY_MS || '1000', 10),
      heartbeatMs: parseInt(process.env.ACTIVEMQ_HEARTBEAT_MS || '5000', 10),
      heartbeatSendMarginMs: parseInt(process.env.ACTIVEMQ_HEARTBEAT_SEND_MARGIN_MS || '1000', 10),
      heartbeatReceiveGraceMs: parseInt(process.env.ACTIVEMQ_HEARTBEAT_RECEIVE_GRACE_MS || '5000', 10),
      stripHeaders: (process.env.ACTIVEMQ_STRIP_HEADERS || DEFAULT_ACTIVEMQ_STRIP_HEADERS).split(',').filter(Boolean),
      broadcastTopics: [advisoryTopic],
      jolokia: jolokiaUrl ? {
        url: jolokiaUrl,
        login: process.env.ACTIVEMQ_JOLOKIA_LOGIN || activemqLogin,
        password: process.env.ACTIVEMQ_JOLOKIA_PASSWORD || activemqPasscode,
        origin: process.env.ACTIVEMQ_JOLOKIA_ORIGIN || 'http://localhost',
        brokerName: process.env.ACTIVEMQ_BROKER_NAME || undefined,
        timeoutMs: parseInt(process.env.ACTIVEMQ_JOLOKIA_TIMEOUT_MS || '5000', 10),
      } : undefined,
    } : undefined,
  };

  const strategyType = (process.env.SCHEDULER_STRATEGY || 'bounded-pool') as StrategyType;
  const poolSize = parseInt(process.env.TIMEOUT_POOL_SIZE || '100', 10);

  return {
    broker,
    ingestTopic,
    advisoryTopic,
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
    bucketDeleteGraceMs: parseInt(process.env.BUCKET_DELETE_GRACE_MS || '30000', 10),
    precreateBuckets: (process.env.PRECREATE_BUCKETS || '').split(',').filter(Boolean),
    schedulerFetchMaxWaitMs: parseInt(process.env.SCHEDULER_FETCH_MAX_WAIT_MS || '100', 10),
    consumerRestartGraceMs: parseInt(process.env.CONSUMER_RESTART_GRACE_MS || '5000', 10),
    consumerStartTimeoutMs: parseInt(process.env.CONSUMER_START_TIMEOUT_MS || '30000', 10),
    routerRetryBackoffMs: parseInt(process.env.ROUTER_RETRY_BACKOFF_MS || '1000', 10),
    topicCreateRetries: parseInt(process.env.TOPIC_CREATE_RETRIES || '5', 10),
    topicCreateRetryBaseMs: parseInt(process.env.TOPIC_CREATE_RETRY_BASE_MS || '1000', 10),
    strategyType,
    strategyConfig: {
      poolSize,
      wheelResolutionMs: parseInt(process.env.WHEEL_RESOLUTION_MS || '100', 10),
      wheelSlots: parseInt(process.env.WHEEL_SLOTS || '600', 10),
      resumeLeadMs: parseInt(process.env.BUCKET_RESUME_LEAD_MS || '0', 10),
    },
    transform: {
      plugin: (process.env.TRANSFORM_PLUGIN || 'none') as TransformPlugin,
      retryBackoffMs: parseInt(process.env.TRANSFORM_RETRY_BACKOFF_MS || '1000', 10),
      http: {
        preUrl: process.env.TRANSFORM_PRE_URL || undefined,
        postUrl: process.env.TRANSFORM_POST_URL || undefined,
        timeoutMs: parseInt(process.env.TRANSFORM_HTTP_TIMEOUT_MS || '5000', 10),
      },
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
