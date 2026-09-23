import { EventEmitter } from 'events';
import { randomUUID } from 'crypto';
import { Broker, Consumer, Producer, Message, TopicAdmin } from './broker/types.js';
import { DelayServiceConfig, isBucketTopic, bucketTopicToDuration } from './config.js';
import { parseDurationToMs } from './duration.js';

export interface BucketInfo {
  topic: string;
  isoDuration: string;
  delayMs: number;
  lastActivity: number;
}

interface AdvisoryEvent {
  type: 'bucket:add' | 'bucket:remove';
  topic: string;
  isoDuration?: string;
  delayMs?: number;
  timestamp: number;
}

export class BucketAdvisory extends EventEmitter {
  private buckets = new Map<string, BucketInfo>();
  private broker: Broker;
  private config: DelayServiceConfig;
  private producer: Producer | null = null;
  private consumer: Consumer | null = null;
  private admin: TopicAdmin | null = null;
  private running = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private cleanupInterval: NodeJS.Timeout | null = null;
  // Removed buckets, kept so discovery does not revive them while the topic still exists.
  // owner: this pod removed it and deletes the topic once every scheduler had time to unsubscribe.
  private pendingDeletion = new Map<string, { removedAt: number; owner: boolean }>();

  constructor(broker: Broker, config: DelayServiceConfig) {
    super();
    this.broker = broker;
    this.config = config;
  }

  async start(): Promise<void> {
    this.admin = this.broker.admin();
    this.producer = await this.broker.createProducer();
    // Every pod must see every event, so each pod consumes in its own group
    const member = this.config.instanceId ?? randomUUID();
    this.consumer = await this.broker.createConsumer({
      groupId: `${this.config.consumerGroupPrefix}-advisory-${member}`,
      instanceId: this.config.instanceId ? `${this.config.instanceId}-advisory` : undefined,
    });

    await this.discoverExistingBuckets();

    await this.consumer.subscribe([this.config.advisoryTopic]);
    this.running = true;
    this.consumeAdvisoryEvents();

    this.syncInterval = setInterval(() => {
      this.discoverExistingBuckets().catch(console.error);
    }, this.config.advisorySyncIntervalMs);

    this.cleanupInterval = setInterval(() => {
      this.cleanupIdleBuckets().catch(console.error);
    }, this.config.cleanupIntervalMs);

    console.log(`Bucket advisory started, syncing via ${this.config.advisoryTopic}`);
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.syncInterval) clearInterval(this.syncInterval);
    if (this.cleanupInterval) clearInterval(this.cleanupInterval);
    if (this.consumer) await this.consumer.close();
    if (this.producer) await this.producer.close();
  }

  private async discoverExistingBuckets(): Promise<void> {
    if (!this.admin) return;

    const topics = await this.admin.listTopics();
    for (const topic of topics) {
      if (isBucketTopic(this.config.ingestTopic, this.config.bucketSeparator, topic)) {
        const isoDuration = bucketTopicToDuration(
          this.config.ingestTopic,
          this.config.bucketSeparator,
          topic
        );
        // Topics awaiting deletion still exist; only new traffic via registerBucket revives them
        if (isoDuration && !this.buckets.has(topic) && !this.pendingDeletion.has(topic)) {
          const delayMs = parseDurationToMs(isoDuration);
          this.addBucket(topic, isoDuration, delayMs, false);
        }
      }
    }
  }

  private async consumeAdvisoryEvents(): Promise<void> {
    while (this.running && this.consumer) {
      try {
        const envelope = await this.consumer.receive();
        const event: AdvisoryEvent = JSON.parse(envelope.message.body.toString());
        this.handleAdvisoryEvent(event);
        await this.consumer.ack(envelope);
      } catch (error) {
        if (this.running) {
          console.error('Advisory consumer error:', error);
        }
      }
    }
  }

  private handleAdvisoryEvent(event: AdvisoryEvent): void {
    if (event.type === 'bucket:add' && event.isoDuration && event.delayMs !== undefined) {
      this.addBucket(event.topic, event.isoDuration, event.delayMs, false);
    } else if (event.type === 'bucket:remove') {
      if (this.buckets.has(event.topic)) {
        this.pendingDeletion.set(event.topic, { removedAt: Date.now(), owner: false });
      }
      this.removeBucket(event.topic, false);
    }
  }

  private addBucket(topic: string, isoDuration: string, delayMs: number, broadcast: boolean): void {
    if (this.buckets.has(topic)) {
      this.buckets.get(topic)!.lastActivity = Date.now();
      return;
    }

    const info: BucketInfo = {
      topic,
      isoDuration,
      delayMs,
      lastActivity: Date.now(),
    };
    this.buckets.set(topic, info);
    this.emit('bucket:added', info);
    console.log(`Bucket added: ${topic}`);

    if (broadcast) {
      this.broadcastEvent({
        type: 'bucket:add',
        topic,
        isoDuration,
        delayMs,
        timestamp: Date.now(),
      });
    }
  }

  private removeBucket(topic: string, broadcast: boolean): void {
    if (!this.buckets.has(topic)) return;

    this.buckets.delete(topic);
    this.emit('bucket:removed', topic);
    console.log(`Bucket removed: ${topic}`);

    if (broadcast) {
      this.broadcastEvent({
        type: 'bucket:remove',
        topic,
        timestamp: Date.now(),
      });
    }
  }

  private async broadcastEvent(event: AdvisoryEvent): Promise<void> {
    if (!this.producer) return;

    const message: Message = {
      headers: {},
      body: Buffer.from(JSON.stringify(event)),
    };
    await this.producer.send(this.config.advisoryTopic, message);
  }

  async registerBucket(topic: string, isoDuration: string, delayMs: number): Promise<void> {
    // Create the topic before announcing it, so schedulers never subscribe to a
    // missing topic and delivery does not depend on broker auto-creation
    if (!this.buckets.has(topic) && this.admin && !(await this.admin.topicExists(topic))) {
      await this.admin.createTopic(topic);
    }
    this.addBucket(topic, isoDuration, delayMs, true);
  }

  updateActivity(topic: string): void {
    const bucket = this.buckets.get(topic);
    if (bucket) {
      bucket.lastActivity = Date.now();
    }
  }

  private async cleanupIdleBuckets(): Promise<void> {
    if (!this.admin) return;

    const now = Date.now();
    const groupId = `${this.config.consumerGroupPrefix}-scheduler`;

    // Phase 2: delete topics removed at least BUCKET_DELETE_GRACE_MS ago. Deleting while a
    // scheduler is still (re)joining with the topic in its subscription stalls the join.
    for (const [topic, { removedAt, owner }] of this.pendingDeletion) {
      if (this.buckets.has(topic)) {
        // Re-registered by new traffic in the meantime: keep it
        this.pendingDeletion.delete(topic);
        continue;
      }
      if (now - removedAt < this.config.bucketDeleteGraceMs) continue;
      if (!owner) {
        // The owning pod deletes it; keep the tombstone until that has had time to happen
        if (now - removedAt >= this.config.bucketDeleteGraceMs + 2 * this.config.cleanupIntervalMs) {
          this.pendingDeletion.delete(topic);
        }
        continue;
      }
      try {
        const lag = await this.admin.getConsumerLag(topic, groupId);
        if (this.buckets.has(topic)) continue;
        if (lag === 0) {
          console.log(`Deleting bucket topic: ${topic}`);
          await this.admin.deleteTopic(topic);
        } else {
          console.warn(`Bucket ${topic} received messages after removal (lag ${lag}); re-adding`);
          const isoDuration = bucketTopicToDuration(this.config.ingestTopic, this.config.bucketSeparator, topic)!;
          this.addBucket(topic, isoDuration, parseDurationToMs(isoDuration), true);
        }
        this.pendingDeletion.delete(topic);
      } catch (error) {
        console.error(`Error deleting bucket ${topic}:`, error);
      }
    }

    // Phase 1: unsubscribe idle, drained buckets everywhere
    for (const [topic, bucket] of this.buckets) {
      const idleTime = now - bucket.lastActivity;
      if (idleTime < this.config.bucketIdleTimeoutMs) continue;

      try {
        const lag = await this.admin.getConsumerLag(topic, groupId);
        if (lag === 0) {
          console.log(`Cleaning up idle bucket: ${topic} (idle for ${idleTime}ms)`);
          // Unsubscribe everywhere first; the topic itself is deleted in a later cycle
          this.removeBucket(topic, true);
          this.pendingDeletion.set(topic, { removedAt: now, owner: true });
        }
      } catch (error) {
        console.error(`Error checking bucket ${topic} for cleanup:`, error);
      }
    }
  }

  getBuckets(): BucketInfo[] {
    return Array.from(this.buckets.values());
  }

  getBucket(topic: string): BucketInfo | undefined {
    return this.buckets.get(topic);
  }
}
