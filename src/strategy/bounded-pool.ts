import { MessageEnvelope } from '../broker/types.js';
import { SchedulerStrategy, SchedulerStats, StrategyConfig } from './types.js';

interface PendingMessage {
  envelope: MessageEnvelope;
  deliverAt: number;
  destination: string;
  timeoutHandle: NodeJS.Timeout | null;
}

interface BucketCacheEntry {
  deliverAt: number;
  destination: string;
  resumeTimer: NodeJS.Timeout;
}

export class BoundedPoolStrategy implements SchedulerStrategy {
  readonly name = 'BoundedPool';

  private poolSize: number;
  private timeoutPool: Map<string, PendingMessage> = new Map();
  private poolIdCounter = 0;
  private bucketCache: Map<string, BucketCacheEntry> = new Map();

  private pauseFn: ((topics: string[]) => void) | null = null;
  private resumeFn: ((topics: string[]) => void) | null = null;
  private deliverFn: ((envelope: MessageEnvelope, destination: string) => Promise<void>) | null = null;
  private ackFn: ((envelope: MessageEnvelope) => Promise<void>) | null = null;
  private nackFn: ((envelope: MessageEnvelope) => Promise<void>) | null = null;

  constructor(config: StrategyConfig) {
    this.poolSize = config.poolSize;
  }

  setPauseControl(pause: (topics: string[]) => void, resume: (topics: string[]) => void): void {
    this.pauseFn = pause;
    this.resumeFn = resume;
  }

  setDeliveryHandler(deliver: (envelope: MessageEnvelope, destination: string) => Promise<void>): void {
    this.deliverFn = deliver;
  }

  setAckHandler(ack: (envelope: MessageEnvelope) => Promise<void>, nack: (envelope: MessageEnvelope) => Promise<void>): void {
    this.ackFn = ack;
    this.nackFn = nack;
  }

  async onMessage(envelope: MessageEnvelope, deliverAt: number, destination: string): Promise<void> {
    const topic = envelope.topic;
    const cached = this.bucketCache.get(topic);

    if (cached) {
      // Bucket was paused and just resumed - use cached data
      deliverAt = cached.deliverAt;
      destination = cached.destination;
      this.bucketCache.delete(topic);
    }

    const pending: PendingMessage = {
      envelope,
      deliverAt,
      destination,
      timeoutHandle: null,
    };

    if (this.timeoutPool.size < this.poolSize) {
      this.addToPool(pending);
    } else {
      const evictCandidate = this.findLongestWaiting();
      if (evictCandidate && pending.deliverAt < evictCandidate.pending.deliverAt) {
        await this.evictPauseAndNack(evictCandidate);
        this.addToPool(pending);
      } else {
        this.cacheAndPause(topic, deliverAt, destination);
        await this.nackFn!(envelope);
        console.log(`BoundedPool: paused bucket ${topic} until ${new Date(deliverAt).toISOString()} (active: ${this.timeoutPool.size}, paused: ${this.bucketCache.size})`);
      }
    }
  }

  private addToPool(pending: PendingMessage): void {
    const id = `pending-${++this.poolIdCounter}`;
    const now = Date.now();
    const delayMs = Math.max(0, pending.deliverAt - now);

    pending.timeoutHandle = setTimeout(() => {
      this.onTimeout(id);
    }, delayMs);

    this.timeoutPool.set(id, pending);
    console.log(`BoundedPool: queued for ${new Date(pending.deliverAt).toISOString()} (active: ${this.timeoutPool.size}, paused: ${this.bucketCache.size})`);
  }

  private cacheAndPause(topic: string, deliverAt: number, destination: string): void {
    const now = Date.now();
    const resumeInMs = Math.max(0, deliverAt - now);

    const resumeTimer = setTimeout(() => {
      this.onBucketResume(topic);
    }, resumeInMs);

    this.bucketCache.set(topic, { deliverAt, destination, resumeTimer });
    this.pauseFn!([topic]);
  }

  private onBucketResume(topic: string): void {
    const cached = this.bucketCache.get(topic);
    if (!cached) return;

    this.resumeFn!([topic]);
    console.log(`BoundedPool: resumed bucket ${topic}`);
  }

  private async evictPauseAndNack(entry: { id: string; pending: PendingMessage }): Promise<void> {
    const { id, pending } = entry;
    const topic = pending.envelope.topic;

    if (pending.timeoutHandle) {
      clearTimeout(pending.timeoutHandle);
    }
    pending.timeoutHandle = null;
    this.timeoutPool.delete(id);

    this.cacheAndPause(topic, pending.deliverAt, pending.destination);
    await this.nackFn!(pending.envelope);
    console.log(`BoundedPool: evicted, paused bucket ${topic} until ${new Date(pending.deliverAt).toISOString()}`);
  }

  private findLongestWaiting(): { id: string; pending: PendingMessage } | null {
    let longest: { id: string; pending: PendingMessage } | null = null;

    for (const [id, pending] of this.timeoutPool) {
      if (!longest || pending.deliverAt > longest.pending.deliverAt) {
        longest = { id, pending };
      }
    }

    return longest;
  }

  private async onTimeout(id: string): Promise<void> {
    const pending = this.timeoutPool.get(id);
    if (!pending) return;

    this.timeoutPool.delete(id);

    try {
      await this.deliverFn!(pending.envelope, pending.destination);
      await this.ackFn!(pending.envelope);
      console.log(`BoundedPool: delivered (active: ${this.timeoutPool.size}, paused: ${this.bucketCache.size})`);
    } catch (error) {
      console.error('BoundedPool: delivery failed', error);
      await this.nackFn!(pending.envelope);
    }
  }

  stop(): void {
    for (const [_id, pending] of this.timeoutPool) {
      if (pending.timeoutHandle) {
        clearTimeout(pending.timeoutHandle);
      }
    }
    this.timeoutPool.clear();

    for (const cached of this.bucketCache.values()) {
      clearTimeout(cached.resumeTimer);
    }
    this.bucketCache.clear();
  }

  getStats(): SchedulerStats {
    return {
      active: this.timeoutPool.size,
      pending: 0,
      paused: this.bucketCache.size,
    };
  }
}
