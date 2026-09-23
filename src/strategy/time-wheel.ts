import { MessageEnvelope } from '../broker/types.js';
import { SchedulerStrategy, SchedulerStats, StrategyConfig } from './types.js';

interface WheelEntry {
  envelope: MessageEnvelope;
  deliverAt: number;
  destination: string;
}

export class TimeWheelStrategy implements SchedulerStrategy {
  readonly name = 'TimeWheel';

  private resolutionMs: number;
  private slots: number;
  private wheel: WheelEntry[][];
  private currentSlot = 0;
  private wheelStartTime: number;
  private tickTimer: NodeJS.Timeout | null = null;
  private overflow: WheelEntry[] = [];

  private deliverFn: ((envelope: MessageEnvelope, destination: string, deliverAt: number) => Promise<void>) | null = null;
  private ackFn: ((envelope: MessageEnvelope) => Promise<void>) | null = null;
  private nackFn: ((envelope: MessageEnvelope) => Promise<void>) | null = null;

  constructor(config: StrategyConfig) {
    this.resolutionMs = config.wheelResolutionMs || 100;
    this.slots = config.wheelSlots || 600; // 60 seconds at 100ms resolution
    this.wheel = Array.from({ length: this.slots }, () => []);
    this.wheelStartTime = Date.now();
    this.startTicking();
  }

  setPauseControl(_pause: (topics: string[]) => void, _resume: (topics: string[]) => void): void {
    // TimeWheel doesn't use pause/resume - it holds all messages in memory
  }

  setDeliveryHandler(deliver: (envelope: MessageEnvelope, destination: string, deliverAt: number) => Promise<void>): void {
    this.deliverFn = deliver;
  }

  setAckHandler(ack: (envelope: MessageEnvelope) => Promise<void>, nack: (envelope: MessageEnvelope) => Promise<void>): void {
    this.ackFn = ack;
    this.nackFn = nack;
  }

  async onMessage(envelope: MessageEnvelope, deliverAt: number, destination: string): Promise<void> {
    const entry: WheelEntry = { envelope, deliverAt, destination };
    const now = Date.now();
    const delayMs = deliverAt - now;

    if (delayMs <= 0) {
      // Already due - deliver immediately
      await this.deliverEntry(entry);
      return;
    }

    const wheelRangeMs = this.slots * this.resolutionMs;

    if (delayMs > wheelRangeMs) {
      // Beyond wheel range - add to overflow
      this.insertOverflow(entry);
      console.log(`TimeWheel: overflow for ${new Date(deliverAt).toISOString()} (wheel: ${this.getWheelCount()}, overflow: ${this.overflow.length})`);
    } else {
      // Calculate target slot
      const ticksFromNow = Math.ceil(delayMs / this.resolutionMs);
      const targetSlot = (this.currentSlot + ticksFromNow) % this.slots;
      this.wheel[targetSlot].push(entry);
      console.log(`TimeWheel: slot ${targetSlot} for ${new Date(deliverAt).toISOString()} (wheel: ${this.getWheelCount()}, overflow: ${this.overflow.length})`);
    }
  }

  private insertOverflow(entry: WheelEntry): void {
    // Keep overflow sorted by deliverAt for efficient promotion
    const insertIndex = this.overflow.findIndex((e) => e.deliverAt > entry.deliverAt);
    if (insertIndex === -1) {
      this.overflow.push(entry);
    } else {
      this.overflow.splice(insertIndex, 0, entry);
    }
  }

  private startTicking(): void {
    this.tickTimer = setInterval(() => {
      this.tick();
    }, this.resolutionMs);
  }

  private async tick(): Promise<void> {
    // Advance to next slot
    this.currentSlot = (this.currentSlot + 1) % this.slots;

    // Fire all entries in current slot
    const entries = this.wheel[this.currentSlot];
    this.wheel[this.currentSlot] = [];

    for (const entry of entries) {
      await this.deliverEntry(entry);
    }

    // Promote overflow entries that now fit in the wheel
    this.promoteOverflow();
  }

  private promoteOverflow(): void {
    const now = Date.now();
    const wheelRangeMs = this.slots * this.resolutionMs;

    while (this.overflow.length > 0) {
      const entry = this.overflow[0];
      const delayMs = entry.deliverAt - now;

      if (delayMs <= wheelRangeMs) {
        this.overflow.shift();
        const ticksFromNow = Math.max(1, Math.ceil(delayMs / this.resolutionMs));
        const targetSlot = (this.currentSlot + ticksFromNow) % this.slots;
        this.wheel[targetSlot].push(entry);
      } else {
        break; // Overflow is sorted, no more can be promoted
      }
    }
  }

  private async deliverEntry(entry: WheelEntry): Promise<void> {
    try {
      await this.deliverFn!(entry.envelope, entry.destination, entry.deliverAt);
      await this.ackFn!(entry.envelope);
      console.log(`TimeWheel: delivered (wheel: ${this.getWheelCount()}, overflow: ${this.overflow.length})`);
    } catch (error) {
      console.error('TimeWheel: delivery failed', error);
      await this.nackFn!(entry.envelope);
    }
  }

  private getWheelCount(): number {
    return this.wheel.reduce((sum, slot) => sum + slot.length, 0);
  }

  stop(): void {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
    this.wheel = Array.from({ length: this.slots }, () => []);
    this.overflow = [];
  }

  getStats(): SchedulerStats {
    return {
      active: this.getWheelCount(),
      pending: this.overflow.length,
      paused: 0,
    };
  }
}
