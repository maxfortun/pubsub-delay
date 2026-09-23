import { MessageEnvelope } from '../broker/types.js';

export interface SchedulerStrategy {
  readonly name: string;

  onMessage(envelope: MessageEnvelope, deliverAt: number, destination: string): Promise<void>;

  setPauseControl(pause: (topics: string[]) => void, resume: (topics: string[]) => void): void;

  setDeliveryHandler(deliver: (envelope: MessageEnvelope, destination: string) => Promise<void>): void;

  setAckHandler(ack: (envelope: MessageEnvelope) => Promise<void>, nack: (envelope: MessageEnvelope) => Promise<void>): void;

  stop(): void;

  getStats(): SchedulerStats;
}

export interface SchedulerStats {
  active: number;
  pending: number;
  paused: number;
}

export interface StrategyConfig {
  poolSize: number;
  wheelResolutionMs: number;
  wheelSlots: number;
}
