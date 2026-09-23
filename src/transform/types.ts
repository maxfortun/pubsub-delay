import { Message } from '../broker/types.js';

// pre: after ingest, before the message is written to its bucket.
// post: after the delay, before the message is produced to its destination.
export type TransformStage = 'pre' | 'post';

export interface TransformContext {
  stage: TransformStage;
  // Topic the message was read from (ingest topic for pre, bucket topic for post)
  topic: string;
  // Delivery topic; only known for post, since pre may still rewrite it
  destination?: string;
}

// forward: continue with this (possibly rewritten) message.
// reject: drop the message for good, e.g. failed auth. It is acked and never delivered.
// Throwing means a transient failure: the message is not acked and is retried.
export type TransformResult =
  | { action: 'forward'; message: Message }
  | { action: 'reject'; reason: string };

export interface Transform {
  readonly name: string;
  apply(message: Message, context: TransformContext): Promise<TransformResult>;
}

export type TransformPlugin = 'none' | 'http';

export interface TransformConfig {
  plugin: TransformPlugin;
  // Wait this long before a failed message is retried, so a down plugin is not hammered
  retryBackoffMs: number;
  http: {
    preUrl?: string;
    postUrl?: string;
    timeoutMs: number;
  };
}
