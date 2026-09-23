import { Broker } from './broker/types.js';
import { DelayServiceConfig } from './config.js';
import { Worker } from './worker.js';
import { BucketAdvisory } from './bucket-advisory.js';

export class WorkerPool {
  private broker: Broker;
  private config: DelayServiceConfig;
  private advisory: BucketAdvisory;
  private workers: Worker[] = [];

  constructor(broker: Broker, config: DelayServiceConfig, advisory: BucketAdvisory) {
    this.broker = broker;
    this.config = config;
    this.advisory = advisory;
  }

  async start(): Promise<void> {
    for (let i = 0; i < this.config.workerPoolSize; i++) {
      const worker = new Worker(`worker-${i}`, this.broker, this.config, this.advisory);
      await worker.start();
      this.workers.push(worker);
    }

    console.log(`Worker pool started with ${this.workers.length} workers`);
  }

  async stop(): Promise<void> {
    await Promise.all(this.workers.map((w) => w.stop()));
  }

  getStats(): { total: number; buckets: number } {
    return {
      total: this.workers.length,
      buckets: this.advisory.getBuckets().length,
    };
  }
}
