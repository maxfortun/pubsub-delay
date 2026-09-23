import client from 'prom-client';

export const registry = new client.Registry();
client.collectDefaultMetrics({ register: registry, prefix: 'pubsub_delay_' });

export const deliveryLateness = new client.Histogram({
  name: 'pubsub_delay_delivery_lateness_ms',
  help: 'Time between scheduled deliverAt and actual produce to destination',
  labelNames: ['strategy'],
  buckets: [1, 2, 5, 10, 20, 50, 100, 250, 500, 1000, 2500, 5000],
  registers: [registry],
});

export const messagesTotal = new client.Counter({
  name: 'pubsub_delay_messages_total',
  help: 'Scheduler message events',
  labelNames: ['strategy', 'event'],
  registers: [registry],
});

type StatsSource = () => { strategy: string; active: number; pending: number; paused: number };
let statsSource: StatsSource | null = null;

export function setSchedulerStatsSource(source: StatsSource): void {
  statsSource = source;
}

new client.Gauge({
  name: 'pubsub_delay_scheduler_messages',
  help: 'Messages currently held by the scheduler strategy',
  labelNames: ['strategy', 'state'],
  registers: [registry],
  collect() {
    if (!statsSource) return;
    const { strategy, active, pending, paused } = statsSource();
    this.set({ strategy, state: 'active' }, active);
    this.set({ strategy, state: 'pending' }, pending);
    this.set({ strategy, state: 'paused' }, paused);
  },
});
