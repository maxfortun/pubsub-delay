import { Kafka, EachMessagePayload } from 'kafkajs';

const KAFKA_BROKERS = (process.env.KAFKA_BROKERS || 'localhost:9092').split(',');
const DURATION_MINUTES = parseFloat(process.env.STRESS_DURATION_MINUTES || '60');
const MESSAGES_PER_SECOND = parseInt(process.env.MESSAGES_PER_SECOND || '100', 10);
const REPORT_INTERVAL_SECONDS = parseInt(process.env.REPORT_INTERVAL_SECONDS || '30', 10);
const BATCHES_PER_SECOND = 10;
const DELAY_OPTIONS_SEC = [1, 2, 3, 4, 5];

interface Target {
  name: string;
  ingest: string;
  output: string;
  statsUrl: string;
}

const TARGETS: Target[] = [
  {
    name: 'BoundedPool',
    ingest: process.env.BP_INGEST_TOPIC || 'bp-ingest',
    output: process.env.BP_OUTPUT_TOPIC || 'bp-output',
    statsUrl: process.env.BP_STATS_URL || 'http://bounded-pool:8080/stats',
  },
  {
    name: 'TimeWheel',
    ingest: process.env.TW_INGEST_TOPIC || 'tw-ingest',
    output: process.env.TW_OUTPUT_TOPIC || 'tw-output',
    statsUrl: process.env.TW_STATS_URL || 'http://time-wheel:8080/stats',
  },
];

interface Stats {
  sent: number;
  received: number;
  duplicates: number;
  totalError: number;
  maxError: number;
  errors: number[];
  service: Record<string, unknown> | null;
  peakRssMb: number;
}

// Bounded reservoir so percentiles cover the whole run, not just the tail
const RESERVOIR_SIZE = 10000;

function createStats(): Stats {
  return { sent: 0, received: 0, duplicates: 0, totalError: 0, maxError: 0, errors: [], service: null, peakRssMb: 0 };
}

function recordError(s: Stats, error: number): void {
  s.received++;
  s.totalError += error;
  s.maxError = Math.max(s.maxError, error);
  if (s.errors.length < RESERVOIR_SIZE) {
    s.errors.push(error);
  } else {
    const idx = Math.floor(Math.random() * s.received);
    if (idx < RESERVOIR_SIZE) s.errors[idx] = error;
  }
}

function percentile(arr: number[], p: number): number {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  return sorted[Math.max(0, Math.ceil((p / 100) * sorted.length) - 1)];
}

function summarize(s: Stats) {
  return {
    sent: s.sent,
    received: s.received,
    inFlight: s.sent - s.received,
    duplicates: s.duplicates,
    avg: s.received > 0 ? +(s.totalError / s.received).toFixed(1) : 0,
    p50: percentile(s.errors, 50),
    p95: percentile(s.errors, 95),
    p99: percentile(s.errors, 99),
    max: s.maxError,
    peakRssMb: s.peakRssMb,
  };
}

async function pollServiceStats(target: Target, s: Stats): Promise<void> {
  try {
    const res = await fetch(target.statsUrl);
    s.service = (await res.json()) as Record<string, unknown>;
    s.peakRssMb = Math.max(s.peakRssMb, Number(s.service.rssMb) || 0);
  } catch {
    s.service = null;
  }
}

async function main() {
  console.log(`\n=== Stress Test Configuration ===`);
  console.log(`Duration: ${DURATION_MINUTES} minutes`);
  console.log(`Rate: ${MESSAGES_PER_SECOND} msg/sec per strategy`);
  console.log(`Delays: ${DELAY_OPTIONS_SEC.map((d) => `PT${d}S`).join(', ')}`);
  console.log(`================================\n`);

  const kafka = new Kafka({ clientId: 'stress-test', brokers: KAFKA_BROKERS });
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      topics: TARGETS.map((t) => ({ topic: t.output, numPartitions: 1 })),
    });
  } catch { /* already exists */ }

  const producer = kafka.producer();
  await producer.connect();

  const stats: Record<string, Stats> = Object.fromEntries(TARGETS.map((t) => [t.name, createStats()]));
  const pending = new Map<string, { sentAt: number; delayMs: number }>();
  const delivered = new Set<string>();

  const consumers = [];
  for (const target of TARGETS) {
    const consumer = kafka.consumer({ groupId: `stress-${target.name}-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic: target.output, fromBeginning: false });
    await consumer.run({
      eachMessage: async ({ message }: EachMessagePayload) => {
        const testId = message.headers?.['TEST_ID']?.toString();
        if (!testId) return;
        const s = stats[target.name];
        const info = pending.get(testId);
        if (!info) {
          if (delivered.has(testId)) s.duplicates++;
          return;
        }
        pending.delete(testId);
        delivered.add(testId);
        recordError(s, Math.abs(Date.now() - info.sentAt - info.delayMs));
      },
    });
    consumers.push(consumer);
  }

  const startTime = Date.now();
  const endTime = startTime + DURATION_MINUTES * 60 * 1000;

  const report = async (label: string) => {
    await Promise.all(TARGETS.map((t) => pollServiceStats(t, stats[t.name])));
    const elapsed = Math.floor((Date.now() - startTime) / 1000);
    console.log(`\n--- ${label} @ ${elapsed}s ---`);
    for (const t of TARGETS) {
      const s = stats[t.name];
      console.log(`${t.name}: ${JSON.stringify(summarize(s))}`);
      console.log(`  service: ${JSON.stringify(s.service)}`);
    }
  };

  const reportTimer = setInterval(() => report('Report'), REPORT_INTERVAL_SECONDS * 1000);

  console.log('Starting stress test...');
  const perBatch = Math.max(1, Math.round(MESSAGES_PER_SECOND / BATCHES_PER_SECOND));
  const batchIntervalMs = 1000 / BATCHES_PER_SECOND;
  let msgCounter = 0;
  let nextBatchAt = Date.now();

  while (Date.now() < endTime) {
    const batches: Record<string, { key: string; value: Buffer; headers: Record<string, string> }[]> =
      Object.fromEntries(TARGETS.map((t) => [t.name, []]));

    for (let i = 0; i < perBatch; i++) {
      const n = ++msgCounter;
      const delaySec = DELAY_OPTIONS_SEC[n % DELAY_OPTIONS_SEC.length];
      const sentAt = Date.now();
      for (const t of TARGETS) {
        const id = `${t.name}-${n}`;
        pending.set(id, { sentAt, delayMs: delaySec * 1000 });
        batches[t.name].push({
          key: id,
          value: Buffer.from(`stress-${n}`),
          headers: {
            DELAY_DURATION: `PT${delaySec}S`,
            DELAY_DESTINATION: t.output,
            DELAY_ENQUEUED_AT: sentAt.toString(),
            TEST_ID: id,
          },
        });
      }
    }

    await producer.sendBatch({
      topicMessages: TARGETS.map((t) => ({ topic: t.ingest, messages: batches[t.name] })),
    });
    for (const t of TARGETS) stats[t.name].sent += perBatch;

    nextBatchAt += batchIntervalMs;
    const sleepMs = nextBatchAt - Date.now();
    if (sleepMs > 0) await new Promise((r) => setTimeout(r, sleepMs));
  }

  console.log('\nSending complete, draining...');
  const drainDeadline = Date.now() + 30000;
  while (pending.size > 0 && Date.now() < drainDeadline) {
    await new Promise((r) => setTimeout(r, 1000));
  }
  // Grace window to catch late duplicates
  await new Promise((r) => setTimeout(r, 5000));
  clearInterval(reportTimer);

  await report('FINAL RESULTS');

  for (const c of consumers) await c.disconnect();
  await producer.disconnect();
  await admin.disconnect();

  let failed = false;
  for (const t of TARGETS) {
    const s = stats[t.name];
    const lost = s.sent - s.received;
    if (lost > 0 || s.duplicates > 0) {
      console.error(`${t.name}: lost=${lost} duplicates=${s.duplicates}`);
      failed = true;
    }
  }
  process.exit(failed ? 1 : 0);
}

main().catch((err) => {
  console.error('Stress test failed:', err);
  process.exit(1);
});
