# Delaying a million messages without scheduling a single one

"Send this again in 30 seconds." "Remind the customer in an hour." "Retry with backoff: 1s, 5s, 30s." Almost every event-driven system needs delayed delivery. Most brokers don't offer it, and Kafka has nothing like it at all.

The usual answer is to schedule each message as its own job: a Quartz trigger, a row in a `scheduled_jobs` table, or an in-memory timer. It works, but look at what one delayed message costs:

* **It is stored twice.** The message already sits durably in the broker, and now it is copied into the scheduler's database too.
* **It costs several writes.** Insert the job, maintain its indexes, mark it acquired, mark it fired, delete it. That is four or five database writes for one message.
* **It has to be found again.** Scheduler threads keep asking the job store for the next triggers due. In a cluster, Quartz's JDBC job store serialises that behind a row lock (`QRTZ_LOCKS`), so adding nodes adds contention, not throughput.
* **Or it lives in RAM.** In-memory timers skip the database, but then the whole backlog has to fit in memory, and a restart loses or replays all of it.

All of this exists to answer one question: which message is due next? That means sorting every pending message by due time.

## The observation

**If every message in a queue has the same delay, the queue is already sorted.**

Messages are appended in arrival order. If they all wait 30 seconds, they also become due in arrival order. The head of the queue is always the next one due. There is nothing to sort, index or poll.

[pubsub-delay](https://github.com/maxfortun/pubsub-delay) builds on that. Producers publish to one ingest topic with two headers:

```
DELAY_DURATION:    PT30S
DELAY_DESTINATION: orders
```

The service forwards each message to a **bucket topic** for its exact duration (`delay-ingest-PT30S`, `delay-ingest-PT1H`, ...) and delivers each bucket's head when it is due. The broker is the only storage: no database, no Redis, no disk. Durability, replication and retention come from the broker you already run.

## What that buys

* **A delayed message costs one extra produce and one consume.** These are the cheapest things a broker does: sequential appends and reads.
* **Memory doesn't grow with the backlog.** The default strategy, BoundedPool, keeps at most N messages on live timers. Every other bucket is paused and costs nothing: no fetches, no memory, just one wake-up timer per bucket. A million messages waiting an hour look the same to it as ten.
* **Nothing polls.** A paused bucket wakes up on one timer for its head message and not before.
* **Crashes are cheap.** Only messages on live timers are uncommitted, so a crash or rebalance replays at most N messages, not the whole backlog.
* **It scales out.** Pods split bucket partitions (Kafka) or share bucket queues (ActiveMQ). There is no shared state and no cluster lock.

In a 60-minute stress test on Kafka it delivered 360,000 messages with zero loss and zero duplicates. Median lateness was 5 ms, p99 was 13 ms, and the process peaked at 73 MB RSS.

It runs on Kafka and ActiveMQ. ActiveMQ message groups (`JMSXGroupID`) keep their consumer affinity and ordering end to end. Optional pre and post transform plugins, such as an HTTP hook, can encrypt, compress, claim-check or authorise messages while they wait.

## When you need a real scheduler

Bucketing works because the delays come from a small set of durations. That covers retries with backoff, reminders, timeouts and debouncing, which is most delayed traffic. It can't do "deliver at 15:00 on the 22nd", cron schedules, or cancelling and replacing a pending job by key.

That is what my other project, [pubsub-scheduler](https://github.com/maxfortun/pubsub-scheduler), is for. It is a broker-neutral scheduler for Kafka and ActiveMQ that supports absolute times, ISO 8601 waits and cron, keyed jobs with QUEUE, REPLACE and SKIP policies, and a REST API. It keeps jobs in the database you already have: PostgreSQL, MySQL, CockroachDB or H2.

Two tools for two shapes of problem:

* **"Wait this long, then send it."** Use pubsub-delay. There is no database, and the cost per message is about as low as it gets.
* **"Send it at this time, on this schedule, unless I change my mind."** Use pubsub-scheduler.

Both are on GitHub, free for non-commercial use. Feedback and war stories are welcome.

---

## Short post version

Most systems delay messages by scheduling each one as a job: a Quartz trigger, a DB row, or an in-memory timer. That means every message is stored twice, costs several writes, and has to be found again by a poller behind a cluster lock.

There's a shortcut: if every message in a queue has the same delay, the queue is already sorted by due time. Its head is always the next one due.

pubsub-delay puts each message in a bucket topic for its duration (PT30S, PT1H, ...) and delivers each bucket's head when it's due. The broker is the only storage. Memory stays bounded however deep the backlog gets, and nothing polls.

60 min on Kafka: 360,000 messages, 0 lost, 0 duplicated, p99 lateness 13 ms, 73 MB RSS. It runs on Kafka and ActiveMQ, including JMSXGroupID ordering.

Need absolute times, cron, or cancel/replace by key? That's what my pubsub-scheduler is for.

https://github.com/maxfortun/pubsub-delay
https://github.com/maxfortun/pubsub-scheduler
