# pubsub-delay

## Configuration

All settings are environment variables.

### Broker

| Variable | Default | Description |
|---|---|---|
| `BROKER_TYPE` | `kafka` | `kafka` or `activemq` |
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated bootstrap servers |
| `KAFKA_CLIENT_ID` | `pubsub-delay` | Kafka client id |
| `ACTIVEMQ_HOST` | `localhost` | STOMP host |
| `ACTIVEMQ_PORT` | `61613` | STOMP port |
| `ACTIVEMQ_LOGIN` / `ACTIVEMQ_PASSCODE` | | STOMP credentials |

### Topics and headers

| Variable | Default | Description |
|---|---|---|
| `INGEST_TOPIC` | `delay-ingest` | Topic producers publish delayed messages to |
| `BUCKET_SEPARATOR` | `-` | Separator between ingest topic and duration in bucket names |
| `HEADER_PREFIX` | `DELAY_` | Prefix for all service headers |
| `DESTINATION_HEADER` | `${HEADER_PREFIX}DESTINATION` | Header naming the delivery topic |
| `DELAY_DURATION_HEADER` | `${HEADER_PREFIX}DURATION` | Header carrying the ISO 8601 delay |
| `ENQUEUED_AT_HEADER` | `${HEADER_PREFIX}ENQUEUED_AT` | Header stamped with the enqueue time (ms) |
| `CONSUMER_GROUP_PREFIX` | `pubsub-delay` | Prefix for router, scheduler and advisory groups |
| `INSTANCE_ID` | `$HOSTNAME` | Static group membership id |

### Buckets

| Variable | Default | Description |
|---|---|---|
| `PRECREATE_BUCKETS` | | Comma-separated durations to create at startup, e.g. `PT1S,PT5M` |
| `BUCKET_IDLE_TIMEOUT_MS` | `3600000` | Idle time after which an empty bucket topic is deleted |
| `CLEANUP_INTERVAL_MS` | `60000` | How often idle buckets are checked |
| `ADVISORY_SYNC_INTERVAL_MS` | `10000` | How often bucket topics are rediscovered from the broker |
| `TOPIC_CREATE_RETRIES` | `5` | Attempts to create required topics at startup |
| `TOPIC_CREATE_RETRY_BASE_MS` | `1000` | Linear backoff step between topic create attempts |

### Scheduler

| Variable | Default | Description |
|---|---|---|
| `SCHEDULER_STRATEGY` | `bounded-pool` | `bounded-pool` or `time-wheel` |
| `SCHEDULER_FETCH_MAX_WAIT_MS` | `100` | Max broker long-poll; bounds how late a resumed bucket is fetched |
| `CONSUMER_RESTART_GRACE_MS` | `5000` | Window for batching new buckets into one consumer restart |
| `TIMEOUT_POOL_SIZE` | `100` | BoundedPool: max messages held with live timers |
| `BUCKET_RESUME_LEAD_MS` | `0` | BoundedPool: resume a paused bucket this early to absorb fetch latency |
| `WHEEL_RESOLUTION_MS` | `100` | TimeWheel: tick length |
| `WHEEL_SLOTS` | `600` | TimeWheel: slots per revolution (span = resolution × slots) |

### Service

| Variable | Default | Description |
|---|---|---|
| `HEALTH_PORT` | `8080` | Serves `/health`, `/ready` and Prometheus `/metrics` |
