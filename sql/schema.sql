-- Observability demo schema for ClickHouse (EDB build). Run:  clickhouse-client --host localhost --port 9094 --multiquery < sql/schema.sql
-- Everything lives in database obs. Kafka topics: logs, metrics (JSONEachRow).

CREATE DATABASE IF NOT EXISTS obs;

-- ---------------------------------------------------------------- raw tables
CREATE TABLE IF NOT EXISTS obs.logs
(
    ts          DateTime64(3)                     CODEC(Delta, ZSTD(1)),
    service     LowCardinality(String),
    host        LowCardinality(String),
    severity    LowCardinality(String),
    status      UInt16                            CODEC(T64, ZSTD(1)),
    duration_ms UInt32                            CODEC(T64, ZSTD(1)),
    trace_id    UUID,
    message     String                            CODEC(ZSTD(3)),
    attrs       Map(LowCardinality(String), String),
    ingested_at DateTime DEFAULT now()            CODEC(Delta, ZSTD(1)),
    INDEX idx_ts ts TYPE minmax GRANULARITY 1,
    INDEX idx_msg message TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1,
    INDEX idx_trace trace_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (service, severity, toStartOfMinute(ts), host)
TTL toDateTime(ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS obs.metrics
(
    ts      DateTime                               CODEC(DoubleDelta, ZSTD(1)),
    host    LowCardinality(String),
    service LowCardinality(String),
    metric  LowCardinality(String),
    value   Float64                                CODEC(Gorilla, ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (metric, service, host, ts)
TTL ts + INTERVAL 90 DAY;

-- ---------------------------------------------------------------- rollups (kept next to the raw rows)
CREATE TABLE IF NOT EXISTS obs.svc_1m
(
    minute  DateTime,
    service LowCardinality(String),
    cnt     SimpleAggregateFunction(sum, UInt64),
    errs    SimpleAggregateFunction(sum, UInt64),
    lat     AggregateFunction(quantilesTDigest(0.5, 0.99), UInt32)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (service, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS obs.svc_1m_mv TO obs.svc_1m AS
SELECT toStartOfMinute(ts) AS minute, service,
       count() AS cnt,
       countIf(severity IN ('ERROR', 'FATAL')) AS errs,
       quantilesTDigestIfState(0.5, 0.99)(duration_ms, duration_ms > 0) AS lat
FROM obs.logs GROUP BY minute, service;

CREATE TABLE IF NOT EXISTS obs.metrics_1m
(
    minute  DateTime,
    service LowCardinality(String),
    host    LowCardinality(String),
    metric  LowCardinality(String),
    avg_v   AggregateFunction(avg, Float64),
    max_v   SimpleAggregateFunction(max, Float64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(minute)
ORDER BY (metric, service, host, minute);

CREATE MATERIALIZED VIEW IF NOT EXISTS obs.metrics_1m_mv TO obs.metrics_1m AS
SELECT toStartOfMinute(ts) AS minute, service, host, metric,
       avgState(value) AS avg_v, max(value) AS max_v
FROM obs.metrics GROUP BY minute, service, host, metric;

-- ---------------------------------------------------------------- Kafka ingestion
-- Timestamps arrive as 'YYYY-MM-DD hh:mm:ss.mmm' strings in UTC.
CREATE TABLE IF NOT EXISTS obs.logs_kafka
(
    ts String, service String, host String, severity String, status UInt16, duration_ms UInt32,
    trace_id String, message String, attrs Map(String, String)
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'logs', kafka_group_name = 'ch-logs',
         kafka_format = 'JSONEachRow', kafka_num_consumers = 8, kafka_thread_per_consumer = 1,
         kafka_max_block_size = 524288, kafka_poll_max_batch_size = 65536, kafka_flush_interval_ms = 1000,
         kafka_skip_broken_messages = 10000;

CREATE MATERIALIZED VIEW IF NOT EXISTS obs.logs_kafka_mv TO obs.logs AS
SELECT parseDateTime64BestEffort(ts, 3, 'UTC') AS ts, service, host, severity, status, duration_ms,
       toUUIDOrZero(trace_id) AS trace_id, message, attrs
FROM obs.logs_kafka;

CREATE TABLE IF NOT EXISTS obs.metrics_kafka
(
    ts String, host String, service String, metric String, value Float64
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'metrics', kafka_group_name = 'ch-metrics',
         kafka_format = 'JSONEachRow', kafka_num_consumers = 2, kafka_thread_per_consumer = 1,
         kafka_max_block_size = 65536, kafka_flush_interval_ms = 1000, kafka_skip_broken_messages = 10000;

CREATE MATERIALIZED VIEW IF NOT EXISTS obs.metrics_kafka_mv TO obs.metrics AS
SELECT parseDateTimeBestEffort(ts, 'UTC') AS ts, host, service, metric, value FROM obs.metrics_kafka;
