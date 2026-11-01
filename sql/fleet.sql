-- Catalog of hosts and metrics used by the metrics backfill (and by the dashboard host counts).
CREATE TABLE IF NOT EXISTS obs.fleet
(
    host String, service LowCardinality(String), metric LowCardinality(String),
    base Float64, inc_mult Float64, inc_scope UInt8
) ENGINE = MergeTree ORDER BY (service, host, metric);

TRUNCATE TABLE obs.fleet;

INSERT INTO obs.fleet
SELECT concat(s.1, '-', leftPad(toString(i), 2, '0')) AS host, s.1 AS service, m.1 AS metric, m.2 AS base, m.3 AS inc_mult,
       toUInt8(s.1 = 'payments' AND cityHash64(host) % 10 < 6) AS inc_scope
FROM (SELECT arrayJoin([('checkout', 40), ('payments', 30), ('auth', 24), ('search', 48), ('inventory', 24), ('notifications', 16)]) AS s)
ARRAY JOIN range(1, s.2 + 1) AS i
CROSS JOIN (SELECT arrayJoin([('cpu_pct', 42., 2.0), ('mem_pct', 56., 1.0), ('req_per_s', 100., 1.0), ('latency_p99_ms', 150., 6.0)]) AS m) AS mt
UNION ALL
SELECT concat('pg-', c.1, '-0', toString(i)) AS host, 'postgres' AS service, m.1 AS metric, m.2 AS base, m.3 AS inc_mult,
       toUInt8(c.1 = 'payments') AS inc_scope
FROM (SELECT arrayJoin([('orders', 6), ('payments', 6), ('users', 4), ('inventory', 4), ('analytics', 3)]) AS c)
ARRAY JOIN range(1, c.2 + 1) AS i
CROSS JOIN (SELECT arrayJoin([('pg_connections', 100., 3.5), ('pg_tps', 1000., 0.25), ('pg_lock_waits', 1., 30.), ('pg_repl_lag_ms', 3., 40.), ('pg_cache_hit_pct', 99.5, 0.97)]) AS m) AS mt;
