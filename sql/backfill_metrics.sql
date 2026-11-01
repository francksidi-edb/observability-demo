-- Synthetic metric history: one sample per host and metric every 30 seconds for `days` days.
--   params: days (UInt32). Needs obs.fleet (sql/fleet.sql). Same two payments incidents as the log backfill.
INSERT INTO obs.metrics (ts, host, service, metric, value)
WITH
    f.base * (1 + 0.12 * sin(toUInt32(t) / 3600.0) + 0.08 * (cityHash64(f.host, f.metric, toUInt32(t)) % 1000) / 1000.0)
         * if(inc AND f.inc_scope = 1, f.inc_mult, 1) AS raw_v,
    toStartOfDay(now()) - INTERVAL 1 DAY + INTERVAL 14 HOUR AS w1_from,
    toStartOfDay(now()) - INTERVAL 3 DAY + INTERVAL 9 HOUR + INTERVAL 10 MINUTE AS w2_from
SELECT t AS ts, f.host AS host, f.service AS service, f.metric AS metric,
       if(f.metric LIKE '%\\_pct', least(raw_v, 100.), raw_v) AS value
FROM
(
    SELECT toDateTime(now()) - toIntervalSecond(30 * number) AS t,
           ((t >= w1_from AND t < w1_from + INTERVAL 25 MINUTE) OR (t >= w2_from AND t < w2_from + INTERVAL 20 MINUTE)) AS inc
    FROM numbers_mt({days:UInt32} * 2880)
) AS ticks
CROSS JOIN obs.fleet AS f
SETTINGS max_insert_threads = 8;
