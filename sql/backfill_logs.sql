-- Synthetic history for obs.logs. One call writes `n` rows starting at row number `off`; the driver
-- (install/backfill.sh or the dashboard) loops over slices of 100M rows.
--   params: total  = rows in the whole backfill      (UInt64)
--           off    = first row number of this slice   (UInt64)
--           n      = rows in this slice               (UInt64)
--           days   = days of history to spread rows over (UInt32)
-- Two historic incidents are built in (payments, bad release v2.42.0): yesterday 14:00-14:25 UTC and
-- three days ago 09:10-09:30 UTC. Message and severity mixes follow the live simulator.
INSERT INTO obs.logs (ts, service, host, severity, status, duration_ms, trace_id, message, attrs)
WITH
    number AS num,
    now64(3) - toIntervalMillisecond(toUInt64(floor(num / {total:UInt64} * {days:UInt32} * 86400000))) AS t,
    (cityHash64(num, 1) % 1000000) / 1e6 AS u_svc,
    (cityHash64(num, 2) % 1000000) / 1e6 AS u_sev,
    (cityHash64(num, 3) % 1000000) / 1e6 AS u_err,
    (cityHash64(num, 4) % 1000000) / 1e6 AS u_host,
    (cityHash64(num, 5) % 1000000) / 1e6 AS u_a,
    (cityHash64(num, 6) % 1000000) / 1e6 AS u_b,
    (cityHash64(num, 7) % 1000000) / 1e6 AS u_c,
    (cityHash64(num, 8) % 1000000) / 1e6 AS u_d,
    (u_a + u_b + u_c + u_d - 2) * 1.732 AS gauss,
    multiIf(u_svc < 0.2128, 'checkout', u_svc < 0.3723, 'payments', u_svc < 0.5213, 'auth', u_svc < 0.7340, 'search',
            u_svc < 0.8298, 'inventory', u_svc < 0.8723, 'notifications', 'postgres') AS svc,
    toStartOfDay(now()) - INTERVAL 1 DAY + INTERVAL 14 HOUR AS w1_from,
    toStartOfDay(now()) - INTERVAL 3 DAY + INTERVAL 9 HOUR + INTERVAL 10 MINUTE AS w2_from,
    ((toDateTime(t) >= w1_from AND toDateTime(t) < w1_from + INTERVAL 25 MINUTE)
      OR (toDateTime(t) >= w2_from AND toDateTime(t) < w2_from + INTERVAL 20 MINUTE)) AS in_inc,
    multiIf(svc = 'checkout', 40, svc = 'payments', 30, svc = 'auth', 24, svc = 'search', 48, svc = 'inventory', 24, svc = 'notifications', 16, 30) AS nhosts,
    arrayElement(['orders', 'payments', 'users', 'inventory', 'analytics'], 1 + toUInt32(u_a * 5) % 5) AS cluster_raw,
    multiIf(svc = 'postgres' AND in_inc AND u_b < 0.30, 'payments', cluster_raw) AS cluster,
    multiIf(svc = 'postgres', concat('pg-', cluster, '-0', toString(1 + toUInt32(u_host * 6) % 6)),
            concat(svc, '-', leftPad(toString(1 + toUInt32(u_host * nhosts) % nhosts), 2, '0'))) AS hostname,
    in_inc AND svc = 'payments' AND (toUInt32(u_host * 1000) % 10) < 6 AS bad_host,
    in_inc AND svc = 'postgres' AND cluster = 'payments' AS hot_pg,
    multiIf(svc = 'checkout', 80, svc = 'payments', 120, svc = 'auth', 30, svc = 'search', 60, svc = 'inventory', 45, svc = 'notifications', 25, 3) AS base_ms,
    toUInt32(base_ms * multiIf(bad_host, 7, hot_pg, 50, 1) * exp(0.55 * gauss)) AS dur,
    multiIf(svc = 'postgres', multiIf(hot_pg, 0.14, 0.004), bad_host, 0.38, 0.015) AS err_rate,
    multiIf(svc = 'postgres', multiIf(hot_pg, 0.41, 0.06), bad_host, 0.30, 0.08) AS warn_rate,
    multiIf(u_sev < err_rate * 0.04, 'FATAL', u_sev < err_rate, 'ERROR', u_sev < err_rate + warn_rate, 'WARN', 'INFO') AS sev,
    arrayElement(['/cart', '/search', '/login', '/payments/authorize', '/stock/check', '/email/send', '/token/refresh', '/payments/capture'],
                 1 + toUInt32(u_c * 8) % 8) AS path,
    multiIf(svc = 'postgres', 0,
            sev = 'FATAL', 500,
            sev = 'ERROR', arrayElement([504, 502, 500, 503], 1 + toUInt32(u_err * 4) % 4),
            200) AS st,
    arrayElement(['payments-gateway', 'inventory-api', 'auth-service', 'search-index', 'postgres-primary', 'redis-cache'], 1 + toUInt32(u_d * 6) % 6) AS dep,
    multiIf(
        svc = 'postgres' AND sev = 'FATAL', 'remaining connection slots are reserved for non-replication superuser connections',
        svc = 'postgres' AND sev = 'ERROR' AND hot_pg AND u_err < 0.6, concat('canceling statement due to lock timeout while updating tuple (0,', toString(toUInt32(u_b * 500)), ') in relation "payments"'),
        svc = 'postgres' AND sev = 'ERROR' AND hot_pg, concat('deadlock detected: process ', toString(1000 + toUInt32(u_c * 9000)), ' waits for ShareLock on transaction ', toString(toUInt32(u_d * 900000))),
        svc = 'postgres' AND sev = 'ERROR', concat('could not receive data from client: Connection reset by peer (process ', toString(1000 + toUInt32(u_c * 9000)), ')'),
        svc = 'postgres' AND sev = 'WARN' AND hot_pg, concat('process ', toString(1000 + toUInt32(u_c * 9000)), ' still waiting for ShareLock on transaction ', toString(toUInt32(u_d * 900000)), ' after 1000.', leftPad(toString(toUInt32(u_b * 1000)), 3, '0'), ' ms'),
        svc = 'postgres' AND sev = 'WARN', concat('checkpoints are occurring too frequently (', toString(5 + toUInt32(u_c * 25)), ' seconds apart)'),
        svc = 'postgres', concat('duration: ', toString(dur), '.', leftPad(toString(toUInt32(u_b * 1000)), 3, '0'), ' ms statement: ',
            arrayElement(['SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at DESC LIMIT 20', 'UPDATE payments SET status = ''captured'' WHERE id = $1',
                          'INSERT INTO audit_log (actor, action, at) VALUES ($1, $2, now())', 'SELECT count(*) FROM sessions WHERE expires_at > now()'], 1 + toUInt32(u_c * 4) % 4)),
        sev = 'FATAL', 'out of memory: worker killed by supervisor, restarting',
        sev = 'ERROR' AND bad_host AND u_err < 0.5, concat('payment authorization failed: timeout acquiring database lock after ', toString(dur + 2000 + toUInt32(u_b * 3000)), ' ms (txn ', toString(toUInt32(u_d * 900000)), ')'),
        sev = 'ERROR' AND u_err < 0.25, concat('upstream timeout calling ', dep, ' after ', toString(dur + 2000 + toUInt32(u_b * 3000)), ' ms'),
        sev = 'ERROR' AND u_err < 0.5, concat('upstream connection refused by ', dep, ' on attempt ', toString(1 + toUInt32(u_b * 3))),
        sev = 'ERROR' AND u_err < 0.75, concat('unhandled exception in handler ', path, ': NullPointerException at line ', toString(10 + toUInt32(u_b * 400))),
        sev = 'ERROR', concat('request ', path, ' failed with status 503: service temporarily unavailable'),
        sev = 'WARN' AND u_b < 0.5, concat('slow request ', path, ' took ', toString(dur + 400), ' ms (threshold 500 ms)'),
        sev = 'WARN', concat('retrying call to ', dep, ', attempt ', toString(1 + toUInt32(u_c * 3)), ' of 3'),
        concat(arrayElement(['GET', 'POST', 'PUT'], 1 + toUInt32(u_a * 3) % 3), ' ', path, ' 200 ', toString(dur), ' ms')) AS msg,
    arrayElement(['v2.41.0', 'v2.41.1', 'v2.40.7'], 1 + toUInt32(u_c * 3) % 3) AS ver_ok
SELECT
    t AS ts, svc AS service, hostname AS host, sev AS severity, toUInt16(st) AS status,
    if(svc = 'postgres' AND sev = 'ERROR' AND hot_pg, 5000, dur) AS duration_ms,
    generateUUIDv4() AS trace_id, msg AS message,
    if(svc = 'postgres', map('region', arrayElement(['eu-west-1', 'us-east-1', 'ap-south-1'], 1 + toUInt32(u_host * 3) % 3), 'cluster', cluster),
       map('region', arrayElement(['eu-west-1', 'us-east-1', 'ap-south-1'], 1 + toUInt32(u_host * 3) % 3), 'version', if(bad_host, 'v2.42.0', ver_ok))) AS attrs
FROM numbers_mt({off:UInt64}, {n:UInt64})
SETTINGS max_insert_threads = 8, max_threads = 16, max_insert_block_size = 4194304;
