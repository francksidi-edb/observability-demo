-- Alerts / Agent: the read-only ClickHouse user the investigation runs as, and the alert table.
-- Run with:  clickhouse-client --port 9094 --multiquery < sql/agent.sql      then   bash sql/agent_user.sh

-- 1. Limits for the agent: SELECT only, bounded time and rows, small results.
CREATE SETTINGS PROFILE IF NOT EXISTS agent_profile SETTINGS
    readonly = 1,
    max_execution_time = 25,
    max_rows_to_read = 3000000000,
    max_result_rows = 500, result_overflow_mode = 'break',
    max_memory_usage = 8000000000;

-- 2. The user. Demo password: change it, and set agent.ch_password in dashboard/config.json to match.
CREATE USER IF NOT EXISTS agent_user IDENTIFIED WITH sha256_password BY 'agent_demo_pw' SETTINGS PROFILE 'agent_profile';

-- 3. Least privilege: the recent raw logs and the per-minute rollup, nothing else.
GRANT SELECT ON obs.logs_hot TO agent_user;
GRANT SELECT ON obs.svc_1m   TO agent_user;

-- 4. The alert table (the dashboard creates it too, if it is missing). ReplacingMergeTree keeps the latest version of each alert.
CREATE TABLE IF NOT EXISTS obs.agent_alerts
(
    id String, created DateTime, updated DateTime, service LowCardinality(String), status LowCardinality(String),
    recent_pct Float64, base_pct Float64, recent_events UInt64,
    diagnosis String, proposal String, trace String, result String
) ENGINE = ReplacingMergeTree(updated) ORDER BY id;

-- The detector (also what the dashboard runs every 15 s): current minute against the 30 minutes before it.
-- WITH toStartOfMinute(now()) AS cut
-- SELECT service, sumIf(errs, minute >= cut) / sumIf(cnt, minute >= cut) AS now_rate,
--                 sumIf(errs, minute <  cut) / sumIf(cnt, minute <  cut) AS baseline_rate
-- FROM obs.svc_1m WHERE minute >= cut - INTERVAL 30 MINUTE GROUP BY service;
