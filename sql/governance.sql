-- Governance demo: roles, row policy, quota, audit. Run as the default (admin) user.
CREATE ROLE IF NOT EXISTS sre;
GRANT SELECT ON obs.* TO sre;
CREATE ROLE IF NOT EXISTS payments_team;
GRANT SELECT ON obs.logs TO payments_team;
GRANT SELECT ON obs.svc_1m TO payments_team;

CREATE USER IF NOT EXISTS sre_user IDENTIFIED BY 'sre_demo_pw' DEFAULT ROLE sre;
CREATE USER IF NOT EXISTS payments_user IDENTIFIED BY 'payments_demo_pw' DEFAULT ROLE payments_team;
GRANT sre TO sre_user; GRANT payments_team TO payments_user;

-- Row policies: sre sees everything, payments_team only its own service.
CREATE ROW POLICY IF NOT EXISTS all_rows ON obs.logs, obs.svc_1m FOR SELECT USING 1 TO sre;
CREATE ROW POLICY IF NOT EXISTS payments_only_logs ON obs.logs FOR SELECT USING service = 'payments' TO payments_team;
CREATE ROW POLICY IF NOT EXISTS payments_only_1m ON obs.svc_1m FOR SELECT USING service = 'payments' TO payments_team;

-- Quota: payments_team gets bounded resources per hour.
CREATE QUOTA IF NOT EXISTS payments_quota FOR INTERVAL 1 hour MAX queries = 2000, read_rows = 20000000000 TO payments_team;
CREATE SETTINGS PROFILE IF NOT EXISTS payments_profile SETTINGS max_execution_time = 30, max_memory_usage = 8000000000 TO payments_team;

-- Audit: who ran what, how long it took and how much it read.
-- SELECT user, query_duration_ms, read_rows, formatReadableSize(read_bytes) FROM system.query_log
--   WHERE type = 'QueryFinish' AND user IN ('sre_user','payments_user') ORDER BY event_time DESC LIMIT 10;
