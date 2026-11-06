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

-- governance_limits.sql
-- Additive guardrails for the Govern tab. Safe to re-run (CREATE OR REPLACE).
-- Assumes sre_user, payments_user and the payments row policy already exist (governance.sql).
-- Run as the admin user:  clickhouse-client --multiquery < governance_limits.sql
--
-- What each statement demonstrates
--   settings profile -> "Cap what one heavy query can consume"   (TOO_MANY_ROWS 158, TIMEOUT_EXCEEDED 159)
--   quota            -> "Cap what one team can consume per minute" (QUOTA_EXCEEDED 201)
--   column grant     -> "Grant each team the tables (and columns) it needs" (ACCESS_DENIED 497)


-- ---------------------------------------------------------------------------
-- 0. SIZE THE ROW LIMIT FIRST (needs data loaded; takes about a second)
-- ---------------------------------------------------------------------------
-- A: rows the Govern panel reads for payments_user (last hour). The limit must stay ABOVE this.
--    SELECT count() FROM obs.logs WHERE service = 'payments' AND ts >= now() - INTERVAL 60 MINUTE;
-- B: rows a full scan reads for payments_user (all history). The limit must stay BELOW this.
--    SELECT count() FROM obs.logs WHERE service = 'payments';
-- Pick a number between A and B (about 2 x A works) and put it in max_rows_to_read below.
-- It changes with the simulator rate, so re-check if you rehearse at a different rate.


-- ---------------------------------------------------------------------------
-- 1. Settings profile: one heavy query cannot take the box
-- ---------------------------------------------------------------------------
CREATE SETTINGS PROFILE OR REPLACE p_payments
    SETTINGS max_rows_to_read = 600000000,      -- TUNE (see step 0)
             read_overflow_mode = 'throw',
             max_execution_time = 20,
             timeout_overflow_mode = 'throw',
             max_memory_usage = 4000000000
    TO payments_user;


-- ---------------------------------------------------------------------------
-- 2. Quota: per-minute budget for the payments team
-- ---------------------------------------------------------------------------
-- The Govern tab "burst" button fires 30 cheap queries, so a limit of 20 trips on the 21st.
-- The window is 1 minute, so the user recovers by itself before the next beat.
CREATE QUOTA OR REPLACE q_payments
    KEYED BY user_name
    FOR INTERVAL 1 minute MAX queries = 20, read_rows = 3000000000
    TO payments_user;


-- ---------------------------------------------------------------------------
-- 3. Column-level grant: payments sees its rows, and only the columns it needs
-- ---------------------------------------------------------------------------
-- Check how payments_user is granted today:   SHOW GRANTS FOR payments_user;
--
-- Case A: grant is directly on obs.logs (or you are happy to re-grant it)
REVOKE SELECT ON obs.logs FROM payments_user;
GRANT SELECT(ts, service, host, severity, duration_ms, message) ON obs.logs TO payments_user;
--
-- Case B: grant is on obs.* or comes from a role. A table-level REVOKE then needs partial revokes
--   (access_control_improvements.enable_partial_revokes = 1), or re-create the grants per table.
--   Adapt this block; do not run it blind.
--
-- trace_id and attrs are intentionally left out: SELECT attrs[...] now fails with ACCESS_DENIED.
-- The row policy filters on service, and the console's payments panel only reads ts, service, severity.


-- ---------------------------------------------------------------------------
-- Check what is in force
-- ---------------------------------------------------------------------------
-- SHOW CREATE SETTINGS PROFILE p_payments;
-- SHOW CREATE QUOTA q_payments;
-- SHOW GRANTS FOR payments_user;
-- SELECT * FROM system.quota_usage WHERE quota_name = 'q_payments';


-- ---------------------------------------------------------------------------
-- Teardown (back to the original demo)
-- ---------------------------------------------------------------------------
-- DROP SETTINGS PROFILE IF EXISTS p_payments;
-- DROP QUOTA IF EXISTS q_payments;
-- REVOKE SELECT(ts, service, host, severity, duration_ms, message) ON obs.logs FROM payments_user;
-- GRANT SELECT ON obs.logs TO payments_user;
