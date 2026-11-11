#!/usr/bin/env bash
# Finishes the agent_user setup: row policies and a check. Safe to run again.
#   bash sql/agent_user.sh            (ClickHouse native port 9094; PORT=9000 bash sql/agent_user.sh for another)
PORT="${PORT:-9094}"
ch() { clickhouse-client --port "$PORT" "$@" 2> >(grep -v 'Unknown settings' >&2); }
# A table that already has row policies hides its rows from any user no policy names, so the agent needs one there too.
# Tables without policies need nothing: adding one would hide the rows from everybody else.
for t in logs_hot svc_1m; do
  n=$(ch -q "SELECT count() FROM system.row_policies WHERE database = 'obs' AND table = '$t'")
  if [ "${n:-0}" -gt 0 ]; then
    ch -q "CREATE ROW POLICY IF NOT EXISTS agent_all ON obs.$t USING 1 TO agent_user"
    echo "row policy agent_all on obs.$t (the table has $n other polic$( [ "$n" = 1 ] && echo y || echo ies ))"
  else
    echo "obs.$t has no row policies: nothing to add"
  fi
done
echo "--- as agent_user (both counts must be above 0)"
ch --user agent_user --password "${AGENT_PW:-agent_demo_pw}" -q "SELECT 'obs.svc_1m' AS t, count() FROM obs.svc_1m WHERE minute >= now() - INTERVAL 10 MINUTE
  UNION ALL SELECT 'obs.logs_hot', count() FROM (SELECT 1 FROM obs.logs_hot WHERE ts >= now() - INTERVAL 1 MINUTE LIMIT 1000)"
echo "--- a write must fail"
ch --user agent_user --password "${AGENT_PW:-agent_demo_pw}" -q "INSERT INTO obs.svc_1m (minute, service) VALUES (now(), 'x')" 2>&1 | head -c 200; echo
