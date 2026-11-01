#!/usr/bin/env bash
# reset-demo.sh - reset the observability demo and set the simulator throughput.
#
# Usage:
#   sudo ./reset-demo.sh                 # restart services at the rate already configured
#   sudo ./reset-demo.sh 200k            # restart at 200,000 events/s
#   sudo ./reset-demo.sh 1m --wipe       # also empty all obs.* data tables first
#   sudo ./reset-demo.sh 100k --live     # change rate only, no restart (uses the console API)
#   sudo ./reset-demo.sh --status        # show state and measured rows/s, change nothing
#
# Rate accepts plain numbers (200000), or k / m suffixes (200k, 1m).

set -uo pipefail

SIM_UNIT=/etc/systemd/system/obs-sim.service
DASH_UNIT=/etc/systemd/system/obs-dash.service
API=http://127.0.0.1:8088
CH="${CH:-clickhouse-client --port 9094}"   # override with CH="clickhouse-client --port N"
DB=obs
MEASURE_SECS=10

RATE=""
WIPE=0
LIVE=0
STATUS_ONLY=0

usage() { sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

parse_rate() {
  local v="${1,,}"
  case "$v" in
    *k) echo $(( ${v%k} * 1000 )) ;;
    *m) echo $(( ${v%m} * 1000000 )) ;;
    *[!0-9]*|"") return 1 ;;
    *) echo "$v" ;;
  esac
}

for arg in "$@"; do
  case "$arg" in
    -h|--help) usage 0 ;;
    --wipe)    WIPE=1 ;;
    --live)    LIVE=1 ;;
    --status)  STATUS_ONLY=1 ;;
    -*)        echo "Unknown option: $arg" >&2; usage 1 ;;
    *)         RATE=$(parse_rate "$arg") || { echo "Bad rate: $arg" >&2; exit 1; } ;;
  esac
done

if [[ $EUID -ne 0 ]]; then echo "Run as root (sudo)." >&2; exit 1; fi

say() { printf '\n=== %s ===\n' "$*"; }

current_rate() { grep -oE '\-rate [0-9]+' "$SIM_UNIT" 2>/dev/null | awk '{print $2}'; }

measure() {
  # rows/s into obs.logs over MEASURE_SECS, taken from the console's counter endpoint
  local a b
  a=$(curl -s --max-time 5 "$API/api/counters") || { echo "console API not answering"; return 1; }
  sleep "$MEASURE_SECS"
  b=$(curl -s --max-time 5 "$API/api/counters") || { echo "console API not answering"; return 1; }
  python3 - "$a" "$b" <<'PY'
import json, sys
a, b = json.loads(sys.argv[1]), json.loads(sys.argv[2])
dt = (b["t"] - a["t"]) or 1
print(f"measured ingest: {(b['logs']-a['logs'])/dt:,.0f} log rows/s "
      f"(total {b['logs']:,} log rows, {b['metrics']:,} metric rows)")
PY
}

status() {
  say "Services"
  for s in clickhouse-server kafka obs-sim obs-dash; do
    printf '%-18s %s\n' "$s" "$(systemctl is-active "$s" 2>&1)"
  done
  say "Ports"
  ss -ltnp | grep -E ':(8088|8090|9092)\b' || echo "none of 8088/8090/9092 listening"
  say "Rate"
  echo "configured in unit file: $(current_rate) events/s"
  curl -s --max-time 5 "$API/api/sim/status" | python3 -c '
import json,sys
try:
    s=json.load(sys.stdin); print("simulator reports: running=%s eps=%s target=%s" % (s.get("running"), s.get("eps"), s.get("target_rate")))
except Exception: print("simulator status unavailable")'
  say "Disk"
  df -h /var/lib/clickhouse 2>/dev/null || df -h /
  say "Throughput"
  measure
}

if [[ $STATUS_ONLY -eq 1 ]]; then status; exit 0; fi

# --- warnings --------------------------------------------------------------
if [[ -n "$RATE" ]]; then
  if (( RATE > 316000 )); then
    echo "WARNING: $RATE is above the console slider maximum (~316k/s); the console slider will clamp it." >&2
  fi
  AVAIL_GB=$(df -BG --output=avail /var/lib/clickhouse 2>/dev/null | tail -1 | tr -dc '0-9')
  echo "Disk free for ClickHouse: ${AVAIL_GB:-?} GB. At $RATE rows/s that is ~$(( RATE * 3600 / 1000000 )) M rows per hour."
fi

# --- live mode: change rate through the API, no restart ---------------------
if [[ $LIVE -eq 1 ]]; then
  [[ -n "$RATE" ]] || { echo "--live needs a rate" >&2; exit 1; }
  if ! systemctl is-active --quiet obs-dash; then
    echo "The console (obs-dash) is not running, so --live cannot reach it." >&2
    echo "Run a full reset instead: sudo $0 $RATE" >&2
    exit 1
  fi
  say "Setting rate live to $RATE events/s"
  curl -s -X POST "$API/api/sim/start?rate=$RATE" >/dev/null || echo "start call failed"
  curl -s -X POST "$API/api/sim/rate?eps=$RATE" || echo "rate call failed"
  echo
  sed -i -E "s/-rate [0-9]+/-rate $RATE/" "$SIM_UNIT" && systemctl daemon-reload
  echo "(also saved to $SIM_UNIT so it survives a restart)"
  sleep 3
  measure
  exit 0
fi

# --- full reset -------------------------------------------------------------
# Dependencies first: start Kafka and ClickHouse if they were stopped, and wait
# until they accept connections. Nothing of the demo is touched until both are ready.
say "1/6 Dependencies (started if down)"
wait_for() {  # wait_for <name> <timeout-seconds> <command...>
  local name=$1 t=$2 i; shift 2
  for ((i = 0; i < t; i++)); do
    if "$@" >/dev/null 2>&1; then echo "$name ready after ${i}s"; return 0; fi
    sleep 1
  done
  return 1
}
for s in kafka clickhouse-server; do
  if ! systemctl is-active --quiet "$s"; then
    echo "$s is down, starting it"
    if ! systemctl start "$s"; then
      echo "Failed to start $s:" >&2
      journalctl -u "$s" -n 15 --no-pager >&2
      echo "Nothing else was stopped or changed." >&2
      exit 1
    fi
  fi
done
wait_for kafka 90 bash -c 'exec 3<>/dev/tcp/127.0.0.1/9092' || {
  echo "Kafka is not accepting connections on 9092 after 90 s." >&2
  journalctl -u kafka -n 15 --no-pager >&2
  echo "Nothing else was stopped or changed." >&2
  exit 1
}
wait_for clickhouse 90 $CH -q "SELECT 1" || {
  echo "Cannot reach ClickHouse with: $CH" >&2
  echo "Find its native port with: ss -ltnp | grep clickhouse" >&2
  echo "Then rerun with: CH=\"clickhouse-client --port <port>\" sudo -E $0 ..." >&2
  echo "Nothing else was stopped or changed." >&2
  exit 1
}

say "2/6 Stop demo services"
systemctl stop obs-dash obs-sim
systemctl reset-failed obs-dash obs-sim 2>/dev/null
pkill -x obs-sim 2>/dev/null
pkill -f 'app.py --port' 2>/dev/null
sleep 1
if ss -ltnp | grep -E ':(8088|8090)\b'; then
  echo "A foreign process still holds 8088/8090 (see above). Stop it, then rerun." >&2
  exit 1
fi
echo "ports 8088/8090 are free"

say "3/6 Data"
if [[ $WIPE -eq 1 ]]; then
  TABLES=$($CH -q "SELECT name FROM system.tables WHERE database='$DB' AND engine LIKE '%MergeTree%'") || {
    echo "Cannot query ClickHouse, aborting wipe." >&2; exit 1; }
  for t in $TABLES; do
    echo "TRUNCATE $DB.\`$t\`"
    $CH -q "TRUNCATE TABLE $DB.\`$t\`" || echo "  failed on $t"
  done
  echo "Note: events already buffered in Kafka will still be consumed after restart."
else
  echo "keeping existing data (use --wipe to empty obs.* tables)"
fi

say "4/6 Configure"
if [[ -n "$RATE" ]]; then
  sed -i -E "s/-rate [0-9]+/-rate $RATE/" "$SIM_UNIT"
fi
echo "simulator rate: $(current_rate) events/s"
# the dashboard must bind to all interfaces to be reachable without a tunnel
if ! grep -q -- '--host' "$DASH_UNIT"; then
  sed -i 's|app.py --port|app.py --host 0.0.0.0 --port|' "$DASH_UNIT"
  echo "added --host 0.0.0.0 to $DASH_UNIT"
fi
systemctl daemon-reload
# re-enable autostart in case stop-demo.sh --disable was used earlier
systemctl enable obs-sim obs-dash >/dev/null 2>&1

say "5/6 Start"
systemctl start obs-sim; sleep 2
systemctl start obs-dash; sleep 2
for s in obs-sim obs-dash; do printf '%-10s %s\n' "$s" "$(systemctl is-active "$s")"; done
ss -ltnp | grep -E ':(8088|8090)\b'
if ! systemctl is-active --quiet obs-sim; then
  echo "obs-sim failed to start:" >&2
  journalctl -u obs-sim -n 15 --no-pager >&2
  exit 1
fi

say "6/6 Verify"
sleep 5
measure
echo
echo "Console on the host:  $API"
echo "From your laptop:     ssh -L 8088:127.0.0.1:8088 ec2-user@<host>   then  http://localhost:8088"
echo "Change rate later:    sudo $0 <rate> --live"

