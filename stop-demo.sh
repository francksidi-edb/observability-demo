#!/usr/bin/env bash
# stop-demo.sh - stop the observability demo.
#
# Usage:
#   sudo ./stop-demo.sh              # stop simulator and console, keep data, leave ClickHouse and Kafka running
#   sudo ./stop-demo.sh --sim-only   # stop only the simulator (console stays up to show the final state)
#   sudo ./stop-demo.sh --all        # also stop ClickHouse and Kafka (frees memory and CPU)
#   sudo ./stop-demo.sh --disable    # also disable obs-sim/obs-dash so they do not start on boot
#
# Data is never deleted. To empty it, use: ./reset-demo.sh <rate> --wipe

set -uo pipefail

CH="${CH:-clickhouse-client --port 9094}"   # override with CH="clickhouse-client --port N"
SIM_ONLY=0
ALL=0
DISABLE=0

usage() { sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

for arg in "$@"; do
  case "$arg" in
    -h|--help)  usage 0 ;;
    --sim-only) SIM_ONLY=1 ;;
    --all)      ALL=1 ;;
    --disable)  DISABLE=1 ;;
    *)          echo "Unknown option: $arg" >&2; usage 1 ;;
  esac
done

if [[ $EUID -ne 0 ]]; then echo "Run as root (sudo)." >&2; exit 1; fi
if [[ $SIM_ONLY -eq 1 && ( $ALL -eq 1 || $DISABLE -eq 1 ) ]]; then
  echo "--sim-only cannot be combined with --all or --disable." >&2; exit 1
fi

say() { printf '\n=== %s ===\n' "$*"; }

rows() { $CH -q "SELECT count() FROM obs.logs" 2>/dev/null; }

say "1/4 Stop the simulator"
systemctl stop obs-sim
pkill -x obs-sim 2>/dev/null
sleep 1
printf '%-10s %s\n' obs-sim "$(systemctl is-active obs-sim 2>&1)"

# give ClickHouse a moment to drain what is already buffered in Kafka
if [[ $ALL -eq 1 ]] && systemctl is-active --quiet antalya; then
  echo "waiting 10 s for Kafka to drain into ClickHouse..."
  sleep 10
fi

if [[ $SIM_ONLY -eq 0 ]]; then
  say "2/4 Stop the console"
  systemctl stop obs-dash
  pkill -f 'app.py --port' 2>/dev/null
  sleep 1
  printf '%-10s %s\n' obs-dash "$(systemctl is-active obs-dash 2>&1)"
else
  say "2/4 Console left running (--sim-only)"
fi

say "3/4 Final state"
if R=$(rows) && [[ -n "$R" ]]; then
  echo "rows kept in obs.logs: $(printf "%'d" "$R")"
else
  echo "ClickHouse not reachable with: $CH (row count skipped)"
fi

if [[ $ALL -eq 1 ]]; then
  say "Stopping ClickHouse and Kafka"
  systemctl stop antalya
  systemctl stop kafka
  for s in antalya kafka; do printf '%-18s %s\n' "$s" "$(systemctl is-active "$s" 2>&1)"; done
fi

if [[ $DISABLE -eq 1 ]]; then
  say "Disabling autostart"
  systemctl disable obs-sim obs-dash 2>&1 | sed 's/^/  /'
fi

say "4/4 Verify"
if ss -ltnp | grep -E ':(8088|8090)\b'; then
  if [[ $SIM_ONLY -eq 1 ]]; then
    echo "(console on 8088 is still up, as requested)"
  else
    echo "Something still listens on 8088/8090 (see above)." >&2
  fi
else
  echo "ports 8088 and 8090 are free"
fi
echo
echo "Start again with: sudo ./reset-demo.sh <rate>"
[[ $ALL -eq 1 ]] && echo "ClickHouse and Kafka were stopped; reset-demo.sh starts them again."
exit 0

