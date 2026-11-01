#!/bin/bash
# Step 5 - history. Easiest: press "Start backfill" on the Pipeline tab. CLI alternative:
#   ./50_backfill.sh 1000000000 7     # rows, days   (default 1 billion over 7 days)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TOTAL=${1:-1000000000}; DAYS=${2:-7}; SLICE=100000000
for ((off=0; off<TOTAL; off+=SLICE)); do
  echo "slice $off"; clickhouse-client --port 9094 --max_threads 16 \
    --param_total=$TOTAL --param_off=$off --param_n=$SLICE --param_days=$DAYS < $ROOT/sql/backfill_logs.sql
done
clickhouse-client --port 9094 --param_days=$DAYS < $ROOT/sql/backfill_metrics.sql
clickhouse-client --port 9094 -q "SELECT formatReadableQuantity(count()) FROM obs.logs"
