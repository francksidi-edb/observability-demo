#!/bin/bash
# Step 4 - schema, governance, fleet catalogue (Kafka must be up first)
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
CH="clickhouse-client --port 9094 --multiquery"
$CH < $ROOT/sql/schema.sql
$CH < $ROOT/sql/governance.sql
$CH < $ROOT/sql/fleet.sql
clickhouse-client --port 9094 -q "SHOW TABLES FROM obs"
