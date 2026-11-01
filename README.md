# Observability demo: logs and metrics on ClickHouse

Go simulator -> Kafka -> ClickHouse (Kafka engine, materialized views, 1-minute rollup) -> FastAPI console.

- `simulator/`  Go event generator (`obs-sim`, control API on :8090)
- `sql/`        schema, views, policies, backfill
- `dashboard/`  FastAPI console (`app.py`, port 8088)
- `install/`    install scripts and systemd units
- `reset-demo.sh <rate> [--wipe|--live]`  reset or retune the demo
- `stop-demo.sh [--sim-only|--all|--disable]`  stop it

Data is synthetic. ClickHouse native port is 9094. Only port 22 needs to be open (use an SSH tunnel to reach :8088).
