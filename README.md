
# Observability without the per-GB meter

A live demo of logs and metrics on ClickHouse, supported by EDB. A Go simulator publishes billions of
synthetic events to Kafka, ClickHouse ingests them and builds one-minute rollups as the data arrives, and a
web console serves dashboards, log search and governance views on top. 

The point of the demo: the volume of telemetry should not set the price. Storage is compressed about 5x,
queries read only the data they need, and dashboards read small pre-aggregated rows instead of raw logs.

> All data is synthetic. Figures below were measured on one reference host and are dated. Re-measure on your own
> hardware before quoting them.

## Architecture   

```mermaid        
flowchart LR      
    S["Go simulator<br/>215 hosts, 7 services"] -->|"logs, metrics<br/>JSON"| K["Kafka<br/>2 topics, 8 partitions, lz4"]
    K -->|"Kafka engine<br/>8 consumers"| MV["Materialized views<br/>parse, cast, route"]
    MV --> L["MergeTree: logs, metrics<br/>sorted, compressed, 90-day TTL"]
    MV --> R["Rollup: obs.svc_1m<br/>AggregatingMergeTree, 1 minute"]
    L --> C["Console (FastAPI)<br/>dashboards, search, governance"]
    R --> C
```

| Component | Role | Port |
| --- | --- | --- |
| `obs-sim` (Go) | Generates logs and metrics, control API for rate and incidents | 8090 |
| Kafka | Buffers bursts and decouples producers from ClickHouse | 9092 |
| ClickHouse (EDB build) | Storage and queries (native protocol) | 9094 |
| `obs-dash` (FastAPI) | Web console over ClickHouse's HTTP interface | 8088 |

## What the console shows

| Tab | What to look at |
| --- | --- |
| Overview | Pipeline health at a glance |
| Pipeline | Start and stop the stream, change the rate, trigger a bad deploy, load history, storage tiles |
| Dashboard | Error rate, p99 and a service heatmap. Each panel shows rows read and time taken. A "Scan raw rows" toggle compares the rollup with the raw table |
| Explore | Search logs by service, severity, window and text. The scan bar shows rows read and the share skipped by the index |
| Govern | Per-user query accounting from the query log, plus row policies and quotas |
| Code | The schema and SQL behind everything above |

## Quick start

Requirements: a Linux host with ClickHouse, Kafka, Python 3 and systemd. The reference host has 16 vCPU, 64 GB RAM
and fast disk.

```bash
# Console and simulator run as systemd units: obs-sim and obs-dash
sudo systemctl enable --now obs-sim obs-dash
```

See `install/` for the install scripts and unit files. The Kafka tarball is not in this repository
(it exceeds GitHub's file size limit): download Apache Kafka from the Apache archive.

Reach the console. Only port 22 needs to be open, so use an SSH tunnel:

```bash
ssh -i <your-key>.pem -L 8088:127.0.0.1:8088 ec2-user@<host>
# then open http://localhost:8088
```

## Run the demo

```bash
sudo ./reset-demo.sh 200k            # restart at 200,000 events per second (starts Kafka and ClickHouse if down)
sudo ./reset-demo.sh 1m --wipe       # also empty all obs.* tables first
sudo ./reset-demo.sh 100k --live     # change the rate only, no restart
sudo ./reset-demo.sh --status        # show services, ports, disk and measured rows per second

sudo ./stop-demo.sh                  # stop simulator and console, keep data
sudo ./stop-demo.sh --all            # also stop ClickHouse and Kafka
```

Rates accept plain numbers or `k` and `m` suffixes. The console slider tops out near 316k events per second.

A suggested run order for a 15-minute live demo: start the stream on the Pipeline tab, watch the Dashboard
(compare rollup and raw scan), press Trigger bad deploy, find the cause on Explore, then show Govern.

## Repository layout

```
simulator/        Go event generator (obs-sim)
sql/              schema, views, policies and the history backfill
dashboard/        FastAPI console (app.py) and requirements
install/          install scripts and systemd units
reset-demo.sh     reset or retune the demo
stop-demo.sh      stop the demo
```

## Reference results

Measured on the reference host (16 vCPU, 64 GB, 4 TB disk at 16,000 IOPS, one node, ClickHouse 26.8 EDB build).

| Measure | Result | Date and size |
| --- | --- | --- |
| Ingest | About 316k events per second, pipeline lag about 1.5 s | 1 Nov, 1.0 B log rows |
| Storage | 23.3 GB for 999.8 M rows, about 25 bytes per row, about 5x compression (116.1 GB uncompressed) | 1 Nov |
| Active parts | 199 across 78 daily partitions | 1 Nov |
| Explore search (payments, ERROR, 15 min, text "timeout") | 195.9 k of 419.8 M rows read, 99.95% skipped, about 21 ms | 30 Oct, 420.7 M rows |
| Dashboard panel on the rollup | About 119 rows read, 7 to 12 ms | 30 Oct |
| Error-rate panel with Scan raw rows | 244.3 M rows, 3.0 GB, 1,568 ms | 30 Oct |

## Why it is fast

- **Columns:** a query reads only the columns it names, and each column compresses on its own.
- **Sort key and sparse index:** rows are sorted by service, severity and time, and the index keeps one entry per
  8,192-row granule, so a search skips ranges that cannot match.
- **Rollups on insert:** a materialized view builds per-minute aggregates, including percentile states, as data arrives.
- **Immutable parts and parallel reads:** writes append sorted parts that merge in the background, and one query can use every core.

## Known limitations

- **Rollup history:** the one-minute rollup is built by a materialized view, which fires only on new inserts. History loaded
  straight into the raw table with the backfill does not reach it, so 7-day and 30-day views are mostly empty until the
  rollup is rebuilt from the raw rows.
- **Single node:** the demo shows no replication or sharding. Do not extrapolate throughput to other hardware.
- **Synthetic data:** compression, selectivity and timings will differ on real logs.
- **Scope:** logs and metrics only. No traces and no alerting.

## Troubleshooting

| Symptom | Likely cause and fix |
| --- | --- |
| Console does not load from a laptop | A corporate network may filter port 8088. Use the SSH tunnel above |
| `clickhouse-client` cannot connect | The native port is 9094, not 9000: `clickhouse-client --port 9094` |
| `obs-sim` fails with "address already in use" on 8090 | A stale `obs-sim` is running. `pkill -x obs-sim`, then `reset-demo.sh` |
| Dashboard blank or "Internal Server Error" | Check `journalctl -u obs-dash -n 50`. Known causes: shared ClickHouse session (use `autogenerate_session_id=False`) and an empty previous window returning NaN |
| Rates look low after a reset | Wait 60 seconds before quoting rows per second |

## Notes

- Keys and credentials are not committed. See `.gitignore`.
- The open-source engine is unmodified ClickHouse. This repository is a demo, not a product.
                                                                                                                                                                                                              
                                                                                                                                                                                               

