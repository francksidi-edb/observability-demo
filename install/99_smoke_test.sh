#!/bin/bash
# Smoke test: simulator -> Kafka -> ClickHouse
set -e
curl -s -X POST localhost:8090/start >/dev/null; sleep 20
clickhouse-client --port 9094 -q "SELECT count(), max(ingested_at) FROM obs.logs WHERE ingested_at > now() - 60"
/opt/kafka_current/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group ch-logs | head
curl -s localhost:8088/api/health | head -c 200; echo
