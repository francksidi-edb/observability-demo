# Observability demo: Go simulator -> Kafka -> ClickHouse -> console

Target host: 16 vCPU, 64 GB RAM, 4 TB NVMe/EBS (16k IOPS, 1 GB/s). Run as root, in order:

    ./00_os_tuning.sh        # sysctl, UTC, /data
    ./10_install_clickhouse.sh
    ./20_install_kafka.sh
    ./40_load_schema.sh      # needs Kafka up (Kafka-engine tables)
    ./30_build_apps.sh       # simulator + console as services
    ./50_backfill.sh         # or press Start backfill in the Pipeline tab
    ./99_smoke_test.sh

Console: http://HOST:8088   Simulator API: :8090   ClickHouse: native 9094, HTTP 8123.
Edit dashboard/config.json if your ClickHouse HTTP port or credentials differ.
Tested in a sandbox: SQL on an embedded ClickHouse, simulator dry-run, console. Not yet run: live Kafka path, EDB build, this host.
