#!/bin/bash
# Step 0 - OS tuning for the ClickHouse + Kafka host (run as root, RHEL/Rocky/Ubuntu)
set -euo pipefail
cat >/etc/sysctl.d/99-clickhouse.conf <<'S'
kernel.task_delayacct = 1
vm.max_map_count = 262144
vm.swappiness = 1
fs.file-max = 1048576
net.core.somaxconn = 4096
S
sysctl --system >/dev/null
# THP: madvise (ClickHouse recommendation)
echo madvise >/sys/kernel/mm/transparent_hugepage/enabled || true
cat >/etc/security/limits.d/99-clickhouse.conf <<'L'
* soft nofile 262144
* hard nofile 262144
L
timedatectl set-timezone UTC
echo "Data disk: format XFS and mount on /data (noatime) if not done:"
echo "  mkfs.xfs /dev/<disk>; mkdir -p /data; echo '/dev/<disk> /data xfs noatime,nodiratime 0 0' >> /etc/fstab; mount -a"
mkdir -p /data/clickhouse /data/kafka
df -h /data
