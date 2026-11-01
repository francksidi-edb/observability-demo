#!/bin/bash
# Step 1 - ClickHouse. Use the EDB-supported package/repo you were given for the PoC build.
# Set EDB_CH_REPO_RPM (or install the RPM/DEB files by hand) before running.
set -euo pipefail
if command -v clickhouse-server >/dev/null; then
  echo "ClickHouse already installed: $(clickhouse-server --version)"
else
  echo "Install the EDB ClickHouse packages here (clickhouse-server, clickhouse-client, clickhouse-common-static)."
  echo "e.g. dnf install -y ./clickhouse-*.rpm   |   apt install -y ./clickhouse-*.deb"; exit 1
fi
mkdir -p /etc/clickhouse-server/config.d /etc/clickhouse-server/users.d
cat >/etc/clickhouse-server/config.d/demo.xml <<'X'
<clickhouse>
  <timezone>UTC</timezone>
  <tcp_port>9094</tcp_port>
  <http_port>8123</http_port>
  <listen_host>0.0.0.0</listen_host>
  <path>/data/clickhouse/</path>
  <tmp_path>/data/clickhouse/tmp/</tmp_path>
  <max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio>
  <mark_cache_size>5368709120</mark_cache_size>
  <uncompressed_cache_size>8589934592</uncompressed_cache_size>
  <background_pool_size>16</background_pool_size>
  <merge_tree><max_suspicious_broken_parts>20</max_suspicious_broken_parts></merge_tree>
</clickhouse>
X
chown -R clickhouse:clickhouse /data/clickhouse
systemctl enable --now clickhouse-server
sleep 5
clickhouse-client --port 9094 -q "SELECT version(), timezone()"
