#!/bin/bash
# Step 3 - Go toolchain, simulator build, dashboard venv, systemd units.
set -x
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
command -v go >/dev/null || { curl -fsSL https://go.dev/dl/go1.22.8.linux-amd64.tar.gz | tar -C /usr/local -xz; ln -sf /usr/local/go/bin/go /usr/local/bin/go; }
(cd $ROOT/simulator && go build -o /usr/local/bin/obs-sim .)
command -v python3 >/dev/null
python3 -m venv /opt/obs-dash && /opt/obs-dash/bin/pip install -q -r $ROOT/dashboard/requirements.txt
cat >/etc/systemd/system/obs-sim.service <<U
[Unit]
Description=Observability demo simulator (Go -> Kafka)
After=kafka.service
[Service]
ExecStart=/usr/local/bin/obs-sim -brokers localhost:9092 -rate 50000 -http :8090
Restart=on-failure
[Install]
WantedBy=multi-user.target
U
cat >/etc/systemd/system/obs-dash.service <<U
[Unit]
Description=Observability console (FastAPI)
After=clickhouse-server.service
[Service]
WorkingDirectory=$ROOT/dashboard
ExecStart=/opt/obs-dash/bin/python app.py --port 8088
Restart=on-failure
[Install]
WantedBy=multi-user.target
U
systemctl daemon-reload; systemctl enable --now obs-sim obs-dash
echo "Console: http://<host>:8088"
