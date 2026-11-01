# --- 1. stop and clear state ---
systemctl stop obs-dash obs-sim
systemctl reset-failed obs-dash obs-sim
pkill -x obs-sim; pkill -f 'app.py --port 8088'; sleep 1

echo "=== who still holds 8088/8090? (should be empty) ==="
ss -ltnp | grep -E ':(8088|8090)\b' || echo "both ports free"

# --- 2. fixes ---
restorecon -v /usr/local/bin/obs-sim 2>/dev/null
APP=/home/ec2-user/demo/observability-demo/dashboard/app.py
grep -nE "host|add_argument|uvicorn.run" $APP

# --- 3. services that must be up first ---
systemctl is-active clickhouse-server kafka

# --- 4. start ---
systemctl start obs-sim; sleep 2
systemctl start obs-dash; sleep 2

# --- 5. verify ---
systemctl is-active obs-sim obs-dash
ss -ltnp | grep -E ':(8088|8090)\b'
curl -s http://127.0.0.1:8090/ | head -c 300; echo
journalctl -u obs-sim -n 15 --no-pager

