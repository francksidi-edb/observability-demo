#!/bin/bash
set -x
set -euo pipefail

# Step 2 - Single-broker Kafka 4.4.0 (KRaft, no ZooKeeper)
# Run as root from the install directory.

V="4.4.0"
S="2.13"
KAFKA_ARCHIVE="kafka_${S}-${V}.tgz"
KAFKA_DIR="/opt/kafka_${S}-${V}"
KAFKA_HOME="/opt/kafka_current"
KAFKA_DATA="/data/kafka"
KAFKA_CONFIG="${KAFKA_HOME}/config/demo.properties"

# Ensure Java 17+ is available
if ! command -v java >/dev/null 2>&1; then
    dnf install -y java-17-openjdk-headless
fi

java -version

# Create Kafka user
id kafka >/dev/null 2>&1 || \
    useradd -r -m -d /opt/kafka kafka

# Install Kafka from the existing local archive
if [ ! -d "$KAFKA_DIR" ]; then
    if [ ! -f "$KAFKA_ARCHIVE" ]; then
        echo "ERROR: $KAFKA_ARCHIVE not found in $(pwd)"
        exit 1
    fi

    tar xzf "$KAFKA_ARCHIVE" -C /opt
fi

ln -sfn "$KAFKA_DIR" "$KAFKA_HOME"

# Create Kafka data directory
mkdir -p "$KAFKA_DATA"

# Kafka 4.4.0 KRaft configuration
cat > "$KAFKA_CONFIG" <<'P'
process.roles=broker,controller
node.id=1

# Kafka 4.x dynamic quorum
controller.quorum.bootstrap.servers=localhost:9093

listeners=PLAINTEXT://:9092,CONTROLLER://:9093
advertised.listeners=PLAINTEXT://localhost:9092

controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT

log.dirs=/data/kafka

num.partitions=8
log.retention.hours=6

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1

message.max.bytes=10485760
P

chown -R kafka:kafka "$KAFKA_DATA" "$KAFKA_DIR"

# Initialize KRaft storage only if not already formatted
if [ ! -f "$KAFKA_DATA/meta.properties" ]; then
    CLUSTER_ID=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)

    runuser -u kafka -- \
        "$KAFKA_HOME/bin/kafka-storage.sh" format \
        --standalone \
        -t "$CLUSTER_ID" \
        -c "$KAFKA_CONFIG"
else
    echo "Kafka storage already formatted. Skipping."
fi

# Create systemd service
cat > /etc/systemd/system/kafka.service <<'U'
[Unit]
Description=Apache Kafka 4.4.0 (KRaft, demo)
After=network.target

[Service]
Type=simple
User=kafka
Group=kafka
Environment="KAFKA_HEAP_OPTS=-Xmx4g -Xms4g"
ExecStart=/opt/kafka_current/bin/kafka-server-start.sh /opt/kafka_current/config/demo.properties
Restart=on-failure
RestartSec=5
LimitNOFILE=262144

[Install]
WantedBy=multi-user.target
U

# Start Kafka
systemctl daemon-reload
systemctl enable kafka
systemctl restart kafka

# Wait for Kafka to become ready
echo "Waiting for Kafka..."

READY=0
for i in {1..30}; do
    if "$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
        --bootstrap-server localhost:9092 \
        >/dev/null 2>&1; then
        READY=1
        break
    fi
    sleep 2
done

if [ "$READY" -ne 1 ]; then
    echo "ERROR: Kafka failed to start."
    journalctl -u kafka -n 50 --no-pager
    exit 1
fi

echo "Kafka is ready."

# Create demo topics
for t in logs metrics; do
    "$KAFKA_HOME/bin/kafka-topics.sh" \
        --bootstrap-server localhost:9092 \
        --create \
        --if-not-exists \
        --topic "$t" \
        --partitions 8 \
        --replication-factor 1
done

# Verify topics
"$KAFKA_HOME/bin/kafka-topics.sh" \
    --bootstrap-server localhost:9092 \
    --list

echo "Kafka 4.4.0 installation completed successfully."


