#!/usr/bin/env bash
#
# Creates the Kafka topics required by the no-kafka integration test suite.
# Idempotent (uses --if-not-exists), so re-running is safe.
#
# Usage:
#   ./test/setup-topics.sh                                  # docker exec on container "kafka"
#   KAFKA_CONTAINER=mybroker ./test/setup-topics.sh         # different container name
#   KAFKA_TOPICS_CMD="kafka/bin/kafka-topics.sh" ./test/setup-topics.sh   # local kafka CLI
#
# Why this is needed:
# Share groups (KIP-932) persist per-record state in the internal
# __share_group_state topic, which the broker auto-creates with the default
# replication factor of 3. On a single-broker cluster that fails with
# INVALID_REPLICATION_FACTOR, the share coordinator can't initialize state, and
# share group members never receive a partition assignment. The relevant broker
# config (share.coordinator.state.topic.replication.factor) is read-only, so we
# pre-create the topic with RF=1 here instead.

set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
RF="${KAFKA_REPLICATION_FACTOR:-1}"

if [ -n "${KAFKA_TOPICS_CMD:-}" ]; then
    topics() { $KAFKA_TOPICS_CMD "$@"; }
else
    CONTAINER="${KAFKA_CONTAINER:-kafka}"
    topics() { docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh "$@"; }
fi

create() {
    echo "Ensuring topic '$1' (partitions=$2, rf=$3)"
    topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
        --topic "$1" --partitions "$2" --replication-factor "$3"
}

# Main data topic used by most integration tests
create kafka-test-topic 3 "$RF"
# Share group (KIP-932) data topic
create test-share-integ 3 "$RF"
# Share group internal state topic (KIP-932) — RF=1 for single-broker clusters
create __share_group_state 50 "$RF"

echo "Test topics ready."
