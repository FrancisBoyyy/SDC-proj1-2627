#!/usr/bin/env bash
# Runs a complete experiment on the local machine: starts replicas node1, ...,
# nodeN (they must exist in the trust store and in babel.conf), runs the automated
# client, and stops the replicas when the client finishes, printing the final
# state digest of each replica. Logs are written to the logs folder.
#
# Usage: scripts/run-local.sh <number of replicas> [extra properties for the client...]
# Example: scripts/run-local.sh 4 num_clients=20 run_time=20000 label=placeholder-20
# Environment: REPLICA_OPTS (extra properties for the replicas), MAX_WAIT (maximum
# time, in seconds, to wait for the replicas to stabilize after the client ends)

set -euo pipefail

REPLICAS=${1:?number of replicas}
shift
JAR=target/project1-1.0.jar
# Netty's native transport for macOS (kqueue) misbehaves in some machines; the NIO
# transport works everywhere. The logging configuration is the one in the jar.
JAVA_OPTS="-Dio.netty.transport.noNative=true"
LOGS=logs

mkdir -p "$LOGS"
PIDS=()
cleanup() {
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
}
trap cleanup EXIT

# Extra properties for the replicas can be given in REPLICA_OPTS, e.g.:
# REPLICA_OPTS="digest_interval=2000" scripts/run-local.sh 4
for i in $(seq 1 "$REPLICAS"); do
    java $JAVA_OPTS -jar "$JAR" crypto_name="node$i" ${REPLICA_OPTS:-} > "$LOGS/node$i.log" 2>&1 &
    PIDS+=($!)
done

java $JAVA_OPTS -cp "$JAR" ClientMain "$@" 2>&1 | tee "$LOGS/client.log"

# Replicas log their state periodically (digest_interval). Wait until the state of
# every replica stops changing (or MAX_WAIT seconds), so that a replica that is
# still catching up is not mistaken for a divergent one.
states() {
    for i in $(seq 1 "$REPLICAS"); do
        grep 'STATE' "$LOGS/node$i.log" | tail -1 | sed 's/.* - //' || true
    done
}
previous=""
waited=0
sleep 6
while [ "$waited" -lt "${MAX_WAIT:-60}" ]; do
    current=$(states)
    if [ "$current" = "$previous" ]; then
        break
    fi
    previous=$current
    sleep 6
    waited=$((waited + 6))
done
echo "Final state of each replica:"
for i in $(seq 1 "$REPLICAS"); do
    echo "node$i: $(grep 'STATE' "$LOGS/node$i.log" | tail -1 | sed 's/.* - //' || true)"
done
