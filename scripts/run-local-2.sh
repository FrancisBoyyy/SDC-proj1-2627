#!/usr/bin/env bash
# Runs a complete experiment on the local machine: starts replicas node1, ...,
# nodeN (they must exist in the trust store and in babel.conf), runs the automated
# client, and stops the replicas when the client finishes, printing the final
# state digest of each replica. Logs are written to the logs folder.
#
# Usage: scripts/run-local.sh [--kill <node>:<seconds>]... <number of replicas> [extra properties for the client...]
# Example: scripts/run-local.sh 4 num_clients=20 run_time=20000 label=placeholder-20
# Example (kill node3 10s after the client starts): scripts/run-local.sh --kill 3:10 4 num_clients=20 run_time=20000
#
# Options:
#   --kill <node>:<seconds>   Kill replica node<node> <seconds> seconds after the client
#                             starts. Can be repeated to kill several replicas.
#                             Must come before the number of replicas.
# Environment: REPLICA_OPTS (extra properties for the replicas), MAX_WAIT (maximum
# time, in seconds, to wait for the replicas to stabilize after the client ends),
# KILL_SIGNAL (signal used by --kill, default KILL, i.e. a hard crash; use TERM for
# a graceful shutdown)

set -euo pipefail

KILLS=()
while [ $# -gt 0 ]; do
    case "$1" in
        --kill)
            [ $# -ge 2 ] || { echo "--kill needs <node>:<seconds>" >&2; exit 1; }
            KILLS+=("$2")
            shift 2
            ;;
        --kill=*)
            KILLS+=("${1#--kill=}")
            shift
            ;;
        --*)
            echo "Unknown option: $1" >&2
            exit 1
            ;;
        *)
            break
            ;;
    esac
done

REPLICAS=${1:?number of replicas}
shift
JAR=target/project1-1.0.jar
# Netty's native transport for macOS (kqueue) misbehaves in some machines; the NIO
# transport works everywhere. The logging configuration is the one in the jar.
JAVA_OPTS="-Dio.netty.transport.noNative=true"
LOGS=logs
KILL_SIGNAL=${KILL_SIGNAL:-KILL}

# Validate the --kill specs before starting anything.
KILLED=()
for spec in ${KILLS[@]+"${KILLS[@]}"}; do
    node=${spec%%:*}
    secs=${spec##*:}
    if ! [[ "$node" =~ ^[0-9]+$ ]] || ! [[ "$secs" =~ ^[0-9]+$ ]] || [ "$spec" = "$node" ]; then
        echo "Invalid --kill spec '$spec' (expected <node>:<seconds>)" >&2
        exit 1
    fi
    if [ "$node" -lt 1 ] || [ "$node" -gt "$REPLICAS" ]; then
        echo "Invalid --kill spec '$spec': node must be between 1 and $REPLICAS" >&2
        exit 1
    fi
done

mkdir -p "$LOGS"
PIDS=()
KILLER_PIDS=()
cleanup() {
    for pid in ${KILLER_PIDS[@]+"${KILLER_PIDS[@]}"}; do
        kill "$pid" 2>/dev/null || true
    done
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

# Schedule the requested replica kills, relative to the moment the client starts.
for spec in ${KILLS[@]+"${KILLS[@]}"}; do
    node=${spec%%:*}
    secs=${spec##*:}
    target=${PIDS[$((node - 1))]}
    (
        sleep "$secs"
        if kill -"$KILL_SIGNAL" "$target" 2>/dev/null; then
            echo ">>> killed node$node (SIG$KILL_SIGNAL) after ${secs}s" | tee -a "$LOGS/node$node.log"
        fi
    ) &
    KILLER_PIDS+=($!)
    KILLED+=("$node")
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
    note=""
    for k in ${KILLED[@]+"${KILLED[@]}"}; do
        if [ "$k" = "$i" ]; then note=" (killed midway)"; fi
    done
    echo "node$i$note: $(grep 'STATE' "$LOGS/node$i.log" | tail -1 | sed 's/.* - //' || true)"
done