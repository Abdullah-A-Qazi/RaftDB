#!/usr/bin/env bash
# Launch a local RaftDB cluster (all nodes from cluster.json) and tear it
# down on Ctrl-C. Data lives under ./data/<node-id>/ and survives restarts —
# delete ./data for a fresh cluster.
#
#   ./scripts/run-cluster.sh                 # defaults
#   SNAPSHOT_THRESHOLD=20 ./scripts/run-cluster.sh   # aggressive compaction demo
set -euo pipefail

cd "$(dirname "$0")/.."

CONFIG="${CONFIG:-cluster.json}"
THRESHOLD="${SNAPSHOT_THRESHOLD:-1000}"

go build -o raftd ./cmd/raftd
go build -o raftctl ./cmd/raftctl

NODES=$(sed -n 's/.*"id": *"\([^"]*\)".*/\1/p' "$CONFIG")

PIDS=()
cleanup() {
    echo
    echo "stopping cluster..."
    for pid in "${PIDS[@]}"; do kill "$pid" 2>/dev/null || true; done
    wait 2>/dev/null || true
}
trap cleanup EXIT INT TERM

for id in $NODES; do
    ./raftd -config "$CONFIG" -id "$id" -data data -snapshot-threshold "$THRESHOLD" \
        > "data-$id.log" 2>&1 &
    PIDS+=($!)
    echo "started $id (pid $!, log data-$id.log)"
done

echo
echo "cluster is up — try in another terminal:"
echo "  ./raftctl status -watch"
echo "  ./raftctl put greeting hello"
echo "  ./raftctl get greeting"
echo
echo "Ctrl-C stops all nodes."
wait
