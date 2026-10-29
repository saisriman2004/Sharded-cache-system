#!/usr/bin/env bash
# Benchmarks a local 3-node ShardCache cluster under several N/R/W
# settings. One memtier_benchmark instance drives each node concurrently
# (any node can coordinate any key), and their throughput is summed.
#
# Usage: benchmarks/cluster.sh [build_dir] [seconds_per_run]
set -euo pipefail

BUILD_DIR=${1:-build}
DURATION=${2:-15}
THREADS=${THREADS:-2}
CLIENTS=${CLIENTS:-25}
KEYS=${KEYS:-100000}
VALUE_SIZE=${VALUE_SIZE:-32}
RATIO=${RATIO:-1:4}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT_DIR="$ROOT/benchmarks/results"
mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/cluster-$(date +%Y%m%d-%H%M%S).md"
TMP=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$TMP"' EXIT

PORTS=(7401 7402 7403)
PEERS="nodeA:127.0.0.1:7401,nodeB:127.0.0.1:7402,nodeC:127.0.0.1:7403"

start_cluster() {
  local ids=(nodeA nodeB nodeC)
  for i in 0 1 2; do
    NODE_ID=${ids[$i]} PORT=${PORTS[$i]} CLUSTER_PEERS="$PEERS" LOG_LEVEL=warn IO_THREADS=2 \
      REPLICATION_FACTOR=$1 READ_QUORUM=$2 WRITE_QUORUM=$3 \
      "$ROOT/$BUILD_DIR/shardcache_app" > "$TMP/node$i.log" 2>&1 &
  done
  for p in "${PORTS[@]}"; do
    for _ in $(seq 1 50); do nc -z 127.0.0.1 "$p" 2>/dev/null && break; sleep 0.1; done
  done
}

stop_cluster() {
  kill $(jobs -p) 2>/dev/null || true
  wait 2>/dev/null || true
}

{
  echo "# ShardCache 3-node cluster"
  echo
  echo "- Date: $(date -u +%Y-%m-%dT%H:%MZ)"
  echo "- Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) cores (all nodes and clients share it)"
  echo "- Load: one memtier_benchmark per node, each $THREADS threads x $CLIENTS connections, SET:GET $RATIO, $KEYS keys, ${VALUE_SIZE}B values, ${DURATION}s"
  echo
  echo "| N | R | W | Guarantee | Ops/sec (sum) | Avg p50 (ms) | Worst p99 (ms) |"
  echo "|---|---|---|---|---:|---:|---:|"
} > "$REPORT"

for cfg in "3 2 2 quorum (R+W>N)" "3 1 3 read-one/write-all" "3 1 1 eventual" "1 1 1 no replication"; do
  read -r n r w label <<< "$cfg"
  echo "Running N=$n R=$r W=$w" >&2
  start_cluster "$n" "$r" "$w"
  for i in 0 1 2; do
    memtier_benchmark -s 127.0.0.1 -p "${PORTS[$i]}" --protocol=redis -t "$THREADS" -c "$CLIENTS" \
      --test-time="$DURATION" --ratio="$RATIO" --key-pattern=R:R --key-maximum="$KEYS" \
      -d "$VALUE_SIZE" --hide-histogram > "$TMP/memtier$i.txt" 2>/dev/null &
  done
  wait $(jobs -p | tail -3) 2>/dev/null || true
  stats=$(awk '/^Totals/ {ops += $2; p50 += $6; if ($7 > p99) p99 = $7} END {printf "%.0f %.3f %.3f", ops, p50 / 3, p99}' "$TMP"/memtier*.txt)
  read -r ops p50 p99 <<< "$stats"
  printf "| %s | %s | %s | %s | %'d | %s | %s |\n" "$n" "$r" "$w" "$label" "$ops" "$p50" "$p99" >> "$REPORT"
  stop_cluster
done

cat "$REPORT"
echo "Report written to $REPORT" >&2
