#!/usr/bin/env bash
# Head-to-head single-node comparison: ShardCache vs Redis vs Memcached,
# driven by memtier_benchmark with identical workloads on the same machine.
#
# Usage: benchmarks/compare.sh [build_dir] [seconds_per_run]
# Requires: memtier_benchmark, redis-server, memcached on PATH.
set -euo pipefail

BUILD_DIR=${1:-build}
DURATION=${2:-15}
THREADS=${THREADS:-4}          # memtier client threads
CLIENTS=${CLIENTS:-50}         # connections per memtier thread
SERVER_THREADS=${SERVER_THREADS:-4}
KEYS=${KEYS:-100000}
VALUE_SIZE=${VALUE_SIZE:-32}

ROOT=$(cd "$(dirname "$0")/.." && pwd)
OUT_DIR="$ROOT/benchmarks/results"
mkdir -p "$OUT_DIR"
REPORT="$OUT_DIR/compare-$(date +%Y%m%d-%H%M%S).md"
TMP=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$TMP"' EXIT

SC_PORT=7301; REDIS_PORT=7302; MC_PORT=7303

# A leftover process on one of our ports would silently be benchmarked
# instead of the server we start, so refuse to run.
for port in $SC_PORT $REDIS_PORT $MC_PORT; do
  if nc -z 127.0.0.1 "$port" 2>/dev/null; then
    echo "port $port is already in use; stop whatever is listening there first" >&2
    exit 1
  fi
done

wait_port() {
  for _ in $(seq 1 50); do nc -z 127.0.0.1 "$1" 2>/dev/null && return 0; sleep 0.1; done
  echo "server on port $1 did not start" >&2; exit 1
}

start_servers() {
  IO_THREADS=$SERVER_THREADS LOG_LEVEL=warn CACHE_CAPACITY=$((KEYS * 2)) \
    "$ROOT/$BUILD_DIR/shardcache_app" $SC_PORT > "$TMP/shardcache.log" 2>&1 &
  redis-server --port $REDIS_PORT --save "" --appendonly no --io-threads $SERVER_THREADS \
    > "$TMP/redis.log" 2>&1 &
  memcached -p $MC_PORT -t $SERVER_THREADS -m 2048 -U 0 > "$TMP/memcached.log" 2>&1 &
  wait_port $SC_PORT; wait_port $REDIS_PORT; wait_port $MC_PORT
  for pid in $(jobs -p); do
    kill -0 "$pid" 2>/dev/null || { echo "a server exited during startup; logs in $TMP" >&2; exit 1; }
  done
}

# memtier "Totals" row -> "ops/sec|avg ms|p50|p99|p99.9"
run_memtier() {
  local port=$1 protocol=$2 extra=$3
  memtier_benchmark -s 127.0.0.1 -p "$port" --protocol="$protocol" \
    -t "$THREADS" -c "$CLIENTS" --test-time="$DURATION" \
    --key-maximum="$KEYS" -d "$VALUE_SIZE" --hide-histogram $extra 2>/dev/null \
    | awk '/^Totals/ {printf "%.0f|%.3f|%.3f|%.3f|%.3f\n", $2, $5, $6, $7, $8}'
}

preload() {
  local port=$1 protocol=$2
  memtier_benchmark -s 127.0.0.1 -p "$port" --protocol="$protocol" -t 1 -c 10 \
    --ratio=1:0 --key-pattern=P:P --key-maximum="$KEYS" -d "$VALUE_SIZE" \
    --requests=$((KEYS / 10)) --hide-histogram > /dev/null 2>&1
}

start_servers
preload $SC_PORT redis; preload $REDIS_PORT redis; preload $MC_PORT memcache_text

declare -a WORKLOADS=(
  "1:10 GET-heavy, uniform keys|--ratio=1:10 --key-pattern=R:R"
  "1:10 GET-heavy, gaussian hot keys|--ratio=1:10 --key-pattern=G:G"
  "1:1 mixed, uniform keys|--ratio=1:1 --key-pattern=R:R"
  "1:10 GET-heavy, pipeline 16|--ratio=1:10 --key-pattern=R:R --pipeline=16"
)

{
  echo "# ShardCache vs Redis vs Memcached"
  echo
  echo "- Date: $(date -u +%Y-%m-%dT%H:%MZ)"
  echo "- Machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(sysctl -n hw.ncpu 2>/dev/null || nproc) cores"
  echo "- Load: memtier_benchmark $THREADS threads x $CLIENTS connections, ${DURATION}s per run, $KEYS keys, ${VALUE_SIZE}B values"
  echo "- Servers: $SERVER_THREADS threads each; $(redis-server --version | awk '{print $3}'), memcached $(memcached -V | awk '{print $2}'); Redis persistence off"
  echo "- Client and servers share the machine, so absolute numbers are a lower bound"
  echo
} > "$REPORT"

for entry in "${WORKLOADS[@]}"; do
  name=${entry%%|*}; args=${entry#*|}
  echo "Running: $name" >&2
  {
    echo "## $name"
    echo
    echo "| Server | Ops/sec | Avg (ms) | p50 (ms) | p99 (ms) | p99.9 (ms) |"
    echo "|---|---:|---:|---:|---:|---:|"
    for target in "ShardCache|$SC_PORT|redis" "Redis|$REDIS_PORT|redis" "Memcached|$MC_PORT|memcache_text"; do
      IFS='|' read -r label port proto <<< "$target"
      # Run first: an IFS prefix on `read` would also apply inside the substitution.
      result=$(run_memtier "$port" "$proto" "$args")
      IFS='|' read -r ops avg p50 p99 p999 <<< "$result"
      printf "| %s | %'d | %s | %s | %s | %s |\n" "$label" "$ops" "$avg" "$p50" "$p99" "$p999"
    done
    echo
  } >> "$REPORT"
done

cat "$REPORT"
echo "Report written to $REPORT" >&2
