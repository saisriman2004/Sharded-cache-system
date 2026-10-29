#!/usr/bin/env bash
# Starts a local 3-node cluster: node A alone, B and C join it by gossip.
# Usage: scripts/start_cluster.sh [build_dir]   (Ctrl-C stops all nodes gracefully)
set -e

BIN=${1:-build}/shardcache_app
pids=()

start() {
  NODE_ID=$1 PORT=$2 ADVERTISE_HOST=127.0.0.1 SEEDS=$3 "$BIN" &
  pids+=($!)
  echo "Node $1 on port $2 (metrics on $(( $2 + 2000 ))), PID $!"
}

start nodeA 7001 ""
sleep 0.5
start nodeB 7002 127.0.0.1:7001
start nodeC 7003 127.0.0.1:7001

cleanup() {
  echo "Stopping cluster nodes..."
  kill -TERM "${pids[@]}" 2>/dev/null || true
  wait
}
trap cleanup EXIT INT TERM
wait
