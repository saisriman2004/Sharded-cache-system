#!/usr/bin/env bash
set -e

PEERS="nodeA:127.0.0.1:7001,nodeB:127.0.0.1:7002,nodeC:127.0.0.1:7003"

echo "Starting 3-node ShardCache cluster..."

NODE_ID=nodeA PORT=7001 ADVERTISE_HOST=127.0.0.1 CLUSTER_PEERS="$PEERS" \
  ./build/shardcache_app &
PID1=$!

NODE_ID=nodeB PORT=7002 ADVERTISE_HOST=127.0.0.1 CLUSTER_PEERS="$PEERS" \
  ./build/shardcache_app &
PID2=$!

NODE_ID=nodeC PORT=7003 ADVERTISE_HOST=127.0.0.1 CLUSTER_PEERS="$PEERS" \
  ./build/shardcache_app &
PID3=$!

echo "Node A running on port 7001 (PID $PID1)"
echo "Node B running on port 7002 (PID $PID2)"
echo "Node C running on port 7003 (PID $PID3)"

cleanup() {
    echo "Stopping cluster nodes..."
    kill $PID1 $PID2 $PID3 2>/dev/null || true
}

trap cleanup EXIT
wait
