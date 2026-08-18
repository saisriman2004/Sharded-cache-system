#!/usr/bin/env bash
set -e

echo "Starting 3-node ShardCache cluster..."

./build/shardcache_app 7001 &
PID1=$!

./build/shardcache_app 7002 &
PID2=$!

./build/shardcache_app 7003 &
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
