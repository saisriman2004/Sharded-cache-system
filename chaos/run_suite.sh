#!/usr/bin/env bash
# Runs the chaos test matrix against both consistency modes and writes a
# Markdown table of results.
#
# Usage: chaos/run_suite.sh [path/to/shardcache_app] [seconds_per_run]
set -uo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
SERVER=$(cd "$(dirname "${1:-$HERE/../build/shardcache_app}")" && pwd)/$(basename "${1:-shardcache_app}")
DURATION=${2:-25}
OUT="$HERE/out"
REPORT="$OUT/suite.md"
mkdir -p "$OUT"

(cd "$HERE" && go build -o "$OUT/chaos" .) || exit 2

SCENARIOS=(
  "none|-faults none -latency 20ms -keys 2"
  "partitions|-faults partition -latency 5ms"
  "process pauses|-faults pause -latency 10ms"
  "partitions + pauses|-faults partition,pause -latency 10ms"
  "crash-restart|-faults kill -latency 2ms -interval 3s"
)

{
  echo "| Mode | Faults | Seed | OK ops | Failed / indeterminate writes | Stale reads | Inversions | Linearizable |"
  echo "|---|---|---|---:|---:|---:|---:|---|"
} > "$REPORT"

for mode in quorum linearizable; do
  for entry in "${SCENARIOS[@]}"; do
    name=${entry%%|*}; args=${entry#*|}
    seed=$RANDOM
    log="$OUT/run-$mode-${name// /_}.txt"
    "$OUT/chaos" -binary "$SERVER" -consistency "$mode" -clients 12 -keys 3 $args \
      -duration "${DURATION}s" -seed "$seed" -out "$OUT/$mode-${name// /_}" > "$log" 2>&1
    ok=$(awk '/ok=/ {split($2, a, "="); s += a[2]} END {print s + 0}' "$log")
    bad=$(awk '/^  (SET|DEL)/ {split($3, f, "="); split($4, i, "="); s += f[2] + i[2]} END {print s + 0}' "$log")
    stale=$(awk -F': ' '/^stale reads/ {print $2}' "$log")
    inv=$(awk -F': ' '/^new-old inversions/ {print $2}' "$log")
    result=$(awk -F': ' '/^linearizable/ {print $2}' "$log")
    printf "| %s | %s | %s | %'d | %s | %s | %s | %s |\n" "$mode" "$name" "$seed" "$ok" "$bad" "$stale" "$inv" "$result" >> "$REPORT"
    echo "$mode / $name: $result" >&2
  done
done

cat "$REPORT"
