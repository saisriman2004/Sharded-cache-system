# Benchmarks

All numbers come from an Apple M3 laptop (4 performance + 4 efficiency
cores) that was also running a desktop session. **Servers and load
generators share the machine**, so absolute throughput is a lower bound,
and it varies by up to about 30% between runs as background load changes.
**Comparisons within a single run are the meaningful part.** Raw reports
are in [`benchmarks/results/`](../benchmarks/results).

Reproduce:

```bash
cmake -S . -B build && cmake --build build -j
benchmarks/compare.sh build 15     # vs Redis and Memcached (needs memtier_benchmark, redis-server, memcached)
benchmarks/cluster.sh build 15     # 3-node cluster under several N/R/W settings
build/cache_benchmark              # in-process storage engine
```

## Single node vs Redis 8.10 and Memcached 1.6

[memtier_benchmark](https://github.com/RedisLabs/memtier_benchmark), 4
threads × 50 connections, 15 s per run, 100K keys (preloaded), 32-byte
values. Each server gets 4 threads (Redis: `--io-threads 4`, persistence
off; Memcached: `-t 4`).

Recorded run ([report](../benchmarks/results)):

| Workload | ShardCache | Redis | Memcached |
|---|---:|---:|---:|
| 1:10 SET:GET, uniform keys | 211K ops/s, p99 2.1 ms | 212K, p99 1.7 ms | 193K, p99 2.7 ms |
| 1:10, gaussian hot keys | 181K, p99 2.7 ms | 160K, p99 2.1 ms | 147K, p99 2.4 ms |
| 1:1 mixed, uniform keys | 145K, p99 2.6 ms | 114K, p99 7.0 ms | 104K, p99 10.8 ms |
| 1:10, pipeline 16 | 1.05M | 1.31M | 1.03M |

The 1:1 row looked like a clear win for ShardCache, so it was repeated
three more times (10 s each, same settings):

| 1:1 mixed, repeated | ShardCache | Redis | Memcached |
|---|---:|---:|---:|
| run 1 | 213K, p99 2.2 ms | 214K, p99 1.6 ms | 209K, p99 2.4 ms |
| run 2 | 210K, p99 2.2 ms | 213K, p99 1.6 ms | 208K, p99 2.4 ms |
| run 3 | 205K, p99 2.2 ms | 209K, p99 1.6 ms | 204K, p99 2.4 ms |

Conclusions that survive the repetition:

- **Throughput is on par with Redis and Memcached** for unpipelined
  traffic: all three sit at about 210K ops/s. At that point the load
  generator, which shares the 8 cores, is the bottleneck, so this shows
  that ShardCache adds no overhead a client can measure. It does not show
  that any of the three is faster. The single-run gaps in the first table
  were machine noise.
- **Redis has the best tail latency** (p99 1.6 ms versus 2.2 ms for
  ShardCache and 2.4 ms for Memcached).
- **Pipelined throughput is in the same range for all three, and noisy.**
  Pipelining removes per-request syscalls and measures command execution.
  Across runs on this machine ShardCache measured 1.05M–2.80M ops/s,
  Redis 1.31M–2.41M and Memcached 1.03M–2.77M. A dedicated machine with
  the client on separate cores would be needed to rank them.

## 3-node cluster

One memtier instance per node (any node coordinates any key), each 2
threads × 25 connections, 1:4 SET:GET, 15 s. Three nodes plus three load
generators on one 8-core laptop, with all replication traffic over
loopback.

| N | R | W | Mode | Ops/s (sum) | p50 (ms) | Worst p99 (ms) |
|---|---|---|---|---:|---:|---:|
| 3 | 2 | 2 | linearizable (ABD) | 68.7K | 1.35 | 15.8 |
| 3 | 2 | 2 | quorum | 66.4K | 1.55 | 15.2 |
| 3 | 1 | 3 | quorum, read-one / write-all | 112.3K | 0.63 | 9.3 |
| 3 | 1 | 1 | quorum, eventual | 122.0K | 0.98 | 7.5 |
| 1 | 1 | 1 | no replication | 80.8K | 1.10 | 11.4 |

- With replicas in agreement, linearizable mode costs nothing measurable
  here: write-back only happens when replicas disagree, and the version
  query is a cheap loopback round trip. Under 10 ms injected network
  jitter, the chaos harness saw it complete about 45% fewer operations
  than quorum mode ([consistency.md](consistency.md)). The cost scales
  with network latency.
- R=1 configurations serve reads from the local replica without any RPC.
- In an A/B run on a quieter machine, the same quorum configuration
  measured 96K ops/s.

## Storage engine (in-process)

`build/cache_benchmark`: pre-populated 10K keys, latency measured per
operation.

| Workload | Threads | Single lock | 16 shards | Speedup |
|---|---:|---:|---:|---:|
| 80% GET / 20% SET | 4 | 1.84M ops/s | 9.53M ops/s | 5.2× |
| 80% GET / 20% SET | 16 | 1.22M ops/s | 5.56M ops/s | 4.6× |
| 95% GET / 5% SET | 4 | 3.15M ops/s | 10.28M ops/s | 3.3× |
| 95% GET / 5% SET | 16 | 1.23M ops/s | 7.67M ops/s | 6.2× |

At 16 threads, p99 latency drops from 236 µs (single lock) to 56 µs.

## Optimization history

Every change below was found by measuring (profiling with `sample`, A/B
against the previous commit) rather than by guessing, and is a separate
commit.

| Change | Before | After |
|---|---|---|
| Release build by default (README numbers came from a debug build) | 471K ops/s | 1.2M ops/s (single lock, 16 threads) |
| Reads under the shared lock (throttled LRU promotion) | 7.3M | 13.0M ops/s (95/5 mix, 4 threads, in-process) |
| Pooled peer connections | 5.8K | 13.0K SET/s (3 nodes, 1 client) |
| Peer RPCs off the I/O threads | deadlock at 4 clients | 50K SET/s at 50 clients |
| Lock-free precomputed routing table | 845K | 2.80M ops/s (single node, pipelined) |
| Async pipelined peer channels | 51.6K | 96.0K ops/s (3 nodes, N=3 R=W=2) |
| Hash finalizer + 256 vnodes | busiest node 1.33× fair share | 1.03× |
| Interruptible background threads | 34 s | 1.5 s (integration suite) |
