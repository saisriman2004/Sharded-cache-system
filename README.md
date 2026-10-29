# ShardCache

A distributed, Redis-compatible in-memory cache in C++20, with leaderless
quorum replication, gossip membership, live rebalancing and an optional
linearizable mode. The consistency guarantees were verified with a
Jepsen-style fault-injection harness and the Porcupine linearizability
checker.

```bash
redis-cli -p 7001 SET user:42 '{"name":"Ada"}' EX 300   # any Redis client or tool works
redis-cli -p 7003 GET user:42                           # any node serves any key
```

## Highlights

- **Speaks RESP2.** Works with `redis-cli`, `redis-benchmark`,
  `memtier_benchmark` and any Redis client library: `GET/SET [EX|PX]`,
  `DEL`, `EXISTS`, `EXPIRE`, `TTL/PTTL`, `INFO`, `DBSIZE` and more, with
  request pipelining.
- **Throughput on par with Redis 8 and Memcached 1.6** in head-to-head
  memtier runs on the same machine (about 210K ops/s unpipelined,
  client-bound; 1–2.8M ops/s pipelined). [Benchmarks →](docs/benchmarks.md)
- **Dynamo-style replication.** N replicas per key on a consistent-hash
  ring, tunable R/W quorums, hybrid-logical-clock versions, read repair,
  hinted handoff and tombstones. The default N=3, R=W=2 tolerates any one
  node failing.
- **Tested for consistency, not just asserted.** The chaos harness
  partitions, pauses and crashes nodes under load. It found two real
  anomalies in the quorum design (new-old inversion, and lost writes from
  lagging clocks); `CONSISTENCY=linearizable` fixes both with the ABD
  algorithm and passed every fault scenario. [Consistency report →](docs/consistency.md)
- **Elastic.** Nodes join through any seed by gossip, data streams to new
  owners automatically, and `SIGTERM` hands a node's data off before it
  exits (883 keys rebalanced in 44 ms).
- **Operable.** Prometheus `/metrics` and `/health`, a Grafana dashboard,
  versioned snapshots for crash recovery, Docker Compose and a Kubernetes
  StatefulSet.
- **Engineered by measurement.** Each optimization was profiled and A/B
  tested: lock-free routing (3.3× pipelined throughput), async pipelined
  replication (+86% cluster throughput), shared-lock reads (1.8×), and a
  cluster deadlock found and fixed. [History →](docs/benchmarks.md#optimization-history)

## Architecture

```
 client ──RESP──▶ any node ──┬─ local shard (16-way sharded LRU, TTL)
                             └─ coordinator ── N replicas on the hash ring
                                  write: version (HLC) → all N, ack after W
                                  read:  ask all N, newest of R wins, repair stale
                                  miss:  hinted handoff when a replica returns
 membership: gossip (join via seed, graceful leave) → rebalancing
```

How a request flows, placement, replication, membership and the trade-offs
are covered in [docs/design.md](docs/design.md).

## Quick start

Requires CMake ≥ 3.20, a C++20 compiler and Boost (header-only Asio):
`brew install boost` or `apt install libboost-dev`.

```bash
cmake -S . -B build && cmake --build build -j
ctest --test-dir build                 # 74 tests: unit, 3-node integration, membership, fuzz smoke

./build/shardcache_app 7001            # single node on :7001, metrics on :9001
redis-cli -p 7001 PING
```

**Local 3-node cluster** (B and C join A by gossip):

```bash
scripts/start_cluster.sh build
redis-cli -p 7001 SET k v && redis-cli -p 7002 GET k
redis-cli -p 7001 CLUSTER NODES
```

**Docker Compose** (3 nodes, Prometheus on :9090, Grafana on :3000):

```bash
docker compose up --build
```

**Kubernetes:** `kubectl apply -f deploy/k8s/shardcache.yaml` (3-pod
StatefulSet; scale it with `kubectl scale`).

## Configuration

All settings are environment variables. `PORT` and `NODE_ID` can also be
given as positional arguments.

| Variable | Default | Meaning |
|---|---|---|
| `PORT` / `NODE_ID` | `7001` / `nodeA` | Client and peer port; unique node name |
| `ADVERTISE_HOST` | `127.0.0.1` | Address peers use to reach this node |
| `SEEDS` | — | `host:port,...` of existing members to join through |
| `CLUSTER_PEERS` | — | Static peer list `id:host:port,...` (alternative to seeds) |
| `REPLICATION_FACTOR` | `3` | N: copies of each key |
| `READ_QUORUM` / `WRITE_QUORUM` | majority of N | R and W |
| `CONSISTENCY` | `quorum` | `quorum` or `linearizable` (ABD) |
| `CACHE_CAPACITY` | `1000000` | Max entries per node (LRU eviction) |
| `IO_THREADS` / `RPC_THREADS` | cores / `64` | Network threads / threads for commands waiting on peers |
| `METRICS_PORT` | `PORT + 2000` | `/metrics` and `/health`; `0` disables |
| `SNAPSHOT_PATH` / `SNAPSHOT_INTERVAL_SEC` | — / `60` | Enable snapshots and their period |
| `LEAVE_ON_SHUTDOWN` | `1` | On SIGTERM, hand data off before exiting |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Testing

| Layer | What runs |
|---|---|
| Unit (52) | Cache, LRU, TTL, versioning and LWW, HLC, RESP codec, parser, snapshots, ring balance |
| Integration (21) | Real 3-node clusters over TCP: node down (detected or not), quorum loss, hinted handoff, read repair, convergence under concurrent writers, pipelining order, both consistency modes, join, rebalance, graceful leave |
| Sanitizers | Every test under ASan, TSan and UBSan in CI |
| Fuzzing | libFuzzer on the RESP parser with a round-trip property (60 s in CI; 2M local inputs) |
| Chaos | `chaos/`: partitions, pauses, crash-restart and jitter, checked by Porcupine ([results](docs/consistency.md#results-on-the-final-implementation)) |

## Repository layout

```
include/shardcache, src/   storage engine, cluster coordinator, membership, snapshots
protocol/                  RESP2 codec and command parser
network/                   sessions, servers, peer transports, metrics endpoint
tests/, fuzz/              GoogleTest suites, fuzz target and corpus
chaos/                     Go fault-injection harness (Porcupine)
benchmarks/                memtier comparison, cluster and in-process benchmarks
deploy/                    Prometheus, Grafana, Kubernetes
docs/                      design, consistency report, benchmarks
```

## License

[MIT](LICENSE)
