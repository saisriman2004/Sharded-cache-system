# ShardCache

A high-performance distributed in-memory cache system written in C++20 featuring concurrent access, O(1) LRU eviction, active/lazy TTL expiration, Boost.Asio async networking, inter-node TCP request forwarding, synchronous/asynchronous replication, live TCP PING health checking with failover, SingleFlight stampede protection, 16-shard lock partitioning, multi-workload performance benchmarking, Docker Compose cluster orchestration, and GitHub Actions CI integration.

```text
                           CLIENTS
                              │
                         TCP Protocol
                              │
                              ▼
                     ┌──────────────────┐
                     │  ShardCache Node │
                     └────────┬─────────┘
                              │
                      Consistent Hash Ring
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
            ▼                 ▼                 ▼
       ┌─────────┐       ┌─────────┐       ┌─────────┐
       │ Node A  │◄─────►│ Node B  │◄─────►│ Node C  │
       ├─────────┤  TCP  ├─────────┤  TCP  ├─────────┤
       │ Sharded │ Forward │ Sharded │ Forward │ Sharded │
       │ Cache   │ & Repl  │ Cache   │ & Repl  │ Cache   │
       │ LRU/TTL │         │ LRU/TTL │         │ LRU/TTL │
       └─────────┘         └─────────┘         └─────────┘
            ▲                 ▲                 ▲
            └──────── Health Checks ────────────┘
```

---

## Key Features

- **C++20 & Standard Concurrency**: Fine-grained `std::shared_mutex` for multi-reader / single-writer synchronization.
- **O(1) Self-Implemented LRU Eviction**: Doubly-linked list (`std::list`) + hash map (`std::unordered_map`) storing list iterators.
- **Lazy & Active TTL Expiration**: High-precision time point checks combined with background thread active purging (`TTLManager`).
- **Inter-Node TCP Request Forwarding**: Automatic request routing based on FNV-1a consistent hash ring node ownership.
- **Real Multi-Node Replication**: Synchronous/Asynchronous replication across backup replica cluster nodes over TCP.
- **Live TCP Socket Health Checker**: Automatic node PING/PONG socket probes with 3-strike failure threshold for dynamic hash ring eviction and failover.
- **SingleFlight Stampede Protection**: Coalesces duplicate concurrent cache miss requests for identical keys to eliminate backend thundering herd, supporting `get_or_load(key, loader)` with C++ exception propagation.
- **Sharded Local Cache Partitioning**: Fine-grained lock partitioning across 16 independent shards delivering over **6.7x throughput improvement** under high thread contention.
- **Safe Boost.Asio Networking & Text Protocol**: High-throughput async TCP server with owned response lifetime management, serialized session write queues, and support for `SET`, `GET`, `DELETE`, `EXISTS`, `EXPIRE`, `TTL`, `STATS`, and `PING`.
- **Docker Compose Cluster Orchestration**: 3-node interconnected containerized cluster configuration (`nodeA`, `nodeB`, `nodeC`).
- **GoogleTest & Benchmarks**: 27 unit/integration tests with 100% pass rate and comprehensive multi-workload benchmark suite.

---

## Supported Protocol Commands

| Command | Format | Response |
| :--- | :--- | :--- |
| `SET` | `SET key value [TTL seconds]` | `OK` |
| `GET` | `GET key` | `VALUE value` or `NOT_FOUND` |
| `DELETE` | `DELETE key` | `OK` or `NOT_FOUND` |
| `EXISTS` | `EXISTS key` | `INT 1` or `INT 0` |
| `EXPIRE` | `EXPIRE key seconds` | `OK` or `NOT_FOUND` |
| `TTL` | `TTL key` | `INT seconds` or `INT -1` |
| `STATS` | `STATS` | Metrics summary |
| `PING` | `PING` | `PONG` |

---

## Performance Benchmark Results

Measured on macOS C++20 Apple Silicon (Pre-populated workload, 16 threads):

### 1. Typical Cache Workload (80% GET / 20% SET)
| Target Implementation | Threads | Throughput (Ops/sec) | $p50$ Latency | $p95$ Latency | $p99$ Latency |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Single Lock Cache** | 1 | 2,222,517 ops/sec | 0.2 µs | 0.3 µs | 0.4 µs |
| **16-Shard Cache** | 1 | **1,934,147 ops/sec** | **0.3 µs** | **0.4 µs** | **0.5 µs** |
| **Single Lock Cache** | 16 | 471,460 ops/sec | 1.9 µs | 164.2 µs | 294.0 µs |
| **16-Shard Cache** | 16 | 🚀 **3,189,100 ops/sec** | **0.5 µs** | **28.0 µs** | **56.1 µs** |

### 2. Read-Heavy Workload (95% GET / 5% SET)
| Target Implementation | Threads | Throughput (Ops/sec) | $p50$ Latency | $p95$ Latency | $p99$ Latency |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Single Lock Cache** | 4 | 1,098,870 ops/sec | 1.2 µs | 14.8 µs | 26.8 µs |
| **16-Shard Cache** | 4 | **4,702,854 ops/sec** | **0.4 µs** | **2.3 µs** | **6.6 µs** |
| **Single Lock Cache** | 16 | 481,210 ops/sec | 2.0 µs | 158.3 µs | 287.4 µs |
| **16-Shard Cache** | 16 | 🚀 **3,206,785 ops/sec** | **0.5 µs** | **28.0 µs** | **54.5 µs** |

---

## Quick Start

### 1. Build & Run Unit Tests
```bash
mkdir -p build && cd build
cmake ..
cmake --build .
./cache_test
```

### 2. Run Workload Benchmarks
```bash
./cache_benchmark
```

### 3. Run Standalone Node or Multi-Node Cluster
```bash
# Standalone Server
./build/shardcache_app 7001

# Docker Multi-Node Cluster
docker-compose up --build
```

### 4. Run Python Load Generator
```bash
python3 scripts/load_test.py 7001 16 1000
```

---

## License

[MIT License](LICENSE)
