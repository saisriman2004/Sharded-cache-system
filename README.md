# ShardCache

A high-performance distributed in-memory cache written in C++20 featuring concurrent access, LRU eviction, TTL expiration, TCP networking, consistent hashing with virtual nodes, SingleFlight stampede protection, lock partitioning, performance benchmarking, and CI integration.

```text
                           CLIENTS
                              │
                         TCP / HTTP
                              │
                              ▼
                    ┌──────────────────┐
                    │   Request Router │
                    └────────┬─────────┘
                             │
                     Consistent Hash Ring
                             │
            ┌────────────────┼────────────────┐
            │                │                │
            ▼                ▼                ▼
       ┌─────────┐      ┌─────────┐      ┌─────────┐
       │ Node A  │      │ Node B  │      │ Node C  │
       ├─────────┤      ├─────────┤      ├─────────┤
       │ KV Map  │      │ KV Map  │      │ KV Map  │
       │ LRU     │      │ LRU     │      │ LRU     │
       │ TTL     │      │ TTL     │      │ TTL     │
       │ Metrics │      │ Metrics │      │ Metrics │
       └────┬────┘      └────┬────┘      └────┬────┘
            │                │                │
            └──────── Replication ────────────┘
```

## Features

- **C++20 & Standard Concurrency**: `std::shared_mutex` for multi-reader / single-writer synchronization.
- **O(1) Self-Implemented LRU**: Doubly-linked list + hash map storing list iterators.
- **Lazy & Active TTL Expiration**: High-precision time point checks and background purging.
- **Virtual Node Consistent Hashing**: Balanced keyspace distribution using FNV-1a hashing with configurable virtual nodes per physical host.
- **SingleFlight Stampede Protection**: Coalesces concurrent cache miss requests for identical keys to eliminate backend thundering herd.
- **Sharded Local Cache**: Fine-grained lock partitioning across $N$ shards (over **7x throughput** under thread contention).
- **Boost.Asio Networking & Text Protocol**: High-throughput async TCP server supporting `SET`, `GET`, `DELETE`, `EXISTS`, `EXPIRE`, `TTL`, `STATS`, and `PING`.
- **GoogleTest & Benchmarks**: 100% pass rate across unit, concurrency, and performance benchmarks.

---

## Supported Operations

| Command | Format | Response |
| :--- | :--- | :--- |
| `SET` | `SET key value [TTL seconds]` | `OK` |
| `GET` | `GET key` | `VALUE value` or `NOT_FOUND` |
| `DELETE` | `DELETE key` | `OK` or `NOT_FOUND` |
| `EXISTS` | `EXISTS key` | `INT 1` or `INT 0` |
| `EXPIRE` | `EXPIRE key seconds` | `OK` or `NOT_FOUND` |
| `STATS` | `STATS` | Metrics summary |
| `PING` | `PING` | `PONG` |

---

## Quick Start

### Build & Run Unit Tests

```bash
mkdir -p build && cd build
cmake ..
cmake --build .
ctest --output-on-failure
```

### Run Server

```bash
./build/shardcache_app
```

### Run Benchmarks

```bash
./build/cache_benchmark
```

---

## Performance Benchmarks

Measured on Apple Silicon (16 threads, mixed 70% GET / 30% SET workload):

| Target Implementation | Threads | Ops/sec | p50 Latency | p95 Latency | p99 Latency |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Single Lock Cache** | 1 | 1,757,271 | 0.3 µs | 0.6 µs | 1.2 µs |
| **16-Shard Cache** | 1 | 1,553,581 | 0.4 µs | 0.8 µs | 0.9 µs |
| **Single Lock Cache** | 16 | 501,091 | 1.5 µs | 153.8 µs | 317.5 µs |
| **16-Shard Cache** | 16 | **3,623,251** | **0.5 µs** | **25.9 µs** | **51.2 µs** |

---

## License

MIT License.
