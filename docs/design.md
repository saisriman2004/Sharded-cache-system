# ShardCache design

A distributed in-memory key-value cache that speaks the Redis protocol.
This document covers how a request flows through a node, how data is
placed and replicated, how nodes find each other, and why things are the
way they are.

```
                 redis-cli / any Redis client / memtier_benchmark
                                  │ RESP2 (pipelined)
            ┌─────────────────────▼──────────────────────┐
            │ node                                        │
            │  I/O threads (one io_context per core)      │
            │    ClientSession: parse → execute → reply   │
            │        │ local?                             │
            │        ├── yes → ShardedCache (16 shards)   │
            │        └── no  → RPC executor               │
            │                   Cluster coordinator       │
            │                   ├ RoutingTable (snapshot) │
            │                   ├ HybridClock             │
            │                   └ PeerChannels ───────────┼──▶ peers (REPL_SET / REPL_GET)
            │  background: health checker · gossip ·      │
            │  hinted handoff · rebalancer · TTL sweep ·  │
            │  snapshots · /metrics HTTP                  │
            └─────────────────────────────────────────────┘
```

## Request path

1. **Framing.** A `ClientSession` reads into a buffer and parses every
   complete request: RESP arrays or inline commands. The parser is
   incremental and binary-safe, and enforces size limits (64 MB bulk
   strings, 1M array elements, 64 KB inline lines) so a client cannot force
   huge allocations. It is fuzzed in CI.
2. **Local or remote.** `CommandHandler::runs_locally` decides whether the
   command can be answered from this node alone: a single node, a read
   with R=1 of a key this node replicates, or internal `REPL_*` RPCs. Those
   run inline on the I/O thread.
3. **Everything else goes to the RPC executor**, a separate thread pool,
   and the session stops parsing until the reply comes back. Pipelined
   commands behind it stay buffered, so replies always leave in request
   order.
4. **Replies are batched.** All replies produced from one read are sent in
   a single write.

*Why the executor?* Early on, cross-node calls ran on the I/O threads. With
four I/O threads per node and a handful of concurrent clients, node A's
threads blocked waiting on B while B's blocked waiting on A, and the
cluster deadlocked: every write timed out. Internal RPCs never need the
executor, so the dependency cycle is gone.

## Storage: `ShardedCache`

- 16 independent shards (key hash mod 16), each an `std::unordered_map`
  into an intrusive LRU `std::list`, guarded by its own `shared_mutex`.
- **Reads take the shared lock.** Moving an entry to the LRU head needs
  the exclusive lock, so reads only do it if the entry was not promoted in
  the last 100 ms (the approach memcached takes). Hot keys are then served
  entirely under shared locks. This took the read-heavy benchmark from
  7.3M to 13.0M ops/s at 4 threads.
- **TTL:** lazy (a read treats expired entries as absent) plus an active
  sweep every second.
- **Entries are versioned** (`version`, `tombstone`). `apply()` stores a
  write only if it supersedes the current entry under last-writer-wins:
  higher version wins; on a tie, deletes win, then the larger value, so
  every replica resolves identically.

## Placement

- **Consistent hashing** with 256 virtual nodes per node. Keys and vnodes
  are hashed with FNV-1a followed by the MurmurHash3 finalizer. Plain
  FNV-1a clustered vnodes of similar names (`node#1`, `node#2`): with 3
  nodes the busiest owned 1.33× its share, and adding vnodes made it
  worse. With the finalizer it is 1.03×.
- A key's **preference list** is the first N distinct nodes clockwise
  from its hash.
- **`RoutingTable`** is an immutable snapshot of the ring with every
  vnode's preference list precomputed. Membership changes publish a new
  table under a globally unique epoch; each thread caches the table and
  re-reads it only when the epoch changes. Routing a key is therefore one
  binary search with no lock or allocation. Before this, routing
  allocated a `std::set` per call and took a `shared_mutex` whose libc++
  implementation serializes readers; the change took single-node
  pipelined throughput from 845K to 2.8M ops/s.

## Replication

Leaderless, as in Dynamo. Any node coordinates any request.

- **Write:** stamp with the coordinator's hybrid logical clock (48-bit ms +
  16-bit counter; never goes backwards, always moves past versions it has
  seen). Send to all N replicas in parallel, and acknowledge once W
  replicas confirm.
- **Read:** query all N replicas, wait for R replies, and return the newest
  version. Replicas that answered with an older version (or nothing) are
  repaired in the background.
- **Hinted handoff:** a write that misses a replica (marked down, or the
  RPC failed) is queued on the coordinator and replayed once the replica is
  healthy. Queues are bounded (100K per node) and replay preserves TTLs.
- **Deletes** are versioned tombstones, kept for `tombstone_ttl` so that a
  delayed older write cannot resurrect the key.
- **Quorum failures are errors** (`-ERR write quorum not reached`), never
  silent misses. A failed write may still have reached some replicas;
  clients should treat it as unknown, as with any distributed store.
- **`CONSISTENCY=linearizable`** adds the ABD read write-back and write
  version query. See [consistency.md](consistency.md) for why, and the
  chaos test results.

Defaults: N=3, R=W=2 (R+W>N, so every read quorum overlaps the latest
write quorum). `REPLICATION_FACTOR`, `READ_QUORUM` and `WRITE_QUORUM`
trade consistency for latency; for example R=1, W=3 serves reads locally
and gave 1.7× the throughput of R=W=2 on a 1:4 SET:GET mix.

### Peer transport

Quorum fan-out uses **`PeerChannel`s**: asynchronous RESP connections, a
few per peer, driven by dedicated threads. Calls from any thread are queued
on a strand, everything queued while a write is in flight is coalesced into
the next write, and replies are matched to callbacks in FIFO order. Every
call has a deadline; on timeout or error the connection is reset and all
outstanding calls fail fast. This replaced one blocking pooled connection
per RPC and took quorum throughput from 51.6K to 96.0K ops/s. Background
work (hint replay, rebalancing, gossip) still uses the simpler blocking
pool.

## Membership

- **Gossip.** Every node owns one entry in a membership table, `{node,
  generation, alive|left}`, and bumps the generation whenever it changes
  it. The generation starts at the node's boot time in ms, so a restarted
  node supersedes its previous self. Merging two views is "higher
  generation wins", which is conflict-free. Every 500 ms a node does a
  push-pull exchange with a random live peer, so views converge in
  O(log n) rounds.
- **Join:** start with `SEEDS=host:port` pointing at any member.
- **Leave:** `SIGTERM` (or `Cluster::leave()`) announces `left` to every
  peer, then hands off all local data before the process exits.
- **Failure detection is separate from membership.** A health checker
  pings peers every second; 3 misses mark a peer unhealthy, so it is
  routed around and given hints, and 2 successes restore it. An
  unreachable node is never removed from the ring automatically, so a
  network partition never triggers data movement. Only an explicit leave
  does.

## Rebalancing

When the ring changes, a background pass walks a snapshot of each shard.
For every key whose preference list gained nodes, it streams the versioned
entry to those nodes; LWW makes duplicate pushes from several old owners
harmless. Keys the node no longer owns are dropped once handed off, and a
failed push is queued as a hint while the local copy is kept. Measured:
883 keys streamed to a joining node in 44 ms.

## Persistence

Optional. With `SNAPSHOT_PATH`, a node writes a snapshot every
`SNAPSHOT_INTERVAL_SEC` and on shutdown, and restores it before serving.
Snapshots keep versions and tombstones, so restoring merges with whatever
newer data peers have. TTLs are stored as absolute deadlines. The format
is a magic header, entries, and an FNV-1a checksum; files are written to a
temp file and renamed into place, and corrupt files are rejected whole. A
node that leaves deletes its snapshot so stale data is never replayed.

## Observability

`METRICS_PORT` (default: port + 2000) serves `/metrics` (Prometheus) and
`/health`. Metrics include per-command throughput, errors and latency
histograms, cache hits, misses, evictions and expirations, quorum
failures, read repairs, hint queue depth and delivery, rebalance volume,
cluster health, and connections. Hot-path counters are striped across
cache lines; an A/B run showed no measurable throughput cost.
`deploy/grafana` has a ready-made dashboard.

## Threading model

| Threads | Count | Work |
|---|---|---|
| Acceptor | 1 | Accepts connections, assigns each to an I/O context (round robin) |
| I/O | `IO_THREADS` (cores) | Parse, local execution, replies; one `io_context` each |
| RPC executor | `RPC_THREADS` (64) | Commands that wait on peers |
| Peer I/O | 2 | Drive `PeerChannel`s |
| Background | 1 each | Health checks, gossip, hint replay, rebalancing, TTL sweep, snapshots |

## Alternatives considered

- **Raft per shard** (strong consistency via leaders) instead of quorums:
  it would give linearizability by construction, but needs leader election
  and logs, and adds failover latency. For a cache, availability and tail
  latency matter more, so leaderless replication plus an opt-in ABD mode
  is the better fit.
- **Removing unreachable nodes from the ring automatically:** that turns
  every network blip into data movement, and a partition into two
  diverging rings. Explicit leave is safer.
- **Vector clocks** instead of a single HLC version: they detect concurrent
  writes instead of picking a winner, but push conflict resolution to
  clients. A cache has no meaningful merge function, so LWW with a
  deterministic tie-break was chosen.
- **CLOCK eviction** for lock-free reads: tried first. It either evicted
  freshly inserted keys or reordered old ones (both visible in tests).
  Throttled promotion keeps exact LRU behaviour where it matters.
