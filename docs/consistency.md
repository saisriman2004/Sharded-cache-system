# Consistency: what ShardCache guarantees, and how it was tested

ShardCache replicates every key to N nodes with Dynamo-style quorums. That
design is known to be *not* linearizable in general. Rather than assert
which guarantees hold, this document shows how they were measured with a
Jepsen-style harness, what it found, and what was changed.

## TL;DR

| Mode | `CONSISTENCY=` | Guarantee | Cost |
|---|---|---|---|
| Quorum (default) | `quorum` | Every acknowledged write reaches W replicas; reads see a version at least as new as some quorum holds. **Not linearizable.** | 1 round trip per op |
| Linearizable | `linearizable` | Linearizable per key while a quorum is reachable (ABD algorithm). | Up to 1 extra round trip |

Both modes stay available for writes and reads as long as W (resp. R) of the
N replicas answer. With N=3, R=W=2 that means one node can be down,
partitioned or paused.

## How it was tested

`chaos/` is a Go harness modelled on [Jepsen](https://jepsen.io):

1. Starts a real 3-node cluster (separate processes). **Every node-to-node
   link goes through a fault-injecting TCP proxy**, so the harness controls
   the network between nodes.
2. Runs 12 concurrent clients against 3 hot keys: 50% `GET`, 40% `SET` with
   a unique value per write, 10% `DEL`. Every operation is recorded with its
   invocation and completion time.
3. A nemesis injects faults every few seconds:
   - **partition**: blackhole every link to and from one node (bytes are
     dropped, not refused, so peers see timeouts as in a real partition)
   - **pause**: `SIGSTOP` a node for 2 s, then `SIGCONT` (a long GC pause or
     a stalled VM)
   - **crash-restart**: `SIGKILL` a node and start it again with empty memory
   - **latency**: random 0–20 ms delay per message on every link, which
     widens race windows that are microseconds wide on localhost
4. Checks the history with [Porcupine](https://github.com/anishathalye/porcupine)
   against a per-key register model.

Two details matter for soundness:

- **A failed or timed-out write is indeterminate, not failed.** It may have
  reached some replicas, and read repair or hints can spread it later. Such
  writes are recorded as possibly taking effect any time after they were
  invoked. Failed reads are dropped: they have no effect.
- Besides Porcupine's yes/no verdict, the harness reports two **definite
  anomaly classes** in a form a human can check from timestamps alone:
  - *stale read*: a read returned the value of write w1, although another
    write w2 started after w1 finished and finished before the read began;
  - *new-old inversion*: a read returned w's value, and a later read
    (starting after the first finished) returned the value of a write that
    finished before w even began.

Run it with `chaos/run_suite.sh build/shardcache_app 25`.

## Finding 1: new-old inversion in quorum mode

Seen under process pauses with 10 ms jitter (Porcupine: `Illegal`, one
inversion). The mechanism:

```
time ─────────────────────────────────────────────────────────────────▶
client 1   SET k=v2 ─────────────────────────────────────────── (slow)
             └ coordinator A applies v2 locally, RPCs to B, C in flight
client 2        GET k via A: quorum {A, B} → A has v2 → returns v2
client 3                             GET k via C: quorum {C, B} → both v1 → returns v1
```

Client 3's read started after client 2's finished, yet returned an older
value. Read repair would eventually fix B and C, but it is asynchronous, so
too late.

## Finding 2: an acknowledged write can be lost

Writes are versioned with a hybrid logical clock (HLC) on the coordinator.
A coordinator that did not take part in an earlier, completed write has not
observed its version, and can stamp a *later* write with a *lower* version.
Last-writer-wins then silently discards the later write, although it was
acknowledged to its client. With real clock skew between machines the
window is the size of the skew.

The chaos run turns this up as *stale reads* (3 in the crash-restart
scenario, where a restarted node's clock state is lost).
`QuorumModeLosesWriteFromCoordinatorWithLaggingClock` in
`tests/integration_test.cpp` reproduces it deterministically on every CI
run.

## The fix: ABD (Attiya, Bar-Noy, Dolev)

`CONSISTENCY=linearizable` adds the two phases of the multi-writer ABD
register on top of the existing quorums:

- **Reads write back.** If the replicas that answered disagree, the
  coordinator writes the winning value to a write quorum *before*
  replying. Once any read has returned a value, every later read quorum
  intersects a quorum that holds it, so finding 1 cannot happen.
- **Writes query versions first.** The coordinator reads the highest
  version from a read quorum, advances its clock past it, then writes.
  Since read and write quorums intersect, a write always stamps above
  every completed write, so finding 2 cannot happen.

The local fast path for reads is disabled in this mode (unless N = 1),
because a single replica cannot prove it holds the latest write.
`LinearizableClusterTest.*` covers both phases deterministically.

## Results on the final implementation

`chaos/run_suite.sh`, 20 s per scenario, 12 clients, 3 keys:

| Mode | Faults | OK ops | Failed / indeterminate writes | Stale reads | Inversions | Linearizable |
|---|---|---:|---:|---:|---:|---|
| quorum | none (20 ms jitter) | 10,215 | 0 | 0 | 0 | Ok |
| quorum | partitions | 28,652 | 31 | 0 | 0 | **Illegal** |
| quorum | process pauses | 15,575 | 8 | 0 | 0 | Ok |
| quorum | partitions + pauses | 15,440 | 18 | 0 | 0 | Ok |
| quorum | crash-restart | 96,678 | 21 | 3 | 0 | **Illegal** |
| linearizable | none (20 ms jitter) | 5,706 | 0 | 0 | 0 | Ok |
| linearizable | partitions | 15,424 | 53 | 0 | 0 | Ok |
| linearizable | process pauses | 9,022 | 9 | 0 | 0 | Ok |
| linearizable | partitions + pauses | 8,999 | 20 | 0 | 0 | Ok |
| linearizable | crash-restart | 56,325 | 23 | 0 | 0 | Ok |

Quorum mode's violations are timing-dependent: a scenario reported `Ok`
above can fail on another seed (pauses did in earlier runs). Linearizable
mode passed every run: the 5 above and 7 earlier ones during development
(partitions, pauses, both, 20 ms jitter, crash-restart).

**What this does and does not show.** The harness found real bugs, and no
violations of the linearizable mode under these faults. That is evidence,
not proof: a chaos test only explores the schedules it happens to hit.

## Limits

- **Losing a quorum loses data.** Memory is the primary store. A single
  crash-restart is survivable because the other two replicas still form a
  quorum and write-back repopulates the restarted node. If two of three
  replicas crash before repair, writes acknowledged only by them are
  gone. `SNAPSHOT_PATH` narrows this to the writes since the last
  snapshot.
- **Linearizable per key, not across keys.** There are no multi-key
  transactions.
- **Deletes rely on tombstones** that expire after `tombstone_ttl`
  (default 60 s). A replica that misses a delete and only comes back after
  the tombstone has expired everywhere could resurrect the key. Hinted
  handoff and read repair normally close this window well before then.
- **Clock skew.** HLC versions never run backwards on a node, but quorum
  mode's last-writer-wins order across coordinators follows physical
  clocks within the skew (finding 2). Linearizable mode does not depend on
  clock accuracy for correctness.
- **Failure detection is local.** Each node probes its peers directly
  (3 missed pings → routed around). Unreachable nodes are not removed from
  the ring, so a partition never moves data; only an explicit leave does.
