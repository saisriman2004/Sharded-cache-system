#ifndef SHARDCACHE_SNAPSHOT_HPP
#define SHARDCACHE_SNAPSHOT_HPP

#include "shardcache/sharded_cache.hpp"

#include <cstdint>
#include <optional>
#include <string>

namespace shardcache {

// Point-in-time dump of a node's cache so a restarted node does not come
// back empty. Entries keep their versions and tombstones, so a restored
// node merges cleanly with newer data from peers (last-writer-wins), and
// TTLs are stored as absolute wall-clock deadlines.
//
// Format (little endian): "SCSNAP01", u64 entry count, then per entry
// u32 key length, key, u32 value length, value, u64 version, u8 tombstone,
// i64 expiry (Unix ms, -1 = none), and finally a u64 FNV-1a checksum of
// everything before it. Writes go to a temp file renamed into place, so a
// crash mid-write never leaves a torn snapshot.
struct SnapshotStats {
    uint64_t entries{0};
    uint64_t bytes{0};
};

// Returns nullopt (and sets `error`) on I/O failure.
std::optional<SnapshotStats> save_snapshot(const ShardedCache& cache, const std::string& path, std::string* error = nullptr);

// Applies every unexpired entry with last-writer-wins. Returns nullopt if
// the file is missing, truncated, corrupt or of another format.
std::optional<SnapshotStats> load_snapshot(ShardedCache& cache, const std::string& path, std::string* error = nullptr);

} // namespace shardcache

#endif // SHARDCACHE_SNAPSHOT_HPP
