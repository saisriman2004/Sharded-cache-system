#include "shardcache/snapshot.hpp"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <vector>

namespace shardcache {

namespace {

constexpr char kMagic[8] = {'S', 'C', 'S', 'N', 'A', 'P', '0', '1'};

int64_t now_unix_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
}

class Writer {
public:
    void bytes(const void* data, std::size_t n) {
        const auto* p = static_cast<const unsigned char*>(data);
        buf_.insert(buf_.end(), p, p + n);
    }
    template <typename T>
    void pod(T v) { bytes(&v, sizeof(v)); }
    void str(const std::string& s) {
        pod<uint32_t>(static_cast<uint32_t>(s.size()));
        bytes(s.data(), s.size());
    }
    std::vector<unsigned char>& buffer() { return buf_; }

private:
    std::vector<unsigned char> buf_;
};

class Reader {
public:
    Reader(const unsigned char* data, std::size_t size) : p_(data), end_(data + size) {}
    bool bytes(void* out, std::size_t n) {
        if (static_cast<std::size_t>(end_ - p_) < n) return false;
        std::memcpy(out, p_, n);
        p_ += n;
        return true;
    }
    template <typename T>
    bool pod(T& v) { return bytes(&v, sizeof(v)); }
    bool str(std::string& s) {
        uint32_t n = 0;
        if (!pod(n) || static_cast<std::size_t>(end_ - p_) < n) return false;
        s.assign(reinterpret_cast<const char*>(p_), n);
        p_ += n;
        return true;
    }
    bool at_end() const { return p_ == end_; }

private:
    const unsigned char* p_;
    const unsigned char* end_;
};

uint64_t checksum(const unsigned char* data, std::size_t n) {
    uint64_t h = 1469598103934665603ull;
    for (std::size_t i = 0; i < n; ++i) {
        h ^= data[i];
        h *= 1099511628211ull;
    }
    return h;
}

void set_error(std::string* error, const std::string& msg) {
    if (error) *error = msg;
}

} // namespace

std::optional<SnapshotStats> save_snapshot(const ShardedCache& cache, const std::string& path, std::string* error) {
    Writer w;
    w.bytes(kMagic, sizeof(kMagic));
    w.pod<uint64_t>(0);  // entry count, patched below
    uint64_t count = 0;
    const int64_t now = now_unix_ms();
    cache.for_each_shard_snapshot([&](std::vector<std::pair<std::string, VersionedValue>>& batch) {
        for (const auto& [key, v] : batch) {
            w.str(key);
            w.str(v.value);
            w.pod<uint64_t>(v.version);
            w.pod<uint8_t>(v.tombstone ? 1 : 0);
            w.pod<int64_t>(v.ttl.has_value() ? now + v.ttl->count() : -1);
            ++count;
        }
    });
    auto& buf = w.buffer();
    std::memcpy(buf.data() + sizeof(kMagic), &count, sizeof(count));
    uint64_t sum = checksum(buf.data(), buf.size());
    w.pod(sum);

    const std::string tmp = path + ".tmp";
    {
        std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
        if (!out) {
            set_error(error, "cannot open " + tmp);
            return std::nullopt;
        }
        out.write(reinterpret_cast<const char*>(buf.data()), static_cast<std::streamsize>(buf.size()));
        out.flush();
        if (!out) {
            set_error(error, "write failed for " + tmp);
            return std::nullopt;
        }
    }
    if (std::rename(tmp.c_str(), path.c_str()) != 0) {
        set_error(error, "rename to " + path + " failed");
        return std::nullopt;
    }
    return SnapshotStats{count, buf.size()};
}

std::optional<SnapshotStats> load_snapshot(ShardedCache& cache, const std::string& path, std::string* error) {
    std::ifstream in(path, std::ios::binary);
    if (!in) {
        set_error(error, "no snapshot at " + path);
        return std::nullopt;
    }
    std::vector<unsigned char> buf((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    if (buf.size() < sizeof(kMagic) + 2 * sizeof(uint64_t) || std::memcmp(buf.data(), kMagic, sizeof(kMagic)) != 0) {
        set_error(error, "not a ShardCache snapshot");
        return std::nullopt;
    }
    uint64_t stored_sum = 0;
    std::memcpy(&stored_sum, buf.data() + buf.size() - sizeof(stored_sum), sizeof(stored_sum));
    if (checksum(buf.data(), buf.size() - sizeof(stored_sum)) != stored_sum) {
        set_error(error, "checksum mismatch (corrupt or truncated snapshot)");
        return std::nullopt;
    }

    Reader r(buf.data() + sizeof(kMagic), buf.size() - sizeof(kMagic) - sizeof(stored_sum));
    uint64_t count = 0;
    if (!r.pod(count)) {
        set_error(error, "truncated header");
        return std::nullopt;
    }
    // Decode everything first: a malformed file must not half-apply.
    struct Entry {
        std::string key;
        VersionedValue value;
        int64_t expiry;
    };
    std::vector<Entry> entries;
    for (uint64_t i = 0; i < count; ++i) {
        Entry e;
        uint8_t tombstone = 0;
        if (!r.str(e.key) || !r.str(e.value.value) || !r.pod(e.value.version) || !r.pod(tombstone) || !r.pod(e.expiry)) {
            set_error(error, "truncated entry " + std::to_string(i));
            return std::nullopt;
        }
        e.value.tombstone = tombstone != 0;
        entries.push_back(std::move(e));
    }
    if (!r.at_end()) {
        set_error(error, "trailing bytes after entries");
        return std::nullopt;
    }

    const int64_t now = now_unix_ms();
    uint64_t applied = 0;
    for (auto& e : entries) {
        if (e.expiry >= 0) {
            if (e.expiry <= now) continue;  // expired while the node was down
            e.value.ttl = std::chrono::milliseconds(e.expiry - now);
        }
        // Tombstones restart their retention window via the cache's tombstone TTL.
        if (e.value.tombstone) e.value.ttl.reset();
        cache.apply(e.key, e.value);
        ++applied;
    }
    return SnapshotStats{applied, buf.size()};
}

} // namespace shardcache
