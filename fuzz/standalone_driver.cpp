// Runs the fuzz target without libFuzzer (e.g. Apple clang): every corpus
// file, then random mutations of them for a fixed number of iterations.
// Build with ASan/UBSan to make memory errors fatal.
//   resp_fuzzer_standalone <corpus-dir> [iterations] [seed]
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size);

int main(int argc, char** argv) {
    if (argc < 2) {
        std::fprintf(stderr, "usage: %s <corpus-dir> [iterations] [seed]\n", argv[0]);
        return 2;
    }
    const long iterations = argc > 2 ? std::atol(argv[2]) : 200000;
    std::mt19937_64 rng(argc > 3 ? std::strtoull(argv[3], nullptr, 10) : 42);

    std::vector<std::string> corpus;
    for (const auto& entry : std::filesystem::directory_iterator(argv[1])) {
        std::ifstream in(entry.path(), std::ios::binary);
        corpus.emplace_back(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    if (corpus.empty()) corpus.emplace_back("*1\r\n$4\r\nPING\r\n");

    for (const auto& seed : corpus) {
        LLVMFuzzerTestOneInput(reinterpret_cast<const uint8_t*>(seed.data()), seed.size());
    }

    const std::string interesting = "*$+-:\r\n0123456789-SETGETDELPX ";
    for (long i = 0; i < iterations; ++i) {
        std::string input = corpus[rng() % corpus.size()];
        int mutations = 1 + static_cast<int>(rng() % 8);
        for (int m = 0; m < mutations; ++m) {
            std::size_t pos = input.empty() ? 0 : rng() % (input.size() + 1);
            switch (rng() % 6) {
                case 0: if (!input.empty() && pos < input.size()) input[pos] = static_cast<char>(rng()); break;
                case 1: input.insert(pos, 1, interesting[rng() % interesting.size()]); break;
                case 2: if (pos < input.size()) input.erase(pos, 1 + rng() % 4); break;
                case 3: input.insert(pos, input.substr(0, rng() % (input.size() + 1))); break;  // duplicate a prefix
                case 4: input.insert(pos, std::to_string(static_cast<int64_t>(rng()) >> (rng() % 60))); break;
                case 5: input += corpus[rng() % corpus.size()]; break;  // pipelining
            }
        }
        if (input.size() > 1 << 16) input.resize(1 << 16);
        LLVMFuzzerTestOneInput(reinterpret_cast<const uint8_t*>(input.data()), input.size());
    }
    std::printf("ran %zu corpus inputs and %ld mutated inputs without failure\n", corpus.size(), iterations);
    return 0;
}
