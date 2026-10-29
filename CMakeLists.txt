cmake_minimum_required(VERSION 3.20)
project(ShardCache VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Default to an optimized build; benchmarks are meaningless without it.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

# Optional sanitizer builds: -DSHARDCACHE_SANITIZE=address|thread|undefined
set(SHARDCACHE_SANITIZE "" CACHE STRING "Sanitizer to enable (address, thread, undefined)")
if(SHARDCACHE_SANITIZE)
  add_compile_options(-fsanitize=${SHARDCACHE_SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${SHARDCACHE_SANITIZE})
endif()

# Compiler warnings
add_compile_options(-Wall -Wextra -Wpedantic)

if(POLICY CMP0167)
  cmake_policy(SET CMP0167 NEW)
endif()
find_package(Boost REQUIRED)
find_package(Threads REQUIRED)

add_library(shardcache_lib
  src/cache.cpp
  src/lru.cpp
  src/ttl_manager.cpp
  src/consistent_hash.cpp
  src/routing_table.cpp
  src/cluster.cpp
  src/health_checker.cpp
  src/single_flight.cpp
  src/sharded_cache.cpp
  src/snapshot.cpp
  src/logger.cpp
  protocol/parser.cpp
  protocol/resp.cpp
  network/command_handler.cpp
  network/session.cpp
  network/tcp_server.cpp
  network/client.cpp
  network/peer_pool.cpp
  network/peer_channel.cpp
  network/io_context_pool.cpp
  network/metrics_server.cpp
)

target_include_directories(shardcache_lib PUBLIC 
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/protocol
  ${CMAKE_CURRENT_SOURCE_DIR}/network
  ${Boost_INCLUDE_DIRS}
)

target_link_libraries(shardcache_lib PUBLIC Threads::Threads)

add_executable(shardcache_app
  src/main.cpp
)
target_link_libraries(shardcache_app PRIVATE shardcache_lib)

option(BUILD_TESTING "Build unit tests and benchmarks" ON)

if(BUILD_TESTING)
  enable_testing()

  include(FetchContent)
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  add_executable(cache_test
    tests/cache_test.cpp
    tests/lru_test.cpp
    tests/ttl_test.cpp
    tests/consistent_hash_test.cpp
    tests/cluster_test.cpp
    tests/health_test.cpp
    tests/single_flight_test.cpp
    tests/sharded_cache_test.cpp
    tests/protocol_test.cpp
    tests/network_test.cpp
    tests/versioning_test.cpp
    tests/snapshot_test.cpp
  )

  target_include_directories(cache_test PRIVATE
    ${googletest_SOURCE_DIR}/googletest/include
    ${googletest_SOURCE_DIR}/googlemock/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol
    ${CMAKE_CURRENT_SOURCE_DIR}/network
  )

  target_link_libraries(cache_test PRIVATE shardcache_lib gtest_main)

  add_executable(integration_test
    tests/integration_test.cpp
    tests/membership_test.cpp
  )
  target_include_directories(integration_test PRIVATE
    ${googletest_SOURCE_DIR}/googletest/include
    ${googletest_SOURCE_DIR}/googlemock/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/protocol
    ${CMAKE_CURRENT_SOURCE_DIR}/network
  )
  target_link_libraries(integration_test PRIVATE shardcache_lib gtest_main)

  add_executable(cache_benchmark
    benchmarks/cache_benchmark.cpp
  )
  target_link_libraries(cache_benchmark PRIVATE shardcache_lib)

  # Fuzzing the untrusted-input path. With clang + libFuzzer:
  #   -DSHARDCACHE_FUZZ=ON -DCMAKE_CXX_COMPILER=clang++  ->  ./resp_fuzzer fuzz/corpus
  # Everywhere else a standalone mutation driver is built instead.
  option(SHARDCACHE_FUZZ "Build the libFuzzer target (requires clang with libFuzzer)" OFF)
  if(SHARDCACHE_FUZZ)
    add_executable(resp_fuzzer fuzz/resp_fuzzer.cpp)
    target_compile_options(resp_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
    target_link_options(resp_fuzzer PRIVATE -fsanitize=fuzzer,address,undefined)
    target_link_libraries(resp_fuzzer PRIVATE shardcache_lib)
  else()
    add_executable(resp_fuzzer_standalone fuzz/resp_fuzzer.cpp fuzz/standalone_driver.cpp)
    target_link_libraries(resp_fuzzer_standalone PRIVATE shardcache_lib)
    add_test(NAME resp_fuzz_smoke
             COMMAND resp_fuzzer_standalone ${CMAKE_CURRENT_SOURCE_DIR}/fuzz/corpus 20000)
  endif()

  include(GoogleTest)
  gtest_discover_tests(cache_test)
  gtest_discover_tests(integration_test)
endif()
