# syntax=docker/dockerfile:1

# --- Build stage -------------------------------------------------------------
FROM ubuntu:24.04 AS build
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake libboost-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY CMakeLists.txt ./
COPY include include
COPY src src
COPY protocol protocol
COPY network network
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF \
    && cmake --build build -j"$(nproc)" --target shardcache_app \
    && strip build/shardcache_app

# --- Runtime stage -----------------------------------------------------------
FROM ubuntu:24.04
RUN useradd --system --uid 10001 --no-create-home shardcache \
    && mkdir /data && chown shardcache /data
COPY --from=build /src/build/shardcache_app /usr/local/bin/shardcache

USER shardcache
# 7001: RESP clients and peers; 9001: Prometheus /metrics and /health
EXPOSE 7001 9001
ENV PORT=7001 \
    METRICS_PORT=9001 \
    SNAPSHOT_PATH=/data/shardcache.snap
VOLUME /data
ENTRYPOINT ["/usr/local/bin/shardcache"]
