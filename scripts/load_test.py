#!/usr/bin/env python3
"""Closed-loop RESP load generator: each client issues SET then GET and waits
for every reply. For serious measurements use memtier_benchmark or
redis-benchmark (see benchmarks/); this script is a dependency-free smoke test.
"""
import concurrent.futures
import socket
import sys
import time

HOST = '127.0.0.1'
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 7001
CONCURRENT_CLIENTS = int(sys.argv[2]) if len(sys.argv) > 2 else 16
OPS_PER_CLIENT = int(sys.argv[3]) if len(sys.argv) > 3 else 1000


def encode(*args):
    out = [f"*{len(args)}\r\n".encode()]
    for a in args:
        b = str(a).encode()
        out.append(f"${len(b)}\r\n".encode() + b + b"\r\n")
    return b"".join(out)


class Conn:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.buf = b""

    def _line(self):
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("server closed connection")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def call(self, *args):
        self.sock.sendall(encode(*args))
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"-":
            raise RuntimeError(rest.decode())
        if kind == b"$":
            n = int(rest)
            if n < 0:
                return None
            while len(self.buf) < n + 2:
                self.buf += self.sock.recv(65536)
            data, self.buf = self.buf[:n], self.buf[n + 2:]
            return data.decode()
        return rest.decode()


def run_client(client_id):
    conn = Conn(HOST, PORT)
    hits = 0
    for i in range(OPS_PER_CLIENT):
        key = f"user_{client_id}_{i}"
        conn.call("SET", key, f"data_{i}")
        if conn.call("GET", key) is not None:
            hits += 1
    return hits


def main():
    print(f"Load test on {HOST}:{PORT}: {CONCURRENT_CLIENTS} clients x {OPS_PER_CLIENT * 2} ops")
    start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_CLIENTS) as pool:
        futures = [pool.submit(run_client, i) for i in range(CONCURRENT_CLIENTS)]
        try:
            total_hits = sum(f.result() for f in futures)
        except Exception as e:  # any failed client invalidates the run
            print(f"FAILED: {e}", file=sys.stderr)
            sys.exit(1)
    duration = time.time() - start
    total_ops = CONCURRENT_CLIENTS * OPS_PER_CLIENT * 2
    print(f"Completed in {duration:.2f}s, {total_ops} ops, {total_hits} hits")
    print(f"Throughput: {total_ops / duration:.0f} ops/sec")


if __name__ == '__main__':
    main()
