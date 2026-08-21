#!/usr/bin/env python3
import socket
import time
import sys
import concurrent.futures

HOST = '127.0.0.1'
PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9090
CONCURRENT_CLIENTS = int(sys.argv[2]) if len(sys.argv) > 2 else 16
OPS_PER_CLIENT = int(sys.argv[3]) if len(sys.argv) > 3 else 1000

def send_command(sock, cmd):
    sock.sendall((cmd + '\r\n').encode('utf-8'))
    data = sock.recv(1024)
    return data.decode('utf-8')

def run_client(client_id):
    hits = 0
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((HOST, PORT))
            for i in range(OPS_PER_CLIENT):
                key = f"user_{client_id}_{i}"
                val = f"data_{i}"
                send_command(s, f"SET {key} {val}")
                res = send_command(s, f"GET {key}")
                if "VALUE" in res:
                    hits += 1
    except Exception as e:
        print(f"Client {client_id} error: {e}")
    return hits

def main():
    print(f"Starting load test on {HOST}:{PORT} with {CONCURRENT_CLIENTS} concurrent clients ({OPS_PER_CLIENT * 2} ops/client)...")
    start_time = time.time()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_CLIENTS) as executor:
        futures = [executor.submit(run_client, i) for i in range(CONCURRENT_CLIENTS)]
        total_hits = sum(f.result() for f in concurrent.futures.as_completed(futures))
        
    duration = time.time() - start_time
    total_ops = CONCURRENT_CLIENTS * OPS_PER_CLIENT * 2
    ops_per_sec = total_ops / duration if duration > 0 else 0

    print(f"Load test completed in {duration:.2f} seconds.")
    print(f"Total Operations: {total_ops}")
    print(f"Successful Hits: {total_hits}")
    print(f"Throughput: {ops_per_sec:.2f} ops/sec")

if __name__ == '__main__':
    main()
