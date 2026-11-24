"""
loadtest.py - Experiment 3: N concurrent clients, each doing OPS roundtrips
(upload + download + checksum check). Run once per row of your table:

    python loadtest.py --clients 1 --ops 20
    python loadtest.py --clients 3 --ops 20
    python loadtest.py --clients 5 --ops 20
"""

import argparse
import os
import socket
import statistics
import threading
import time
from collections import Counter
from client import get, put


def worker(cid, args, start_gate, results):
    data = os.urandom(args.size_kib * 1024)
    name = f"load-{cid}.bin"              # one file per client, so no overwrites
    start_gate.wait()                     # all clients start at the same moment

    try:
        sock = socket.create_connection((args.host, args.port), timeout=30)

    except OSError as e:
        results.extend([(False, 0.0, f"connect failed: {e}")] * args.ops)
        return
    
    with sock:
        for _ in range(args.ops):
            t0 = time.perf_counter()
            try:
                put(sock, name, data)
                ok = get(sock, name) == data
                err = "" if ok else "data mismatch"

            except Exception as e:
                ok, err = False, f"{type(e).__name__}: {e}"

            results.append((ok, time.perf_counter() - t0, err))  # append is thread-safe


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=9000)
    ap.add_argument("--clients", type=int, default=1)
    ap.add_argument("--ops", type=int, default=20, help="roundtrips per client")
    ap.add_argument("--size-kib", type=int, default=256, help="file size per op")
    args = ap.parse_args()

    results = []
    gate = threading.Barrier(args.clients + 1)
    threads = [threading.Thread(target=worker, args=(i, args, gate, results))
               for i in range(args.clients)]
    
    for t in threads:
        t.start()

    gate.wait()
    t0 = time.perf_counter()

    for t in threads:
        t.join()
    elapsed = time.perf_counter() - t0

    times_ms = [t * 1000 for ok, t, _ in results if ok]
    total = args.clients * args.ops
    print(f"clients={args.clients}  ops/client={args.ops}  file={args.size_kib} KiB  "
          f"wall time={elapsed:.2f}s")
    
    print(f"successful roundtrips: {len(times_ms)}/{total}")

    if times_ms:
        mib = len(times_ms) * 2 * args.size_kib / 1024      # up + down

        print(f"response time: mean {statistics.mean(times_ms):.1f} ms, "
              f"median {statistics.median(times_ms):.1f} ms, max {max(times_ms):.1f} ms")
        print(f"throughput: {len(times_ms) / elapsed:.1f} roundtrips/s, {mib / elapsed:.1f} MiB/s")

    for err, n in Counter(e for ok, _, e in results if not ok).items():
        print(f"  {n}x {err}")


if __name__ == "__main__":
    main()
