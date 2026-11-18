"""
traffic_a3.py - Fixed-rate traffic generator for Assignment 3.

Sends HTTP GET /api/v1/products to Envoy (:8080) and unary gRPC GetOrder
calls to Envoy (:50050) at a fixed rate, without waiting for earlier
responses before dispatching the next request (open-loop).

The gRPC order must EXIST, otherwise every call returns NOT_FOUND and the
run measures the error path. Pass a real ID with --grpc-order-id.

Usage:
    py -3 traffic_a3.py --grpc-order-id <ID> [--http-rate 5] [--grpc-rate 2]
                        [--duration 60] [--label part-b-baseline]

Counting rules:
    Sent    = requests dispatched during the timed window
    Success = HTTP status < 400, gRPC status OK
    Fail    = anything else (including gRPC NOT_FOUND, timeouts, 5xx)
"""

import argparse
import csv
import os
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import grpc
import requests
from requests.adapters import HTTPAdapter

from proto import order_service_pb2, order_service_pb2_grpc

HTTP_URL = "http://localhost:8080/api/v1/products"
GRPC_TARGET = "localhost:50050"

_lock = threading.Lock()
_stop_event = threading.Event()


class Stats:
    def __init__(self) -> None:
        self.dispatched = 0
        self.success = 0
        self.failure = 0
        self.latencies: list[float] = []
        self.errors: dict[str, int] = {}

    def record(self, ok: bool, latency: float, err: str = "") -> None:
        with _lock:
            if ok:
                self.success += 1
            else:
                self.failure += 1
                self.errors[err] = self.errors.get(err, 0) + 1
            self.latencies.append(latency)


http_stats = Stats()
grpc_stats = Stats()


def percentile(values: list[float], q: float) -> float:
    """Nearest-rank percentile."""
    if not values:
        return 0.0
    s = sorted(values)
    rank = max(1, int(round(q * len(s) + 0.5)))
    return s[min(rank, len(s)) - 1]


def new_ids() -> dict[str, str]:
    trace_id = uuid.uuid4().hex  # 32 hex chars
    return {"x-request-id": str(uuid.uuid4()), "x-trace-id": trace_id}


# ---------------------------------------------------------------------------
# Single requests
# ---------------------------------------------------------------------------

def send_one_http(session: requests.Session, stats: Stats | None) -> None:
    t0 = time.perf_counter()
    try:
        resp = session.get(HTTP_URL, headers=new_ids(), timeout=5.0)
        ok = resp.status_code < 400
        err = "" if ok else f"HTTP {resp.status_code}"
    except Exception as exc:
        ok, err = False, type(exc).__name__
    if stats is not None:
        stats.record(ok, time.perf_counter() - t0, err)


def send_one_grpc(stub, order_id: str, stats: Stats | None) -> None:
    t0 = time.perf_counter()
    try:
        stub.GetOrder(
            order_service_pb2.GetOrderRequest(order_id=order_id),
            metadata=list(new_ids().items()),
            timeout=5.0,
        )
        ok, err = True, ""
    except grpc.RpcError as exc:
        ok, err = False, f"gRPC {exc.code().name}"
    except Exception as exc:
        ok, err = False, type(exc).__name__
    if stats is not None:
        stats.record(ok, time.perf_counter() - t0, err)


# ---------------------------------------------------------------------------
# Fixed-rate schedulers
# ---------------------------------------------------------------------------

def schedule(rate: float, stats: Stats, submit_fn) -> None:
    interval = 1.0 / rate
    next_time = time.perf_counter()
    while not _stop_event.is_set():
        with _lock:
            stats.dispatched += 1
        submit_fn()
        next_time += interval
        delay = next_time - time.perf_counter()
        if delay > 0:
            _stop_event.wait(timeout=delay)
        else:
            next_time = time.perf_counter()  # lagging: do not burst


def main() -> None:
    p = argparse.ArgumentParser(description="Assignment 3 fixed-rate workload")
    p.add_argument("--http-rate", type=float, default=5.0)
    p.add_argument("--grpc-rate", type=float, default=2.0)
    p.add_argument("--duration", type=float, default=60.0)
    p.add_argument("--grpc-order-id", required=True,
                   help="ID of an order that EXISTS (otherwise all calls are NOT_FOUND)")
    p.add_argument("--warmup", type=float, default=3.0,
                   help="seconds of untimed, unrecorded traffic first (default 3)")
    p.add_argument("--label", default="",
                   help="run name; appends one row per interface to results.csv")
    args = p.parse_args()

    pool = ThreadPoolExecutor(max_workers=64)

    session = requests.Session()
    session.mount("http://", HTTPAdapter(pool_connections=4, pool_maxsize=64))
    channel = grpc.insecure_channel(GRPC_TARGET)  # one connection, as in the brief
    stub = order_service_pb2_grpc.OrderServiceStub(channel)

    # Fail fast if the order does not exist.
    try:
        stub.GetOrder(order_service_pb2.GetOrderRequest(order_id=args.grpc_order_id),
                      timeout=5.0)
    except grpc.RpcError as exc:
        print(f"ERROR: GetOrder('{args.grpc_order_id}') returned {exc.code().name}. "
              "Use an order ID that exists.", file=sys.stderr)
        sys.exit(2)

    print("=" * 64)
    print(" CRISTAUX JEWELLERY - Assignment 3 Workload Generator")
    print(f" Target: Envoy (HTTP :8080, gRPC :50050)  label='{args.label}'")
    print(f" Config: http={args.http_rate} rps | grpc={args.grpc_rate} rps | "
          f"duration={args.duration}s | warmup={args.warmup}s")
    print("=" * 64)

    # Warm-up: connection setup and first-call cost are excluded.
    end_warm = time.perf_counter() + args.warmup
    while time.perf_counter() < end_warm:
        pool.submit(send_one_http, session, None)
        pool.submit(send_one_grpc, stub, args.grpc_order_id, None)
        time.sleep(0.2)

    start = datetime.now(timezone.utc)
    t_start = time.perf_counter()

    threads = [
        threading.Thread(target=schedule, daemon=True, args=(
            args.http_rate, http_stats,
            lambda: pool.submit(send_one_http, session, http_stats))),
        threading.Thread(target=schedule, daemon=True, args=(
            args.grpc_rate, grpc_stats,
            lambda: pool.submit(send_one_grpc, stub, args.grpc_order_id, grpc_stats))),
    ]
    for t in threads:
        t.start()

    time.sleep(args.duration)
    _stop_event.set()
    elapsed = time.perf_counter() - t_start
    end = datetime.now(timezone.utc)
    for t in threads:
        t.join(timeout=5)
    pool.shutdown(wait=True)  # let in-flight requests finish
    channel.close()

    rows = []
    print()
    print("=" * 64)
    print(" WORKLOAD RESULTS SUMMARY")
    print(f" Window (UTC): {start:%H:%M:%S} -> {end:%H:%M:%S}  ({elapsed:.1f}s)")
    print("=" * 64)
    for name, rate, st in (("HTTP", args.http_rate, http_stats),
                           ("gRPC", args.grpc_rate, grpc_stats)):
        actual = st.dispatched / elapsed if elapsed else 0.0
        p50 = percentile(st.latencies, 0.50) * 1000
        p95 = percentile(st.latencies, 0.95) * 1000
        err_rate = 100.0 * st.failure / st.dispatched if st.dispatched else 0.0
        print(f" {name} | target {rate:.1f} / actual {actual:.2f} rps | "
              f"Sent {st.dispatched} | OK {st.success} | Fail {st.failure} "
              f"({err_rate:.1f}%) | p50 {p50:.1f} ms | p95 {p95:.1f} ms")
        if st.errors:
            print(f"      errors: {st.errors}")
        rows.append([args.label, name, rate, f"{actual:.2f}", st.dispatched,
                     st.success, st.failure, f"{err_rate:.2f}",
                     f"{p50:.2f}", f"{p95:.2f}",
                     start.isoformat(timespec="seconds"),
                     end.isoformat(timespec="seconds")])
    print("=" * 64)

    if args.label:
        new_file = not os.path.exists("results.csv")
        with open("results.csv", "a", newline="") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["label", "interface", "target_rps", "actual_rps",
                            "sent", "success", "fail", "error_pct",
                            "p50_ms", "p95_ms", "start_utc", "end_utc"])
            w.writerows(rows)


if __name__ == "__main__":
    main()
