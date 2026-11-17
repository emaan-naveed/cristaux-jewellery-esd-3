"""
traffic_a3.py — Fixed-rate traffic generator for Assignment 3.

Sends HTTP GET /api/v1/products to Envoy HTTP port (8080) and
gRPC GetOrder calls to Envoy gRPC port (50050) at a fixed rate
without waiting for prior responses before dispatching the next request.

Usage:
    py -3 traffic_a3.py [--http-rate N] [--grpc-rate N] [--duration S]
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import threading
import time
import uuid

import grpc
import requests

from proto import order_service_pb2, order_service_pb2_grpc

# ---------------------------------------------------------------------------
# Statistics tracking (guarded by lock)
# ---------------------------------------------------------------------------
_lock = threading.Lock()

http_sent = 0
http_success = 0
http_failure = 0
http_latencies: list[float] = []

grpc_sent = 0
grpc_success = 0
grpc_failure = 0
grpc_latencies: list[float] = []

_stop_event = threading.Event()


def p95(values: list[float]) -> float:
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    idx = int(len(sorted_vals) * 0.95)
    idx = min(idx, len(sorted_vals) - 1)
    return sorted_vals[idx]


# ---------------------------------------------------------------------------
# Request dispatchers
# ---------------------------------------------------------------------------

def send_one_http(url: str, session: requests.Session) -> None:
    global http_sent, http_success, http_failure
    req_id = str(uuid.uuid4())
    t0 = time.perf_counter()
    try:
        resp = session.get(
            url,
            headers={"x-request-id": req_id},
            timeout=5.0,
        )
        latency = time.perf_counter() - t0
        with _lock:
            http_sent += 1
            http_latencies.append(latency)
            if resp.status_code < 400:
                http_success += 1
            else:
                http_failure += 1
    except Exception:
        latency = time.perf_counter() - t0
        with _lock:
            http_sent += 1
            http_failure += 1
            http_latencies.append(latency)


def send_one_grpc(stub: order_service_pb2_grpc.OrderServiceStub) -> None:
    global grpc_sent, grpc_success, grpc_failure
    req_id = str(uuid.uuid4())
    t0 = time.perf_counter()
    try:
        stub.GetOrder(
            order_service_pb2.GetOrderRequest(order_id="nonexistent-order-a3"),
            metadata=[("x-request-id", req_id)],
            timeout=5.0,
        )
        latency = time.perf_counter() - t0
        with _lock:
            grpc_sent += 1
            grpc_success += 1
            grpc_latencies.append(latency)
    except grpc.RpcError as exc:
        latency = time.perf_counter() - t0
        with _lock:
            grpc_sent += 1
            grpc_latencies.append(latency)
            # NOT_FOUND (5) is an expected valid response for a non-existent order
            if exc.code() == grpc.StatusCode.NOT_FOUND:
                grpc_success += 1
            else:
                grpc_failure += 1
    except Exception:
        latency = time.perf_counter() - t0
        with _lock:
            grpc_sent += 1
            grpc_failure += 1
            grpc_latencies.append(latency)


# ---------------------------------------------------------------------------
# Fixed-rate schedulers (dispatch without waiting for prior responses)
# ---------------------------------------------------------------------------

def run_http_scheduler(rate: float, pool: ThreadPoolExecutor) -> None:
    interval = 1.0 / rate if rate > 0 else 1.0
    url = "http://localhost:8080/api/v1/products"
    session = requests.Session()

    next_time = time.perf_counter()
    while not _stop_event.is_set():
        pool.submit(send_one_http, url, session)
        next_time += interval
        sleep_dur = next_time - time.perf_counter()
        if sleep_dur > 0:
            _stop_event.wait(timeout=sleep_dur)
        else:
            # Caught up if lagging
            next_time = time.perf_counter()


def run_grpc_scheduler(rate: float, pool: ThreadPoolExecutor) -> None:
    interval = 1.0 / rate if rate > 0 else 1.0
    channel = grpc.insecure_channel("localhost:50050")
    stub = order_service_pb2_grpc.OrderServiceStub(channel)

    next_time = time.perf_counter()
    while not _stop_event.is_set():
        pool.submit(send_one_grpc, stub)
        next_time += interval
        sleep_dur = next_time - time.perf_counter()
        if sleep_dur > 0:
            _stop_event.wait(timeout=sleep_dur)
        else:
            next_time = time.perf_counter()

    channel.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Assignment 3 Part A/B fixed-rate workload generator")
    parser.add_argument("--http-rate", type=float, default=5.0,
                        help="HTTP requests per second dispatched to Envoy :8080 (default: 5.0)")
    parser.add_argument("--grpc-rate", type=float, default=2.0,
                        help="gRPC calls per second dispatched to Envoy :50050 (default: 2.0)")
    parser.add_argument("--duration", type=float, default=30.0,
                        help="Benchmark duration in seconds (default: 30.0)")
    args = parser.parse_args()

    print("=" * 64)
    print(" CRISTAUX JEWELLERY — Assignment 3 Workload Generator")
    print(f" Target: Envoy Proxy (HTTP :8080, gRPC :50050)")
    print(f" Config: http-rate={args.http_rate} rps | grpc-rate={args.grpc_rate} rps | duration={args.duration}s")
    print(" Dispatching at fixed rate without waiting for response...")
    print("=" * 64)

    pool = ThreadPoolExecutor(max_workers=32)

    t_http = threading.Thread(target=run_http_scheduler, args=(args.http_rate, pool), daemon=True)
    t_grpc = threading.Thread(target=run_grpc_scheduler, args=(args.grpc_rate, pool), daemon=True)

    t_http.start()
    t_grpc.start()

    time.sleep(args.duration)
    _stop_event.set()

    t_http.join(timeout=5)
    t_grpc.join(timeout=5)
    pool.shutdown(wait=True)

    with _lock:
        h_sent = http_sent
        h_ok = http_success
        h_fail = http_failure
        h_p95 = p95(http_latencies) * 1000

        g_sent = grpc_sent
        g_ok = grpc_success
        g_fail = grpc_failure
        g_p95 = p95(grpc_latencies) * 1000

    print()
    print("=" * 64)
    print(" WORKLOAD RESULTS SUMMARY")
    print("=" * 64)
    print(f" HTTP  | Rate: {args.http_rate:.1f} rps | Sent: {h_sent:4d} | Success: {h_ok:4d} | Fail: {h_fail:4d} | Client p95: {h_p95:6.2f} ms")
    print(f" gRPC  | Rate: {args.grpc_rate:.1f} rps | Sent: {g_sent:4d} | Success: {g_ok:4d} | Fail: {g_fail:4d} | Client p95: {g_p95:6.2f} ms")
    print("=" * 64)


if __name__ == "__main__":
    main()
