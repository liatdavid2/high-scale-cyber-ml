
import json
import os
import threading
import time
from collections import defaultdict, deque
from pathlib import Path

import redis
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer
from pydantic import BaseModel, Field

STATIC_DIR = Path(__file__).resolve().parent / "static"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "unsw-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "feature-service")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
FEATURE_TTL_SECONDS = int(os.getenv("FEATURE_TTL_SECONDS", "3600"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))
STREAMING_SERVICE_URL = os.getenv("STREAMING_SERVICE_URL", "http://streaming-service:8000").rstrip("/")

BENCHMARK_RATES = [250, 500, 750, 1000, 1500, 2000, 3000, 5000]

app = FastAPI(
    title="Feature Service",
    description="Stage 3 of high-scale-cyber-ml",
    version="3.0.0",
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class BenchmarkRequest(BaseModel):
    dataset: str = "training"
    duration_seconds: int = Field(default=30, ge=10, le=120)


class Runtime:
    def __init__(self):
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.running = False

        self.kafka_status = "UNKNOWN"
        self.redis_status = "UNKNOWN"
        self.consumer_status = "STOPPED"

        self.total_events = 0
        self.total_features = 0
        self.redis_writes = 0
        self.errors = 0
        self.last_error = None

        self.events_per_sec = 0.0
        self.features_per_sec = 0.0
        self.redis_writes_per_sec = 0.0

        self._events_window = 0
        self._features_window = 0
        self._redis_window = 0
        self._window_started = time.monotonic()

        self.latencies_ms = deque(maxlen=5000)
        self.last_event_ts = None
        self.started_at = None

        self.proto_events = defaultdict(deque)
        self.service_events = defaultdict(deque)
        self.total_bytes_events = deque()

        self.benchmark_running = False
        self.benchmark_stop_event = threading.Event()
        self.benchmark_thread = None
        self.benchmark_results = []
        self.recommended = None
        self.benchmark_progress = {
            "current": 0,
            "total": len(BENCHMARK_RATES),
            "status": "IDLE",
            "message": "Not started",
        }

    def reset_metrics(self, clear_state=False):
        with self.lock:
            self.total_events = 0
            self.total_features = 0
            self.redis_writes = 0
            self.errors = 0
            self.last_error = None

            self.events_per_sec = 0.0
            self.features_per_sec = 0.0
            self.redis_writes_per_sec = 0.0

            self._events_window = 0
            self._features_window = 0
            self._redis_window = 0
            self._window_started = time.monotonic()

            self.latencies_ms.clear()
            self.last_event_ts = None

            if clear_state:
                self.proto_events.clear()
                self.service_events.clear()
                self.total_bytes_events.clear()

    def update_rates(self):
        now = time.monotonic()
        elapsed = now - self._window_started
        if elapsed >= 1.0:
            self.events_per_sec = round(self._events_window / elapsed, 1)
            self.features_per_sec = round(self._features_window / elapsed, 1)
            self.redis_writes_per_sec = round(self._redis_window / elapsed, 1)

            self._events_window = 0
            self._features_window = 0
            self._redis_window = 0
            self._window_started = now

    def percentile(self, p):
        values = sorted(self.latencies_ms)
        if not values:
            return 0.0
        idx = min(int((len(values) - 1) * p), len(values) - 1)
        return round(values[idx], 2)

    def snapshot(self):
        with self.lock:
            self.update_rates()

            freshness_ms = 0.0
            if self.last_event_ts:
                freshness_ms = max((time.time() - self.last_event_ts) * 1000, 0)

            uptime = int(time.time() - self.started_at) if self.started_at else 0

            return {
                "running": self.running,
                "benchmark_running": self.benchmark_running,
                "consumer_status": self.consumer_status,
                "kafka_status": self.kafka_status,
                "redis_status": self.redis_status,
                "topic": KAFKA_TOPIC,
                "group_id": KAFKA_GROUP_ID,
                "total_events": self.total_events,
                "total_features": self.total_features,
                "redis_writes": self.redis_writes,
                "errors": self.errors,
                "events_per_sec": self.events_per_sec,
                "features_per_sec": self.features_per_sec,
                "redis_writes_per_sec": self.redis_writes_per_sec,
                "latency_p50_ms": self.percentile(0.50),
                "latency_p95_ms": self.percentile(0.95),
                "latency_p99_ms": self.percentile(0.99),
                "feature_freshness_ms": round(freshness_ms, 1),
                "uptime_seconds": uptime,
                "last_error": self.last_error,
            }


runtime = Runtime()


def as_float(payload, key, default=0.0):
    try:
        value = payload.get(key, default)
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def prune_queue(q, now_ts):
    cutoff = now_ts - WINDOW_SECONDS
    while q and q[0] < cutoff:
        q.popleft()


def compute_features(event: dict) -> dict:
    payload = event.get("payload", {})
    now_ts = float(event.get("event_ts") or time.time())

    sbytes = as_float(payload, "sbytes")
    dbytes = as_float(payload, "dbytes")
    spkts = as_float(payload, "spkts")
    dpkts = as_float(payload, "dpkts")
    dur = as_float(payload, "dur")
    rate = as_float(payload, "rate")

    proto = str(payload.get("proto", "unknown"))
    service = str(payload.get("service", "unknown"))

    total_bytes = sbytes + dbytes
    total_packets = spkts + dpkts

    proto_q = runtime.proto_events[proto]
    service_q = runtime.service_events[service]

    prune_queue(proto_q, now_ts)
    prune_queue(service_q, now_ts)

    proto_q.append(now_ts)
    service_q.append(now_ts)

    while runtime.total_bytes_events and runtime.total_bytes_events[0][0] < now_ts - WINDOW_SECONDS:
        runtime.total_bytes_events.popleft()
    runtime.total_bytes_events.append((now_ts, total_bytes))

    rolling_avg_total_bytes = 0.0
    if runtime.total_bytes_events:
        rolling_avg_total_bytes = (
            sum(v for _, v in runtime.total_bytes_events)
            / len(runtime.total_bytes_events)
        )

    return {
        "event_id": event.get("event_id"),
        "event_ts": now_ts,
        "source_dataset": event.get("source_dataset"),
        "raw": {
            "dur": dur,
            "rate": rate,
            "proto": proto,
            "service": service,
        },
        "derived": {
            "total_bytes": round(total_bytes, 3),
            "total_packets": round(total_packets, 3),
            "byte_ratio_src_dst": round(sbytes / (dbytes + 1.0), 6),
            "packet_ratio_src_dst": round(spkts / (dpkts + 1.0), 6),
            "bytes_per_packet": round(total_bytes / (total_packets + 1.0), 6),
            "proto_events_last_60s": len(proto_q),
            "service_events_last_60s": len(service_q),
            "avg_total_bytes_last_60s": round(rolling_avg_total_bytes, 3),
        },
    }


def create_redis():
    client = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    client.ping()
    return client


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=KAFKA_GROUP_ID,
        client_id="feature-service-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        max_poll_records=1000,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )


def worker():
    consumer = None
    redis_client = None

    try:
        runtime.consumer_status = "CONNECTING"

        redis_client = create_redis()
        runtime.redis_status = "CONNECTED"

        consumer = create_consumer()
        runtime.kafka_status = "CONNECTED"
        runtime.consumer_status = "RUNNING"

        while not runtime.stop_event.is_set():
            records = consumer.poll(timeout_ms=500, max_records=1000)

            for _, messages in records.items():
                for msg in messages:
                    if runtime.stop_event.is_set():
                        break

                    started = time.perf_counter()

                    try:
                        event = msg.value
                        features = compute_features(event)

                        event_id = features.get("event_id") or f"{msg.partition}:{msg.offset}"
                        key = f"features:{event_id}"

                        redis_client.setex(
                            key,
                            FEATURE_TTL_SECONDS,
                            json.dumps(features),
                        )

                        latency_ms = (time.perf_counter() - started) * 1000

                        with runtime.lock:
                            runtime.total_events += 1
                            runtime.total_features += 1
                            runtime.redis_writes += 1
                            runtime._events_window += 1
                            runtime._features_window += 1
                            runtime._redis_window += 1
                            runtime.latencies_ms.append(latency_ms)
                            runtime.last_event_ts = features["event_ts"]
                            runtime.update_rates()

                    except Exception as exc:
                        with runtime.lock:
                            runtime.errors += 1
                            runtime.last_error = str(exc)

    except Exception as exc:
        with runtime.lock:
            runtime.errors += 1
            runtime.last_error = str(exc)
            runtime.consumer_status = "ERROR"
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass
        runtime.consumer_status = "STOPPED"


def start_feature_worker():
    if runtime.running:
        return

    runtime.stop_event.clear()
    runtime.running = True
    runtime.started_at = time.time()
    runtime.thread = threading.Thread(
        target=worker,
        daemon=True,
        name="feature-worker",
    )
    runtime.thread.start()


def stop_feature_worker():
    if not runtime.running:
        return

    runtime.stop_event.set()

    if runtime.thread:
        runtime.thread.join(timeout=5)

    runtime.running = False


def start_streaming(dataset, target_rate):
    response = requests.post(
        f"{STREAMING_SERVICE_URL}/api/start",
        json={
            "dataset": dataset,
            "target_rate": target_rate,
            "consumers": 1,
        },
        timeout=10,
    )

    if response.status_code >= 400:
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = response.text
        raise RuntimeError(f"Stage 2 could not start: {detail}")


def stop_streaming():
    try:
        requests.post(
            f"{STREAMING_SERVICE_URL}/api/stop",
            timeout=10,
        )
    except Exception:
        pass


def classify_result(result):
    if result["errors"] > 0:
        return "ERROR"

    throughput_ratio = (
        result["features_per_sec"] / result["input_rate"]
        if result["input_rate"]
        else 0
    )

    if (
        throughput_ratio >= 0.95
        and result["p95_ms"] <= 20
        and result["freshness_ms"] <= 2000
    ):
        return "STABLE"

    if (
        throughput_ratio >= 0.90
        and result["p95_ms"] <= 50
        and result["freshness_ms"] <= 10000
    ):
        return "NEAR_LIMIT"

    return "BOTTLENECK"


def choose_recommended(results):
    stable = [r for r in results if r["result"] == "STABLE"]
    if not stable:
        stable = [r for r in results if r["result"] == "NEAR_LIMIT"]

    if not stable:
        return None

    stable.sort(
        key=lambda r: (
            -r["features_per_sec"],
            r["p95_ms"],
            r["freshness_ms"],
        )
    )
    return stable[0]


def benchmark_loop(dataset, duration_seconds):
    try:
        runtime.benchmark_results = []
        runtime.recommended = None
        runtime.benchmark_progress = {
            "current": 0,
            "total": len(BENCHMARK_RATES),
            "status": "RUNNING",
            "message": "Preparing benchmark...",
        }

        start_feature_worker()
        time.sleep(1)

        for index, rate in enumerate(BENCHMARK_RATES, start=1):
            if runtime.benchmark_stop_event.is_set():
                runtime.benchmark_progress = {
                    "current": index - 1,
                    "total": len(BENCHMARK_RATES),
                    "status": "STOPPED",
                    "message": "Benchmark stopped.",
                }
                return

            runtime.benchmark_progress = {
                "current": index,
                "total": len(BENCHMARK_RATES),
                "status": "RUNNING",
                "message": f"Testing {rate} events/sec",
            }

            stop_streaming()
            runtime.reset_metrics(clear_state=True)
            time.sleep(1)

            start_streaming(dataset, rate)

            warmup = max(2, int(duration_seconds * 0.2))
            time.sleep(warmup)

            samples = []

            for _ in range(max(duration_seconds - warmup, 1)):
                if runtime.benchmark_stop_event.is_set():
                    break

                time.sleep(1)
                samples.append(runtime.snapshot())

            stop_streaming()
            final = runtime.snapshot()

            if not samples:
                samples = [final]

            result = {
                "input_rate": rate,
                "events_per_sec": round(
                    sum(s["events_per_sec"] for s in samples) / len(samples),
                    1,
                ),
                "features_per_sec": round(
                    sum(s["features_per_sec"] for s in samples) / len(samples),
                    1,
                ),
                "redis_writes_per_sec": round(
                    sum(s["redis_writes_per_sec"] for s in samples) / len(samples),
                    1,
                ),
                "p50_ms": round(
                    sum(s["latency_p50_ms"] for s in samples) / len(samples),
                    2,
                ),
                "p95_ms": round(
                    sum(s["latency_p95_ms"] for s in samples) / len(samples),
                    2,
                ),
                "p99_ms": round(
                    sum(s["latency_p99_ms"] for s in samples) / len(samples),
                    2,
                ),
                "freshness_ms": round(
                    sum(s["feature_freshness_ms"] for s in samples) / len(samples),
                    1,
                ),
                "errors": final["errors"],
                "duration_seconds": duration_seconds,
            }

            result["result"] = classify_result(result)
            runtime.benchmark_results.append(result)

            # Adaptive stop once a clear bottleneck is found.
            if result["result"] == "BOTTLENECK":
                break

            time.sleep(1)

        runtime.recommended = choose_recommended(runtime.benchmark_results)
        runtime.benchmark_progress = {
            "current": len(runtime.benchmark_results),
            "total": len(runtime.benchmark_results),
            "status": "COMPLETED",
            "message": "Feature benchmark completed.",
        }

    except Exception as exc:
        runtime.last_error = str(exc)
        runtime.benchmark_progress = {
            "current": len(runtime.benchmark_results),
            "total": len(BENCHMARK_RATES),
            "status": "ERROR",
            "message": str(exc),
        }
    finally:
        stop_streaming()
        runtime.benchmark_running = False


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "feature-service",
        "topic": KAFKA_TOPIC,
        "group_id": KAFKA_GROUP_ID,
        "redis": f"{REDIS_HOST}:{REDIS_PORT}",
        "streaming_service": STREAMING_SERVICE_URL,
    }


@app.get("/api/metrics")
def metrics():
    return runtime.snapshot()


@app.get("/api/benchmark")
def benchmark():
    return {
        "running": runtime.benchmark_running,
        "progress": runtime.benchmark_progress,
        "results": runtime.benchmark_results,
        "recommended": runtime.recommended,
        "rates": BENCHMARK_RATES,
    }


@app.post("/api/start")
def start():
    if runtime.running:
        raise HTTPException(
            status_code=409,
            detail="Feature service is already running.",
        )

    runtime.reset_metrics(clear_state=True)
    start_feature_worker()
    return {"status": "started"}


@app.post("/api/stop")
def stop():
    if runtime.benchmark_running:
        runtime.benchmark_stop_event.set()
        stop_streaming()
        return {"status": "benchmark_stop_requested"}

    stop_feature_worker()
    return {"status": "stopped", "metrics": runtime.snapshot()}


@app.post("/api/reset")
def reset():
    if runtime.running or runtime.benchmark_running:
        raise HTTPException(
            status_code=409,
            detail="Stop processing before resetting metrics.",
        )

    runtime.reset_metrics(clear_state=True)
    runtime.started_at = None
    runtime.kafka_status = "UNKNOWN"
    runtime.redis_status = "UNKNOWN"
    runtime.benchmark_results = []
    runtime.recommended = None
    runtime.benchmark_progress = {
        "current": 0,
        "total": len(BENCHMARK_RATES),
        "status": "IDLE",
        "message": "Not started",
    }

    return {"status": "reset"}


@app.post("/api/benchmark/start")
def start_benchmark(request: BenchmarkRequest):
    if runtime.benchmark_running:
        raise HTTPException(
            status_code=409,
            detail="Feature benchmark is already running.",
        )

    if runtime.running:
        stop_feature_worker()

    runtime.benchmark_stop_event.clear()
    runtime.benchmark_running = True
    runtime.benchmark_results = []
    runtime.recommended = None

    runtime.benchmark_thread = threading.Thread(
        target=benchmark_loop,
        args=(request.dataset, request.duration_seconds),
        daemon=True,
        name="feature-benchmark",
    )
    runtime.benchmark_thread.start()

    return {
        "status": "started",
        "rates": BENCHMARK_RATES,
        "duration_seconds": request.duration_seconds,
    }
