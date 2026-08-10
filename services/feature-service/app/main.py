import json
import os
import threading
import time
from collections import defaultdict, deque
from pathlib import Path
from statistics import median

import redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaConsumer
from pydantic import BaseModel

STATIC_DIR = Path(__file__).resolve().parent / "static"

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "unsw-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "feature-service")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
FEATURE_TTL_SECONDS = int(os.getenv("FEATURE_TTL_SECONDS", "3600"))
WINDOW_SECONDS = int(os.getenv("WINDOW_SECONDS", "60"))

app = FastAPI(
    title="Feature Service",
    description="Stage 3 of high-scale-cyber-ml",
    version="1.0.0",
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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

        # Lightweight local rolling state.
        self.proto_events = defaultdict(deque)
        self.service_events = defaultdict(deque)
        self.total_bytes_events = deque()

    def reset_metrics(self):
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

    def snapshot(self):
        with self.lock:
            self.update_rates()
            latencies = sorted(self.latencies_ms)

            def pct(p):
                if not latencies:
                    return 0.0
                idx = min(int((len(latencies) - 1) * p), len(latencies) - 1)
                return round(latencies[idx], 2)

            freshness_ms = 0.0
            if self.last_event_ts:
                freshness_ms = max((time.time() - self.last_event_ts) * 1000, 0)

            uptime = int(time.time() - self.started_at) if self.started_at else 0

            return {
                "running": self.running,
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
                "latency_p50_ms": pct(0.50),
                "latency_p95_ms": pct(0.95),
                "latency_p99_ms": pct(0.99),
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

    rolling_total_bytes = 0.0
    if runtime.total_bytes_events:
        rolling_total_bytes = sum(v for _, v in runtime.total_bytes_events) / len(runtime.total_bytes_events)

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
            "avg_total_bytes_last_60s": round(rolling_total_bytes, 3),
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
    }


@app.get("/api/metrics")
def metrics():
    return runtime.snapshot()


@app.post("/api/start")
def start():
    if runtime.running:
        raise HTTPException(status_code=409, detail="Feature service is already running.")

    runtime.reset_metrics()
    runtime.stop_event.clear()
    runtime.running = True
    runtime.started_at = time.time()
    runtime.thread = threading.Thread(target=worker, daemon=True, name="feature-worker")
    runtime.thread.start()

    return {"status": "started"}


@app.post("/api/stop")
def stop():
    if not runtime.running:
        return {"status": "already_stopped"}

    runtime.stop_event.set()

    if runtime.thread:
        runtime.thread.join(timeout=5)

    runtime.running = False
    return {"status": "stopped", "metrics": runtime.snapshot()}


@app.post("/api/reset")
def reset():
    if runtime.running:
        raise HTTPException(status_code=409, detail="Stop the service before resetting metrics.")

    runtime.reset_metrics()
    runtime.started_at = None
    runtime.kafka_status = "UNKNOWN"
    runtime.redis_status = "UNKNOWN"
    return {"status": "reset"}
