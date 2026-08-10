import json
import os
import threading
import time
import uuid
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import KafkaError, TopicAlreadyExistsError, NoBrokersAvailable
from pydantic import BaseModel, Field

STATIC_DIR = Path(__file__).resolve().parent / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", "/shared/data/raw"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "unsw-events")
KAFKA_PARTITIONS = int(os.getenv("KAFKA_PARTITIONS", "4"))

TRAIN_NAMES = ["UNSW_NB15_training-set.csv", "UNSW_NB15_training_set.csv"]
TEST_NAMES = ["UNSW_NB15_testing-set.csv", "UNSW_NB15_testing_set.csv"]

# Higher search range because 1 consumer already sustained ~2000 events/sec.
SEARCH_RATES = [500, 1000, 2000, 3000, 5000, 7500, 10000, 15000]
SCALE_CONSUMERS = [2, 4]

app = FastAPI(title="UNSW-NB15 Streaming Service", version="4.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class ManualStartRequest(BaseModel):
    target_rate: int = Field(default=1000, ge=1, le=30000)
    dataset: Literal["training", "testing"] = "training"
    consumers: Literal[1, 2, 4] = 1


class BenchmarkRequest(BaseModel):
    dataset: Literal["training", "testing"] = "training"
    duration_seconds: int = Field(default=30, ge=10, le=120)


class Runtime:
    def __init__(self):
        self.lock = threading.Lock()
        self.stop_event = threading.Event()
        self.running = False

        self.benchmark_running = False
        self.benchmark_stop_event = threading.Event()
        self.benchmark_thread = None
        self.benchmark_results = []
        self.recommended = None
        self.detected_limit = None
        self.benchmark_progress = {
            "current": 0,
            "total": 0,
            "status": "IDLE",
            "message": "Not started",
            "phase": "IDLE",
        }

        self.target_rate = 1000
        self.dataset_name = "training"
        self.dataset_rows = 0
        self.consumer_count = 1

        self.total_produced = 0
        self.total_processed = 0
        self.producer_errors = 0
        self.consumer_errors = 0

        self.produced_sec = 0.0
        self.processed_sec = 0.0
        self._produced_window = 0
        self._processed_window = 0
        self._last_window = time.monotonic()

        self.producer_thread = None
        self.consumer_threads = []

        self.producer_status = "STOPPED"
        self.consumer_status = "STOPPED"
        self.kafka_status = "UNKNOWN"
        self.started_at = None
        self.last_error = None

    def reset_metrics(self):
        with self.lock:
            self.total_produced = 0
            self.total_processed = 0
            self.producer_errors = 0
            self.consumer_errors = 0
            self.produced_sec = 0.0
            self.processed_sec = 0.0
            self._produced_window = 0
            self._processed_window = 0
            self._last_window = time.monotonic()
            self.last_error = None

    def update_window(self):
        now = time.monotonic()
        elapsed = now - self._last_window
        if elapsed >= 1.0:
            self.produced_sec = round(self._produced_window / elapsed, 1)
            self.processed_sec = round(self._processed_window / elapsed, 1)
            self._produced_window = 0
            self._processed_window = 0
            self._last_window = now

    def snapshot(self):
        with self.lock:
            self.update_window()
            lag = max(self.total_produced - self.total_processed, 0)
            errors = self.producer_errors + self.consumer_errors
            uptime = int(time.time() - self.started_at) if self.started_at else 0
            lag_growth = round(self.produced_sec - self.processed_sec, 1)

            return {
                "running": self.running,
                "benchmark_running": self.benchmark_running,
                "dataset": self.dataset_name,
                "dataset_rows": self.dataset_rows,
                "target_rate": self.target_rate,
                "consumer_count": self.consumer_count,
                "partitions": KAFKA_PARTITIONS,
                "produced_per_sec": self.produced_sec,
                "processed_per_sec": self.processed_sec,
                "total_produced": self.total_produced,
                "total_processed": self.total_processed,
                "lag": lag,
                "lag_growth_per_sec": lag_growth,
                "producer_errors": self.producer_errors,
                "consumer_errors": self.consumer_errors,
                "errors": errors,
                "producer_status": self.producer_status,
                "consumer_status": self.consumer_status,
                "kafka_status": self.kafka_status,
                "uptime_seconds": uptime,
                "last_error": self.last_error,
                "topic": KAFKA_TOPIC,
            }


runtime = Runtime()


def find_file(dataset_name: str) -> Path:
    names = TRAIN_NAMES if dataset_name == "training" else TEST_NAMES
    for name in names:
        path = DATA_DIR / name
        if path.exists():
            return path
    raise HTTPException(404, f"{dataset_name} dataset not found in {DATA_DIR}")


def ensure_topic():
    last_error = None

    for _ in range(20):
        admin = None
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                client_id="streaming-service-admin",
                request_timeout_ms=5000,
                api_version_auto_timeout_ms=5000,
            )

            topics = admin.list_topics()
            if KAFKA_TOPIC not in topics:
                admin.create_topics([
                    NewTopic(
                        name=KAFKA_TOPIC,
                        num_partitions=KAFKA_PARTITIONS,
                        replication_factor=1,
                    )
                ])

            runtime.kafka_status = "CONNECTED"
            runtime.last_error = None
            return

        except TopicAlreadyExistsError:
            runtime.kafka_status = "CONNECTED"
            runtime.last_error = None
            return

        except Exception as exc:
            last_error = exc
            runtime.kafka_status = "CONNECTING"
            runtime.last_error = str(exc)
            time.sleep(2)

        finally:
            if admin:
                admin.close()

    runtime.kafka_status = "ERROR"
    raise RuntimeError(f"Kafka unavailable after retries: {last_error}")


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        acks=1,
        linger_ms=5,
        batch_size=32768,
        retries=5,
        request_timeout_ms=10000,
        api_version_auto_timeout_ms=10000,
    )


def create_consumer(consumer_id: int):
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="high-scale-cyber-ml-streaming",
        client_id=f"stream-consumer-{consumer_id}",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        max_poll_records=1000,
        request_timeout_ms=30000,
        api_version_auto_timeout_ms=10000,
    )


def producer_loop(df: pd.DataFrame):
    producer = None
    try:
        runtime.producer_status = "CONNECTING"
        producer = create_producer()
        runtime.producer_status = "RUNNING"

        rows = df.to_dict(orient="records")
        row_index = 0

        while not runtime.stop_event.is_set():
            rate = max(runtime.target_rate, 1)
            slice_size = min(max(rate // 10, 1), 1500)
            started = time.perf_counter()

            for _ in range(slice_size):
                if runtime.stop_event.is_set():
                    break

                current_row = row_index
                source = rows[row_index]
                row_index = (row_index + 1) % len(rows)

                event = {
                    "event_id": str(uuid.uuid4()),
                    "event_ts": time.time(),
                    "source_dataset": runtime.dataset_name,
                    "source_row": current_row,
                    "payload": source,
                }

                try:
                    producer.send(KAFKA_TOPIC, value=event)
                    with runtime.lock:
                        runtime.total_produced += 1
                        runtime._produced_window += 1
                except KafkaError as exc:
                    with runtime.lock:
                        runtime.producer_errors += 1
                        runtime.last_error = str(exc)

            producer.flush(timeout=5)

            expected = slice_size / rate
            elapsed = time.perf_counter() - started
            wait = expected - elapsed
            if wait > 0:
                runtime.stop_event.wait(wait)

            with runtime.lock:
                runtime.update_window()

    except Exception as exc:
        with runtime.lock:
            runtime.producer_errors += 1
            runtime.last_error = str(exc)
            runtime.kafka_status = "ERROR"
    finally:
        runtime.producer_status = "STOPPED"
        if producer:
            try:
                producer.close(timeout=3)
            except Exception:
                pass


def consumer_loop(consumer_id: int):
    consumer = None
    try:
        consumer = create_consumer(consumer_id)
        runtime.consumer_status = "RUNNING"

        while not runtime.stop_event.is_set():
            records = consumer.poll(timeout_ms=500, max_records=1000)
            processed_now = sum(len(messages) for messages in records.values())

            if processed_now:
                with runtime.lock:
                    runtime.total_processed += processed_now
                    runtime._processed_window += processed_now
                    runtime.update_window()

    except Exception as exc:
        with runtime.lock:
            runtime.consumer_errors += 1
            runtime.last_error = str(exc)
            runtime.kafka_status = "ERROR"
    finally:
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass


def start_internal(df: pd.DataFrame, dataset: str, rate: int, consumers: int):
    runtime.reset_metrics()
    runtime.stop_event.clear()
    runtime.running = True
    runtime.target_rate = rate
    runtime.dataset_name = dataset
    runtime.dataset_rows = len(df)
    runtime.consumer_count = consumers
    runtime.started_at = time.time()
    runtime.producer_status = "STARTING"
    runtime.consumer_status = "STARTING"

    runtime.consumer_threads = []
    for consumer_id in range(1, consumers + 1):
        t = threading.Thread(
            target=consumer_loop,
            args=(consumer_id,),
            daemon=True,
            name=f"consumer-{consumer_id}",
        )
        runtime.consumer_threads.append(t)
        t.start()

    time.sleep(1)

    runtime.producer_thread = threading.Thread(
        target=producer_loop,
        args=(df,),
        daemon=True,
        name="producer",
    )
    runtime.producer_thread.start()


def stop_internal():
    runtime.stop_event.set()

    if runtime.producer_thread:
        runtime.producer_thread.join(timeout=5)

    for t in runtime.consumer_threads:
        t.join(timeout=5)

    runtime.running = False
    runtime.producer_status = "STOPPED"
    runtime.consumer_status = "STOPPED"


def classify_result(result: dict) -> str:
    if result["errors"] > 0:
        return "ERROR"

    target = max(result["target_rate"], 1)
    coverage = result["processed_per_sec"] / target

    if result["lag_growth_per_sec"] <= 5 and coverage >= 0.95:
        return "STABLE"

    if result["lag_growth_per_sec"] <= 50 and coverage >= 0.90:
        return "NEAR_LIMIT"

    return "BOTTLENECK"


def run_experiment(df, dataset, rate, consumers, duration_seconds):
    start_internal(df, dataset, rate, consumers)

    warmup = max(2, int(duration_seconds * 0.2))
    time.sleep(warmup)

    samples = []
    for _ in range(max(duration_seconds - warmup, 1)):
        if runtime.benchmark_stop_event.is_set():
            break
        time.sleep(1)
        samples.append(runtime.snapshot())

    final = runtime.snapshot()
    stop_internal()

    if not samples:
        samples = [final]

    result = {
        "consumers": consumers,
        "target_rate": rate,
        "produced_per_sec": round(sum(s["produced_per_sec"] for s in samples) / len(samples), 1),
        "processed_per_sec": round(sum(s["processed_per_sec"] for s in samples) / len(samples), 1),
        "lag": final["lag"],
        "lag_growth_per_sec": round(sum(s["lag_growth_per_sec"] for s in samples) / len(samples), 1),
        "errors": final["errors"],
        "duration_seconds": duration_seconds,
    }
    result["result"] = classify_result(result)
    return result


def choose_recommended(results):
    stable = [r for r in results if r["result"] == "STABLE"]
    candidates = stable if stable else [r for r in results if r["result"] == "NEAR_LIMIT"]

    if not candidates:
        return None

    candidates = sorted(
        candidates,
        key=lambda r: (-r["processed_per_sec"], r["consumers"], r["lag_growth_per_sec"]),
    )

    best = candidates[0]

    close = [
        r for r in candidates
        if r["processed_per_sec"] >= best["processed_per_sec"] * 0.98
    ]

    close = sorted(
        close,
        key=lambda r: (r["consumers"], -r["processed_per_sec"], r["lag_growth_per_sec"]),
    )

    return close[0]


def benchmark_loop(dataset: str, duration_seconds: int):
    try:
        df = pd.read_csv(find_file(dataset))
        ensure_topic()

        runtime.benchmark_results = []
        runtime.recommended = None
        runtime.detected_limit = None

        current = 0
        total = len(SEARCH_RATES)

        # Phase 1: find the 1-consumer saturation point.
        first_problem_rate = None

        for rate in SEARCH_RATES:
            if runtime.benchmark_stop_event.is_set():
                return

            current += 1
            runtime.benchmark_progress = {
                "current": current,
                "total": total,
                "status": "RUNNING",
                "phase": "FIND_LIMIT",
                "message": f"Finding 1-consumer limit: {rate} events/sec",
            }

            result = run_experiment(df, dataset, rate, 1, duration_seconds)
            runtime.benchmark_results.append(result)

            if result["result"] in ("NEAR_LIMIT", "BOTTLENECK", "ERROR"):
                first_problem_rate = rate
                break

            time.sleep(1)

        # If everything was stable, no saturation was found in the current range.
        if first_problem_rate is None:
            runtime.detected_limit = {
                "status": "ABOVE_RANGE",
                "rate": SEARCH_RATES[-1],
                "message": f"No saturation found up to {SEARCH_RATES[-1]} events/sec with 1 consumer.",
            }
            runtime.recommended = choose_recommended(runtime.benchmark_results)
            runtime.benchmark_progress = {
                "current": current,
                "total": current,
                "status": "COMPLETED",
                "phase": "DONE",
                "message": "Benchmark completed. Saturation is above tested range.",
            }
            return

        runtime.detected_limit = {
            "status": "FOUND",
            "rate": first_problem_rate,
            "message": f"1-consumer saturation detected around {first_problem_rate} events/sec.",
        }

        # Phase 2: test scaling around the saturation point.
        scale_rates = [first_problem_rate]

        idx = SEARCH_RATES.index(first_problem_rate)
        if idx + 1 < len(SEARCH_RATES):
            scale_rates.append(SEARCH_RATES[idx + 1])

        # Add only the additional experiments now that we know where they matter.
        total = current + len(scale_rates) * len(SCALE_CONSUMERS)

        for consumers in SCALE_CONSUMERS:
            for rate in scale_rates:
                if runtime.benchmark_stop_event.is_set():
                    return

                current += 1
                runtime.benchmark_progress = {
                    "current": current,
                    "total": total,
                    "status": "RUNNING",
                    "phase": "SCALE_OUT",
                    "message": f"Testing scaling: {rate} events/sec with {consumers} consumers",
                }

                result = run_experiment(df, dataset, rate, consumers, duration_seconds)
                runtime.benchmark_results.append(result)
                time.sleep(1)

        runtime.recommended = choose_recommended(runtime.benchmark_results)
        runtime.benchmark_progress = {
            "current": total,
            "total": total,
            "status": "COMPLETED",
            "phase": "DONE",
            "message": "Adaptive benchmark completed.",
        }

    except Exception as exc:
        runtime.benchmark_progress = {
            "current": runtime.benchmark_progress.get("current", 0),
            "total": runtime.benchmark_progress.get("total", 0),
            "status": "ERROR",
            "phase": "ERROR",
            "message": str(exc),
        }
    finally:
        if runtime.running:
            stop_internal()
        runtime.benchmark_running = False


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/metrics")
def metrics():
    return runtime.snapshot()


@app.get("/api/benchmark")
def benchmark_status():
    return {
        "running": runtime.benchmark_running,
        "progress": runtime.benchmark_progress,
        "results": runtime.benchmark_results,
        "recommended": runtime.recommended,
        "detected_limit": runtime.detected_limit,
        "search_rates": SEARCH_RATES,
    }


@app.post("/api/benchmark/start")
def start_benchmark(request: BenchmarkRequest):
    if runtime.running or runtime.benchmark_running:
        raise HTTPException(409, "Streaming or benchmark is already running.")

    runtime.benchmark_stop_event.clear()
    runtime.benchmark_running = True
    runtime.benchmark_results = []
    runtime.recommended = None
    runtime.detected_limit = None
    runtime.benchmark_progress = {
        "current": 0,
        "total": len(SEARCH_RATES),
        "status": "STARTING",
        "phase": "FIND_LIMIT",
        "message": "Preparing adaptive benchmark...",
    }

    runtime.benchmark_thread = threading.Thread(
        target=benchmark_loop,
        args=(request.dataset, request.duration_seconds),
        daemon=True,
    )
    runtime.benchmark_thread.start()

    return {"status": "started"}


@app.post("/api/start")
def manual_start(request: ManualStartRequest):
    if runtime.running or runtime.benchmark_running:
        raise HTTPException(409, "Streaming or benchmark is already running.")

    df = pd.read_csv(find_file(request.dataset))
    ensure_topic()
    start_internal(df, request.dataset, request.target_rate, request.consumers)

    return {"status": "started"}


@app.post("/api/stop")
def stop():
    if runtime.benchmark_running:
        runtime.benchmark_stop_event.set()
        return {"status": "benchmark_stop_requested"}

    if runtime.running:
        stop_internal()
        return {"status": "stopped"}

    return {"status": "already_stopped"}


@app.post("/api/reset")
def reset():
    if runtime.running or runtime.benchmark_running:
        raise HTTPException(409, "Stop before resetting.")

    runtime.reset_metrics()
    runtime.benchmark_results = []
    runtime.recommended = None
    runtime.detected_limit = None
    runtime.benchmark_progress = {
        "current": 0,
        "total": 0,
        "status": "IDLE",
        "phase": "IDLE",
        "message": "Not started",
    }

    return {"status": "reset"}
