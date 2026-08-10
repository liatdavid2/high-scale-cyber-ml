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
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from pydantic import BaseModel, Field

STATIC_DIR = Path(__file__).resolve().parent / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", "/shared/data/raw"))
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "unsw-events")

TRAIN_NAMES = ["UNSW_NB15_training-set.csv", "UNSW_NB15_training_set.csv"]
TEST_NAMES = ["UNSW_NB15_testing-set.csv", "UNSW_NB15_testing_set.csv"]

app = FastAPI(
    title="UNSW-NB15 Streaming Service",
    description="Stage 2 of high-scale-cyber-ml",
    version="1.0.0",
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class StartRequest(BaseModel):
    target_rate: int = Field(default=1000, ge=1, le=20000)
    dataset: Literal["training", "testing"] = "training"


class StreamingRuntime:
    def __init__(self):
        self.lock = threading.Lock()
        self.running = False
        self.stop_event = threading.Event()
        self.producer_thread = None
        self.consumer_thread = None

        self.target_rate = 1000
        self.dataset_name = "training"
        self.dataset_rows = 0

        self.total_produced = 0
        self.total_processed = 0
        self.producer_errors = 0
        self.consumer_errors = 0

        self.produced_sec = 0
        self.processed_sec = 0

        self._produced_window = 0
        self._processed_window = 0
        self._last_window = time.monotonic()
        self.started_at = None

        self.producer_status = "STOPPED"
        self.consumer_status = "STOPPED"
        self.kafka_status = "UNKNOWN"
        self.last_error = None

    def reset_metrics(self):
        with self.lock:
            self.total_produced = 0
            self.total_processed = 0
            self.producer_errors = 0
            self.consumer_errors = 0
            self.produced_sec = 0
            self.processed_sec = 0
            self._produced_window = 0
            self._processed_window = 0
            self._last_window = time.monotonic()
            self.last_error = None

    def update_windows(self):
        now = time.monotonic()
        elapsed = now - self._last_window
        if elapsed >= 1.0:
            self.produced_sec = round(self._produced_window / elapsed, 1)
            self.processed_sec = round(self._processed_window / elapsed, 1)
            self._produced_window = 0
            self._processed_window = 0
            self._last_window = now

    def metrics(self):
        with self.lock:
            self.update_windows()
            lag = max(self.total_produced - self.total_processed, 0)
            errors = self.producer_errors + self.consumer_errors
            uptime = 0
            if self.started_at:
                uptime = int(time.time() - self.started_at)

            error_rate = 0.0
            attempts = self.total_produced + errors
            if attempts:
                error_rate = round((errors / attempts) * 100, 4)

            return {
                "running": self.running,
                "dataset": self.dataset_name,
                "dataset_rows": self.dataset_rows,
                "target_rate": self.target_rate,
                "produced_per_sec": self.produced_sec,
                "processed_per_sec": self.processed_sec,
                "total_produced": self.total_produced,
                "total_processed": self.total_processed,
                "lag": lag,
                "producer_errors": self.producer_errors,
                "consumer_errors": self.consumer_errors,
                "errors": errors,
                "error_rate_pct": error_rate,
                "producer_status": self.producer_status,
                "consumer_status": self.consumer_status,
                "kafka_status": self.kafka_status,
                "uptime_seconds": uptime,
                "last_error": self.last_error,
                "topic": KAFKA_TOPIC,
            }


runtime = StreamingRuntime()


def find_file(dataset_name: str) -> Path:
    candidates = TRAIN_NAMES if dataset_name == "training" else TEST_NAMES
    for name in candidates:
        path = DATA_DIR / name
        if path.exists():
            return path

    raise HTTPException(
        status_code=404,
        detail=f"{dataset_name} UNSW-NB15 CSV not found in {DATA_DIR}",
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        acks=1,
        linger_ms=5,
        batch_size=32768,
        retries=3,
        request_timeout_ms=10000,
    )


def create_consumer():
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="high-scale-cyber-ml-streaming",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=1000,
        max_poll_records=1000,
    )


def producer_loop(df: pd.DataFrame):
    producer = None
    try:
        runtime.producer_status = "CONNECTING"
        producer = create_producer()
        runtime.kafka_status = "CONNECTED"
        runtime.producer_status = "RUNNING"

        rows = df.to_dict(orient="records")
        row_index = 0

        while not runtime.stop_event.is_set():
            batch_started = time.perf_counter()
            rate = max(runtime.target_rate, 1)

            # Produce in small slices to remain responsive to Stop.
            slice_size = min(max(rate // 10, 1), 1000)

            for _ in range(slice_size):
                if runtime.stop_event.is_set():
                    break

                source = rows[row_index]
                row_index = (row_index + 1) % len(rows)

                event = {
                    "event_id": str(uuid.uuid4()),
                    "event_ts": time.time(),
                    "source_dataset": runtime.dataset_name,
                    "source_row": row_index,
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

            expected_seconds = slice_size / rate
            elapsed = time.perf_counter() - batch_started
            sleep_for = expected_seconds - elapsed
            if sleep_for > 0:
                runtime.stop_event.wait(sleep_for)

            with runtime.lock:
                runtime.update_windows()

    except Exception as exc:
        with runtime.lock:
            runtime.producer_errors += 1
            runtime.last_error = str(exc)
            runtime.kafka_status = "ERROR"
    finally:
        runtime.producer_status = "STOPPED"
        if producer:
            try:
                producer.flush(timeout=3)
                producer.close(timeout=3)
            except Exception:
                pass


def consumer_loop():
    consumer = None
    try:
        runtime.consumer_status = "CONNECTING"
        consumer = create_consumer()
        runtime.kafka_status = "CONNECTED"
        runtime.consumer_status = "RUNNING"

        while not runtime.stop_event.is_set():
            records = consumer.poll(timeout_ms=500, max_records=1000)

            processed_now = 0
            for _, messages in records.items():
                processed_now += len(messages)

            if processed_now:
                with runtime.lock:
                    runtime.total_processed += processed_now
                    runtime._processed_window += processed_now
                    runtime.update_windows()

    except Exception as exc:
        with runtime.lock:
            runtime.consumer_errors += 1
            runtime.last_error = str(exc)
            runtime.kafka_status = "ERROR"
    finally:
        runtime.consumer_status = "STOPPED"
        if consumer:
            try:
                consumer.close()
            except Exception:
                pass


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "streaming-service",
        "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
        "data_dir": str(DATA_DIR),
    }


@app.get("/api/metrics")
def get_metrics():
    return runtime.metrics()


@app.post("/api/start")
def start_streaming(request: StartRequest):
    if runtime.running:
        raise HTTPException(status_code=409, detail="Streaming is already running.")

    csv_path = find_file(request.dataset)
    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read dataset: {exc}") from exc

    if df.empty:
        raise HTTPException(status_code=400, detail="Dataset is empty.")

    runtime.reset_metrics()
    runtime.target_rate = request.target_rate
    runtime.dataset_name = request.dataset
    runtime.dataset_rows = len(df)
    runtime.started_at = time.time()
    runtime.stop_event.clear()
    runtime.running = True
    runtime.producer_status = "STARTING"
    runtime.consumer_status = "STARTING"

    runtime.consumer_thread = threading.Thread(
        target=consumer_loop,
        daemon=True,
        name="stream-consumer",
    )
    runtime.producer_thread = threading.Thread(
        target=producer_loop,
        args=(df,),
        daemon=True,
        name="stream-producer",
    )

    # Start consumer first to reduce initial lag.
    runtime.consumer_thread.start()
    time.sleep(0.5)
    runtime.producer_thread.start()

    return {
        "status": "started",
        "dataset": request.dataset,
        "dataset_rows": len(df),
        "target_rate": request.target_rate,
    }


@app.post("/api/stop")
def stop_streaming():
    if not runtime.running:
        return {"status": "already_stopped"}

    runtime.stop_event.set()

    if runtime.producer_thread:
        runtime.producer_thread.join(timeout=5)
    if runtime.consumer_thread:
        runtime.consumer_thread.join(timeout=5)

    runtime.running = False
    runtime.producer_status = "STOPPED"
    runtime.consumer_status = "STOPPED"

    return {"status": "stopped", "metrics": runtime.metrics()}


@app.post("/api/reset")
def reset_metrics():
    if runtime.running:
        raise HTTPException(
            status_code=409,
            detail="Stop streaming before resetting metrics.",
        )

    runtime.reset_metrics()
    runtime.dataset_rows = 0
    runtime.started_at = None
    runtime.kafka_status = "UNKNOWN"

    return {"status": "reset"}
