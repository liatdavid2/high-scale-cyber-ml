import json
import os
import statistics
import threading
import time
from collections import deque

import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

STREAMING_URL = os.getenv("STREAMING_URL", "http://streaming-service:8000").rstrip("/")
FEATURE_URL = os.getenv("FEATURE_URL", "http://feature-service:8000").rstrip("/")
INFERENCE_URL = os.getenv("INFERENCE_URL", "http://inference-service:8000").rstrip("/")
MONITORING_URL = os.getenv("MONITORING_URL", "http://monitoring-service:8000").rstrip("/")

DEFAULT_RATES = [250, 500, 750, 1000, 1250, 1500, 2000]
WARMUP_SECONDS = int(os.getenv("LOAD_TEST_WARMUP_SECONDS", "5"))

app = FastAPI(title="End-to-End Load Test Service", version="1.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class BenchmarkRequest(BaseModel):
    dataset: str = "training"
    duration_seconds: int = Field(default=20, ge=10, le=120)
    rates: list[int] | None = None


class Runtime:
    def __init__(self):
        self.running = False
        self.stop_event = threading.Event()
        self.thread = None
        self.results = []
        self.recommended = None
        self.progress = {
            "current": 0,
            "total": len(DEFAULT_RATES),
            "status": "IDLE",
            "message": "Not started",
        }
        self.last_error = None

    def reset(self):
        self.results = []
        self.recommended = None
        self.last_error = None
        self.progress = {
            "current": 0,
            "total": len(DEFAULT_RATES),
            "status": "IDLE",
            "message": "Not started",
        }


runtime = Runtime()


def safe_get(url, timeout=5):
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception:
        return {}


def safe_post(url, payload=None, timeout=10):
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}


def streaming_start(dataset, rate):
    # Compatible with the Stage 2 continuous streaming API used earlier.
    payload = {
        "dataset": dataset,
        "target_rate": rate,
        "consumers": 1,
    }
    return safe_post(f"{STREAMING_URL}/api/start", payload, timeout=15)


def streaming_stop():
    try:
        requests.post(f"{STREAMING_URL}/api/stop", timeout=10)
    except Exception:
        pass


def feature_start():
    try:
        safe_post(f"{FEATURE_URL}/api/start", {}, timeout=10)
    except Exception:
        # If already running, we can still benchmark.
        pass


def feature_stop():
    try:
        requests.post(f"{FEATURE_URL}/api/stop", timeout=10)
    except Exception:
        pass


def fetch_feature_metrics():
    return safe_get(f"{FEATURE_URL}/api/metrics", timeout=5)


def fetch_inference_metrics():
    return safe_get(f"{INFERENCE_URL}/api/evaluation", timeout=5)


def percentile(values, q):
    if not values:
        return 0.0
    arr = sorted(values)
    idx = min(int((len(arr) - 1) * q), len(arr) - 1)
    return round(float(arr[idx]), 3)


def probe_inference():
    payload = {
        "dur": 0.5,
        "rate": 1200.0,
        "total_bytes": 5000.0,
        "total_packets": 20.0,
        "byte_ratio_src_dst": 2.1,
        "packet_ratio_src_dst": 1.3,
        "bytes_per_packet": 238.0,
    }
    start = time.perf_counter()
    try:
        r = requests.post(f"{INFERENCE_URL}/api/predict", json=payload, timeout=10)
        ok = 200 <= r.status_code < 300
        latency_ms = (time.perf_counter() - start) * 1000
        return ok, latency_ms
    except Exception:
        return False, (time.perf_counter() - start) * 1000


def classify(target_rate, completed_rate, p95_e2e, feature_freshness, errors):
    if errors > 0:
        return "ERROR"

    ratio = completed_rate / target_rate if target_rate else 0

    if ratio >= 0.95 and p95_e2e <= 100 and feature_freshness <= 2000:
        return "STABLE"

    if ratio >= 0.90 and p95_e2e <= 300 and feature_freshness <= 10000:
        return "NEAR_LIMIT"

    return "BOTTLENECK"


def detect_bottleneck(row):
    candidates = []

    if row["feature_ratio"] < 0.90 or row["feature_freshness_ms"] > 10000:
        candidates.append(("Feature Service", row["feature_ratio"]))

    if row["inference_error_rate"] > 0.01 or row["inference_p95_ms"] > 100:
        candidates.append(("Inference Service", 0.5))

    if row["completed_ratio"] < 0.90 and not candidates:
        candidates.append(("Streaming / Kafka", row["completed_ratio"]))

    return candidates[0][0] if candidates else "None"


def choose_recommended(results):
    stable = [r for r in results if r["result"] == "STABLE"]
    if not stable:
        stable = [r for r in results if r["result"] == "NEAR_LIMIT"]
    if not stable:
        return None

    stable.sort(
        key=lambda r: (
            -r["target_rate"],
            r["e2e_p95_ms"],
            r["feature_freshness_ms"],
        )
    )
    return stable[0]


def build_recommendation(results):
    recommended = choose_recommended(results)
    if not recommended:
        return None

    next_failed = None
    for row in results:
        if row["target_rate"] > recommended["target_rate"] and row["result"] in ("BOTTLENECK", "ERROR"):
            next_failed = row
            break

    return {
        **recommended,
        "next_tested_rate": next_failed["target_rate"] if next_failed else None,
        "first_bottleneck": next_failed["bottleneck"] if next_failed else "None",
        "first_failed_result": next_failed["result"] if next_failed else None,
    }


def benchmark_loop(dataset, duration_seconds, rates):
    runtime.running = True
    runtime.stop_event.clear()
    runtime.results = []
    runtime.recommended = None
    runtime.last_error = None

    try:
        feature_start()
        time.sleep(2)

        for idx, rate in enumerate(rates, start=1):
            if runtime.stop_event.is_set():
                runtime.progress = {
                    "current": idx - 1,
                    "total": len(rates),
                    "status": "STOPPED",
                    "message": "Benchmark stopped.",
                }
                break

            runtime.progress = {
                "current": idx,
                "total": len(rates),
                "status": "RUNNING",
                "message": f"Testing {rate} events/sec",
            }

            streaming_stop()
            time.sleep(1)

            # Capture counters before the rate test.
            before_feature = fetch_feature_metrics()
            before_inference = fetch_inference_metrics()

            streaming_start(dataset, rate)
            time.sleep(WARMUP_SECONDS)

            feature_rates = []
            feature_freshness = []
            inference_p95 = []
            inference_errors = []
            probe_latencies = []
            probe_success = 0
            probe_total = 0

            started = time.monotonic()

            while time.monotonic() - started < duration_seconds:
                if runtime.stop_event.is_set():
                    break

                fm = fetch_feature_metrics()
                im = fetch_inference_metrics()

                if fm:
                    feature_rates.append(float(fm.get("features_per_sec", 0) or 0))
                    feature_freshness.append(float(fm.get("feature_freshness_ms", 0) or 0))

                if im:
                    inference_p95.append(float(im.get("p95_ms", 0) or 0))
                    inference_errors.append(float(im.get("error_rate", 0) or 0))

                # A small serving probe makes sure the deployed model is still reachable
                # while the upstream pipeline is under load.
                ok, lat = probe_inference()
                probe_total += 1
                if ok:
                    probe_success += 1
                probe_latencies.append(lat)

                time.sleep(1)

            streaming_stop()

            after_feature = fetch_feature_metrics()
            after_inference = fetch_inference_metrics()

            avg_features = statistics.mean(feature_rates) if feature_rates else 0.0
            avg_freshness = statistics.mean(feature_freshness) if feature_freshness else 0.0
            avg_inference_p95 = statistics.mean(inference_p95) if inference_p95 else 0.0
            avg_inference_error = statistics.mean(inference_errors) if inference_errors else 0.0

            # "Completed" is the feature stage throughput because every streamed event
            # must pass feature engineering before it is usable downstream.
            completed = avg_features

            e2e_p50 = percentile(probe_latencies, 0.50)
            e2e_p95 = percentile(probe_latencies, 0.95)
            e2e_p99 = percentile(probe_latencies, 0.99)

            feature_ratio = completed / rate if rate else 0
            completed_ratio = completed / rate if rate else 0
            errors = probe_total - probe_success

            row = {
                "target_rate": rate,
                "completed_per_sec": round(completed, 1),
                "completed_ratio": round(completed_ratio, 4),
                "feature_ratio": round(feature_ratio, 4),
                "feature_freshness_ms": round(avg_freshness, 1),
                "feature_p95_ms": round(float(after_feature.get("latency_p95_ms", 0) or 0), 2),
                "inference_p95_ms": round(avg_inference_p95, 2),
                "inference_error_rate": round(avg_inference_error, 6),
                "e2e_p50_ms": e2e_p50,
                "e2e_p95_ms": e2e_p95,
                "e2e_p99_ms": e2e_p99,
                "probe_errors": errors,
            }

            row["result"] = classify(
                target_rate=rate,
                completed_rate=completed,
                p95_e2e=e2e_p95,
                feature_freshness=avg_freshness,
                errors=errors,
            )
            row["bottleneck"] = detect_bottleneck(row)

            runtime.results.append(row)

            # Adaptive stop after the first clear bottleneck.
            if row["result"] in ("BOTTLENECK", "ERROR"):
                break

            time.sleep(1)

        runtime.recommended = build_recommendation(runtime.results)

        if runtime.progress["status"] != "STOPPED":
            bottleneck_seen = any(
                r["result"] in ("BOTTLENECK", "ERROR")
                for r in runtime.results
            )

            if bottleneck_seen:
                runtime.progress = {
                    "current": len(runtime.results),
                    "total": len(rates),
                    "status": "COMPLETED",
                    "message": f"Benchmark stopped after first bottleneck. {len(runtime.results)} rates tested.",
                }
            else:
                runtime.progress = {
                    "current": len(runtime.results),
                    "total": len(rates),
                    "status": "COMPLETED",
                    "message": f"Benchmark completed. {len(runtime.results)} rates tested.",
                }

    except Exception as exc:
        runtime.last_error = str(exc)
        runtime.progress = {
            "current": len(runtime.results),
            "total": len(rates),
            "status": "ERROR",
            "message": str(exc),
        }
    finally:
        streaming_stop()
        runtime.running = False


@app.get("/")
def home():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "load-test-service",
        "streaming_url": STREAMING_URL,
        "feature_url": FEATURE_URL,
        "inference_url": INFERENCE_URL,
    }


@app.get("/api/status")
def status():
    return {
        "running": runtime.running,
        "progress": runtime.progress,
        "results": runtime.results,
        "recommended": runtime.recommended,
        "last_error": runtime.last_error,
        "rates": DEFAULT_RATES,
    }


@app.post("/api/start")
def start(req: BenchmarkRequest):
    if runtime.running:
        raise HTTPException(status_code=409, detail="Load test is already running.")

    rates = req.rates or DEFAULT_RATES
    rates = [int(r) for r in rates if int(r) > 0]

    runtime.thread = threading.Thread(
        target=benchmark_loop,
        args=(req.dataset, req.duration_seconds, rates),
        daemon=True,
        name="e2e-load-test",
    )
    runtime.thread.start()

    return {
        "status": "started",
        "rates": rates,
        "duration_seconds": req.duration_seconds,
    }


@app.post("/api/stop")
def stop():
    runtime.stop_event.set()
    streaming_stop()
    return {"status": "stop_requested"}


@app.post("/api/reset")
def reset():
    if runtime.running:
        raise HTTPException(status_code=409, detail="Stop the benchmark before reset.")
    runtime.reset()
    return {"status": "reset"}
