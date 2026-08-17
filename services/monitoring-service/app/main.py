
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from scipy.stats import ks_2samp
from sklearn.metrics import (
    average_precision_score,
    roc_auc_score,
    f1_score,
    recall_score,
    precision_score,
    confusion_matrix,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", "/shared/data/raw"))
INFERENCE_URL = os.getenv("INFERENCE_URL", "http://inference-service:8000").rstrip("/")

TRAIN_NAMES = ["UNSW_NB15_training-set.csv", "UNSW_NB15_training_set.csv"]
TEST_NAMES = ["UNSW_NB15_testing-set.csv", "UNSW_NB15_testing_set.csv"]

FEATURES = [
    "dur",
    "rate",
    "total_bytes",
    "total_packets",
    "byte_ratio_src_dst",
    "packet_ratio_src_dst",
    "bytes_per_packet",
]

app = FastAPI(title="Monitoring Service", version="3.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class DriftRequest(BaseModel):
    sample_size: int = Field(default=20000, ge=1000, le=80000)


class QualityRequest(BaseModel):
    baseline_rows: int = Field(default=2000, ge=500, le=10000)
    current_rows: int = Field(default=2000, ge=500, le=10000)


def find_file(names):
    for name in names:
        path = DATA_DIR / name
        if path.exists():
            return path
    raise FileNotFoundError(f"Could not find one of {names} in {DATA_DIR}")


def build_features(df):
    required = ["dur", "rate", "sbytes", "dbytes", "spkts", "dpkts"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    d = df[required].copy()
    for col in required:
        d[col] = pd.to_numeric(d[col], errors="coerce").fillna(0)

    total_bytes = d["sbytes"] + d["dbytes"]
    total_packets = d["spkts"] + d["dpkts"]

    return pd.DataFrame({
        "dur": d["dur"],
        "rate": d["rate"],
        "total_bytes": total_bytes,
        "total_packets": total_packets,
        "byte_ratio_src_dst": d["sbytes"] / (d["dbytes"] + 1.0),
        "packet_ratio_src_dst": d["spkts"] / (d["dpkts"] + 1.0),
        "bytes_per_packet": total_bytes / (total_packets + 1.0),
    })


def psi(expected, actual, bins=10):
    expected = np.asarray(expected, dtype=float)
    actual = np.asarray(actual, dtype=float)

    if len(expected) < 2 or len(actual) < 2:
        return 0.0

    edges = np.unique(np.quantile(expected, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0

    edges = edges.astype(float)
    edges[0] = -np.inf
    edges[-1] = np.inf

    expected_count, _ = np.histogram(expected, bins=edges)
    actual_count, _ = np.histogram(actual, bins=edges)

    expected_pct = np.clip(expected_count / max(expected_count.sum(), 1), 1e-6, None)
    actual_pct = np.clip(actual_count / max(actual_count.sum(), 1), 1e-6, None)

    return float(np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct)))


def drift_level(value):
    if value >= 0.25:
        return "HIGH"
    if value >= 0.10:
        return "MEDIUM"
    return "LOW"


def inference_eval():
    try:
        response = requests.get(f"{INFERENCE_URL}/api/evaluation", timeout=5)
        response.raise_for_status()
        return response.json()
    except Exception as exc:
        return {
            "model_loaded": False,
            "model_uri": "-",
            "p50_ms": 0,
            "p95_ms": 0,
            "p99_ms": 0,
            "throughput_rps": 0,
            "error_rate": 0,
            "requests": 0,
            "success": 0,
            "model_error": str(exc),
        }


def predict_batch(X):
    probabilities = []
    predictions = []
    api_errors = 0
    latency_ms = []

    for _, row in X.iterrows():
        started = time.perf_counter()
        try:
            response = requests.post(
                f"{INFERENCE_URL}/api/predict",
                json={k: float(row[k]) for k in FEATURES},
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            probabilities.append(float(result["probability"]))
            predictions.append(int(result["prediction"]))
            latency_ms.append((time.perf_counter() - started) * 1000)
        except Exception:
            api_errors += 1
            probabilities.append(0.0)
            predictions.append(0)

    return (
        np.asarray(probabilities, dtype=float),
        np.asarray(predictions, dtype=int),
        api_errors,
        latency_ms,
    )


def quality_metrics(y_true, probabilities, predictions):
    tn, fp, fn, tp = confusion_matrix(y_true, predictions, labels=[0, 1]).ravel()
    fpr = fp / max(fp + tn, 1)
    fnr = fn / max(fn + tp, 1)

    return {
        "pr_auc": float(average_precision_score(y_true, probabilities)),
        "roc_auc": float(roc_auc_score(y_true, probabilities)),
        "f1": float(f1_score(y_true, predictions, zero_division=0)),
        "recall": float(recall_score(y_true, predictions, zero_division=0)),
        "precision": float(precision_score(y_true, predictions, zero_division=0)),
        "fpr": float(fpr),
        "fnr": float(fnr),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
    }


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "monitoring-service",
        "data_dir": str(DATA_DIR),
        "inference_url": INFERENCE_URL,
    }


@app.get("/api/serving")
def serving():
    return inference_eval()


@app.post("/api/drift")
def run_drift(request: DriftRequest):
    try:
        train_path = find_file(TRAIN_NAMES)
        test_path = find_file(TEST_NAMES)

        train_df = pd.read_csv(train_path)
        test_df = pd.read_csv(test_path)

        baseline_n = min(request.sample_size, len(train_df))
        current_n = min(request.sample_size, len(test_df))

        baseline_raw = train_df.sample(n=baseline_n, random_state=42)
        current_raw = test_df.sample(n=current_n, random_state=43)

        baseline = build_features(baseline_raw)
        current = build_features(current_raw)

        results = []
        counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}

        for feature in FEATURES:
            psi_value = psi(baseline[feature], current[feature])
            ks_result = ks_2samp(baseline[feature], current[feature])
            level = drift_level(psi_value)
            counts[level] += 1

            results.append({
                "feature": feature,
                "psi": round(psi_value, 4),
                "ks_statistic": round(float(ks_result.statistic), 4),
                "ks_pvalue": round(float(ks_result.pvalue), 6),
                "drift_level": level,
            })

        overall = "HIGH" if counts["HIGH"] else ("MEDIUM" if counts["MEDIUM"] else "LOW")

        return {
            "baseline_dataset": train_path.name,
            "current_dataset": test_path.name,
            "baseline_rows": baseline_n,
            "current_rows": current_n,
            "overall_drift": overall,
            "high_features": counts["HIGH"],
            "medium_features": counts["MEDIUM"],
            "low_features": counts["LOW"],
            "results": results,
            "evaluated_at": int(time.time()),
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/quality")
def run_quality(request: QualityRequest):
    try:
        train_path = find_file(TRAIN_NAMES)
        test_path = find_file(TEST_NAMES)

        train_df = pd.read_csv(train_path)
        test_df = pd.read_csv(test_path)

        if "label" not in train_df.columns or "label" not in test_df.columns:
            raise ValueError("Both CSV files must contain the label column.")

        baseline_n = min(request.baseline_rows, len(train_df))
        current_n = min(request.current_rows, len(test_df))

        baseline_raw = train_df.sample(n=baseline_n, random_state=101)
        current_raw = test_df.sample(n=current_n, random_state=102)

        baseline_X = build_features(baseline_raw)
        current_X = build_features(current_raw)
        baseline_y = pd.to_numeric(baseline_raw["label"], errors="coerce").fillna(0).astype(int).to_numpy()
        current_y = pd.to_numeric(current_raw["label"], errors="coerce").fillna(0).astype(int).to_numpy()

        baseline_prob, baseline_pred, baseline_errors, baseline_lat = predict_batch(baseline_X)
        current_prob, current_pred, current_errors, current_lat = predict_batch(current_X)

        baseline_metrics = quality_metrics(baseline_y, baseline_prob, baseline_pred)
        current_metrics = quality_metrics(current_y, current_prob, current_pred)

        changes = {}
        for metric in ["pr_auc", "roc_auc", "f1", "recall", "precision", "fpr", "fnr"]:
            changes[metric] = current_metrics[metric] - baseline_metrics[metric]

        return {
            "baseline_dataset": train_path.name,
            "current_dataset": test_path.name,
            "baseline_rows": baseline_n,
            "current_rows": current_n,
            "baseline": {k: round(v, 4) if isinstance(v, float) else v for k, v in baseline_metrics.items()},
            "current": {k: round(v, 4) if isinstance(v, float) else v for k, v in current_metrics.items()},
            "change": {k: round(v, 4) for k, v in changes.items()},
            "baseline_api_errors": baseline_errors,
            "current_api_errors": current_errors,
            "baseline_p95_ms": round(float(np.percentile(baseline_lat, 95)), 3) if baseline_lat else 0,
            "current_p95_ms": round(float(np.percentile(current_lat, 95)), 3) if current_lat else 0,
            "checked_at": int(time.time()),
            "note": "Reference quality uses a labeled sample from the training CSV; current quality uses a labeled sample from the testing CSV.",
        }

    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
