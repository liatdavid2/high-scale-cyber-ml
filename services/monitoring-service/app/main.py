
import io
import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from scipy.stats import ks_2samp
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", "/shared/data/raw"))
INFERENCE_URL = os.getenv("INFERENCE_URL", "http://inference-service:8000").rstrip("/")

TRAIN_NAMES = [
    "UNSW_NB15_training-set.csv",
    "UNSW_NB15_training_set.csv",
]

RAW_REQUIRED = ["dur", "rate", "sbytes", "dbytes", "spkts", "dpkts"]

MODEL_FEATURES = [
    "dur",
    "rate",
    "total_bytes",
    "total_packets",
    "byte_ratio_src_dst",
    "packet_ratio_src_dst",
    "bytes_per_packet",
]

LABEL_CANDIDATES = ["label", "target", "class", "y"]

app = FastAPI(title="Monitoring Service", version="4.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def find_training_file():
    for name in TRAIN_NAMES:
        path = DATA_DIR / name
        if path.exists():
            return path
    raise FileNotFoundError(
        f"Training baseline not found in {DATA_DIR}. Expected one of: {TRAIN_NAMES}"
    )


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    missing = [c for c in RAW_REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(
            "CSV schema is incompatible. Missing raw columns required for monitoring: "
            + ", ".join(missing)
        )

    d = df[RAW_REQUIRED].copy()
    for col in RAW_REQUIRED:
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


def detect_label_column(df: pd.DataFrame):
    lower_map = {str(c).lower(): c for c in df.columns}
    for candidate in LABEL_CANDIDATES:
        if candidate in lower_map:
            return lower_map[candidate]
    return None


def psi(expected, actual, bins=10):
    expected = np.asarray(expected, dtype=float)
    actual = np.asarray(actual, dtype=float)

    expected = expected[np.isfinite(expected)]
    actual = actual[np.isfinite(actual)]

    if len(expected) < 2 or len(actual) < 2:
        return 0.0

    edges = np.unique(np.quantile(expected, np.linspace(0, 1, bins + 1)))
    if len(edges) < 3:
        return 0.0

    edges = edges.astype(float)
    edges[0] = -np.inf
    edges[-1] = np.inf

    e_count, _ = np.histogram(expected, bins=edges)
    a_count, _ = np.histogram(actual, bins=edges)

    e_pct = np.clip(e_count / max(e_count.sum(), 1), 1e-6, None)
    a_pct = np.clip(a_count / max(a_count.sum(), 1), 1e-6, None)

    return float(np.sum((a_pct - e_pct) * np.log(a_pct / e_pct)))


def drift_level(psi_value):
    if psi_value >= 0.25:
        return "HIGH"
    if psi_value >= 0.10:
        return "MEDIUM"
    return "LOW"


def fetch_serving():
    try:
        r = requests.get(f"{INFERENCE_URL}/api/evaluation", timeout=5)
        r.raise_for_status()
        return r.json()
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


def predict_batch(X: pd.DataFrame):
    probabilities = []
    predictions = []
    errors = 0
    latencies = []

    for _, row in X.iterrows():
        started = time.perf_counter()
        try:
            response = requests.post(
                f"{INFERENCE_URL}/api/predict",
                json={name: float(row[name]) for name in MODEL_FEATURES},
                timeout=10,
            )
            response.raise_for_status()
            result = response.json()
            probabilities.append(float(result["probability"]))
            predictions.append(int(result["prediction"]))
            latencies.append((time.perf_counter() - started) * 1000)
        except Exception:
            errors += 1
            probabilities.append(0.0)
            predictions.append(0)

    return (
        np.asarray(probabilities, dtype=float),
        np.asarray(predictions, dtype=int),
        errors,
        latencies,
    )


def quality_metrics(y_true, probabilities, predictions):
    unique = np.unique(y_true)
    if not set(unique).issubset({0, 1}):
        raise ValueError(
            f"Detected label column is not binary. Found values: {unique[:10].tolist()}"
        )

    tn, fp, fn, tp = confusion_matrix(y_true, predictions, labels=[0, 1]).ravel()

    result = {
        "f1": float(f1_score(y_true, predictions, zero_division=0)),
        "recall": float(recall_score(y_true, predictions, zero_division=0)),
        "precision": float(precision_score(y_true, predictions, zero_division=0)),
        "fpr": float(fp / max(fp + tn, 1)),
        "fnr": float(fn / max(fn + tp, 1)),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
        "tp": int(tp),
    }

    if len(unique) == 2:
        result["pr_auc"] = float(average_precision_score(y_true, probabilities))
        result["roc_auc"] = float(roc_auc_score(y_true, probabilities))
    else:
        result["pr_auc"] = None
        result["roc_auc"] = None

    return result


def format_pvalue(value):
    # Return enough precision for the UI to show decimals such as 0.0003.
    return round(float(value), 6)


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "monitoring-service",
        "baseline": str(find_training_file()),
        "inference_url": INFERENCE_URL,
    }


@app.get("/api/serving")
def serving():
    return fetch_serving()


@app.post("/api/evaluate-csv")
async def evaluate_csv(file: UploadFile = File(...)):
    filename = file.filename or "uploaded.csv"

    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a CSV file.")

    try:
        raw = await file.read()
        current_df = pd.read_csv(io.BytesIO(raw))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Could not read CSV: {exc}")

    if current_df.empty:
        raise HTTPException(status_code=400, detail="Uploaded CSV is empty.")

    try:
        baseline_path = find_training_file()
        baseline_df = pd.read_csv(baseline_path)

        baseline_features = build_features(baseline_df)
        current_features = build_features(current_df)

        sample_size = min(len(baseline_features), len(current_features), 20000)
        if sample_size < 100:
            raise ValueError("At least 100 compatible rows are required for drift evaluation.")

        baseline_sample = baseline_features.sample(n=sample_size, random_state=42)
        current_sample = current_features.sample(n=sample_size, random_state=43)

        drift_rows = []
        counts = {"LOW": 0, "MEDIUM": 0, "HIGH": 0}

        for feature in MODEL_FEATURES:
            psi_value = psi(baseline_sample[feature], current_sample[feature])
            ks = ks_2samp(baseline_sample[feature], current_sample[feature])
            level = drift_level(psi_value)
            counts[level] += 1

            drift_rows.append({
                "feature": feature,
                "psi": round(psi_value, 4),
                "ks_statistic": round(float(ks.statistic), 4),
                "ks_pvalue": format_pvalue(ks.pvalue),
                "drift_level": level,
            })

        overall = (
            "HIGH"
            if counts["HIGH"] > 0
            else "MEDIUM"
            if counts["MEDIUM"] > 0
            else "LOW"
        )

        label_column = detect_label_column(current_df)

        response = {
            "file": {
                "name": filename,
                "rows": int(len(current_df)),
                "columns": int(len(current_df.columns)),
                "schema_valid": True,
                "label_found": label_column is not None,
                "label_column": str(label_column) if label_column is not None else None,
            },
            "drift": {
                "baseline_dataset": baseline_path.name,
                "current_dataset": filename,
                "rows_compared": sample_size,
                "overall_drift": overall,
                "high_features": counts["HIGH"],
                "medium_features": counts["MEDIUM"],
                "low_features": counts["LOW"],
                "results": drift_rows,
                "rule": "PSI determines drift severity; KS is shown as supporting statistical evidence.",
            },
            "quality": None,
        }

        if label_column is not None:
            quality_n = min(len(current_df), 2000)
            labeled = current_df.sample(n=quality_n, random_state=99).copy()
            X = build_features(labeled)
            y = pd.to_numeric(labeled[label_column], errors="coerce")

            valid = y.notna()
            X = X.loc[valid].reset_index(drop=True)
            y = y.loc[valid].astype(int).to_numpy()

            if len(y) >= 100:
                probabilities, predictions, api_errors, latency_ms = predict_batch(X)
                metrics = quality_metrics(y, probabilities, predictions)

                response["quality"] = {
                    "available": True,
                    "label_column": str(label_column),
                    "rows_evaluated": int(len(y)),
                    "api_errors": int(api_errors),
                    "p95_inference_ms": round(
                        float(np.percentile(latency_ms, 95)), 3
                    ) if latency_ms else 0,
                    **{
                        key: round(value, 4) if isinstance(value, float) else value
                        for key, value in metrics.items()
                    },
                }
            else:
                response["quality"] = {
                    "available": False,
                    "reason": "Too few valid labeled rows after label parsing.",
                    "label_column": str(label_column),
                }

        return response

    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))
