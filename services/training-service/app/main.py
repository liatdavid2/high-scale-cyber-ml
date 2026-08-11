
import os, threading, time
from pathlib import Path

import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow import MlflowClient
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from sklearn.ensemble import RandomForestClassifier, HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score, f1_score, recall_score, precision_score, accuracy_score

from xgboost import XGBClassifier
from lightgbm import LGBMClassifier
from catboost import CatBoostClassifier

STATIC_DIR = Path(__file__).resolve().parent / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", "/shared/data/raw"))

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_PUBLIC_URL = os.getenv("MLFLOW_PUBLIC_URL", "http://localhost:2350")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "cyber-intrusion-model-selection")
REGISTERED_MODEL_NAME = os.getenv("MLFLOW_REGISTERED_MODEL_NAME", "cyber-intrusion-model")

TRAIN_NAMES = ["UNSW_NB15_training-set.csv", "UNSW_NB15_training_set.csv"]
TEST_NAMES = ["UNSW_NB15_testing-set.csv", "UNSW_NB15_testing_set.csv"]

FEATURE_COLUMNS = [
    "dur",
    "rate",
    "total_bytes",
    "total_packets",
    "byte_ratio_src_dst",
    "packet_ratio_src_dst",
    "bytes_per_packet",
]

app = FastAPI(title="Training + MLflow Service", version="2.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)


class TrainRequest(BaseModel):
    train_rows: int = Field(default=50000, ge=5000, le=175341)
    test_rows: int = Field(default=25000, ge=5000, le=82332)


class Runtime:
    def __init__(self):
        self.running = False
        self.thread = None
        self.results = []
        self.best = None
        self.last_error = None
        self.progress = {"current": 0, "total": 8, "status": "IDLE", "message": "Not started"}

    def reset(self):
        self.running = False
        self.results = []
        self.best = None
        self.last_error = None
        self.progress = {"current": 0, "total": 8, "status": "IDLE", "message": "Not started"}


runtime = Runtime()


def find_file(names):
    for name in names:
        p = DATA_DIR / name
        if p.exists():
            return p
    raise FileNotFoundError(f"Dataset not found in {DATA_DIR}")


def stratified_sample(df, n):
    if n >= len(df):
        return df.copy()

    pieces = []
    for label, g in df.groupby("label"):
        take = max(1, round(n * len(g) / len(df)))
        pieces.append(g.sample(n=min(take, len(g)), random_state=42))

    out = pd.concat(pieces, ignore_index=True)

    if len(out) > n:
        out = out.sample(n=n, random_state=42)

    return out.sample(frac=1, random_state=42).reset_index(drop=True)


def build_features(df):
    cols = ["dur", "rate", "sbytes", "dbytes", "spkts", "dpkts", "label"]
    d = df[cols].copy()

    for c in cols:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)

    total_bytes = d["sbytes"] + d["dbytes"]
    total_packets = d["spkts"] + d["dpkts"]

    X = pd.DataFrame({
        "dur": d["dur"],
        "rate": d["rate"],
        "total_bytes": total_bytes,
        "total_packets": total_packets,
        "byte_ratio_src_dst": d["sbytes"] / (d["dbytes"] + 1.0),
        "packet_ratio_src_dst": d["spkts"] / (d["dpkts"] + 1.0),
        "bytes_per_packet": total_bytes / (total_packets + 1.0),
    })

    return X, d["label"].astype(int)


def configs():
    return [
        (
            "XGBoost",
            "xgboost",
            {"n_estimators": 200, "max_depth": 6, "learning_rate": 0.1},
            XGBClassifier(
                n_estimators=200,
                max_depth=6,
                learning_rate=0.1,
                subsample=0.9,
                colsample_bytree=0.9,
                eval_metric="logloss",
                random_state=42,
                n_jobs=-1,
            ),
        ),
        (
            "XGBoost",
            "xgboost",
            {"n_estimators": 400, "max_depth": 8, "learning_rate": 0.05},
            XGBClassifier(
                n_estimators=400,
                max_depth=8,
                learning_rate=0.05,
                subsample=0.9,
                colsample_bytree=0.9,
                eval_metric="logloss",
                random_state=42,
                n_jobs=-1,
            ),
        ),
        (
            "LightGBM",
            "lightgbm",
            {"n_estimators": 200, "num_leaves": 31, "learning_rate": 0.1},
            LGBMClassifier(
                n_estimators=200,
                num_leaves=31,
                learning_rate=0.1,
                random_state=42,
                n_jobs=-1,
                verbosity=-1,
            ),
        ),
        (
            "LightGBM",
            "lightgbm",
            {"n_estimators": 400, "num_leaves": 63, "learning_rate": 0.05},
            LGBMClassifier(
                n_estimators=400,
                num_leaves=63,
                learning_rate=0.05,
                random_state=42,
                n_jobs=-1,
                verbosity=-1,
            ),
        ),
        (
            "CatBoost",
            "catboost",
            {"iterations": 300, "depth": 6, "learning_rate": 0.1},
            CatBoostClassifier(
                iterations=300,
                depth=6,
                learning_rate=0.1,
                verbose=False,
                random_seed=42,
                thread_count=-1,
            ),
        ),
        (
            "CatBoost",
            "catboost",
            {"iterations": 500, "depth": 8, "learning_rate": 0.05},
            CatBoostClassifier(
                iterations=500,
                depth=8,
                learning_rate=0.05,
                verbose=False,
                random_seed=42,
                thread_count=-1,
            ),
        ),
        (
            "Random Forest",
            "random_forest",
            {"n_estimators": 300, "max_depth": 20},
            RandomForestClassifier(
                n_estimators=300,
                max_depth=20,
                random_state=42,
                n_jobs=-1,
                class_weight="balanced_subsample",
            ),
        ),
        (
            "HistGradientBoosting",
            "hist_gradient_boosting",
            {"learning_rate": 0.1, "max_iter": 300},
            HistGradientBoostingClassifier(
                learning_rate=0.1,
                max_iter=300,
                random_state=42,
            ),
        ),
    ]


def model_version_for_run(client, run_id):
    try:
        for v in client.search_model_versions(f"name='{REGISTERED_MODEL_NAME}'"):
            if v.run_id == run_id:
                return str(v.version)
    except Exception:
        pass
    return "-"


def run_suite(train_rows, test_rows):
    try:
        runtime.reset()
        runtime.running = True
        runtime.progress = {"current": 0, "total": 8, "status": "PREPARING", "message": "Loading UNSW-NB15..."}

        train_df = stratified_sample(pd.read_csv(find_file(TRAIN_NAMES)), train_rows)
        test_df = stratified_sample(pd.read_csv(find_file(TEST_NAMES)), test_rows)

        X_train, y_train = build_features(train_df)
        X_test, y_test = build_features(test_df)

        mlflow.set_experiment(EXPERIMENT_NAME)
        client = MlflowClient()

        results = []

        for i, (name, family, params, model) in enumerate(configs(), 1):
            runtime.progress = {
                "current": i,
                "total": 8,
                "status": "RUNNING",
                "message": f"Training {name}: {params}",
            }

            with mlflow.start_run(run_name=f"{family}-{i}") as run:
                mlflow.set_tags({
                    "dataset": "UNSW-NB15",
                    "task": "binary_intrusion_detection",
                    "model_family": family,
                    "stage": "stage4-training",
                })

                mlflow.log_param("model_family", family)
                mlflow.log_param("train_rows", len(X_train))
                mlflow.log_param("test_rows", len(X_test))
                mlflow.log_param("feature_count", len(FEATURE_COLUMNS))

                for k, v in params.items():
                    mlflow.log_param(k, v)

                start = time.perf_counter()
                model.fit(X_train, y_train)
                training_time = time.perf_counter() - start

                start = time.perf_counter()
                probs = model.predict_proba(X_test)[:, 1]
                preds = (probs >= 0.5).astype(int)
                infer_time = time.perf_counter() - start

                metrics = {
                    "pr_auc": float(average_precision_score(y_test, probs)),
                    "roc_auc": float(roc_auc_score(y_test, probs)),
                    "f1": float(f1_score(y_test, preds, zero_division=0)),
                    "recall": float(recall_score(y_test, preds, zero_division=0)),
                    "precision": float(precision_score(y_test, preds, zero_division=0)),
                    "accuracy": float(accuracy_score(y_test, preds)),
                    "training_time_sec": float(training_time),
                    "inference_ms_per_1000": float(infer_time / len(X_test) * 1_000_000),
                }

                mlflow.log_metrics(metrics)
                mlflow.log_dict({"features": FEATURE_COLUMNS, "target": "label"}, "training_schema.json")

                log_kwargs = {}

                if family == "xgboost":
                    log_kwargs["skops_trusted_types"] = [
                        "xgboost.core.Booster",
                        "xgboost.sklearn.XGBClassifier",
                    ]

                mlflow.sklearn.log_model(
                    model,
                    name="model",
                    registered_model_name=REGISTERED_MODEL_NAME,
                    input_example=X_train.head(5),
                    **log_kwargs,
                )

                run_id = run.info.run_id

            result = {
                "run_id": run_id,
                "model": name,
                "family": family,
                "params": params,
                "model_version": model_version_for_run(client, run_id),
                **metrics,
            }

            results.append(result)
            runtime.results = list(results)

        best = sorted(
            results,
            key=lambda r: (-r["pr_auc"], -r["f1"], r["inference_ms_per_1000"]),
        )[0]

        try:
            client.set_tag(best["run_id"], "best_run", "true")
            if best["model_version"] != "-":
                client.set_registered_model_alias(
                    REGISTERED_MODEL_NAME,
                    "champion",
                    best["model_version"],
                )
        except Exception:
            pass

        runtime.best = best
        runtime.results = results
        runtime.progress = {
            "current": 8,
            "total": 8,
            "status": "COMPLETED",
            "message": "Experiment suite completed.",
        }

    except Exception as exc:
        runtime.last_error = str(exc)
        runtime.progress = {
            "current": runtime.progress.get("current", 0),
            "total": 8,
            "status": "ERROR",
            "message": str(exc),
        }
    finally:
        runtime.running = False


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "service": "training-service", "mlflow_tracking_uri": MLFLOW_TRACKING_URI}


@app.get("/api/status")
def status():
    return {
        "running": runtime.running,
        "progress": runtime.progress,
        "results": runtime.results,
        "best": runtime.best,
        "last_error": runtime.last_error,
        "mlflow_public_url": MLFLOW_PUBLIC_URL,
        "experiment_name": EXPERIMENT_NAME,
        "registered_model_name": REGISTERED_MODEL_NAME,
        "features": FEATURE_COLUMNS,
    }


@app.post("/api/train")
def train(req: TrainRequest):
    if runtime.running:
        raise HTTPException(409, "Training suite is already running.")

    runtime.thread = threading.Thread(
        target=run_suite,
        args=(req.train_rows, req.test_rows),
        daemon=True,
    )
    runtime.thread.start()

    return {"status": "started", "runs": 8}


@app.post("/api/reset")
def reset():
    if runtime.running:
        raise HTTPException(409, "Training is still running.")
    runtime.reset()
    return {"status": "reset"}
