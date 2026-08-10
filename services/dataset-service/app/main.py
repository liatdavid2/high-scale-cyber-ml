import os
from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

STATIC_DIR = Path(__file__).resolve().parent / "static"
DATA_DIR = Path(os.getenv("DATA_DIR", "/shared/data/raw"))

TRAIN_NAMES = [
    "UNSW_NB15_training-set.csv",
    "UNSW_NB15_training_set.csv",
]

TEST_NAMES = [
    "UNSW_NB15_testing-set.csv",
    "UNSW_NB15_testing_set.csv",
]

app = FastAPI(
    title="UNSW-NB15 Dataset Service",
    description="Stage 1 of high-scale-cyber-ml",
    version="1.0.0",
)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


def find_file(candidates: list[str]) -> Optional[Path]:
    for name in candidates:
        path = DATA_DIR / name
        if path.exists():
            return path
    return None


def load_csv(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path)
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"Could not read {path.name}: {exc}",
        ) from exc


def detect_label_column(df: pd.DataFrame) -> Optional[str]:
    for name in ("attack_cat", "label"):
        if name in df.columns:
            return name
    return None


def profile_dataframe(df: pd.DataFrame, filename: str) -> dict:
    label_col = detect_label_column(df)

    missing = df.isna().sum()
    missing_by_column = {
        str(col): int(value)
        for col, value in missing.items()
        if int(value) > 0
    }

    class_distribution = {}
    classes = None

    if label_col:
        counts = (
            df[label_col]
            .fillna("<missing>")
            .astype(str)
            .value_counts(dropna=False)
        )
        class_distribution = {
            str(label): int(count)
            for label, count in counts.items()
        }
        classes = int(df[label_col].nunique(dropna=True))

    return {
        "file": filename,
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
        "column_names": [str(c) for c in df.columns],
        "label_column": label_col,
        "classes": classes,
        "class_distribution": class_distribution,
        "missing_cells": int(df.isna().sum().sum()),
        "missing_by_column": missing_by_column,
        "duplicate_rows": int(df.duplicated().sum()),
        "memory_mb": round(
            float(df.memory_usage(deep=True).sum()) / (1024 * 1024),
            2,
        ),
    }


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "dataset-service",
        "data_dir": str(DATA_DIR),
        "train_found": find_file(TRAIN_NAMES) is not None,
        "test_found": find_file(TEST_NAMES) is not None,
    }


@app.get("/api/profile")
def profile():
    paths = [
        path
        for path in (
            find_file(TRAIN_NAMES),
            find_file(TEST_NAMES),
        )
        if path is not None
    ]

    if not paths:
        raise HTTPException(
            status_code=404,
            detail=(
                "No UNSW-NB15 CSV files found. Copy "
                "UNSW_NB15_training-set.csv and/or "
                "UNSW_NB15_testing-set.csv into shared/data/raw/."
            ),
        )

    profiled = []

    for path in paths:
        df = load_csv(path)
        profiled.append(profile_dataframe(df, path.name))

    return {
        "dataset": "UNSW-NB15",
        "service": "dataset-service",
        "data_dir": str(DATA_DIR),
        "files": profiled,
        "total_rows": sum(item["rows"] for item in profiled),
        "total_files": len(profiled),
    }
