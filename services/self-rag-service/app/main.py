import os
import time
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct, Filter, FieldCondition, MatchValue

from app.store import (
    DATASET_PATH,
    COLLECTION,
    build_index,
    dataset_status,
    query_by_row,
    query_by_features,
    benchmark_retrieval,
    get_row_info,
    get_random_row,
)
from app.explain import explain_cases, llm_self_check
from app.history import init_db, save_query, list_queries

STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(title="UNSW Similar Cases Self-RAG", version="1.0.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

init_db()


class RowQuery(BaseModel):
    row_index: int = Field(ge=0)
    top_k: int = Field(default=5, ge=1, le=20)
    use_llm: bool = False


class FeatureQuery(BaseModel):
    dur: float
    rate: float
    sbytes: float
    dbytes: float
    spkts: float
    dpkts: float
    top_k: int = Field(default=5, ge=1, le=20)
    use_llm: bool = False


class BenchmarkRequest(BaseModel):
    questions: int = Field(default=100, ge=10, le=1000)
    top_k: int = Field(default=5, ge=1, le=20)


@app.get("/")
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "self-rag-service",
        "dataset": str(DATASET_PATH),
        "collection": COLLECTION,
    }


@app.get("/api/status")
def status():
    return dataset_status()


@app.get("/api/row/{row_index}")
def row_info(row_index: int):
    try:
        return get_row_info(row_index)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.get("/api/random-row")
def random_row(label: Optional[int] = None):
    try:
        return get_random_row(label)
    except Exception as exc:
        raise HTTPException(400, str(exc)) from exc


@app.post("/api/ingest")
def ingest():
    try:
        return build_index()
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.post("/api/query/row")
def query_row(req: RowQuery):
    started = time.perf_counter()
    result = query_by_row(req.row_index, req.top_k)
    deterministic = explain_cases(result["query_features"], result["matches"])

    matches = result["matches"]
    query_label = result["query_label"]
    query_attack = result["query_attack_cat"]

    label_matches = [int(m.get("label") == query_label) for m in matches]
    attack_matches = [int(bool(query_attack) and m.get("attack_cat") == query_attack) for m in matches]

    ground_truth_eval = {
        "top1_label_match": bool(label_matches[0]) if label_matches else False,
        "top1_attack_match": bool(attack_matches[0]) if attack_matches else False,
        "label_agreement_at_k": (sum(label_matches) / len(label_matches)) if label_matches else 0.0,
        "attack_agreement_at_k": (sum(attack_matches) / len(attack_matches)) if attack_matches else 0.0,
    }

    self_check = None
    self_check_ms = 0.0

    if req.use_llm:
        judge_started = time.perf_counter()
        self_check = llm_self_check(
            query_features=result["query_features"],
            matches=result["matches"],
            deterministic_explanation=deterministic,
        )
        self_check_ms = (time.perf_counter() - judge_started) * 1000

    total_ms = (time.perf_counter() - started) * 1000

    record_id = save_query(
        query_type="row",
        query_payload={"row_index": req.row_index},
        top_k=req.top_k,
        matches=result["matches"],
        explanation=deterministic,
        self_check=self_check,
        retrieval_ms=result["retrieval_ms"],
        total_ms=total_ms,
    )

    return {
        **result,
        "explanation": deterministic,
        "ground_truth_eval": ground_truth_eval,
        "self_check": self_check,
        "self_check_ms": self_check_ms,
        "total_ms": total_ms,
        "record_id": record_id,
    }


@app.post("/api/query/features")
def query_features(req: FeatureQuery):
    payload = req.model_dump()
    top_k = payload.pop("top_k")
    use_llm = payload.pop("use_llm")

    started = time.perf_counter()
    result = query_by_features(payload, top_k)
    deterministic = explain_cases(result["query_features"], result["matches"])

    self_check = None
    self_check_ms = 0.0

    if use_llm:
        judge_started = time.perf_counter()
        self_check = llm_self_check(
            query_features=result["query_features"],
            matches=result["matches"],
            deterministic_explanation=deterministic,
        )
        self_check_ms = (time.perf_counter() - judge_started) * 1000

    total_ms = (time.perf_counter() - started) * 1000

    record_id = save_query(
        query_type="features",
        query_payload=payload,
        top_k=top_k,
        matches=result["matches"],
        explanation=deterministic,
        self_check=self_check,
        retrieval_ms=result["retrieval_ms"],
        total_ms=total_ms,
    )

    return {
        **result,
        "explanation": deterministic,
        "self_check": self_check,
        "self_check_ms": self_check_ms,
        "total_ms": total_ms,
        "record_id": record_id,
    }


@app.post("/api/benchmark")
def benchmark(req: BenchmarkRequest):
    try:
        return benchmark_retrieval(req.questions, req.top_k)
    except Exception as exc:
        raise HTTPException(500, str(exc)) from exc


@app.get("/api/history")
def history():
    return list_queries(limit=200)
