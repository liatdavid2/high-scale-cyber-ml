import os
import time
from pathlib import Path

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct

DATASET_PATH = Path(
    os.getenv(
        "UNSW_DATASET_PATH",
        "/shared/data/raw/UNSW_NB15_training-set.csv",
    )
)
QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "unsw_similar_cases")
INGEST_BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "1000"))
MAX_INDEX_ROWS = int(os.getenv("MAX_INDEX_ROWS", "175341"))

FEATURE_COLUMNS = [
    "dur",
    "rate",
    "total_bytes",
    "total_packets",
    "byte_ratio_src_dst",
    "packet_ratio_src_dst",
    "bytes_per_packet",
]

RAW_COLUMNS = ["dur", "rate", "sbytes", "dbytes", "spkts", "dpkts"]

_client = QdrantClient(url=QDRANT_URL)

_stats = None


def _load_df():
    if not DATASET_PATH.exists():
        raise FileNotFoundError(f"Dataset not found: {DATASET_PATH}")

    df = pd.read_csv(DATASET_PATH)

    required = RAW_COLUMNS + ["label"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    if len(df) > MAX_INDEX_ROWS:
        df = df.iloc[:MAX_INDEX_ROWS].copy()

    return df.reset_index(drop=True)


def _engineer(df):
    d = df.copy()
    for c in RAW_COLUMNS:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0.0)

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

    # Log-transform heavy-tailed non-negative traffic features.
    for c in FEATURE_COLUMNS:
        X[c] = np.log1p(np.clip(X[c].astype(float), 0, None))

    return X


def _fit_stats(X):
    mean = X.mean().to_numpy(dtype=np.float32)
    std = X.std(ddof=0).replace(0, 1).to_numpy(dtype=np.float32)
    return mean, std


def _to_vectors(X, mean, std):
    arr = X[FEATURE_COLUMNS].to_numpy(dtype=np.float32)
    arr = (arr - mean) / std
    norms = np.linalg.norm(arr, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    return arr / norms


def _ensure_collection(dim):
    names = {c.name for c in _client.get_collections().collections}
    if COLLECTION in names:
        _client.delete_collection(COLLECTION)

    _client.create_collection(
        collection_name=COLLECTION,
        vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
    )


def build_index():
    global _stats

    started = time.perf_counter()
    df = _load_df()
    X = _engineer(df)

    mean, std = _fit_stats(X)
    _stats = (mean, std)

    _ensure_collection(len(FEATURE_COLUMNS))

    indexed = 0

    for start in range(0, len(df), INGEST_BATCH_SIZE):
        end = min(start + INGEST_BATCH_SIZE, len(df))

        batch_x = X.iloc[start:end]
        vectors = _to_vectors(batch_x, mean, std)

        points = []

        for local_i, vector in enumerate(vectors):
            row_idx = start + local_i
            row = df.iloc[row_idx]
            original_features = {
                c: float(pd.to_numeric(row[c], errors="coerce") or 0.0)
                for c in RAW_COLUMNS
            }

            engineered = _raw_to_engineered(original_features)

            payload = {
                "row_index": int(row_idx),
                "label": int(pd.to_numeric(row.get("label", 0), errors="coerce") or 0),
                "attack_cat": str(row.get("attack_cat", "")),
                "features": engineered,
            }

            points.append(
                PointStruct(
                    id=int(row_idx),
                    vector=vector.tolist(),
                    payload=payload,
                )
            )

        _client.upsert(
            collection_name=COLLECTION,
            points=points,
            wait=True,
        )

        indexed += len(points)

    elapsed = (time.perf_counter() - started) * 1000

    return {
        "status": "ok",
        "indexed": indexed,
        "collection": COLLECTION,
        "features_used": FEATURE_COLUMNS,
        "label_used_for_retrieval": False,
        "attack_cat_used_for_retrieval": False,
        "elapsed_ms": elapsed,
    }


def _ensure_stats():
    global _stats

    if _stats is None:
        df = _load_df()
        X = _engineer(df)
        _stats = _fit_stats(X)

    return _stats


def _raw_to_engineered(raw):
    sbytes = float(raw.get("sbytes", 0))
    dbytes = float(raw.get("dbytes", 0))
    spkts = float(raw.get("spkts", 0))
    dpkts = float(raw.get("dpkts", 0))

    total_bytes = sbytes + dbytes
    total_packets = spkts + dpkts

    return {
        "dur": float(raw.get("dur", 0)),
        "rate": float(raw.get("rate", 0)),
        "total_bytes": total_bytes,
        "total_packets": total_packets,
        "byte_ratio_src_dst": sbytes / (dbytes + 1.0),
        "packet_ratio_src_dst": spkts / (dpkts + 1.0),
        "bytes_per_packet": total_bytes / (total_packets + 1.0),
    }


def _vector_for_engineered(engineered):
    mean, std = _ensure_stats()

    values = np.array(
        [np.log1p(max(float(engineered[c]), 0.0)) for c in FEATURE_COLUMNS],
        dtype=np.float32,
    )

    vec = (values - mean) / std
    norm = np.linalg.norm(vec)
    if norm == 0:
        norm = 1.0

    return (vec / norm).tolist()


def _search(engineered, top_k, exclude_row=None):
    vector = _vector_for_engineered(engineered)

    # Ask for one extra result when querying an indexed row, because the row itself
    # is normally the nearest neighbour and must be excluded from evaluation.
    limit = top_k + (1 if exclude_row is not None else 0)

    started = time.perf_counter()

    response = _client.query_points(
        collection_name=COLLECTION,
        query=vector,
        limit=limit,
        with_payload=True,
    )

    retrieval_ms = (time.perf_counter() - started) * 1000

    points = getattr(response, "points", response)

    matches = []

    for point in points:
        p = point.payload or {}

        if exclude_row is not None and int(p.get("row_index", -1)) == exclude_row:
            continue

        matches.append({
            "row_index": p.get("row_index"),
            "similarity": float(point.score),
            "label": p.get("label"),
            "attack_cat": p.get("attack_cat"),
            "features": p.get("features", {}),
        })

        if len(matches) >= top_k:
            break

    return matches, retrieval_ms


def query_by_row(row_index, top_k):
    df = _load_df()

    if row_index >= len(df):
        raise IndexError(f"row_index {row_index} out of range 0..{len(df)-1}")

    row = df.iloc[row_index]

    raw = {
        c: float(pd.to_numeric(row[c], errors="coerce") or 0.0)
        for c in RAW_COLUMNS
    }

    engineered = _raw_to_engineered(raw)
    matches, retrieval_ms = _search(engineered, top_k, exclude_row=row_index)

    return {
        "query_row_index": row_index,
        "query_label": int(pd.to_numeric(row.get("label", 0), errors="coerce") or 0),
        "query_attack_cat": str(row.get("attack_cat", "")),
        "query_features": engineered,
        "matches": matches,
        "retrieval_ms": retrieval_ms,
    }


def query_by_features(raw, top_k):
    engineered = _raw_to_engineered(raw)
    matches, retrieval_ms = _search(engineered, top_k)

    return {
        "query_features": engineered,
        "matches": matches,
        "retrieval_ms": retrieval_ms,
    }


def benchmark_retrieval(questions=100, top_k=5):
    df = _load_df()

    if questions > len(df):
        questions = len(df)

    sample = df.sample(n=questions, random_state=42)

    rows = []
    latencies = []

    for idx in sample.index:
        result = query_by_row(int(idx), top_k)
        query_label = result["query_label"]
        query_attack = result["query_attack_cat"]
        matches = result["matches"]

        same_label = [
            int(m["label"] == query_label)
            for m in matches
        ]

        same_attack = [
            int(bool(query_attack) and m.get("attack_cat") == query_attack)
            for m in matches
        ]

        latencies.append(result["retrieval_ms"])

        rows.append({
            "row_index": int(idx),
            "label": query_label,
            "attack_cat": query_attack,
            "top1_similarity": matches[0]["similarity"] if matches else 0.0,
            "mean_similarity": float(np.mean([m["similarity"] for m in matches])) if matches else 0.0,
            "label_agreement_at_k": float(np.mean(same_label)) if same_label else 0.0,
            "attack_cat_agreement_at_k": float(np.mean(same_attack)) if same_attack else 0.0,
            "top1_same_label": same_label[0] if same_label else 0,
            "retrieval_ms": result["retrieval_ms"],
        })

    latencies_arr = np.array(latencies, dtype=float)

    return {
        "questions": len(rows),
        "top_k": top_k,
        "top1_label_accuracy": float(np.mean([r["top1_same_label"] for r in rows])),
        "mean_label_agreement_at_k": float(np.mean([r["label_agreement_at_k"] for r in rows])),
        "mean_attack_cat_agreement_at_k": float(np.mean([r["attack_cat_agreement_at_k"] for r in rows])),
        "mean_top1_similarity": float(np.mean([r["top1_similarity"] for r in rows])),
        "avg_retrieval_ms": float(np.mean(latencies_arr)),
        "p95_retrieval_ms": float(np.percentile(latencies_arr, 95)),
        "details": rows,
    }


def dataset_status():
    exists = DATASET_PATH.exists()

    collection_exists = False
    points = 0

    try:
        names = {c.name for c in _client.get_collections().collections}
        collection_exists = COLLECTION in names
        if collection_exists:
            info = _client.get_collection(COLLECTION)
            points = int(info.points_count or 0)
    except Exception:
        pass

    return {
        "dataset_path": str(DATASET_PATH),
        "dataset_exists": exists,
        "collection": COLLECTION,
        "collection_exists": collection_exists,
        "indexed_points": points,
        "features_used": FEATURE_COLUMNS,
        "label_used_for_retrieval": False,
        "attack_cat_used_for_retrieval": False,
    }


def get_row_info(row_index):
    df = _load_df()
    if row_index < 0 or row_index >= len(df):
        raise IndexError(f"row_index {row_index} out of range 0..{len(df)-1}")
    row = df.iloc[row_index]
    label = int(pd.to_numeric(row.get("label", 0), errors="coerce") or 0)
    return {
        "row_index": int(row_index),
        "label": label,
        "label_name": "Attack" if label == 1 else "Normal",
        "attack_cat": str(row.get("attack_cat", "")),
    }


def get_random_row(label=None):
    df = _load_df()
    if label is not None:
        labels = pd.to_numeric(df["label"], errors="coerce").fillna(0).astype(int)
        df = df[labels == int(label)]
    if len(df) == 0:
        raise ValueError("No rows match the requested label")
    row = df.sample(n=1).iloc[0]
    row_index = int(row.name)
    row_label = int(pd.to_numeric(row.get("label", 0), errors="coerce") or 0)
    return {
        "row_index": row_index,
        "label": row_label,
        "label_name": "Attack" if row_label == 1 else "Normal",
        "attack_cat": str(row.get("attack_cat", "")),
    }
