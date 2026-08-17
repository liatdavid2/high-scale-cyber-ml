import os
import time
from functools import lru_cache
from pathlib import Path

import pandas as pd

from app.store import search_knowledge
from app.llm import answer_with_context

ATTACKQA_FILE = Path(os.getenv("ATTACKQA_FILE", "/data/benchmark/attackqa.parquet"))


def _clean_id(value):
    if value is None:
        return None
    value = str(value).strip()
    return value if value and value.lower() != "nan" else None


def _technique_id(row):
    # AttackQA relation_id can contain the ATT&CK technique when the subject is
    # software/group/campaign. Prefer it when it is a Txxxx id; otherwise use
    # subject_id when the subject itself is a technique.
    relation_id = _clean_id(row.get("relation_id"))
    subject_id = _clean_id(row.get("subject_id"))
    if relation_id and relation_id.startswith("T"):
        return relation_id
    if subject_id and subject_id.startswith("T"):
        return subject_id
    return None


def _matches_expected(retrieved_id: str | None, expected_id: str) -> bool:
    rid = _clean_id(retrieved_id)
    if not rid:
        return False
    if rid == expected_id:
        return True
    # If the official expected id is a parent technique, a retrieved sub-technique
    # still counts as a hit for the parent-level benchmark.
    return "." not in expected_id and rid.startswith(expected_id + ".")


@lru_cache(maxsize=1)
def load_attackqa():
    if not ATTACKQA_FILE.exists():
        raise FileNotFoundError(
            f"AttackQA file not found at {ATTACKQA_FILE}. "
            "Mount ./shared/data/benchmark to /data/benchmark in docker-compose."
        )

    df = pd.read_parquet(ATTACKQA_FILE)
    required = {"question", "answer", "subject_id", "relation_id"}
    missing = required.difference(df.columns)
    if missing:
        raise ValueError(f"AttackQA parquet is missing expected columns: {sorted(missing)}")

    df = df.copy()
    df["expected_id"] = df.apply(_technique_id, axis=1)
    df = df[df["expected_id"].notna()]
    df = df[df["question"].notna() & df["answer"].notna()]
    return df.reset_index(drop=True)


def attackqa_info():
    df = load_attackqa()
    return {
        "dataset": "sambanovasystems/attackqa",
        "file": str(ATTACKQA_FILE),
        "usable_rows": int(len(df)),
        "human_questions": int(df.get("human_question", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
        if "human_question" in df.columns else None,
        "human_answers": int(df.get("human_answer", pd.Series(dtype=bool)).fillna(False).astype(bool).sum())
        if "human_answer" in df.columns else None,
    }


def _select_rows(df, count: int, sample_mode: str):
    work = df
    if sample_mode == "human_question" and "human_question" in work.columns:
        work = work[work["human_question"].fillna(False).astype(bool)]
    elif sample_mode == "human_qa" and {"human_question", "human_answer"}.issubset(work.columns):
        work = work[
            work["human_question"].fillna(False).astype(bool)
            & work["human_answer"].fillna(False).astype(bool)
        ]

    if work.empty:
        raise ValueError(f"No AttackQA rows available for sample_mode={sample_mode}")

    count = min(count, len(work))
    return work.sample(n=count, random_state=42).reset_index(drop=True)


def run_attackqa_benchmark(count: int = 50, top_k: int = 5, sample_mode: str = "all", include_generation: bool = False):
    df = _select_rows(load_attackqa(), count, sample_mode)

    results = []
    reciprocal_ranks = []
    hit1 = 0
    hitk = 0
    retrieval_times = []
    generation_times = []
    total_tokens = 0

    for _, row in df.iterrows():
        question = str(row["question"])
        expected_id = str(row["expected_id"])

        started = time.perf_counter()
        matches = search_knowledge(question, top_k)
        retrieval_ms = (time.perf_counter() - started) * 1000
        retrieval_times.append(retrieval_ms)

        rank = None
        for idx, match in enumerate(matches, 1):
            if _matches_expected(match.get("id"), expected_id):
                rank = idx
                break

        if rank == 1:
            hit1 += 1
        if rank is not None:
            hitk += 1
            reciprocal_ranks.append(1.0 / rank)
        else:
            reciprocal_ranks.append(0.0)

        generated = None
        generation_ms = 0.0
        tokens = 0
        if include_generation:
            generated = answer_with_context(question, matches)
            if isinstance(generated, dict):
                generation_ms = float(generated.get("generation_ms", 0.0) or 0.0)
                tokens = int(generated.get("total_tokens", 0) or 0)
                generation_times.append(generation_ms)
                total_tokens += tokens

        results.append({
            "question": question,
            "expected_id": expected_id,
            "reference_answer": str(row.get("answer", "")),
            "subject_name": str(row.get("subject_name", "") or ""),
            "source": str(row.get("source", "") or ""),
            "human_question": bool(row.get("human_question", False)) if pd.notna(row.get("human_question")) else False,
            "human_answer": bool(row.get("human_answer", False)) if pd.notna(row.get("human_answer")) else False,
            "rank": rank,
            "hit_at_1": rank == 1,
            "hit_at_k": rank is not None,
            "reciprocal_rank": round(1.0 / rank, 4) if rank else 0.0,
            "retrieval_ms": round(retrieval_ms, 3),
            "returned_ids": [m.get("id") for m in matches],
            "generated_answer": generated.get("text") if isinstance(generated, dict) else None,
            "generation_ms": round(generation_ms, 3),
            "tokens": tokens,
        })

    retrieval_sorted = sorted(retrieval_times)
    p95_idx = max(0, min(len(retrieval_sorted) - 1, round(0.95 * (len(retrieval_sorted) - 1))))

    n = len(results)
    response = {
        "dataset": "sambanovasystems/attackqa",
        "benchmark_size": n,
        "sample_mode": sample_mode,
        "top_k": top_k,
        "hit_at_1": round(hit1 / n, 4),
        "hit_at_k": round(hitk / n, 4),
        "mrr": round(sum(reciprocal_ranks) / n, 4),
        "avg_retrieval_ms": round(sum(retrieval_times) / n, 3),
        "p95_retrieval_ms": round(retrieval_sorted[p95_idx], 3),
        "include_generation": include_generation,
        "avg_generation_ms": round(sum(generation_times) / len(generation_times), 3) if generation_times else None,
        "total_tokens": total_tokens,
        "results": results,
    }
    return response
