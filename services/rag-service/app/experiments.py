import json
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(os.getenv("EVALUATION_DB_PATH", "/app/data/rag_evaluations.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def _connect():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _connect() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS experiment_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                query TEXT NOT NULL,
                top_k INTEGER NOT NULL,
                use_llm INTEGER NOT NULL,
                answer_text TEXT,
                answer_model TEXT,
                retrieved_json TEXT NOT NULL,
                retrieval_ms REAL NOT NULL,
                generation_ms REAL NOT NULL,
                total_ms REAL NOT NULL,
                input_tokens INTEGER NOT NULL,
                output_tokens INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                returned_chunks INTEGER NOT NULL,
                top1_similarity REAL,
                mean_similarity REAL,
                source_diversity INTEGER NOT NULL,
                eval_groundedness REAL,
                eval_relevance REAL,
                eval_notes TEXT,
                eval_model TEXT,
                eval_input_tokens INTEGER,
                eval_output_tokens INTEGER,
                evaluated_at TEXT
            )
            """
        )
        conn.commit()


def save_experiment(query: str, top_k: int, use_llm: bool, answer: dict | None, matches: list[dict], perf: dict) -> int:
    scores = [float(m.get("score")) for m in matches if m.get("score") is not None]
    sources = {str(m.get("source")) for m in matches if m.get("source")}
    answer = answer or {}

    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO experiment_records (
                created_at, query, top_k, use_llm, answer_text, answer_model,
                retrieved_json, retrieval_ms, generation_ms, total_ms,
                input_tokens, output_tokens, total_tokens, returned_chunks,
                top1_similarity, mean_similarity, source_diversity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                datetime.now(timezone.utc).isoformat(),
                query,
                int(top_k),
                1 if use_llm else 0,
                answer.get("text") or answer.get("message"),
                answer.get("model"),
                json.dumps(matches, ensure_ascii=False),
                float(perf.get("retrieval_ms", 0.0)),
                float(perf.get("generation_ms", 0.0)),
                float(perf.get("total_ms", 0.0)),
                int(perf.get("input_tokens", 0)),
                int(perf.get("output_tokens", 0)),
                int(perf.get("total_tokens", 0)),
                int(perf.get("returned_chunks", len(matches))),
                scores[0] if scores else None,
                (sum(scores) / len(scores)) if scores else None,
                len(sources),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)


def _row_to_dict(row):
    d = dict(row)
    try:
        d["matches"] = json.loads(d.pop("retrieved_json") or "[]")
    except Exception:
        d["matches"] = []
        d.pop("retrieved_json", None)
    d["use_llm"] = bool(d.get("use_llm"))
    return d


def list_experiments(limit: int = 500):
    with _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM experiment_records ORDER BY id DESC LIMIT ?",
            (int(limit),),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def get_unevaluated(limit: int = 200):
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT * FROM experiment_records
            WHERE use_llm = 1
              AND answer_text IS NOT NULL
              AND answer_text != ''
              AND eval_groundedness IS NULL
            ORDER BY id ASC
            LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    return [_row_to_dict(r) for r in rows]


def save_evaluation(record_id: int, evaluation: dict):
    with _connect() as conn:
        conn.execute(
            """
            UPDATE experiment_records
            SET eval_groundedness = ?, eval_relevance = ?, eval_notes = ?,
                eval_model = ?, eval_input_tokens = ?, eval_output_tokens = ?,
                evaluated_at = ?
            WHERE id = ?
            """,
            (
                float(evaluation.get("groundedness", 0.0)),
                float(evaluation.get("relevance", 0.0)),
                evaluation.get("notes", ""),
                evaluation.get("model"),
                int(evaluation.get("input_tokens", 0)),
                int(evaluation.get("output_tokens", 0)),
                datetime.now(timezone.utc).isoformat(),
                int(record_id),
            ),
        )
        conn.commit()


def summary():
    with _connect() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN eval_groundedness IS NOT NULL THEN 1 ELSE 0 END) AS evaluated,
                AVG(eval_groundedness) AS avg_groundedness,
                AVG(eval_relevance) AS avg_relevance,
                AVG(top1_similarity) AS avg_top1_similarity,
                AVG(mean_similarity) AS avg_mean_similarity,
                AVG(retrieval_ms) AS avg_retrieval_ms,
                AVG(generation_ms) AS avg_generation_ms,
                AVG(total_ms) AS avg_total_ms,
                SUM(input_tokens) AS input_tokens,
                SUM(output_tokens) AS output_tokens
            FROM experiment_records
            """
        ).fetchone()
    return dict(row)


init_db()
