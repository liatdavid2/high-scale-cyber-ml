import json
import os
import sqlite3
from pathlib import Path

DB_PATH = Path(os.getenv("SELF_RAG_DB_PATH", "/app/data/self_rag_history.db"))


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                query_type TEXT NOT NULL,
                query_payload TEXT NOT NULL,
                top_k INTEGER NOT NULL,
                matches TEXT NOT NULL,
                explanation TEXT NOT NULL,
                self_check TEXT,
                retrieval_ms REAL,
                total_ms REAL
            )
        """)


def save_query(
    query_type,
    query_payload,
    top_k,
    matches,
    explanation,
    self_check,
    retrieval_ms,
    total_ms,
):
    with _connect() as conn:
        cur = conn.execute(
            """
            INSERT INTO queries (
                query_type, query_payload, top_k, matches,
                explanation, self_check, retrieval_ms, total_ms
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                query_type,
                json.dumps(query_payload),
                top_k,
                json.dumps(matches),
                json.dumps(explanation),
                json.dumps(self_check) if self_check is not None else None,
                retrieval_ms,
                total_ms,
            ),
        )
        return cur.lastrowid


def list_queries(limit=200):
    with _connect() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM queries
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    out = []

    for row in rows:
        item = dict(row)

        for key in ["query_payload", "matches", "explanation", "self_check"]:
            if item.get(key):
                try:
                    item[key] = json.loads(item[key])
                except Exception:
                    pass

        out.append(item)

    return out
