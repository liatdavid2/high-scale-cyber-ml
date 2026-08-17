import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.ingest import ingest_all
from app.store import search_knowledge
from app.llm import answer_with_context
from app.metrics import metrics_snapshot, record_query

app = FastAPI(title="High Scale Cyber RAG", version="1.1.0")
STATIC_DIR = Path(__file__).resolve().parent / "static"


class QueryRequest(BaseModel):
    query: str = Field(min_length=2)
    top_k: int = Field(default=5, ge=1, le=20)
    use_llm: bool = True


class RetrievalEvalRequest(BaseModel):
    top_k: int = Field(default=5, ge=1, le=20)


RETRIEVAL_BENCHMARK = [
    {"query": "How can attackers dump credentials from operating system memory?", "expected_id": "T1003"},
    {"query": "How do attackers use command and scripting interpreters?", "expected_id": "T1059"},
    {"query": "What technique describes phishing attacks?", "expected_id": "T1566"},
    {"query": "What ATT&CK technique covers remote services?", "expected_id": "T1021"},
    {"query": "How do attackers communicate using application layer protocols?", "expected_id": "T1071"},
]


@app.get("/", include_in_schema=False)
def home():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health")
def health():
    return {"status": "ok", "service": "rag-service"}


@app.post("/ingest")
def ingest():
    try:
        return ingest_all()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/query")
def query(req: QueryRequest):
    started = time.perf_counter()
    try:
        retrieval_started = time.perf_counter()
        matches = search_knowledge(req.query, req.top_k)
        retrieval_ms = (time.perf_counter() - retrieval_started) * 1000

        answer = answer_with_context(req.query, matches) if req.use_llm else None
        generation_ms = answer.get("generation_ms", 0.0) if isinstance(answer, dict) else 0.0
        input_tokens = answer.get("input_tokens", 0) if isinstance(answer, dict) else 0
        output_tokens = answer.get("output_tokens", 0) if isinstance(answer, dict) else 0
        total_ms = (time.perf_counter() - started) * 1000

        perf = {
            "retrieval_ms": round(retrieval_ms, 3),
            "generation_ms": round(generation_ms, 3),
            "total_ms": round(total_ms, 3),
            "input_tokens": int(input_tokens or 0),
            "output_tokens": int(output_tokens or 0),
            "total_tokens": int((input_tokens or 0) + (output_tokens or 0)),
            "top_k": req.top_k,
            "returned_chunks": len(matches),
            "use_llm": req.use_llm,
        }
        record_query(perf)

        return {
            "query": req.query,
            "answer": answer,
            "matches": matches,
            "performance": perf,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/evaluate/retrieval")
def evaluate_retrieval(req: RetrievalEvalRequest):
    results = []
    reciprocal_ranks = []
    hits = 0
    latencies = []

    for item in RETRIEVAL_BENCHMARK:
        started = time.perf_counter()
        matches = search_knowledge(item["query"], req.top_k)
        latency_ms = (time.perf_counter() - started) * 1000
        latencies.append(latency_ms)

        rank = None
        for idx, match in enumerate(matches, 1):
            match_id = str(match.get("id") or "")
            if match_id == item["expected_id"] or match_id.startswith(item["expected_id"] + "."):
                rank = idx
                break

        hit = rank is not None
        if hit:
            hits += 1
            reciprocal_ranks.append(1.0 / rank)
        else:
            reciprocal_ranks.append(0.0)

        results.append({
            "query": item["query"],
            "expected_id": item["expected_id"],
            "hit": hit,
            "rank": rank,
            "latency_ms": round(latency_ms, 3),
            "returned_ids": [m.get("id") for m in matches],
        })

    n = len(RETRIEVAL_BENCHMARK)
    return {
        "benchmark_size": n,
        "top_k": req.top_k,
        "hit_rate_at_k": round(hits / n, 4),
        "mrr": round(sum(reciprocal_ranks) / n, 4),
        "avg_retrieval_ms": round(sum(latencies) / n, 3),
        "results": results,
    }


@app.get("/metrics")
def metrics():
    return metrics_snapshot()
