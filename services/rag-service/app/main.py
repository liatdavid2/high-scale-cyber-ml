import time
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from app.ingest import ingest_all
from app.store import search_knowledge, embed_query, retrieve_vector
from app.llm import answer_with_context
from app.metrics import metrics_snapshot, record_query
from app.experiments import save_experiment, list_experiments, get_unevaluated, save_evaluation, summary as experiments_summary
from app.evaluator import evaluate_generation
from app.experiment_run import experiment_run

app = FastAPI(title="High Scale Cyber RAG", version="1.1.0")
STATIC_DIR = Path(__file__).resolve().parent / "static"


class QueryRequest(BaseModel):
    query: str = Field(min_length=2)
    top_k: int = Field(default=5, ge=1, le=20)
    use_llm: bool = True
    record_experiment: bool = True
    auto_evaluate: bool = True


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
    track_run = bool(req.record_experiment)

    if track_run:
        experiment_run.start(req.query, req.top_k, req.use_llm, req.auto_evaluate)
        # Document chunking happens during ingestion, not for every query.
        experiment_run.update_stage(
            "chunking",
            "COMPLETED",
            "Knowledge was already chunked and indexed during ingestion.",
            0.0,
        )

    try:
        if track_run:
            experiment_run.update_stage("embedding", "RUNNING", "Creating query embedding with FastEmbed...")
        stage_started = time.perf_counter()
        vector = embed_query(req.query)
        embedding_ms = (time.perf_counter() - stage_started) * 1000
        if track_run:
            experiment_run.update_stage("embedding", "COMPLETED", "Query embedding created.", embedding_ms)

        if track_run:
            experiment_run.update_stage("retrieval", "RUNNING", f"Searching Qdrant for Top-{req.top_k} chunks...")
        stage_started = time.perf_counter()
        matches = retrieve_vector(vector, req.top_k)
        vector_search_ms = (time.perf_counter() - stage_started) * 1000
        retrieval_ms = embedding_ms + vector_search_ms
        if track_run:
            experiment_run.update_stage(
                "retrieval", "COMPLETED", f"Retrieved {len(matches)} chunks from Qdrant.", vector_search_ms
            )

        answer = None
        if req.use_llm:
            if track_run:
                experiment_run.update_stage("generation", "RUNNING", "Generating answer from retrieved context...")
            answer = answer_with_context(req.query, matches)
            generation_ms = answer.get("generation_ms", 0.0) if isinstance(answer, dict) else 0.0
            if track_run:
                experiment_run.update_stage("generation", "COMPLETED", "Answer generated.", generation_ms)
        else:
            generation_ms = 0.0
            if track_run:
                experiment_run.update_stage("generation", "SKIPPED", "Retrieval-only mode; generation disabled.", 0.0)

        input_tokens = answer.get("input_tokens", 0) if isinstance(answer, dict) else 0
        output_tokens = answer.get("output_tokens", 0) if isinstance(answer, dict) else 0
        total_ms_before_eval = (time.perf_counter() - started) * 1000

        perf = {
            "embedding_ms": round(embedding_ms, 3),
            "vector_search_ms": round(vector_search_ms, 3),
            "retrieval_ms": round(retrieval_ms, 3),
            "generation_ms": round(generation_ms, 3),
            "total_ms": round(total_ms_before_eval, 3),
            "input_tokens": int(input_tokens or 0),
            "output_tokens": int(output_tokens or 0),
            "total_tokens": int((input_tokens or 0) + (output_tokens or 0)),
            "top_k": req.top_k,
            "returned_chunks": len(matches),
            "use_llm": req.use_llm,
        }
        record_query(perf)

        evaluation = None
        if req.record_experiment and req.auto_evaluate and req.use_llm:
            if track_run:
                experiment_run.update_stage("evaluation", "RUNNING", "Evaluating groundedness and relevance...")
            stage_started = time.perf_counter()
            try:
                evaluation = evaluate_generation(
                    req.query,
                    (answer or {}).get("text", "") if isinstance(answer, dict) else str(answer or ""),
                    matches,
                )
                eval_ms = (time.perf_counter() - stage_started) * 1000
                if track_run:
                    experiment_run.set_evaluation(evaluation)
                    experiment_run.update_stage(
                        "evaluation",
                        "COMPLETED",
                        f"Groundedness {evaluation.get('groundedness', 0):.3f}, relevance {evaluation.get('relevance', 0):.3f}.",
                        eval_ms,
                    )
            except Exception as eval_exc:
                eval_ms = (time.perf_counter() - stage_started) * 1000
                evaluation = {"error": str(eval_exc)}
                if track_run:
                    experiment_run.set_evaluation(evaluation)
                    experiment_run.update_stage("evaluation", "ERROR", str(eval_exc), eval_ms)
        elif track_run:
            reason = "Auto-evaluation disabled."
            if not req.use_llm:
                reason = "No generated answer to evaluate."
            experiment_run.update_stage("evaluation", "SKIPPED", reason, 0.0)

        experiment_id = None
        if req.record_experiment:
            if track_run:
                experiment_run.update_stage("save", "RUNNING", "Saving query, sources and performance metrics to SQLite...")
            stage_started = time.perf_counter()
            experiment_id = save_experiment(req.query, req.top_k, req.use_llm, answer, matches, perf)
            if evaluation and not evaluation.get("error"):
                save_evaluation(experiment_id, evaluation)
            save_ms = (time.perf_counter() - stage_started) * 1000
            if track_run:
                experiment_run.set_experiment_id(experiment_id)
                experiment_run.update_stage("save", "COMPLETED", f"Saved as experiment #{experiment_id}.", save_ms)

        total_ms = (time.perf_counter() - started) * 1000
        if track_run:
            experiment_run.finish(total_ms)

        return {
            "query": req.query,
            "answer": answer,
            "matches": matches,
            "performance": perf,
            "experiment_id": experiment_id,
            "evaluation": evaluation,
        }
    except Exception as exc:
        if track_run:
            experiment_run.fail(str(exc), (time.perf_counter() - started) * 1000)
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/experiment-run")
def experiment_run_status():
    return experiment_run.snapshot()


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


@app.get("/experiments")
def experiments(limit: int = 500):
    limit = max(1, min(int(limit), 2000))
    return {"records": list_experiments(limit)}


@app.get("/experiments/summary")
def experiments_summary_endpoint():
    return experiments_summary()


@app.post("/experiments/evaluate-all")
def evaluate_all_experiments():
    records = get_unevaluated(limit=200)
    evaluated = []
    failures = []

    for record in records:
        try:
            result = evaluate_generation(
                record["query"],
                record.get("answer_text") or "",
                record.get("matches") or [],
            )
            save_evaluation(record["id"], result)
            evaluated.append({"id": record["id"], **result})
        except Exception as exc:
            failures.append({"id": record["id"], "error": str(exc)})

    return {
        "requested": len(records),
        "evaluated": len(evaluated),
        "failed": len(failures),
        "results": evaluated,
        "failures": failures,
        "summary": experiments_summary(),
    }
