from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from app.ingest import ingest_all
from app.store import search_knowledge
from app.llm import answer_with_context
from app.metrics import metrics_snapshot

app = FastAPI(title="High Scale Cyber RAG", version="1.0.0")

class QueryRequest(BaseModel):
    query: str = Field(min_length=2)
    top_k: int = Field(default=5, ge=1, le=20)
    use_llm: bool = True

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
    try:
        matches = search_knowledge(req.query, req.top_k)
        answer = answer_with_context(req.query, matches) if req.use_llm else None
        return {
            "query": req.query,
            "answer": answer,
            "matches": matches,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

@app.get("/metrics")
def metrics():
    return metrics_snapshot()
