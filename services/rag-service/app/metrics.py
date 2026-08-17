import os
import time
from collections import deque

_STARTED = time.time()
_RECENT = deque(maxlen=500)


def record_query(metric: dict):
    _RECENT.append(metric)


def _percentile(values, p):
    if not values:
        return None
    values = sorted(values)
    if len(values) == 1:
        return round(values[0], 3)
    idx = (len(values) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    frac = idx - lo
    value = values[lo] * (1 - frac) + values[hi] * frac
    return round(value, 3)


def _resource_snapshot():
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return {
            "cpu_percent": round(process.cpu_percent(interval=0.05), 2),
            "memory_mb": round(process.memory_info().rss / (1024 * 1024), 2),
            "system_memory_percent": round(psutil.virtual_memory().percent, 2),
        }
    except Exception:
        return {
            "cpu_percent": None,
            "memory_mb": None,
            "system_memory_percent": None,
        }


def metrics_snapshot():
    recent = list(_RECENT)
    total_ms = [m.get("total_ms", 0) for m in recent]
    retrieval_ms = [m.get("retrieval_ms", 0) for m in recent]
    generation_ms = [m.get("generation_ms", 0) for m in recent if m.get("generation_ms") is not None]

    input_tokens = sum(m.get("input_tokens", 0) or 0 for m in recent)
    output_tokens = sum(m.get("output_tokens", 0) or 0 for m in recent)

    return {
        "service": "rag-service",
        "uptime_seconds": round(time.time() - _STARTED, 2),
        "vector_db": "qdrant",
        "embedding": "BAAI/bge-small-en-v1.5 via FastEmbed",
        "collection": "cyber_knowledge",
        "queries_recorded": len(recent),
        "latency_ms": {
            "p50_total": _percentile(total_ms, 0.50),
            "p95_total": _percentile(total_ms, 0.95),
            "p99_total": _percentile(total_ms, 0.99),
            "p50_retrieval": _percentile(retrieval_ms, 0.50),
            "p95_retrieval": _percentile(retrieval_ms, 0.95),
            "p50_generation": _percentile(generation_ms, 0.50),
            "p95_generation": _percentile(generation_ms, 0.95),
        },
        "tokens": {
            "input": input_tokens,
            "output": output_tokens,
            "total": input_tokens + output_tokens,
        },
        "resources": _resource_snapshot(),
    }
