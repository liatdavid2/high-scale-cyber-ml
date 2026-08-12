import time

_STARTED = time.time()

def metrics_snapshot():
    return {
        "service": "rag-service",
        "uptime_seconds": round(time.time() - _STARTED, 2),
        "vector_db": "qdrant",
        "embedding": "BAAI/bge-small-en-v1.5 via FastEmbed",
        "collection": "cyber_knowledge",
    }
