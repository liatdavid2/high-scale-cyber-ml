import gc
import os
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from app.embedder import embed_texts

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "cyber_knowledge")
INGEST_BATCH_SIZE = int(os.getenv("INGEST_BATCH_SIZE", "128"))

_client = QdrantClient(url=QDRANT_URL)


def ensure_collection(vector_size: int):
    names = {c.name for c in _client.get_collections().collections}
    if COLLECTION not in names:
        _client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )


def upsert_documents(documents: list[dict], batch_size: int = INGEST_BATCH_SIZE):
    if not documents:
        return 0

    indexed = 0
    collection_ready = False

    for start in range(0, len(documents), batch_size):
        batch = documents[start:start + batch_size]
        vectors = embed_texts([d["text"] for d in batch])

        if not vectors:
            continue

        if not collection_ready:
            ensure_collection(len(vectors[0]))
            collection_ready = True

        points = [
            PointStruct(
                id=start + i,
                vector=vectors[i],
                payload=batch[i],
            )
            for i in range(len(batch))
        ]

        _client.upsert(
            collection_name=COLLECTION,
            points=points,
            wait=True,
        )

        indexed += len(points)
        del vectors, points, batch
        gc.collect()

    return indexed


def embed_query(query: str):
    return embed_texts([query])[0]


def retrieve_vector(vector: list[float], top_k: int = 5):
    result = _client.query_points(
        collection_name=COLLECTION,
        query=vector,
        limit=top_k,
        with_payload=True,
    )

    points = getattr(result, "points", result)
    matches = []
    for point in points:
        payload = point.payload or {}
        matches.append({
            "score": float(point.score),
            "source": payload.get("source"),
            "id": payload.get("id"),
            "name": payload.get("name"),
            "text": payload.get("text"),
        })
    return matches


def search_knowledge(query: str, top_k: int = 5):
    vector = embed_query(query)
    return retrieve_vector(vector, top_k)
