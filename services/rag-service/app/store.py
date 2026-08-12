import os
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from app.embedder import embed_texts

QDRANT_URL = os.getenv("QDRANT_URL", "http://qdrant:6333")
COLLECTION = os.getenv("QDRANT_COLLECTION", "cyber_knowledge")

_client = QdrantClient(url=QDRANT_URL)

def ensure_collection(vector_size: int):
    names = {c.name for c in _client.get_collections().collections}
    if COLLECTION not in names:
        _client.create_collection(
            collection_name=COLLECTION,
            vectors_config=VectorParams(size=vector_size, distance=Distance.COSINE),
        )

def upsert_documents(documents: list[dict]):
    if not documents:
        return 0

    vectors = embed_texts([d["text"] for d in documents])
    ensure_collection(len(vectors[0]))

    points = [
        PointStruct(
            id=i,
            vector=vectors[i],
            payload=documents[i],
        )
        for i in range(len(documents))
    ]
    _client.upsert(collection_name=COLLECTION, points=points)
    return len(points)

def search_knowledge(query: str, top_k: int = 5):
    vector = embed_texts([query])[0]

    # qdrant-client versions differ; query_points is the current API.
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
