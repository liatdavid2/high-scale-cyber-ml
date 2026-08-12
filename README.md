# High Scale Cyber RAG

This ZIP contains the RAG service that plugs into the high-scale cyber/ML project.

Chosen stack:
- Qdrant
- FastEmbed local CPU embeddings
- FastAPI
- MITRE ATT&CK + CAPEC
- Optional OpenAI generation

Ports preserved from the project plan:
- Dataset service: 2000
- Streaming: 2050
- Feature service: 2100
- RAG: 2300
- Qdrant: 6333
- Kafka external: 29092

Start this RAG slice with:

```bash
docker compose -f docker-compose.rag.yml up --build
```

Then:

```bash
curl.exe -X POST http://localhost:2300/ingest
```
