# Cyber RAG Service

Architecture from the high-scale cyber/ML project:

- FastAPI RAG service: port `2300`
- Qdrant: port `6333`
- Local CPU embeddings: FastEmbed / `BAAI/bge-small-en-v1.5`
- Qdrant collection: `cyber_knowledge`
- Knowledge: MITRE ATT&CK + CAPEC
- Optional LLM generation through OpenAI; retrieval still works without an API key.

## Expected data

Keep the already-downloaded knowledge outside the service:

```text
shared/
  data/
    knowledge/
      mitre/
        enterprise-attack.json
      capec/
        capec.json
```

For CAPEC the loader also accepts:
- `capec-stix.json`
- `stix-capec.json`

## Run

From the project root:

```bash
docker compose -f docker-compose.rag.yml up --build
```

Then ingest once:

```bash
curl.exe -X POST http://localhost:2300/ingest
```

Query:

```bash
curl.exe -X POST http://localhost:2300/query ^
  -H "Content-Type: application/json" ^
  -d "{\"query\":\"Explain credential access techniques\",\"top_k\":5,\"use_llm\":false}"
```

API docs:
`http://localhost:2300/docs`

Qdrant dashboard:
`http://localhost:6333/dashboard`

## Endpoints

- `GET /health`
- `POST /ingest`
- `POST /query`
- `GET /metrics`

## Evaluation UI

Open `http://localhost:2300/` for the RAG Evaluation Console.
It includes Query, Retrieval Benchmark, Performance/Token metrics, and a browser-driven Scale Test.
Swagger remains available at `http://localhost:2300/docs`.

## Low-memory ingestion

Knowledge ingestion is processed in batches instead of embedding all MITRE/CAPEC documents at once.
The default batch size is 128 documents. Override it with:

```text
INGEST_BATCH_SIZE=64
```

Use a smaller value if Docker is still memory constrained.


## Experiment history

Normal `/query` requests are persisted as experiment records in SQLite at:

```text
/app/data/rag_evaluations.db
```

For an explicit host folder in Docker Compose, mount:

```yaml
volumes:
  - ./shared/rag_evaluations:/app/data
```

Load-test requests from the UI use `record_experiment=false`, so they do not flood the experiment history.
The **Evaluation History** tab can evaluate all stored, unevaluated generated answers for groundedness and relevance. This uses additional OpenAI calls.

## Live Experiment Run

The UI includes an **Experiment Run** tab. Normal queries are tracked live through:

`Chunking / knowledge prep -> Query embedding -> Vector retrieval -> LLM generation -> Answer evaluation -> Save experiment`

Chunking is an ingestion-time step, so during a query it is reported as already prepared. "Evaluate this run" can be disabled to avoid the additional OpenAI judge call.
