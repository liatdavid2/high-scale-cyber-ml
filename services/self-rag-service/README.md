# UNSW Similar Cases Self-RAG Service

A second RAG-style service for the high-scale cyber ML project.

## Goal

Given a UNSW-NB15 network record, retrieve the most similar historical traffic cases,
explain why they are similar, and evaluate whether the similarity is meaningful.

## Important leakage rule

`label` and `attack_cat` are **never included in the retrieval vector**.

Retrieval uses only:

- dur
- rate
- total_bytes
- total_packets
- byte_ratio_src_dst
- packet_ratio_src_dst
- bytes_per_packet

The label and attack category are kept only in Qdrant payload for post-retrieval evaluation.

## Self-RAG-style flow

```text
UNSW query case
    ↓
feature engineering
    ↓
normalized case vector
    ↓
Qdrant Top-K similar cases
    ↓
deterministic feature-level explanation
    ↓
optional OpenAI self-check
    ↓
saved experiment record
```

## Evaluation

The benchmark samples known rows, excludes the exact same row from retrieval,
then checks:

- Top-1 label accuracy
- Label Agreement@K
- Attack Category Agreement@K
- Mean Top-1 similarity
- Average retrieval latency
- P95 retrieval latency

These are not classification metrics. They evaluate whether nearest neighbours are
actually semantically/class-wise similar.

## Add to docker-compose

Copy `compose-snippet.yml` into the main project's compose configuration.

Expected dataset:

```text
shared/data/raw/UNSW_NB15_training-set.csv
```

The service reuses the project's Qdrant instance and creates a separate collection:

```text
unsw_similar_cases
```

## Run

```bash
docker compose up --build self-rag-service
```

Open:

```text
http://localhost:2400/
```

First click:

```text
Ingest / Rebuild Index
```

Then query any dataset row, e.g.:

```text
row_index = 100
Top K = 5
```


## Updated UI
- automatic row lookup shows Label + Attack Category before retrieval
- Random Normal / Random Attack buttons
- similarity shown with 5 decimal places
- explanations show query value, retrieved-case value, and relative difference
- latency split into Retrieval / Self-RAG Judge / Total
