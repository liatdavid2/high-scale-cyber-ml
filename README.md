# high-scale-cyber-ml

Incremental high-scale cyber ML engineering project.

The project is designed to run locally on CPU with Docker Compose, while keeping the architecture easy to extend later to cloud infrastructure.

## Current stage

### Stage 1 — Dataset Service

Loads and evaluates UNSW-NB15 CSV files.

Current checks:

- row count
- schema / column names
- detected label column
- number of classes
- class distribution
- missing values
- duplicate rows
- approximate memory usage

## Repository structure

```text
high-scale-cyber-ml/
├── docker-compose.yml
├── README.md
├── .env
├── services/
│   └── dataset-service/
│       ├── app/
│       │   ├── main.py
│       │   └── static/
│       │       ├── index.html
│       │       ├── style.css
│       │       └── app.js
│       ├── Dockerfile
│       ├── requirements.txt
│       └── README.md
└── shared/
    ├── data/
    │   ├── raw/
    │   └── processed/
    └── schemas/
```

## Why data is under `shared/`

The dataset is intentionally outside `dataset-service`.

Later services will need the same data:

```text
dataset-service
streaming-service
training-service
evaluation-service
```

Keeping the data in `shared/data/` avoids copying large files between services.

## Add UNSW-NB15

Copy one or both files to:

```text
shared/data/raw/
```

Expected names:

```text
UNSW_NB15_training-set.csv
UNSW_NB15_testing-set.csv
```

Underscore variants are also accepted.

## Run

From the repository root:

```bash
docker compose up --build
```

Open:

```text
http://localhost:8000
```

Health:

```text
http://localhost:8000/health
```

JSON profile:

```text
http://localhost:8000/api/profile
```

## Stop

```bash
docker compose down
```

## Planned services

```text
dataset-service
streaming-service
feature-service
training-service
inference-service
load-test-service
monitoring-service
```

Infrastructure services such as Kafka, Redis and MinIO will also be added to the root Docker Compose as needed.
