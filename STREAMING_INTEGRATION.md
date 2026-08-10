# Add Stage 2 to the existing repository

Extract this ZIP into the root of:

```text
high-scale-cyber-ml/
```

It will add:

```text
services/streaming-service/
```

Then update the root `docker-compose.yml`.

## Recommended complete Compose for Stage 1 + Stage 2

Keep your existing dataset-service configuration if it already works, but add Kafka and streaming-service.

```yaml
services:
  dataset-service:
    build:
      context: ./services/dataset-service
      dockerfile: Dockerfile
    container_name: high-scale-cyber-ml-dataset
    ports:
      - "2000:8000"
    environment:
      DATA_DIR: /shared/data/raw
    volumes:
      - ./shared/data:/shared/data
    restart: unless-stopped

  kafka:
    image: bitnami/kafka:3.7
    container_name: high-scale-cyber-ml-kafka
    ports:
      - "29092:29092"
    environment:
      KAFKA_ENABLE_KRAFT: "yes"
      KAFKA_CFG_NODE_ID: "1"
      KAFKA_CFG_PROCESS_ROLES: "broker,controller"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
      KAFKA_CFG_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:29092"
      KAFKA_CFG_ADVERTISED_LISTENERS: "INTERNAL://kafka:9092,EXTERNAL://localhost:29092"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "1@kafka:9093"
      KAFKA_CFG_INTER_BROKER_LISTENER_NAME: "INTERNAL"
      ALLOW_PLAINTEXT_LISTENER: "yes"
    volumes:
      - kafka-data:/bitnami/kafka
    restart: unless-stopped

  streaming-service:
    build:
      context: ./services/streaming-service
      dockerfile: Dockerfile
    container_name: high-scale-cyber-ml-streaming
    ports:
      - "2050:8000"
    environment:
      DATA_DIR: /shared/data/raw
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: unsw-events
    volumes:
      - ./shared/data:/shared/data:ro
    depends_on:
      - kafka
    restart: unless-stopped

volumes:
  kafka-data:
```

## Run

```bash
docker compose down
docker compose up --build
```

Dataset UI:

```text
http://localhost:2000
```

Streaming UI:

```text
http://localhost:2050
```

## First test

Start low:

```text
100 events/sec
```

Then try:

```text
500
1000
2000
5000
```

Watch:

```text
Produced/sec
Processed/sec
Kafka Lag
Errors
```

If produced/sec is consistently higher than processed/sec, lag should grow.
That is the first high-scale bottleneck experiment.
