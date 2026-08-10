# Streaming Service v2 integration

Replace the existing:

```text
services/streaming-service/
```

with the folder from this ZIP.

## Docker Compose

Keep your existing services and make sure `streaming-service` includes:

```yaml
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
      KAFKA_PARTITIONS: 4
    volumes:
      - ./shared/data:/shared/data:ro
    depends_on:
      - kafka
    restart: unless-stopped
```

## Important: existing Kafka topic

If `unsw-events` was already created with one partition, changing the environment variable does NOT change the existing topic.

For a clean local experiment, easiest option:

```bash
docker compose down -v
docker compose up --build
```

This removes the local Kafka volume and recreates the topic with 4 partitions.

Use this only if you do not care about keeping old Kafka messages.

## UI

```text
http://localhost:2050
```

## Experiment

Run each for 30–60 seconds:

```text
500
750
1000
1250
1500
2000
```

First with:

```text
1 consumer
```

Find where lag begins to grow.

Then repeat around that point with:

```text
2 consumers
4 consumers
```

This evaluates horizontal consumer scaling on the same local CPU machine.
