# Integration

Replace your existing:

```text
services/streaming-service/
```

with the folder in this ZIP.

Keep the root Compose service similar to:

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

Then:

```bash
docker compose down
docker compose up --build
```

If the existing Kafka topic was created with 1 partition, recreate local Kafka state:

```bash
docker compose down -v
docker compose up --build
```

Open:

```text
http://localhost:2050
```

Click:

```text
Run Full Benchmark
```
