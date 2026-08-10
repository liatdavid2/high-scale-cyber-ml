# v4 integration

Replace:

```text
services/streaming-service/
```

with the folder from this ZIP.

Make sure the root Docker Compose still contains:

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

Rebuild:

```bash
docker compose down
docker compose up --build -d
```

Open:

```text
http://localhost:2050
```
