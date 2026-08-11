# Feature Service v3 integration

Replace:

```text
services/feature-service/
```

with the folder from this ZIP.

Update the root Docker Compose `feature-service` environment with:

```yaml
      STREAMING_SERVICE_URL: http://streaming-service:8000
```

Recommended service block:

```yaml
  feature-service:
    build:
      context: ./services/feature-service
      dockerfile: Dockerfile
    container_name: high-scale-cyber-ml-feature
    ports:
      - "2100:8000"
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      KAFKA_TOPIC: unsw-events
      KAFKA_GROUP_ID: feature-service
      REDIS_HOST: redis
      REDIS_PORT: 6379
      FEATURE_TTL_SECONDS: 3600
      WINDOW_SECONDS: 60
      STREAMING_SERVICE_URL: http://streaming-service:8000
    depends_on:
      - kafka
      - redis
      - streaming-service
    restart: unless-stopped
```

Rebuild:

```bash
docker compose down
docker compose up --build -d
```

Open:

```text
http://localhost:2100
```

Then click:

```text
Run Full Benchmark
```

Stage 3 will automatically start and stop Stage 2 streaming at each benchmark rate.
