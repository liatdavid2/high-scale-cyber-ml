# Add Stage 3 to high-scale-cyber-ml

Extract this ZIP into the repository root.

It adds:

```text
services/feature-service/
```

Then add Redis and Feature Service to the root `docker-compose.yml`.

```yaml
  redis:
    image: redis:7.4-alpine
    container_name: high-scale-cyber-ml-redis
    command: ["redis-server", "--appendonly", "no"]
    restart: unless-stopped

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
    depends_on:
      - kafka
      - redis
    restart: unless-stopped
```

No host Redis port is required because only Docker services need to access Redis.

Rebuild:

```bash
docker compose down
docker compose up --build -d
```

Open:

```text
Dataset UI:   http://localhost:2000
Streaming UI: http://localhost:2050
Feature UI:   http://localhost:2100
```

## How Stage 2 and Stage 3 coexist

Stage 2 benchmark consumers use their own group.

Stage 3 uses:

```text
group_id = feature-service
```

Kafka therefore lets Feature Service receive the same `unsw-events` independently.

For an end-to-end run:

```text
UNSW-NB15
  -> Stage 2 producer
  -> Kafka
  -> Feature Service consumer
  -> feature engineering
  -> Redis
```
