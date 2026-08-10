# Streaming Service v5

Replace:

```text
services/streaming-service/
```

with the folder from this ZIP.

No Docker Compose changes are required compared with v4.

Rebuild:

```bash
docker compose down
docker compose up --build -d
```

Open:

```text
http://localhost:2050
```

For Stage 3:

1. In Stage 2 click `Start Continuous Streaming`.
2. Open `http://localhost:2100`.
3. Click `Start Feature Service`.
4. Stage 3 will consume the new Kafka events and write features to Redis.
