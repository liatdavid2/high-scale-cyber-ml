# streaming-service

Stage 2 of `high-scale-cyber-ml`.

## Flow

```text
shared/data/raw/UNSW-NB15
        ↓
Streaming Service
        ↓
Kafka Producer
        ↓
unsw-events topic
        ↓
Kafka Consumer
        ↓
Live Metrics
```

The service reads UNSW-NB15 rows, converts each row to a JSON event, publishes events at a configurable target rate, consumes them back, and measures throughput / lag / errors.

## UI

After integrating the service into the root Docker Compose:

```text
http://localhost:2050
```

## API

```text
GET  /health
GET  /api/metrics
POST /api/start
POST /api/stop
POST /api/reset
```

Start body example:

```json
{
  "target_rate": 1000,
  "dataset": "training"
}
```

## Notes

- CPU only.
- No Spark/Flink yet.
- One Kafka broker.
- Dataset is mounted from the repository-level `shared/data`.
- When the CSV reaches the end, the generator loops from the beginning so load tests can continue without creating huge local files.
