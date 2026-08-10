# streaming-service v3

Automatic scalability benchmark for Kafka streaming.

## Full benchmark

The UI automatically runs:

- Rates: 500, 750, 1000, 1250, 1500, 2000 events/sec
- Consumers: 1, 2, 4
- Total: 18 experiments

Each experiment records:

- produced/sec
- processed/sec
- Kafka lag
- lag growth/sec
- errors
- result: STABLE / NEAR_LIMIT / BOTTLENECK / ERROR

## Recommendation algorithm

1. Exclude configurations with errors.
2. Prefer configurations classified as STABLE.
3. Require roughly >=95% of target throughput for STABLE.
4. Select highest processed throughput.
5. If multiple configurations are within 2% throughput, prefer fewer consumers.

## UI

`http://localhost:2050`

## Important

The topic should have 4 partitions. If the old local topic was created with 1 partition and old Kafka state does not matter:

```bash
docker compose down -v
docker compose up --build
```
