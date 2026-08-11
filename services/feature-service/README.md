# Feature Service — Stage 3

Consumes `unsw-events` from Kafka, derives online features, and stores them in Redis.

## Consumer group

```text
feature-service
```

This is intentionally different from the Stage 2 benchmark consumer group.

## Generated features

- total_bytes
- total_packets
- byte_ratio_src_dst
- packet_ratio_src_dst
- bytes_per_packet
- proto_events_last_60s
- service_events_last_60s
- avg_total_bytes_last_60s

`attack_cat` and `label` are not used to generate features.

## UI

```text
http://localhost:2100
```

## Typical flow

1. Start Docker Compose.
2. Open Feature Service and click `Start Feature Service`.
3. Start streaming from Stage 2.
4. Watch events/sec, features/sec, Redis writes/sec, p50/p95/p99 latency and freshness.


## UI feature lineage

The UI explicitly shows for every generated feature:

```text
new feature
→ source columns
→ formula / meaning
```

This makes the feature-engineering logic visible and easier to review.


## Automatic capacity benchmark

Stage 3 can now control Stage 2 continuous streaming through its API.

Benchmark rates:

```text
250
500
750
1000
1500
2000
3000
5000 events/sec
```

For each rate it measures:

- features/sec
- Redis writes/sec
- p50 / p95 / p99 latency
- feature freshness
- errors

Classification:

```text
STABLE
NEAR_LIMIT
BOTTLENECK
ERROR
```

The benchmark stops after a clear bottleneck and recommends the highest stable capacity.
