# Load Test Service

Stage 6 end-to-end capacity benchmark.

UI:

```text
http://localhost:2250
```

Pipeline under test:

```text
Streaming / Kafka
        ↓
Feature Service
        ↓
Redis
        ↓
Inference availability / latency
```

Automatic rates:

```text
250
500
750
1000
1250
1500
2000 events/sec
```

The benchmark records:

- completed events/sec
- Feature Service p95
- feature freshness
- Inference Service p95
- end-to-end probe p50 / p95 / p99
- errors
- detected bottleneck
- STABLE / NEAR_LIMIT / BOTTLENECK
- recommended end-to-end capacity

The test stops after the first clear bottleneck.
