# streaming-service v4 — Adaptive saturation benchmark

This version avoids brute-force testing every rate with every consumer count.

## Phase 1 — Find the 1-consumer saturation point

Rates:

```text
500
1000
2000
3000
5000
7500
10000
15000
```

The benchmark stops increasing once a 1-consumer configuration becomes:

```text
NEAR_LIMIT
BOTTLENECK
ERROR
```

## Phase 2 — Scale around the bottleneck

It then tests the detected rate, plus the next higher rate when available, using:

```text
2 consumers
4 consumers
```

## Recommendation

The service chooses the highest stable processed throughput.

If configurations are within 2% throughput, fewer consumers are preferred.

## Run

```text
http://localhost:2050
```

Use 10 seconds for a quick smoke test and 30–60 seconds for a more meaningful benchmark.
