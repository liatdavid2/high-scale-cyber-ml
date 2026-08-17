# Load Test Service Update

The recommendation section now reports:

- Recommended Stable Rate
- Completed/sec
- E2E p95
- Feature Freshness
- Next Tested Rate
- First Bottleneck

Example:

```text
Recommended Stable Rate: 750/sec
Next Tested Rate:         1000/sec
First Bottleneck:         Feature Service
```

When adaptive testing stops after the first bottleneck, progress now says:

```text
Benchmark stopped after first bottleneck. 4 rates tested.
```

The configured total rate ladder is still preserved separately.
