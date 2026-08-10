# dataset-service

Stage 1 of `high-scale-cyber-ml`.

This service reads UNSW-NB15 data from the shared mounted directory:

```text
/shared/data/raw
```

The root `docker-compose.yml` maps:

```text
./shared/data -> /shared/data
```

The service exposes:

```text
GET /
GET /health
GET /api/profile
```

It has no GPU dependency.
