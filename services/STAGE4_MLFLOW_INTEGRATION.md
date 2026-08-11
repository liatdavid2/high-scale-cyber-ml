# Stage 4 integration

Extract into the root of HIGH-SCALE-CYBER-ML.

Adds:

```text
services/training-service/
```

Add the services from `docker-compose.stage4.yml` to the existing root `docker-compose.yml`.

Also add:

```yaml
volumes:
  mlflow-data:
```

under the existing volumes section.

Run:

```bash
docker compose down
docker compose up --build -d
```

Open:

```text
Training UI: http://localhost:2150
MLflow UI:   http://localhost:2350
```
