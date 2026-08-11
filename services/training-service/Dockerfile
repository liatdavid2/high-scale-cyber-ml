FROM python:3.11-slim

WORKDIR /service

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV DATA_DIR=/shared/data/raw
ENV MLFLOW_TRACKING_URI=http://mlflow:5000
ENV MLFLOW_PUBLIC_URL=http://localhost:2350
ENV MLFLOW_EXPERIMENT_NAME=cyber-intrusion-model-selection
ENV MLFLOW_REGISTERED_MODEL_NAME=cyber-intrusion-model

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

RUN pip install \
    --default-timeout=600 \
    --retries 10 \
    --no-cache-dir \
    -r requirements.txt

COPY app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]