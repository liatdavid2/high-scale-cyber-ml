# Stage 4 — Training + MLflow

CPU-only model selection on UNSW-NB15.

Training UI: http://localhost:2150
MLflow UI: http://localhost:2350

Runs:
- Logistic Regression: C=0.1 / 1.0
- Random Forest: 100 trees depth 12 / 200 trees unlimited
- HistGradientBoosting: lr 0.05 x100 / lr 0.1 x200

Metrics:
PR-AUC, ROC-AUC, F1, Recall, Precision, Accuracy, training time, inference latency.

Best run:
PR-AUC first, then F1, then lower inference latency.


## Updated experiment suite

8 runs:

- XGBoost × 2
- LightGBM × 2
- CatBoost × 2
- Random Forest × 1
- HistGradientBoosting × 1

This suite is intentionally focused on strong tabular models for UNSW-NB15.
