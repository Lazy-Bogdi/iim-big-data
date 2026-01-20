"""Module Machine Learning pour enrichir automatiquement les données Gold"""

from .feature_engineering import extract_features_auto
from .ml_models import (
    detect_anomalies_ml,
    cluster_data,
    predict_scores,
    enrich_with_ml
)

__all__ = [
    "extract_features_auto",
    "detect_anomalies_ml",
    "cluster_data",
    "predict_scores",
    "enrich_with_ml",
]

