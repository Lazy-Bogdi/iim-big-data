"""Modules d'agrégation pour la couche Gold"""

from .dimensions import create_dim_clients, create_dim_produits, create_dim_temps
from .fact_tables import create_fact_achats
from .time_aggregations import (
    aggregate_by_day,
    aggregate_by_week,
    aggregate_by_month,
    aggregate_by_hour,
)
from .geographic_aggregations import aggregate_by_country
from .rfm_segmentation import calculate_rfm_segmentation
from .clv_metrics import calculate_clv_metrics
from .retention_metrics import calculate_retention_metrics
from .product_metrics import calculate_product_metrics
from .cohort_analysis import calculate_cohort_analysis
from .seasonality_analysis import calculate_seasonality
from .statistical_distributions import calculate_statistical_distributions
from .concentration_metrics import calculate_concentration_metrics
from .kpis import calculate_global_kpis, calculate_growth_metrics

__all__ = [
    "create_dim_clients",
    "create_dim_produits",
    "create_dim_temps",
    "create_fact_achats",
    "aggregate_by_day",
    "aggregate_by_week",
    "aggregate_by_month",
    "aggregate_by_hour",
    "aggregate_by_country",
    "calculate_rfm_segmentation",
    "calculate_clv_metrics",
    "calculate_retention_metrics",
    "calculate_product_metrics",
    "calculate_cohort_analysis",
    "calculate_seasonality",
    "calculate_statistical_distributions",
    "calculate_concentration_metrics",
    "calculate_global_kpis",
    "calculate_growth_metrics",
]

