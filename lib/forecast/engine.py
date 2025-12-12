import numpy as np
import pandas as pd
from typing import List, Dict, Any
import math

def forecast_damped_persistence(
    current_value: float,
    current_date: pd.Timestamp,
    climatology_means: List[float],
    climatology_stds: List[float],
    horizon_months: int = 24
) -> List[Dict[str, Any]]:
    """
    Calculates the Damped Persistence forecast (Zero-Order Forecast).
    
    Formula:
        F(t) = C_mean(m) + A(t_current) * e^(-lambda * t)
        
    Args:
        current_value: The observed value at the start time.
        current_date: The date of the current observation.
        climatology_means: List of 12 monthly means (Jan-Dec).
        climatology_stds: List of 12 monthly stds (Jan-Dec).
        horizon_months: Number of months to forecast.
        
    Returns:
        List of dictionaries containing forecast steps:
        [
            {
                "date": "2024-02-01",
                "mean": 290.5,
                "p05": 288.0,
                "p50": 290.5,
                "p95": 293.0
            },
            ...
        ]
    """
    
    # 1. Identify current month index (0-11)
    # current_date.month is 1-12, so subtract 1
    current_month_idx = current_date.month - 1
    
    # 2. Calculate Initial Anomaly
    # Anomaly = Value - Climatology
    current_clim_mean = climatology_means[current_month_idx]
    initial_anomaly = current_value - current_clim_mean
    
    forecast_steps = []
    
    # 3. Iterate over horizon
    for h in range(1, horizon_months + 1):
        # Calculate future date
        future_date = current_date + pd.DateOffset(months=h)
        future_month_idx = future_date.month - 1
        
        # Get Climatology for future month
        future_clim_mean = climatology_means[future_month_idx]
        future_clim_std = climatology_stds[future_month_idx]
        
        # Decayed Anomaly
        # Lambda = 0.5 (as per requirements)
        decay_factor = math.exp(-0.5 * h)
        predicted_anomaly = initial_anomaly * decay_factor
        
        # Forecast Mean
        forecast_mean = future_clim_mean + predicted_anomaly
        
        # Probability Bands (Normal Distribution assumption)
        # z-score for 90% CI (5% to 95%) is approx 1.645
        z_score = 1.645
        margin = future_clim_std * z_score
        
        p05 = forecast_mean - margin
        p95 = forecast_mean + margin
        p50 = forecast_mean # Mean = Median for Normal Dist
        
        forecast_steps.append({
            "date": future_date.strftime("%Y-%m-%d"),
            "mean": float(forecast_mean),
            "p05": float(p05),
            "p50": float(p50),
            "p95": float(p95)
        })
        
    return forecast_steps
