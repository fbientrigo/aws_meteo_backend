import pytest
import pandas as pd
import numpy as np
from lib.forecast.engine import forecast_damped_persistence

def test_forecast_convergence(mock_era5_monthly, mock_climatology):
    """
    Test 1: Convergence to the Mean
    In long horizons (t=24), the forecast should approach the climatology
    (anomaly should be almost 0).
    """
    # Setup Data
    ds_era5 = mock_era5_monthly()
    ds_clim = mock_climatology()
    
    # Pick a specific location
    lat = ds_era5.latitude.values[0]
    lon = ds_era5.longitude.values[0]
    
    # Current State
    current_date = pd.Timestamp("2021-12-01")
    current_val = ds_era5.sel(time=current_date, latitude=lat, longitude=lon)["t2m"].item()
    
    # Climatology Lists
    clim_means = ds_clim.sel(latitude=lat, longitude=lon)["mean"].values.tolist()
    clim_stds = ds_clim.sel(latitude=lat, longitude=lon)["std"].values.tolist()
    
    # Run Forecast
    horizon = 24
    forecast = forecast_damped_persistence(
        current_value=current_val,
        current_date=current_date,
        climatology_means=clim_means,
        climatology_stds=clim_stds,
        horizon_months=horizon
    )
    
    # Check last step
    last_step = forecast[-1]
    last_month_idx = (current_date.month - 1 + horizon) % 12
    expected_clim_mean = clim_means[last_month_idx]
    
    # Assert convergence (difference < epsilon)
    # With decay 0.5, e^(-0.5 * 24) is extremely small (~6e-6)
    # So forecast mean should be virtually identical to climatology mean
    assert abs(last_step["mean"] - expected_clim_mean) < 0.01, \
        f"Forecast did not converge to climatology. Got {last_step['mean']}, expected {expected_clim_mean}"

def test_forecast_integrity_bands(mock_era5_monthly, mock_climatology):
    """
    Test 2: Integrity of Bands
    Assert p05 < p50 < p95 for all steps.
    """
    # Setup Data (Simplified)
    clim_means = [288.0] * 12
    clim_stds = [2.0] * 12
    current_val = 290.0
    current_date = pd.Timestamp("2022-01-01")
    
    forecast = forecast_damped_persistence(
        current_value=current_val,
        current_date=current_date,
        climatology_means=clim_means,
        climatology_stds=clim_stds,
        horizon_months=12
    )
    
    for step in forecast:
        assert step["p05"] < step["p50"], f"p05 ({step['p05']}) >= p50 ({step['p50']}) at {step['date']}"
        assert step["p50"] < step["p95"], f"p50 ({step['p50']}) >= p95 ({step['p95']}) at {step['date']}"

def test_forecast_continuity(mock_era5_monthly, mock_climatology):
    """
    Test 3: Continuity
    The date of the first forecast should be current_date + 1 month.
    """
    current_date = pd.Timestamp("2022-01-15") # Mid-month
    # Note: Our engine logic adds MonthOffset, so 2022-01-15 + 1 month -> 2022-02-15
    
    clim_means = [288.0] * 12
    clim_stds = [2.0] * 12
    
    forecast = forecast_damped_persistence(
        current_value=288.0,
        current_date=current_date,
        climatology_means=clim_means,
        climatology_stds=clim_stds,
        horizon_months=1
    )
    
    first_step_date = pd.Timestamp(forecast[0]["date"])
    expected_date = current_date + pd.DateOffset(months=1)
    
    assert first_step_date == expected_date, \
        f"First forecast date mismatch. Got {first_step_date}, expected {expected_date}"
