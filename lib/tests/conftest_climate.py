import pytest
import xarray as xr
import numpy as np
import pandas as pd

def generate_mock_era5_monthly():
    """
    Generates a synthetic ERA5 monthly dataset.
    Structure:
        - Time: Monthly frequency (24 months history)
        - Lat/Lon: Small grid for speed
        - Variable: t2m (Temperature at 2m)
    Logic:
        - Sinusoidal seasonality + Random Noise
    """
    def _generator(start_date="2020-01-01", periods=24):
        # 1. Coordinates
        times = pd.date_range(start=start_date, periods=periods, freq="MS")
        lats = np.linspace(-90, 90, 10)  # Small grid
        lons = np.linspace(0, 360, 10)
        
        # 2. Generate Data (Sinusoidal Seasonality)
        # Shape: (time, lat, lon)
        # Seasonality: sin(month)
        months = times.month.values[:, None, None]  # Broadcastable
        
        # Base temperature (Kelvin approx) + Seasonality + Noise
        base_temp = 288.0 
        seasonality = 10.0 * np.sin(2 * np.pi * months / 12.0)
        noise = np.random.normal(0, 1.0, size=(len(times), len(lats), len(lons)))
        
        data = base_temp + seasonality + noise
        
        # 3. Create xarray Dataset
        ds = xr.Dataset(
            data_vars={
                "t2m": (("time", "latitude", "longitude"), data)
            },
            coords={
                "time": times,
                "latitude": lats,
                "longitude": lons
            }
        )
        return ds

    return _generator

def generate_mock_climatology():
    """
    Generates synthetic Climatology (Means and Stds).
    Structure:
        - Month: 1-12
        - Lat/Lon: Same as ERA5
    """
    def _generator():
        # 1. Coordinates
        months = np.arange(1, 13)
        lats = np.linspace(-90, 90, 10)
        lons = np.linspace(0, 360, 10)
        
        # 2. Generate Data
        # Mean: Pure seasonality without noise
        # Std: Constant or slightly varying
        
        # Shape: (month, lat, lon)
        # We need to ensure the data matches the coordinate sizes (12, 10, 10)
        mean_val = 288.0 + 10.0 * np.sin(2 * np.pi * months[:, None, None] / 12.0)
        mean_data = np.zeros((len(months), len(lats), len(lons))) + mean_val
        
        std_data = np.ones_like(mean_data) * 2.0 # Standard deviation of 2 degrees
        
        ds = xr.Dataset(
            data_vars={
                "mean": (("month", "latitude", "longitude"), mean_data),
                "std": (("month", "latitude", "longitude"), std_data)
            },
            coords={
                "month": months,
                "latitude": lats,
                "longitude": lons
            }
        )
        return ds

    return _generator

@pytest.fixture
def mock_era5_monthly():
    return generate_mock_era5_monthly()

@pytest.fixture
def mock_climatology():
    return generate_mock_climatology()
