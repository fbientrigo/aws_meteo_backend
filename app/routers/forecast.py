from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any
import pandas as pd
import numpy as np
from app.lib.forecast.engine import forecast_damped_persistence
from app.lib.tests.conftest_climate import generate_mock_era5_monthly, generate_mock_climatology

router = APIRouter(prefix="/forecast", tags=["forecast"])

class ForecastRequest(BaseModel):
    latitude: float
    longitude: float

class ForecastStep(BaseModel):
    date: str
    mean: float
    p05: float
    p50: float
    p95: float

class ForecastResponse(BaseModel):
    history: List[Dict[str, Any]]
    forecast: List[ForecastStep]

from app.s3_helpers import list_runs, list_steps, load_dataset
# We keep s3_helpers imports to not break if we revert, but we won't use them for the primary flow
# from app.lib.indices.construct import OUT_ALL, build_era5_t2m_monthly_chile  <-- REMOVED (cdsapi dependency)
# from app.lib.tests.conftest_climate import generate_mock_era5_monthly, generate_mock_climatology <-- REMOVED (potential heavy deps)
import os
import xarray as xr

# Define defaults locally to avoid importing from 'construct.py' which imports 'cdsapi'
OUT_ALL = "tmp/ERA5_T2M_monthly_1991_2025_chile.nc"

# Simple mocks if needed, or just relying on fallback logic
DATASET_CLIM = None # _mock_clim_gen()
DATASET_ERA5 = None # _mock_era5_gen()

def get_local_data():
    """
    Attempts to load the local ERA5 NetCDF file.
    Path: tmp/ERA5_T2M_monthly_1991_2025_chile.nc
    """
    if os.path.exists(OUT_ALL):
        return xr.open_dataset(OUT_ALL)
    return None

@router.post("/predict", response_model=ForecastResponse)
async def predict_forecast(request: ForecastRequest):
    """
    Generates a Damped Persistence forecast for a given location.
    Uses LOCAL CDSAPI DATA (if available) or MOCKS.
    """
    lat = request.latitude
    lon = request.longitude
    
    # 1. Try Get Real Data from Local File
    ds_local = get_local_data()
    
    ds_point = None
    if ds_local is not None:
        try:
            # Select nearest point
            # Note: ERA5 from CDSAPI might have 'latitude'/'longitude' or 'lat'/'lon'
            # The construct.py normalizes to 'latitude'/'longitude'
            ds_point_sel = ds_local.sel(latitude=lat, longitude=lon, method="nearest")
            
            # Extract value (t2m)
            if "t2m" in ds_point_sel:
                # Ensure we get a scalar (last time step)
                last_step = ds_point_sel["t2m"].isel(time=-1)
                # If there are other dims like 'valid_time', we might need to select/reduce them too
                # The download script output showed 'valid_time' dim. 
                # Let's check if 'valid_time' exists and select the last one if so.
                if "valid_time" in last_step.dims:
                    last_step = last_step.isel(valid_time=-1)
                    
                current_val = float(last_step.values)
                
                # Get date
                if "time" in ds_point_sel.coords:
                     current_date = pd.Timestamp(ds_point_sel["time"].isel(time=-1).values)
                else:
                     current_date = pd.Timestamp.now()
            else:
                # Fallback
                current_val = 288.0
                current_date = pd.Timestamp.now()
                
            ds_local.close()
            # If successful, we have our values, so we don't need ds_point for fallback
            # But we need to flag that we found data.
            # Let's just set ds_point to something not None to skip fallback
            ds_point = True 
            
        except Exception as e:
            # Log error and fall back to mock
            print(f"Error reading local data: {e}")
            ds_local.close()
            ds_point = None
    
    # 2. Fallback to Mock if Local Data Missing or Failed
    if ds_point is None:
        # Fallback to Mock (Synthetic)
        if DATASET_ERA5 is not None:
             ds_point_mock = DATASET_ERA5.sel(latitude=lat, longitude=lon, method="nearest")
             current_val = float(ds_point_mock["t2m"].isel(time=-1).values)
             current_date = pd.Timestamp(ds_point_mock["time"].isel(time=-1).values)
        else:
             # Pure dummy fallback if no mock available
             current_val = 290.0
             current_date = pd.Timestamp.now()

    # 3. Get Climatology (Mocked for now)
    try:
        if DATASET_CLIM is not None:
             clim_point = DATASET_CLIM.sel(latitude=lat, longitude=lon, method="nearest")
             clim_means = clim_point["mean"].values.tolist()
             clim_stds = clim_point["std"].values.tolist()
        else:
             raise ValueError("No Climatology Dataset")
    except Exception:
        clim_means = [288.0] * 12
        clim_stds = [2.0] * 12
    
    # 4. Run Forecast Engine
    forecast_steps = forecast_damped_persistence(
        current_value=current_val,
        current_date=current_date,
        climatology_means=clim_means,
        climatology_stds=clim_stds,
        horizon_months=24
    )
    
    # 5. Format History
    history = [{
        "date": current_date.strftime("%Y-%m-%d"),
        "value": current_val
    }]
        
    return {
        "history": history,
        "forecast": forecast_steps
    }
