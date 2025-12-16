import os
import sys
import boto3
import xarray as xr
import numpy as np
import json
import math

# Add current directory to path to import s3_helpers
sys.path.append(os.getcwd())

from s3_helpers import list_runs, list_steps, build_nc_key, BUCKET

def check_json_compliance(data_dict):
    try:
        # FastAPI uses generic JSONResponse which relies on starlette's json, 
        # but let's test with standard json to see if it explodes on NaNs
        # (standard json does NOT allow NaNs by default strictly, but python's allow_nan=True is default)
        # However, many frontends crash on NaN.
        # But a 500 error in python usually means an exception.
        
        # Simulating standard strict JSON (like some parsers)
        text = json.dumps(data_dict, allow_nan=False)
        print("JSON serialization (strict) successful.")
    except ValueError as e:
        print(f"JSON serialization (strict) FAILED: {e}")
        print("This confirms NaNs are present and standard JSON doesn't like them.")
        
    try:
        text = json.dumps(data_dict, allow_nan=True)
        print("JSON serialization (allow_nan=True) successful.")
        if "NaN" in text:
            print("WARNING: output contains 'NaN'. valid in Python/JS, invalid in strict JSON specs.")
    except Exception as e:
        print(f"JSON serialization (allow_nan=True) FAILED: {e}")

def reproduce():
    # 1. reuse logic to get file
    # simple fallback
    run = "2025121200"
    step = "048"
    key = build_nc_key(run, step)
    local_file = "reproduce_test.nc"
    
    if not os.path.exists(local_file):
        print(f"Downloading {key}...")
        s3 = boto3.client("s3")
        s3.download_file(BUCKET, key, local_file)
    
    ds = xr.open_dataset(local_file, engine="h5netcdf")
    
    # Simulate main.py get_subset
    sub = ds["sti"]
    
    # Flattening logic
    lons_in = sub["longitude"].values
    lats_in = sub["latitude"].values
    lon_grid, lat_grid = np.meshgrid(lons_in, lats_in)

    flat_lats = lat_grid.flatten().tolist()
    flat_lons = lon_grid.flatten().tolist()
    flat_sti = sub.values.flatten().tolist()
    
    # Check for NaNs
    nan_count = np.isnan(flat_sti).sum()
    print(f"Total points: {len(flat_sti)}")
    print(f"NaN count: {nan_count}")
    
    response_payload = {
        "run": run,
        "step": step,
        "latitudes": flat_lats,
        "longitudes": flat_lons,
        "sti": flat_sti,
    }
    
    print("Attempting JSON serialization...")
    check_json_compliance(response_payload)
    
    ds.close()

if __name__ == "__main__":
    reproduce()
