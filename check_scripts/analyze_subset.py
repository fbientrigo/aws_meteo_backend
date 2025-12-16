import os
import sys
import boto3
import xarray as xr
import numpy as np
import time

# Add current directory to path to import s3_helpers
sys.path.append(os.getcwd())

from s3_helpers import list_runs, list_steps, build_nc_key, BUCKET

def analyze():
    print("--- 1. Listing Runs ---")
    try:
        runs = list_runs()
        if not runs:
            print("No runs found.")
            return
        run = runs[-1]
        print(f"Selected Run: {run}")
        
        steps = list_steps(run)
        if not steps:
            print("No steps found.")
            return
        step = steps[0]
        print(f"Selected Step: {step}")
        
    except Exception as e:
        print(f"Error listing: {e}")
        # Fallback for testing if list fails
        run = "2025111500"
        step = "072"
        print(f"Using fallback: {run} / {step}")

    key = build_nc_key(run, step)
    local_file = "analyze_test.nc"
    
    print(f"\n--- 2. Downloading {key} ---")
    s3 = boto3.client("s3")
    try:
        s3.download_file(BUCKET, key, local_file)
        print(f"Downloaded {os.path.getsize(local_file)} bytes.")
    except Exception as e:
        print(f"Download failed: {e}")
        return

    print("\n--- 3. Opening Dataset ---")
    try:
        ds = xr.open_dataset(local_file, engine="h5netcdf")
        print(f"Dimensions: {ds.dims}")
        print(f"Variables: {ds.data_vars}")
        
        if "sti" not in ds.data_vars and len(ds.data_vars) == 1:
            var_name = list(ds.data_vars)[0]
            print(f"Using variable '{var_name}' as STI")
            sti = ds[var_name]
        else:
            sti = ds["sti"]
            
        print("\n--- 4. analyzing Payload Size (The 'Subset' logic) ---")
        # Logic from main.py
        lons = sti["longitude"].values
        lats = sti["latitude"].values
        
        n_lons = len(lons)
        n_lats = len(lats)
        total = n_lons * n_lats
        
        print(f"Grid: {n_lats} lats x {n_lons} lons")
        print(f"Total points: {total:,}")
        
        # Estimate JSON size
        # [lat, lon, val] * total
        # Each float in JSON is approx 10 bytes (e.g. -33.12345)
        # 3 lists of length 'total'
        estimated_bytes = total * 3 * 10
        print(f"Estimated JSON payload size: {estimated_bytes / 1024 / 1024:.2f} MB")
        
        if total > 500_000:
            print("CRITICAL: Payload is massive. This is likely causing the 500 error.")
        else:
            print("Payload seems manageable.")
            
        ds.close()
        
    except Exception as e:
        print(f"Error analyzing dataset: {e}")
        
if __name__ == "__main__":
    analyze()
