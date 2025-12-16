import os
import boto3
import xarray as xr
import traceback

BUCKET = "pangu-mvp-data"
KEY = "indices/sti/run=2025111500/step=072/sti_chile_run=2025111500_step=072.nc"
LOCAL = "/tmp/debug_test.nc"

def check_env():
    print("--- 1. Checking Environment ---")
    try:
        import h5netcdf
        print("h5netcdf installed:", h5netcdf.__version__)
    except ImportError:
        print("h5netcdf NOT installed")

    try:
        import netCDF4
        print("netCDF4 installed:", netCDF4.__version__)
    except ImportError:
        print("netCDF4 NOT installed (CRITICAL if engine='netcdf4')")

def download_file():
    print(f"\n--- 2. Downloading {KEY} ---")
    s3 = boto3.client("s3")
    try:
        s3.download_file(BUCKET, KEY, LOCAL)
        size = os.path.getsize(LOCAL)
        print(f"Success! Downloaded to {LOCAL}")
        print(f"File size: {size} bytes")
        if size < 1000:
            print("WARNING: File size is suspiciously small!")
            print("Content preview:", open(LOCAL, 'rb').read())
    except Exception as e:
        print("Download FAILED:")
        traceback.print_exc()

def open_file():
    print("\n--- 3. Opening with Xarray ---")
    engines = ["netcdf4", "h5netcdf"]
    
    for eng in engines:
        print(f"\n[Trying engine='{eng}']")
        try:
            ds = xr.open_dataset(LOCAL, engine=eng)
            print("SUCCESS! Dataset opened.")
            print("Dimensions:", ds.dims)
            print("Variables:", ds.data_vars)
            ds.close()
            return # Exit on first success
        except Exception as e:
            print(f"FAILED with engine='{eng}':")
            print(e)

if __name__ == "__main__":
    check_env()
    download_file()
    if os.path.exists(LOCAL):
        open_file()
    print("\n--- End of Test ---")
