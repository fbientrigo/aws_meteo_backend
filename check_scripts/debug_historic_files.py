import xarray as xr
from pathlib import Path

FILES = [
    "../historic/ERA5_T2M_monthly_1991_2024_chile.nc",
    "../historic/ERA5_T2M_monthly_1991_2025_chile.nc",
    "../historic/ERA5_T2M_monthly_2025_01_10_chile.nc",
]

for f in FILES:
    p = Path(f)
    if not p.exists():
        print(f"\nMISSING: {f}")
        continue

    ds = xr.open_dataset(p)  # engine default, solo para inspección
    print(f"\n=== {f} ===")
    print("dims:", dict(ds.dims))
    print("coords:", list(ds.coords))
    print("vars:", list(ds.data_vars))
    for cand in ["time", "valid_time", "latitude", "longitude", "lat", "lon"]:
        if cand in ds.coords or cand in ds.dims:
            print(f"has {cand}: yes")
    ds.close()
