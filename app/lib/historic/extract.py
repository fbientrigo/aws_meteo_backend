from typing import List, Dict, Any, Optional
import numpy as np
import pandas as pd
import xarray as xr
from .loader import load_merged_dataset

MAX_POINTS = 200

def normalize_longitude(lon: float, ds_is_360: bool) -> float:
    """
    Normalizes longitude to match the dataset convention.
    - If dataset is 0..360, maps [-180, 180] -> [0, 360]
    - If dataset is -180..180, keeps as is.
    """
    if ds_is_360:
        return lon % 360.0
    else:
        # Normalize to -180..180
        return ((lon + 180) % 360) - 180

def extract_points(
    points: List[Dict[str, float]],
    units: str = "K",  # 'C' or 'K'
    # date_start: Optional[str] = None,
    # date_end: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Extracts time series for a list of points.
    
    Args:
        points: List of dicts with 'lat', 'lon'.
        units: Target units 'C' (Celsius) or 'K' (Kelvin).
        date_start: ISO date string.
        date_end: ISO date string.
    """
    if not points:
        return []
    if len(points) > MAX_POINTS:
        raise ValueError(f"Too many points requested. Max is {MAX_POINTS}")

    ds = load_merged_dataset()
    
    # Detect dataset longitude convention
    ds_lons = ds.coords["longitude"].values
    ds_is_360 = np.any(ds_lons > 180)
    
    # Determine grid resolution for tolerance
    lat_res = np.abs(np.diff(ds.coords["latitude"].values)).mean()
    lon_res = np.abs(np.diff(ds_lons)).mean()
    # Tolerance: slightly more than half a pixel to allow for float errors but avoid far-away matches
    # actually 'nearest' in xarray matches anything. We need to manually check distance.
    # Let's say 0.6 * resolution.
    tol_lat = 0.6 * lat_res
    tol_lon = 0.6 * lon_res

    results = []

    # Time slicing (if requested)
    # We slice the dataset ONCE if all points share the timeline, which is more efficient.
    # But often users want full history.
    sub_ds = ds
    # if date_start or date_end:
    #     sub_ds = ds.sel(time=slice(date_start, date_end))

    # Unit conversion factor
    is_kelvin = False
    if "units" in sub_ds["t2m"].attrs:
        u = sub_ds["t2m"].attrs["units"]
        if "K" in u or "kelvin" in u.lower():
            is_kelvin = True
    
    # Pre-load needed data into memory? 
    # If generic, no. But for 'sel', dask is good.
    # We iterate.
    
    for pt in points:
        req_lat = pt["lat"]
        req_lon = pt["lon"]
        
        # Validation
        if not (-90 <= req_lat <= 90):
             # Skip or error? Let's return error payload for this point
             results.append({
                 "lat_requested": req_lat, "lon_requested": req_lon,
                 "error": "Invalid latitude"
             })
             continue
             
        norm_lon = normalize_longitude(req_lon, ds_is_360)
        
        try:
            # Selection
            # method='nearest' finds the closest point indiscriminately.
            # We must check HOW close it is.
            selection = sub_ds["t2m"].sel(
                latitude=req_lat, 
                longitude=norm_lon, 
                method="nearest"
            )
            
            # Verify tolerance
            found_lat = float(selection.latitude.values)
            found_lon = float(selection.longitude.values)
            
            # Check diffs (handle circular longitude diff if needed)
            diff_lat = abs(found_lat - req_lat)
            
            # Simple absolute diff for lon (ignoring wrap around edge case for MVP unless strictly needed)
            # To handle wrap around diff properly for 0/360 boundary:
            diff_lon = abs(found_lon - norm_lon)
            if diff_lon > 180:
                diff_lon = 360 - diff_lon
                
            if diff_lat > tol_lat or diff_lon > tol_lon:
                results.append({
                    "lat_requested": req_lat, "lon_requested": req_lon,
                    "error": "Point out of bounds (no grid cell near enough)",
                    "nearest_grid": {"lat": found_lat, "lon": found_lon}
                })
                continue

            # Extract Series
            # Convert to Pandas Series for easy JSON serialization
            series = selection.to_pandas()
            
            # Unit Conversion
            final_units = units
            if is_kelvin and units == "C":
                series = series - 273.15
            elif not is_kelvin and units == "C":
                # Already C?
                pass
            elif is_kelvin and units == "K":
                pass
            
            # Replacement of NaN
            # series.where(series.notnull(), None) # method for series
            # Or use json dump default=str for Nan? standard lib doesn't like NaN.
            # We replace NaN with None
            series_clean = series.where(pd.notnull(series), other=None)
            
            # Format output
            # timestamps to string YYYY-MM-DD
            # values to float
            
            ts_list = []
            for date, val in series_clean.items():
                val_out = val
                if val is not None:
                    val_out = float(val)
                
                ts_list.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "value": val_out
                })
            
            results.append({
                "lat_requested": req_lat,
                "lon_requested": req_lon,
                "lat_used": found_lat,
                "lon_used": found_lon,
                "variable": "t2m",
                "units": final_units,
                "series": ts_list
            })
            
        except KeyError:
            results.append({
                 "lat_requested": req_lat, "lon_requested": req_lon,
                 "error": "Data extraction failed generic"
            })
        except Exception as e:
            results.append({
                 "lat_requested": req_lat, "lon_requested": req_lon,
                 "error": str(e)
            })

    return results
