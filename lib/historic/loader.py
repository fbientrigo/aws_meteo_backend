import threading
import cachetools
import xarray as xr
import numpy as np
import pandas as pd
import logging
from pathlib import Path
from typing import List, Tuple

from .catalog import get_ordered_sources

logger = logging.getLogger(__name__)

# Cache: keep 2 merged datasets (by files signature)
CACHE = cachetools.LRUCache(maxsize=2)
CACHE_LOCK = threading.RLock()

# Global lock for HDF5 / NetCDF reads (open + load)
HDF5_GLOBAL_LOCK = threading.RLock()

# Configuration
VAR_NAME = "t2m"
TIME_DIM = "valid_time"
LAT = "latitude"
LON = "longitude"

ENGINE_PRIMARY = "netcdf4"
ENGINE_FALLBACK = "h5netcdf"

# If dataset is "small enough", load into RAM and close files => avoids open handles & read races later
EAGER_LOAD_BYTES_THRESHOLD = 256 * 1024 * 1024  # 256 MB


def _cache_key(paths: List[Path]) -> Tuple[Tuple[str, int, int], ...]:
    """
    Cache key based on (name, mtime_ns, size). If files change, cache busts.
    """
    sig = []
    for p in paths:
        st = p.stat()
        sig.append((p.name, int(st.st_mtime_ns), int(st.st_size)))
    return tuple(sig)


def _open_dataset_safe(path: Path) -> xr.Dataset:
    """
    Opens a single dataset safely under a global lock.
    """
    logger.info(f"Opening historic dataset: {path.name}")
    with HDF5_GLOBAL_LOCK:
        try:
            return xr.open_dataset(
                path,
                engine=ENGINE_PRIMARY,
                decode_cf=True,
                mask_and_scale=True,
                chunks="auto",
            )
        except Exception as e1:
            logger.warning(f"open_dataset failed with {ENGINE_PRIMARY} for {path.name}: {e1!r}. Trying {ENGINE_FALLBACK}.")
            return xr.open_dataset(
                path,
                engine=ENGINE_FALLBACK,
                decode_cf=True,
                mask_and_scale=True,
                chunks="auto",
            )


def _strip_extras(ds: xr.Dataset) -> xr.Dataset:
    """
    Keep only the variable of interest and its required coords.
    Drop auxiliary coords (e.g., expver/number) that can break concat alignment.
    """
    if VAR_NAME not in ds.data_vars:
        raise ValueError(f"Variable '{VAR_NAME}' not found. Vars: {list(ds.data_vars)}")

    # Keep only t2m
    ds = ds[[VAR_NAME]]

    # Drop coords that are not needed (avoid concat warnings/mismatches)
    keep = {TIME_DIM, LAT, LON}
    drop_vars = [c for c in ds.coords if c not in keep]
    if drop_vars:
        ds = ds.drop_vars(drop_vars, errors="ignore")

    return ds


def _collapse_time_layer_if_present(ds: xr.Dataset) -> xr.Dataset:
    """
    Canonicalize ds so VAR_NAME has dims (valid_time, latitude, longitude).
    If VAR_NAME has extra dim 'time' (as in your weird file), collapse it:
      merged = time=0 combine_first time=1 combine_first ...
    """
    da = ds[VAR_NAME]

    # Ensure we have valid_time; if not, try to map from 'time' (only if it is the temporal axis)
    if TIME_DIM not in da.dims:
        if "valid_time" in ds.dims:
            # Should not happen, but keep consistent
            pass
        elif "valid_time" in ds.coords:
            pass
        elif "time" in da.dims and TIME_DIM != "time":
            # Not your current case; included defensively
            da = da.rename({"time": TIME_DIM})
        else:
            raise ValueError(f"'{VAR_NAME}' has no '{TIME_DIM}' dim. dims={da.dims}")

    # Collapse useless 'time' layer if present (your dataset: time=2 with one layer all-NaN)
    if "time" in da.dims:
        merged = da.isel(time=0, drop=True)
        for i in range(1, ds.sizes["time"]):
            merged = merged.combine_first(da.isel(time=i, drop=True))
        da = merged  # now without dim 'time'

    # Build clean dataset from DataArray
    clean = da.to_dataset(name=VAR_NAME)

    # Ensure coords exist and are attached (some backends can be finicky)
    for c in [TIME_DIM, LAT, LON]:
        if c in ds.coords and c not in clean.coords:
            clean = clean.assign_coords({c: ds[c]})

    # Sort by valid_time (safe for slicing later)
    if TIME_DIM in clean.coords:
        clean = clean.sortby(TIME_DIM)

    return clean


def _ensure_latlon_names(ds: xr.Dataset) -> xr.Dataset:
    """
    Normalize lat/lon names if needed.
    """
    rename_map = {}
    if LAT not in ds.coords and "lat" in ds.coords:
        rename_map["lat"] = LAT
    if LON not in ds.coords and "lon" in ds.coords:
        rename_map["lon"] = LON
    if rename_map:
        ds = ds.rename(rename_map)
    return ds

def _normalize_lon_180(ds: xr.Dataset) -> xr.Dataset:
    """
    Si longitudes vienen en 0..360, pásalas a -180..180.
    Luego ordena para que el vector quede comparable archivo-a-archivo.
    """
    if LON not in ds.coords:
        return ds

    lon = ds[LON]

    # Si es 0..360 (o 0..359.75), el max será > 180
    try:
        lon_max = float(lon.max())
    except Exception:
        return ds

    if lon_max > 180.0:
        logger.warning(
            f"[GRID] Converting longitude 0..360 -> -180..180 (file has lon.max={lon_max:.3f})"
        )
        lon2 = ((lon.astype("float64") + 180.0) % 360.0) - 180.0
        ds = ds.assign_coords({LON: lon2})

    # Ordena siempre: incluso si no convirtió, te deja determinista
    ds = ds.sortby(LON)
    return ds


def _dedupe_dim_keep_last(ds: xr.Dataset, dim: str) -> xr.Dataset:
    """
    Remove duplicate coordinate values along `dim`, keeping last.
    Works even if xarray.drop_duplicates not available.
    """
    vals = np.asarray(ds[dim].values)
    idx = pd.Index(vals)
    mask = ~idx.duplicated(keep="last")
    return ds.isel({dim: mask})


def load_merged_dataset() -> xr.Dataset:
    """
    Returns the merged historic dataset, cached.
    Uses valid_time as official time axis.
    Thread-safe.
    """
    sources = get_ordered_sources()
    if not sources:
        raise FileNotFoundError("No historic NetCDF files found in 'historic/' directory.")

    cache_key = _cache_key(sources)

    with CACHE_LOCK:
        if cache_key in CACHE:
            return CACHE[cache_key]

        datasets: List[xr.Dataset] = []
        try:
            # Open + canonicalize each dataset
            for p in sources:
                ds = _open_dataset_safe(p)
                ds = _ensure_latlon_names(ds)
                ds = _strip_extras(ds)
                ds = _collapse_time_layer_if_present(ds)
                ds = _normalize_lon_180(ds)
                logger.warning(f"[GRID] {p.name}: lon_min={float(ds[LON].min()):.3f} lon_max={float(ds[LON].max()):.3f}")

                datasets.append(ds)

            if len(datasets) == 1:
                final_ds = datasets[0]
            else:
                # Concat by valid_time (official axis)
                final_ds = xr.concat(
                    datasets,
                    dim=TIME_DIM,
                    data_vars="minimal",
                    coords="minimal",
                    join="exact",          # fail early if lat/lon grid differs
                    compat="override",
                    combine_attrs="override",
                )

                final_ds = final_ds.sortby(TIME_DIM)
                final_ds = _dedupe_dim_keep_last(final_ds, TIME_DIM)

            # Basic validation
            if VAR_NAME not in final_ds.data_vars:
                raise ValueError(f"After merge, '{VAR_NAME}' missing. Vars={list(final_ds.data_vars)}")

            # Optional: eager load if small-ish, then close originals to avoid file handles + HDF5 read races later
            try:
                est_bytes = final_ds[VAR_NAME].size * final_ds[VAR_NAME].dtype.itemsize
            except Exception:
                est_bytes = None

            if est_bytes is not None and est_bytes <= EAGER_LOAD_BYTES_THRESHOLD:
                logger.info(f"Eager-loading merged dataset into RAM (~{est_bytes/1024/1024:.1f} MB)")
                with HDF5_GLOBAL_LOCK:
                    final_ds = final_ds.load()

                # Now safe to close intermediate datasets (they no longer back final_ds)
                for ds in datasets:
                    try:
                        ds.close()
                    except Exception:
                        pass

            # Store in cache
            CACHE[cache_key] = final_ds
            return final_ds

        except Exception:
            # On failure, close what we opened
            for ds in datasets:
                try:
                    ds.close()
                except Exception:
                    pass
            raise
