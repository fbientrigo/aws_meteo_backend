# indices/spei.py
from __future__ import annotations
import math, os, tempfile
from pathlib import Path
from typing import Tuple, Dict, Optional, Literal

import numpy as np
import pandas as pd
import requests
import app.lib.utils.xarray_functions as xr

SPEI_BASE = "https://spei.csic.es/spei_database_2_11/nc"

# --------- Geometría simple (sin shapely) ---------
def km2deg_lat(km: float) -> float:
    return km / 111.0

def km2deg_lon(km: float, lat_deg: float) -> float:
    return km / (111.320 * math.cos(math.radians(lat_deg)) + 1e-9)

# --------- IO helpers ---------
def _safe_open_nc(path: str) -> xr.Dataset:
    """Abre NetCDF robustamente."""
    try:
        return xr.open_dataset(path, engine="h5netcdf", decode_times=False)
    except Exception:
        # Fallback to scipy implies older format, but usually h5netcdf works for NetCDF4.
        # Scipy works for NetCDF3. We keep it as safe fallback if h5netcdf fails on NC3.
        return xr.open_dataset(path, engine="scipy", decode_times=False)

def _download_to_tmp(url: str, retries: int = 3, timeout: int = 60) -> str:
    """
    Descarga en archivo temporal y retorna la ruta local.
    Reintenta y valida que haya >0 bytes.
    """
    last_err = None
    for i in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as tmp:
                    total = 0
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        if chunk:
                            tmp.write(chunk)
                            total += len(chunk)
                    path = tmp.name
            if total <= 0:
                raise IOError("Descarga vacía (0 bytes).")
            return path
        except Exception as e:
            last_err = e
            time.sleep(min(2 * i, 5))
    raise last_err

def _ensure_dir(p: Path) -> None:
    """
    Si p tiene sufijo (parece archivo), asegura su directorio padre.
    Si p es directorio, lo crea.
    """
    p = Path(p)
    if p.suffix:
        p.parent.mkdir(parents=True, exist_ok=True)
    else:
        p.mkdir(parents=True, exist_ok=True)

# --------- Dataset parsing ---------
def _find_var_name(ds: xr.Dataset, candidates=("spei","SPEI")) -> str:
    for name in ds.data_vars:
        if name.lower() in candidates:
            return name
    # fallback: primera var float con 'time'
    for name, da in ds.data_vars.items():
        if da.dtype.kind in "fc" and ("time" in da.dims):
            return name
    raise KeyError("No se encontró variable SPEI en el NetCDF.")

def _decode_months_since_1901(ds: xr.Dataset) -> xr.Dataset:
    """Convierte 'time' (meses desde 1901-01) a timestamps mensuales."""
    if "time" not in ds.coords:
        return ds
    n = ds.sizes.get("time", len(ds["time"]))
    dates = pd.date_range("1901-01-01", periods=n, freq="MS")
    return ds.assign_coords(time=("time", dates))

def _coord_names(da: xr.DataArray) -> Tuple[str,str]:
    if "longitude" in da.coords:
        lon_name = "longitude"
    elif "lon" in da.coords:
        lon_name = "lon"
    else:
        raise KeyError("No se encontró coordenada de longitud.")
    if "latitude" in da.coords:
        lat_name = "latitude"
    elif "lat" in da.coords:
        lat_name = "lat"
    else:
        raise KeyError("No se encontró coordenada de latitud.")
    return lon_name, lat_name

def _slice_by_bbox_da(da: xr.DataArray,
                      lon_min: float, lat_min: float,
                      lon_max: float, lat_max: float) -> xr.DataArray:
    lon_name, lat_name = _coord_names(da)
    lons = da.coords[lon_name].values
    lats = da.coords[lat_name].values
    lon_slice = slice(lon_min, lon_max) if lons[0] < lons[-1] else slice(lon_max, lon_min)
    lat_slice = slice(lat_min, lat_max) if lats[0] < lats[-1] else slice(lat_max, lat_min)
    return da.sel({lon_name: lon_slice, lat_name: lat_slice})

# --------- API pública del módulo ---------
def download_spei_to_cache(time_scale: int,
                           cache_dir: str | Path = "./data") -> Path:
    """
    Descarga spei{time_scale:02d}.nc y lo deja en cache_dir/spei/speiXX.nc.
    Si ya existe, no re-descarga.
    """
    if not (1 <= time_scale <= 48):
        raise ValueError("time_scale debe estar entre 1 y 48.")
    cache_dir = Path(cache_dir) / "spei"
    local_nc = cache_dir / f"spei{time_scale:02d}.nc"
    _ensure_dir(local_nc)  # <--- asegura directorio destino

    if local_nc.exists() and local_nc.stat().st_size > 0:
        return local_nc

    url = f"{SPEI_BASE}/spei{time_scale:02d}.nc"
    # No dependemos de HEAD: vamos directo a GET con _download_to_tmp (con reintentos)
    tmp_path = _download_to_tmp(url)
    try:
        # mover de forma atómica si es posible
        shutil.move(tmp_path, local_nc)
    finally:
        try:
            Path(tmp_path).unlink(missing_ok=True)
        except Exception:
            pass
    return local_nc

def clip_spei_latest(nc_path: str | Path,
                     bbox: Tuple[float,float,float,float],
                     padding_km: float = 0.0) -> xr.DataArray:
    """
    Abre el NetCDF SPEI, toma el último 'time' y recorta a bbox (con padding opcional).
    Retorna DataArray 2D (lat,lon) con el último mes SPEI.
    """
    nc_path = Path(nc_path)
    ds = _safe_open_nc(str(nc_path))
    ds = _decode_months_since_1901(ds)
    var = _find_var_name(ds)
    da = ds[var]
    # limpiar fill values y NaNs raros
    da = da.where(np.isfinite(da) & (da != 3.0e33))

    last_t = da["time"].values[-1]
    da_last = da.sel(time=last_t)

    lon_min, lat_min, lon_max, lat_max = bbox
    lat0 = (lat_min + lat_max) / 2.0
    if padding_km > 0:
        dlat = km2deg_lat(padding_km)
        dlon = km2deg_lon(padding_km, lat0)
        lon_min, lon_max = max(-179.75, lon_min - dlon), min(179.75, lon_max + dlon)
        lat_min, lat_max = max(-89.75,  lat_min - dlat),  min(89.75,  lat_max + dlat)

    clip = _slice_by_bbox_da(da_last, lon_min, lat_min, lon_max, lat_max)
    return clip

def save_clip(clip: xr.DataArray,
              cache_dir: str | Path = "./data",
              time_scale: int = 12,
              tag: str = "last") -> Path:
    """
    Guarda el recorte como NetCDF en ./data/spei/spei{XX}_{tag}_clip.nc
    """
    out = Path(cache_dir) / "spei" / f"spei{time_scale:02d}_{tag}_clip.nc"
    _ensure_dir(out)
    clip.to_netcdf(out)
    return out

def grid_series_from_nc(nc_path: str | Path,
                        bbox: Tuple[float,float,float,float],
                        agg: Literal["mean","median","min","max"] = "mean") -> pd.Series:
    """
    Extrae SERIE MENSUAL (promedio espacial del bbox) del NetCDF SPEI completo.
    Retorna pd.Series (fecha mensual -> valor SPEI).
    """
    ds = _safe_open_nc(str(nc_path))
    ds = _decode_months_since_1901(ds)
    var = _find_var_name(ds)
    da = ds[var].where(lambda x: np.isfinite(x) & (x != 3.0e33))
    # recorte espacial
    da_clip = _slice_by_bbox_da(da, *bbox)
    if agg == "mean":
        ts = da_clip.mean(dim=[d for d in da_clip.dims if d != "time"])
    elif agg == "median":
        ts = da_clip.median(dim=[d for d in da_clip.dims if d != "time"])
    elif agg == "min":
        ts = da_clip.min(dim=[d for d in da_clip.dims if d != "time"])
    elif agg == "max":
        ts = da_clip.max(dim=[d for d in da_clip.dims if d != "time"])
    else:
        raise ValueError("agg inválido.")

    idx = pd.to_datetime(ts["time"].values)
    return pd.Series(ts.values, index=idx)

def monthly_to_daily_ffill(monthly: pd.Series) -> pd.Series:
    """
    Convierte una serie MENSUAL (index de fechas cualq.) a DIARIA por forward-fill.
    - Normaliza el índice mensual al INICIO de mes.
    - Crea un rango diario desde el primer día del primer mes al último día del último mes.
    - Reindexa con ffill para rellenar cada día del mes con el valor mensual.
    """
    if not isinstance(monthly.index, pd.DatetimeIndex):
        monthly = monthly.copy()
        monthly.index = pd.to_datetime(monthly.index)

    # Normaliza cada timestamp al INICIO de mes
    monthly_ms_index = monthly.index.to_period('M').to_timestamp(how='start')
    monthly_ms = pd.Series(monthly.values, index=monthly_ms_index)

    # Rango diario: desde inicio del primer mes hasta FIN del último mes
    start = monthly_ms.index.min()
    end = monthly_ms.index.max().to_period('M').to_timestamp(how='end')
    daily_index = pd.date_range(start, end, freq='D')

    # Forward-fill por mes
    daily = monthly_ms.reindex(daily_index, method='ffill')

    # (opcional) asegura dtype float
    return daily.astype('float32')

def load_or_prepare_spei_series(
    time_scale: int,
    bbox: Tuple[float,float,float,float],
    cache_dir: str | Path = "./data",
    padding_km: float = 0.0,
    prefer_cache: bool = True
) -> Dict[str, object]:
    """
    Flujo completo:
      - Descarga speiXX.nc (si no existe) a ./data/spei/
      - Extrae serie mensual promediada en bbox
      - Genera versión diaria por ffill
      - Devuelve rutas y series
    """
    raw_nc = Path(cache_dir) / "spei" / f"spei{time_scale:02d}.nc"
    if not (prefer_cache and raw_nc.exists()):
        raw_nc = download_spei_to_cache(time_scale=time_scale, cache_dir=cache_dir)

    monthly = grid_series_from_nc(raw_nc, bbox=bbox, agg="mean")
    daily = monthly_to_daily_ffill(monthly)
    return {
        "raw_nc": raw_nc,
        "monthly": monthly,
        "daily": daily
    }
