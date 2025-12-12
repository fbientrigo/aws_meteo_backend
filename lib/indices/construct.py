# Crea el "clim" object
# contiene toda la info climática necesaria para índices
import os, pathlib
import numpy as np
import xarray as xr
import pandas as pd # Import pandas
import cdsapi

# 1) Parámetros y rutas
CACHE_DIR = "tmp"
OUT_ALL   = os.path.join(CACHE_DIR, "ERA5_T2M_monthly_1991_2025_chile.nc")
TMP_1991_2024 = os.path.join(CACHE_DIR, "ERA5_T2M_monthly_1991_2024_chile.nc")
TMP_2025_01_10 = os.path.join(CACHE_DIR, "ERA5_T2M_monthly_2025_01_10_chile.nc")

pathlib.Path(CACHE_DIR).mkdir(parents=True, exist_ok=True)

# 2) Parámetros de área (Chile) y utilidades
AREA_CHILE = [-17, -76, -56, -66]  # [N, W, S, E] en grados, longitudes en −180..180

def cds_retrieve_monthly_t2m_years(outfile: str, years: list[str], months: list[str]):
    """
    Descarga ERA5 monthly means (T2M) con cdsapi para el dominio de Chile y guarda en 'outfile'.
    """
    if os.path.exists(outfile):
        print(f"[SKIP] Ya existe: {outfile}")
        return

    c = cdsapi.Client()
    req = {
        "product_type": "monthly_averaged_reanalysis",
        "format": "netcdf",
        "variable": ["2m_temperature"],
        "year": years,
        "month": months,
        "time": "00:00",
        "area": AREA_CHILE,  # [North, West, South, East] con longitudes −180..180
    }
    print(f"[CDS] Solicitando {len(years)} years x {len(months)} months -> {outfile}")
    c.retrieve("reanalysis-era5-single-levels-monthly-means", req, outfile)
    print(f"[OK] Guardado: {outfile}")

def _normalize_coords(ds: xr.Dataset) -> xr.Dataset:
    ren = {}
    if 'lat' in ds.coords and 'latitude' not in ds.coords: ren['lat'] = 'latitude'
    if 'lon' in ds.coords and 'longitude' not in ds.coords: ren['lon'] = 'longitude'
    if ren:
        ds = ds.rename(ren)
    # ERA5 mensual viene con longitudes en −180..180 -> conviértelas a 0..360 si se desea coherencia global
    if float(ds.longitude.min()) < 0.0:
        ds = ds.assign_coords(longitude=(ds.longitude % 360)).sortby('longitude')
    return ds


# lib/indices/construct.py
# Update2

import os
import pathlib
from typing import List, Optional, Tuple

import numpy as np
import xarray as xr
import pandas as pd


# ----------------------------------------------------------
# 1) Ensamblado ERA5 T2M mensual 1991–2025-10 para Chile
# ----------------------------------------------------------

def build_era5_t2m_monthly_chile(
    cache_dir: str = "tmp",
    out_all_name: str = "ERA5_T2M_monthly_1991_2025_chile.nc",
    tmp_1991_2024_name: str = "ERA5_T2M_monthly_1991_2024_chile.nc",
    tmp_2025_partial_name: str = "ERA5_T2M_monthly_2025_01_10_chile.nc",
    start_full: int = 1991,
    end_full: int = 2024,
    partial_year: int = 2025,
    partial_last_month: int = 10,
    overwrite: bool = False,
) -> xr.Dataset:
    """
    Descarga y construye un NetCDF mensual de T2M ERA5 para Chile (1991–2025-10),
    usando la función cds_retrieve_monthly_t2m_years ya definida en la librería.

    Devuelve el xr.Dataset ensamblado (t2m mensual sobre Chile).
    """

    cache_path = pathlib.Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)

    out_all = cache_path / out_all_name
    tmp_1991_2024 = cache_path / tmp_1991_2024_name
    tmp_2025_partial = cache_path / tmp_2025_partial_name

    # Si ya existe el archivo final y no quieres sobrescribir, solo lo cargas
    if out_all.exists() and not overwrite:
        print(f"[SKIP] Archivo final ya existe: {out_all}")
        ds_all = xr.open_dataset(out_all, decode_times=True)
        return ds_all

    # ---------------------------
    # 1. Descarga 1991–2024
    # ---------------------------
    years_1991_2024 = [str(y) for y in range(start_full, end_full + 1)]
    months_full = [f"{m:02d}" for m in range(1, 13)]

    cds_retrieve_monthly_t2m_years(
        str(tmp_1991_2024),
        years_1991_2024,
        months_full,
    )

    # ---------------------------
    # 2. Descarga tramo parcial 2025
    # ---------------------------
    months_partial = [f"{m:02d}" for m in range(1, partial_last_month + 1)]
    tmp_2025_path: Optional[pathlib.Path] = tmp_2025_partial

    try:
        cds_retrieve_monthly_t2m_years(
            str(tmp_2025_partial),
            [str(partial_year)],
            months_partial,
        )
    except Exception as e:
        print("[WARN] No se pudo descargar el tramo parcial "
              f"{partial_year}-01 a {partial_year}-{partial_last_month:02d}. "
              "Continuando solo con 1991–2024. Error:", e)
        tmp_2025_path = None

    # ---------------------------
    # 3. Abrir y normalizar datasets
    # ---------------------------
    datasets = []

    # 1991–2024
    if not tmp_1991_2024.exists():
        raise FileNotFoundError(f"No se encontró {tmp_1991_2024}")

    with xr.open_dataset(tmp_1991_2024, decode_times=True) as ds_1991_2024:
        ds_1991_2024 = _normalize_coords(ds_1991_2024).load()
        datasets.append(ds_1991_2024)

    # 2025 parcial (si existe)
    if tmp_2025_path is not None and tmp_2025_path.exists():
        with xr.open_dataset(tmp_2025_path, decode_times=True) as ds_2025:
            ds_2025 = _normalize_coords(ds_2025).load()
            datasets.append(ds_2025)
    else:
        print("[INFO] No se incluirá el tramo parcial 2025 en el ensamblado.")

    if not datasets:
        raise RuntimeError("No se pudo cargar ningún dataset para T2M mensual.")

    # ---------------------------
    # 4. Concatenar sobre 'time'
    # ---------------------------
    ds_all = xr.concat(datasets, dim="time").sortby("time")

    # Convertimos la coordenada de tiempo a timestamps de inicio de mes
    ds_all["time"] = (
        pd.to_datetime(ds_all.time.values)
          .to_period("M")
          .to_timestamp()
    )

    # Renombrar variable si viene como '2m_temperature'
    if "2m_temperature" in ds_all.data_vars and "t2m" not in ds_all.data_vars:
        ds_all = ds_all.rename({"2m_temperature": "t2m"})

    # ---------------------------
    # 5. Guardar en NetCDF final
    # ---------------------------
    comp = dict(zlib=True, complevel=4)
    encoding = {var: comp for var in ds_all.data_vars}

    # Para evitar errores de permisos, eliminamos si existe y overwrite=True
    if out_all.exists() and overwrite:
        out_all.unlink()

    ds_all.to_netcdf(out_all, encoding=encoding)
    print(f"[OK] Ensamblado final: {out_all}  |  tiempo: "
          f"{ds_all.time.min().values} -> {ds_all.time.max().values}")

    return ds_all


# ----------------------------------------------------------
# 2) Climatología mensual 1991–2020 a partir de T2M mensual
# ----------------------------------------------------------

def build_t2m_climatology_from_monthly(
    monthly_nc_path: str,
    cache_dir: str = "tmp",
    clim_name: str = "ERA5_T2M_climatology_1991_2020_chile.nc",
    base_start: str = "1991-01-01",
    base_end: str = "2020-12-31",
    overwrite: bool = True,
) -> xr.Dataset:
    """
    Construye la climatología mensual 1991–2020 de T2M a partir del NetCDF mensual
    (como el generado por build_era5_t2m_monthly_chile).

    Devuelve un xr.Dataset con:
    - t2m_mean(month, latitude, longitude)
    - t2m_std(month, latitude, longitude)
    """

    cache_path = pathlib.Path(cache_dir)
    cache_path.mkdir(parents=True, exist_ok=True)
    clim_path = cache_path / clim_name

    if clim_path.exists() and not overwrite:
        print(f"[SKIP] Climatología ya existe: {clim_path}")
        return xr.open_dataset(clim_path, decode_times=False)

    if not os.path.exists(monthly_nc_path):
        raise FileNotFoundError(f"No existe el archivo mensual: {monthly_nc_path}")

    # Abrimos el dataset mensual
    ds = xr.open_dataset(monthly_nc_path, decode_times=True)

    # 0) Normalizaciones de variable y coords problemáticas
    if "2m_temperature" in ds.data_vars and "t2m" not in ds.data_vars:
        ds = ds.rename({"2m_temperature": "t2m"})

    # 'expver' a veces llega como coord con dims (time, ...): la eliminamos
    if "expver" in ds.coords:
        ds = ds.reset_coords(names=["expver"], drop=True)

    # Determinar dimensión temporal a usar: 'valid_time' o 'time'
    if "valid_time" in ds.dims:
        time_dim = "valid_time"
    elif "time" in ds.dims:
        time_dim = "time"
    else:
        raise KeyError("No se encontró ni 'valid_time' ni 'time' como dimensión temporal en el dataset.")

    da = ds["t2m"].sortby(time_dim)

    # 1) Recorte periodo base sobre la dimensión temporal
    da_base = da.sel({time_dim: slice(base_start, base_end)})

    # 2) Climatología mensual (media y std)
    clim_mean = (
        da_base
        .groupby(f"{time_dim}.month")
        .mean(time_dim, keep_attrs=True)
        .reset_coords(drop=True)
    )
    clim_std = (
        da_base
        .groupby(f"{time_dim}.month")
        .std(time_dim, keep_attrs=True)
        .reset_coords(drop=True)
    )

    # 3) Seguridad numérica (evitar std ~ 0)
    clim_std = xr.where(clim_std < 1e-6, 1e-6, clim_std)

    # 4) Dataset final
    clim = xr.Dataset(
        {
            "t2m_mean": clim_mean,
            "t2m_std": clim_std,
        }
    )

    # 5) Guardar
    comp = dict(zlib=True, complevel=4)
    encoding = {k: comp for k in clim.data_vars}

    if clim_path.exists() and overwrite:
        clim_path.unlink()

    clim.to_netcdf(clim_path, encoding=encoding)
    print(f"[OK] Climatología guardada en: {clim_path} | months: {clim.sizes.get('month')}")

    return clim


# ----------------------------------------------------------
# 3) Función “todo en uno” (opcional)
# ----------------------------------------------------------

def build_era5_t2m_monthly_and_clim(
    cache_dir: str = "tmp",
    overwrite_monthly: bool = False,
    overwrite_clim: bool = True,
) -> Tuple[xr.Dataset, xr.Dataset]:
    """
    Helper que corre todo el pipeline:
    - Descarga/ensambla T2M mensual 1991–2025-10
    - Construye climatología 1991–2020

    Devuelve (ds_all, clim).
    """
    monthly_nc = os.path.join(cache_dir, "ERA5_T2M_monthly_1991_2025_chile.nc")

    ds_all = build_era5_t2m_monthly_chile(
        cache_dir=cache_dir,
        out_all_name=os.path.basename(monthly_nc),
        overwrite=overwrite_monthly,
    )

    clim = build_t2m_climatology_from_monthly(
        monthly_nc_path=monthly_nc,
        cache_dir=cache_dir,
        overwrite=overwrite_clim,
    )

    return ds_all, clim
