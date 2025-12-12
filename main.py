# main.py
from __future__ import annotations

from typing import Dict, Any, List
import numpy as np

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from s3_helpers import list_runs, list_steps, load_dataset
from routers import forecast

app = FastAPI(
    title="Pangu MVP STI API",
    description="API para servir índices STI desde NetCDF en S3",
    version="0.1.0",
)

# --- INICIO HOTFIX CORS ---
from fastapi.middleware.cors import CORSMiddleware

# Orígenes permitidos (Frontend Local + Producción)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Permitir GET, POST, OPTIONS, etc.
    allow_headers=["*"],  # Permitir todos los headers
)
# --- FIN HOTFIX CORS ---

app.include_router(forecast.router)


# --------------------------------------------------------------------
# Endpoints básicos
# --------------------------------------------------------------------
@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/sti/runs")
def get_runs():
    """
    Devuelve la lista de runs disponibles (YYYYMMDDHH).
    """
    runs = list_runs()
    return {"runs": runs}


@app.get("/sti/{run}/steps")
def get_steps(run: str):
    """
    Devuelve la lista de steps disponibles (XXX) para un run dado.
    """
    steps = list_steps(run)
    if not steps:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontraron steps para run={run}",
        )
    return {"run": run, "steps": steps}


# --------------------------------------------------------------------
# Endpoints que abren NetCDF
# --------------------------------------------------------------------
@app.get("/sti/{run}/{step}/summary")
def get_summary(run: str, step: str):
    """
    Devuelve estadísticas básicas del dataset:
    - dimensiones
    - variables
    - min/max/mean de la variable 'sti'
    """
    try:
        ds = load_dataset(run, step)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="NetCDF no encontrado en S3 para el run/step especificado",
        )
    except Exception as e:
        # Aquí pueden caer errores de IO, HDF5, netCDF corrupto, etc.
        raise HTTPException(
            status_code=500,
            detail=f"Error abriendo NetCDF: {e}",
        )

    try:
        if "sti" not in ds.data_vars:
            raise HTTPException(
                status_code=500,
                detail="Variable 'sti' no encontrada en el dataset",
            )

        sti = ds["sti"]

        summary: Dict[str, Any] = {
            "run": run,
            "step": step,
            "dims": {k: int(v) for k, v in ds.dims.items()},
            "coords": list(ds.coords.keys()),
            "vars": list(ds.data_vars.keys()),
            "sti_stats": {
                "min": float(sti.min().values),
                "max": float(sti.max().values),
                "mean": float(sti.mean().values),
            },
        }
        return JSONResponse(summary)
    finally:
        # Nos aseguramos de cerrar el Dataset incluso si algo falla
        ds.close()


@app.get("/sti/{run}/{step}/subset")
def get_subset(
    run: str,
    step: str,
    lat_min: float = Query(..., description="Latitud mínima (grados)"),
    lat_max: float = Query(..., description="Latitud máxima (grados)"),
    lon_min: float = Query(..., description="Longitud mínima (grados)"),
    lon_max: float = Query(..., description="Longitud máxima (grados)"),
):
    """
    Devuelve un recorte geográfico de la variable 'sti' como JSON.
    Ojo con el tamaño: para MVP, usar bounding boxes razonables.

    Nota: asumimos esquema tipo ERA5 con coords "latitude" y "longitude".
    Muchas veces latitude viene de 90 -> -90, por eso usamos slice(lat_max, lat_min).
    """
    try:
        ds = load_dataset(run, step)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="NetCDF no encontrado en S3 para el run/step especificado",
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error abriendo NetCDF: {e}",
        )

    try:
        if "sti" not in ds.data_vars:
            raise HTTPException(
                status_code=500,
                detail="Variable 'sti' no encontrada en el dataset",
            )

        try:
        # Helper to normalize longitude to 0-360 if needed
        # ERA5 typically uses 0-360. Frontend uses -180 to 180.
        def to_360(lon):
            return lon % 360

        lmin_360 = to_360(lon_min)
        lmax_360 = to_360(lon_max)

        # Handle dateline crossing if needed, but for Chile/Americas typically lmin < lmax in 360 too
        # e.g -75 -> 285, -70 -> 290. 285 < 290.

        try:
            # First try direct slice (in case data is -180/180)
            sub = ds["sti"].sel(
                latitude=slice(lat_max, lat_min),
                longitude=slice(lon_min, lon_max),
            )
            
            # If empty, try 0-360 format
            if sub.size == 0:
                sub = ds["sti"].sel(
                    latitude=slice(lat_max, lat_min),
                    longitude=slice(lmin_360, lmax_360),
                )

        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Error en recorte lat/lon: {e}",
            )

        # Flattening logic for frontend (Leaflet Heatmap)
        # 1. Generate 2D grids
        # lats_in = sub["latitude"].values  # 1D array
        # lons_in = sub["longitude"].values # 1D array
        lons_in = sub["longitude"].values
        lats_in = sub["latitude"].values

        # Note: meshgrid order is (x, y) -> (lon, lat) usually, but check dims
        # xarray .sel usually preserves order. sub dims: (latitude, longitude) ideally
        # If we want flattened arrays corresponding to (lat, lon) pairs:
        # We need to ensure the order of meshgrid matches how we flatten the data.
        # sub.values is (lat, lon).
        # np.meshgrid(lons, lats) returns (lon_grid, lat_grid) with shape (lat, lon) by default (indexing='xy') NO, wait.
        # indexing='xy' (default): output shape (N_y, N_x) where N_y = len(lats), N_x = len(lons). Correct.
        
        lon_grid, lat_grid = np.meshgrid(lons_in, lats_in)

        # 2. Flatten everything
        # flatten() is row-major (C-style) by default.
        # array([[1, 2], [3, 4]]) -> [1, 2, 3, 4]
        # Correspond to iterating last dim (longitude) fastest, then latitude.
        # This matches (lat, lon) array layout.
        
        flat_lats = lat_grid.flatten().tolist()
        flat_lons = lon_grid.flatten().tolist()
        flat_sti = sub.values.flatten().tolist()

        return {
            "run": run,
            "step": step,
            "latitudes": flat_lats,
            "longitudes": flat_lons,
            "sti": flat_sti,
        }
    finally:
        ds.close()
