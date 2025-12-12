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

        # Recorte Geográfico
        try:
            # 1. Intentar recorte directo (coordenadas negativas, ej: -75)
            sub = ds["sti"].sel(
                latitude=slice(lat_max, lat_min),
                longitude=slice(lon_min, lon_max),
            )
            
            # 2. Si vacío, intentar formato 0-360 (ej: 285)
            if sub.size == 0:
                lmin_360 = lon_min % 360
                lmax_360 = lon_max % 360
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
        lons_in = sub["longitude"].values
        lats_in = sub["latitude"].values
        
        lon_grid, lat_grid = np.meshgrid(lons_in, lats_in)

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
