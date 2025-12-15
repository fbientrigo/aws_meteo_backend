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
    ds = None
    try:
        ds = load_dataset(run, step)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="NetCDF no encontrado en S3 para el run/step especificado",
        )
    except Exception as e:
        # Aquí pueden caer errores de IO, HDF5, netCDF corrupto, etc.
        import traceback
        import sys
        print(f"CRITICAL ERROR LOADING DATASET in get_summary: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        
        raise HTTPException(
            status_code=500,
            detail=f"Error abriendo NetCDF: {e}",
        )

    try:
        # La variable ya viene normalizada como 'sti' desde s3_helpers
        if "sti" not in ds.data_vars:
             raise HTTPException(
                status_code=500,
                detail=f"Variable 'sti' no encontrada. Vars: {list(ds.data_vars)}",
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
        if ds:
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
    ds = None
    try:
        ds = load_dataset(run, step)
    except FileNotFoundError:
        raise HTTPException(
            status_code=404,
            detail="NetCDF no encontrado en S3 para el run/step especificado",
        )
    except Exception as e:
        import traceback
        import sys
        
        # Log error to stderr so uvicorn captures it
        print(f"CRITICAL ERROR LOADING DATASET in get_subset: {e}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        
        raise HTTPException(
            status_code=500,
            detail=f"Error abriendo NetCDF: {e}",
        )

    try:
        # Note: variable normalization (var->sti) is now handled in s3_helpers.load_dataset
        
        # Recorte Geográfico
        # DEBUG: User requested "verlo todo" (see everything). Bypassing subset for debugging.
        print(f"DEBUG: Dataset loaded. Dimensions: {ds.dims}, Coords: {ds.coords}")
        
        # Bypass subset logic - return full dataset (files are small ~400KB)
        sub = ds["sti"]
        
        # Flattening logic for frontend (Leaflet Heatmap)
        lons_in = sub["longitude"].values
        lats_in = sub["latitude"].values
        
        lon_grid, lat_grid = np.meshgrid(lons_in, lats_in)

        flat_lats = lat_grid.flatten().tolist()
        flat_lons = lon_grid.flatten().tolist()
        flat_sti = sub.values.flatten().tolist()
        
        print(f"DEBUG: Returning {len(flat_sti)} points.")

        return {
            "run": run,
            "step": step,
            "latitudes": flat_lats,
            "longitudes": flat_lons,
            "sti": flat_sti,
        }
    finally:
        if ds:
            ds.close()
