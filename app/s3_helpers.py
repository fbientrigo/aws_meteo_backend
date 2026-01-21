# s3_helpers.py
from __future__ import annotations

from typing import List, Set
import re
import logging

import boto3
import fsspec
import xarray as xr

logger = logging.getLogger(__name__)

# Configuración básica de S3 / índice
BUCKET = "pangu-mvp-data"
BASE_PREFIX = "indices/sti/"
INDEX_NAME = "sti"
REGION_NAME = "chile"  # usado en el nombre del archivo

# Clientes globales (se comparten entre llamadas)
s3_client = boto3.client("s3")
s3_fs = fsspec.filesystem("s3")  # usa IAM role de la instancia


# --------------------------------------------------------------------
# Helpers internos
# --------------------------------------------------------------------
def _normalize_step(step: str | int) -> str:
    """
    Normaliza el step a 3 dígitos (e.g. 48 -> '048').
    """
    return f"{int(step):03d}"


def _object_exists(key: str) -> bool:
    """
    Verifica existencia de un objeto en S3 usando s3fs.
    Espera un path tipo 'bucket/key'.
    """
    path = f"{BUCKET}/{key}"
    try:
        return s3_fs.exists(path)
    except Exception as exc:  # fallo raro de red / IAM
        logger.error("Error verificando existencia en S3 para %s: %s", path, exc)
        return False


# --------------------------------------------------------------------
# API pública para listar runs / steps
# --------------------------------------------------------------------
def list_runs() -> List[str]:
    """
    Lista los 'run=YYYYMMDDHH' leyendo las sub-carpetas (CommonPrefixes).
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    runs: Set[str] = set()
    
    print(f"DEBUG: Listing runs in bucket={BUCKET}, prefix={BASE_PREFIX}, delimiter='/'")

    # Usamos Delimiter='/' para ver "carpetas" en vez de listar todos los archivos recursivamente
    for page in paginator.paginate(Bucket=BUCKET, Prefix=BASE_PREFIX, Delimiter="/"):
        print(f"DEBUG: Processing page. CommonPrefixes count: {len(page.get('CommonPrefixes', []))}")
        for prefix_info in page.get("CommonPrefixes", []):
            prefix = prefix_info.get("Prefix")  # ej: indices/sti/run=2025111500/
            print(f"DEBUG: Found prefix: {prefix}")
            m = re.search(r"run=(\d{10})/?", prefix)
            if m:
                print(f"DEBUG: Match found! Run ID: {m.group(1)}")
                runs.add(m.group(1))
            else:
                print(f"DEBUG: No regex match for prefix: {prefix}")

    sorted_runs = sorted(runs)
    print(f"DEBUG: Final runs found: {sorted_runs}")
    return sorted_runs


def list_steps(run: str) -> List[str]:
    """
    Lista los 'step=XXX' leyendo las sub-carpetas (CommonPrefixes) dentro de un run.
    """
    prefix_path = f"{BASE_PREFIX}run={run}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    steps: Set[str] = set()

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix_path, Delimiter="/"):
        for prefix_info in page.get("CommonPrefixes", []):
            prefix = prefix_info.get("Prefix")  # ej: indices/sti/run=.../step=072/
            m = re.search(r"step=(\d{3})/?", prefix)
            if m:
                steps.add(m.group(1))

    return sorted(steps)


# --------------------------------------------------------------------
# Construcción de rutas
# --------------------------------------------------------------------
def build_nc_key(run: str, step: str | int) -> str:
    """
    Construye el key del NetCDF, según convención real en S3:

    indices/sti/run=YYYYMMDDHH/step=XXX/sti_chile_run=YYYYMMDDHH_step=XXX.nc
                                    ^                         ^
                                 step=048                step=048
    """
    step_str = _normalize_step(step)
    filename = f"{INDEX_NAME}_{REGION_NAME}_run={run}_step={step_str}.nc"
    key = f"{BASE_PREFIX}run={run}/step={step_str}/{filename}"
    return key


def build_nc_s3_uri(run: str, step: str | int) -> str:
    """
    Construye la URI tipo 's3://bucket/...' para uso informativo o logging.
    """
    key = build_nc_key(run, step)
    return f"s3://{BUCKET}/{key}"


# --------------------------------------------------------------------
# Carga de Dataset
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Carga de Dataset
# --------------------------------------------------------------------
# --------------------------------------------------------------------
# Carga de Dataset (Robust Patch)
# --------------------------------------------------------------------
import os
import tempfile
import uuid
import shutil
from filelock import FileLock  # Dependencia externa robusta

def pick_data_var(ds: xr.Dataset, preferred: str = "sti") -> str:
    """
    Selecciona la variable de datos correcta.
    1. Si 'preferred' existe, se usa.
    2. Si no, y solo hay 1 variable de datos, se usa esa.
    3. Si no, error explícito.
    """
    if preferred in ds.data_vars:
        return preferred
    
    # Filtrar coordenadas o variables auxiliares que a veces xarray marca como data_vars
    # Estrategia simple: si "var" está, usémosla.
    if "var" in ds.data_vars:
        return "var"
        
    # Fallback genérico: si hay exactamente una, la devolvemos
    if len(ds.data_vars) == 1:
        found = next(iter(ds.data_vars))
        logger.warning(f"Variable preferida '{preferred}' no encontrada. Usando única variable: '{found}'")
        return found
        
    raise KeyError(f"Variable '{preferred}' no encontrada y no se pudo deducir una única variable. Disponibles: {list(ds.data_vars)}")

# --------------------------------------------------------------------
# Global lock para HDF5 (Library-level safety)
# --------------------------------------------------------------------
import threading
_HDF5_LOCK = threading.Lock()

def load_dataset(run: str, step: str | int) -> xr.Dataset:
    """
    Descarga robusta y thread-safe de NetCDF desde S3.
    Implementa:
    1. Lock por archivo (FileLock) para coordinar procesos concurrentes.
    2. Descarga atómica (temp -> rename).
    3. Validación de apertura antes de exponer el archivo.
    4. Auto-recovery si el archivo final está corrupto.
    """
    key = build_nc_key(run, step)
    step_str = _normalize_step(step)
    local_filename = f"sti_{run}_{step_str}.nc"
    temp_dir = tempfile.gettempdir()
    final_path = os.path.join(temp_dir, local_filename)
    lock_path = final_path + ".lock"

    # Usamos FileLock para asegurar que SOLO UN proceso/hilo descargue el archivo a la vez.
    # Timeout de 60s para evitar hangs eternos.
    lock = FileLock(lock_path, timeout=60)
    
    with lock:
        # 1. Verificar si ya existe y es válido
        if os.path.exists(final_path):
            try:
                # Intento rápido de apertura para validar integridad
                # cache=False es CRÍTICO para h5netcdf + uvicorn
                # GLOBAL LOCK required for any HDF5 op
                with _HDF5_LOCK:
                    with xr.open_dataset(final_path, engine="h5netcdf", cache=False) as ds_check:
                        pass 
                logger.info(f"Cache HIT y fichero válido: {final_path}")
            except Exception as e:
                logger.warning(f"Cache corrupto detectado en {final_path} ({e}). Borrando para re-descargar.")
                try:
                    os.remove(final_path)
                except OSError:
                    pass

        # 2. Descargar si no existe (o se borró por corrupto)
        if not os.path.exists(final_path):
            # Nombre temporal único en el mismo FS para permitir rename atómico
            tmp_download_path = os.path.join(temp_dir, f"{local_filename}.{uuid.uuid4()}.tmp")
            
            logger.info(f"Iniciando descarga: {key} -> {tmp_download_path}")
            try:
                s3_client.download_file(BUCKET, key, tmp_download_path)
                
                # Validar el archivo recién bajado ANTES de renombrarlo
                if os.path.getsize(tmp_download_path) < 100:
                    raise ValueError("El archivo descargado es demasiado pequeño (<100B).")
                
                # Prueba de fuego con Xarray (+ Global Lock)
                with _HDF5_LOCK:
                    with xr.open_dataset(tmp_download_path, engine="h5netcdf", cache=False) as ds_test:
                        pass
                
                # 3. Rename atómico: Esto es lo que "publica" el archivo
                # os.replace es atómico en POSIX y Windows (Py3.3+)
                os.replace(tmp_download_path, final_path)
                logger.info(f"Descarga validada y publicada en: {final_path}")
                
            except Exception as e:
                logger.error(f"Fallo en descarga/validación de {key}: {e}")
                # Limpieza de basura
                if os.path.exists(tmp_download_path):
                    try:
                        os.remove(tmp_download_path)
                    except:
                        pass
                raise  # Re-raise para que el endpoint devuelva 500

    # ----------------------------------------------------------
    # 4. Abrir para devolver el objeto (Fuera del lock de descarga, pero Safe)
    # ----------------------------------------------------------
    # Nota: Ya está validado que existe y es íntegro.
    # h5netcdf no es thread-safe en lectura concurrente del mismo handle, 
    # pero cache=False nos da handles distintos.
    # Si aún así falla, el problema es de la lib HDF5 a nivel C global.
    try:
        # Importante: El lock aquí también
        logger.info(f"Acquiring HDF5 lock for {final_path}")
        with _HDF5_LOCK:
            logger.info(f"Opening dataset {final_path} with h5netcdf")
            ds = xr.open_dataset(final_path, engine="h5netcdf", cache=False)
        
            # 5. Normalizar variable (dentro del lock es más seguro si hace lazy load de metadatos)
            target_var = pick_data_var(ds, preferred="sti")
            if target_var != "sti":
                logger.info(f"Renombrando variable '{target_var}' -> 'sti'")
                ds = ds.rename({target_var: "sti"})
            
            # CRITICAL FIX: Eager load inside the lock to prevent HDF5 concurrency issues
            logger.info(f"Starting eager load for {final_path}")
            ds.load()
            logger.info(f"Finished eager load for {final_path}")
            
        return ds

    except Exception as e:
        import traceback
        logger.error(f"Error fatal abriendo/cargando dataset {final_path}: {e}")
        logger.error(traceback.format_exc())
        # Si falla aquí, es muy raro porque ya se validó. Podría ser race condition externa muy agresiva
        # o fallo de memoria.
        raise
