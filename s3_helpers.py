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
    Lista los 'run=YYYYMMDDHH' disponibles bajo indices/sti/.
    Se basa en las keys reales del bucket.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    runs: Set[str] = set()

    for page in paginator.paginate(Bucket=BUCKET, Prefix=BASE_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]  # ej: indices/sti/run=2025111500/step=072/sti_chile_run=...
            m = re.search(r"run=(\d{10})/", key)
            if m:
                runs.add(m.group(1))

    return sorted(runs)


def list_steps(run: str) -> List[str]:
    """
    Lista los 'step=XXX' disponibles para un run dado.
    Devuelve siempre steps normalizados a 3 dígitos (e.g. '048').
    """
    prefix = f"{BASE_PREFIX}run={run}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    steps: Set[str] = set()

    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]  # ej: indices/sti/run=2025111500/step=072/...
            m = re.search(r"step=(\d{3})/", key)
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
def load_dataset(run: str, step: str | int) -> xr.Dataset:
    """
    Abre el Dataset directamente desde S3 usando fsspec + h5netcdf,
    sin descargar a disco.

    Lanza FileNotFoundError si el objeto no existe.
    Puede lanzar OSError / ValueError si el NetCDF está corrupto.
    """
    key = build_nc_key(run, step)
    s3_uri = build_nc_s3_uri(run, step)

    logger.info("Abriendo NetCDF desde S3: %s", s3_uri)

    if not _object_exists(key):
        logger.warning("NetCDF no encontrado en S3 para run=%s, step=%s", run, step)
        raise FileNotFoundError(f"Objeto no encontrado en S3: {s3_uri}")

    path = f"{BUCKET}/{key}"

    # NO usamos 'with': dejamos el file abierto y confiamos en ds.close() más arriba
    f = s3_fs.open(path, mode="rb")
    try:
        ds = xr.open_dataset(f, engine="h5netcdf")
    except Exception as exc:
        # Si falla al abrir, cerramos el file para no dejarlo colgando
        f.close()
        logger.error("Error leyendo NetCDF desde %s: %s", s3_uri, exc)
        raise

    return ds

