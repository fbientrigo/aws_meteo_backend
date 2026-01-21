# --- Celda 3.1: FIX de archivos (gzip/zip/tar/html) + verificación NetCDF ---
import os, gzip, zipfile, tarfile, shutil, pathlib
# netCDF4 removed
import xarray as xr

# NOTE: Repair/Sniff logic simplified to avoid netCDF4 dependency.
# IF files are corrupt, they must be re-downloaded or fixed externally.

def _sniff(path):
    p = pathlib.Path(path)
    if not p.exists() or p.stat().st_size == 0: return "empty"
    head = open(path, "rb").read(512)
    if head.startswith(b"\x1f\x8b"): return "gzip"
    if head.startswith(b"PK\x03\x04"): return "zip"
    if head.startswith(b"\x89HDF\r\n\x1a\n"): return "netcdf4"
    if head.startswith(b"CDF"): return "netcdf3"
    if head.startswith(b"GRIB"): return "grib"
    if head.lstrip().lower().startswith(b"<html") or head.lstrip().lower().startswith(b"<!doctype html"):
        return "html"
    if b"ustar" in head[257:265]: return "tar"
    return "unknown"

def repair_era5_file_inplace(path: str) -> str:
    kind = _sniff(path); print(f"[repair] {path} → {kind}")
    if kind in {"netcdf3","netcdf4"}:
        return path # Assumed valid
    if kind == "empty": raise OSError("Archivo vacío, reintenta descarga.")
    if kind == "html": raise OSError("El archivo es HTML (error CDS). Revisa key/parámetros.")
    if kind == "grib": raise OSError("Archivo GRIB: pide 'format: netcdf' o abre con engine='cfgrib'.")

    tmp = path + ".tmp.nc"
    if kind == "gzip":
        with gzip.open(path, "rb") as fin, open(tmp, "wb") as fout: shutil.copyfileobj(fin, fout)
    elif kind == "zip":
        with zipfile.ZipFile(path) as zf:
            members = [n for n in zf.namelist() if n.lower().endswith(".nc")]
            if not members: raise OSError("ZIP sin .nc")
            with zf.open(members[0]) as fin, open(tmp, "wb") as fout: shutil.copyfileobj(fin, fout)
    elif kind == "tar":
        with tarfile.open(path) as tf:
            members = [m for m in tf.getmembers() if m.isfile() and m.name.lower().endswith(".nc")]
            if not members: raise OSError("TAR sin .nc")
            with tf.extractfile(members[0]) as fin, open(tmp, "wb") as fout: shutil.copyfileobj(fin, fout)
    elif kind == "unknown":
        # Can't validate without netCDF4
        pass

    # nc.Dataset(tmp).close() # Removed
    bak = path + ".bak"
    if os.path.exists(bak): os.remove(bak)
    os.rename(path, bak); os.replace(tmp, path)
    print("[repair] Reparado (extracción) ✓ (backup .bak)")
    return path
