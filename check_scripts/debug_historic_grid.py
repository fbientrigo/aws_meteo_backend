# ops/debug_historic_grid.py
from __future__ import annotations

from pathlib import Path
import hashlib
import numpy as np
import xarray as xr

# Ajusta si tu catálogo vive en otro módulo
from lib.historic.catalog import get_ordered_sources

ENGINE_PRIMARY = "netcdf4"
ENGINE_FALLBACK = "h5netcdf"

def open_ds(path: Path) -> xr.Dataset:
    try:
        return xr.open_dataset(path, engine=ENGINE_PRIMARY, decode_cf=True, mask_and_scale=True)
    except Exception as e1:
        print(f"[WARN] open_dataset {ENGINE_PRIMARY} failed for {path.name}: {e1!r} -> trying {ENGINE_FALLBACK}")
        return xr.open_dataset(path, engine=ENGINE_FALLBACK, decode_cf=True, mask_and_scale=True)

def pick_coord_name(ds: xr.Dataset, preferred: str, alt: str) -> str:
    if preferred in ds.coords:
        return preferred
    if alt in ds.coords:
        return alt
    # A veces vienen como dims
    if preferred in ds.dims:
        return preferred
    if alt in ds.dims:
        return alt
    raise KeyError(f"No coord/dim '{preferred}' or '{alt}'. coords={list(ds.coords)} dims={list(ds.dims)}")

def sha16(a: np.ndarray) -> str:
    a = np.asarray(a)
    return hashlib.sha256(a.tobytes()).hexdigest()[:16]

def coord_summary(name: str, arr: np.ndarray) -> dict:
    a = np.asarray(arr)
    info = {
        "name": name,
        "n": int(a.size),
        "dtype": str(a.dtype),
        "nan": bool(np.isnan(a).any()) if np.issubdtype(a.dtype, np.floating) else False,
    }
    # cast float64 para métricas (no para hash raw)
    af = a.astype("float64", copy=False) if np.issubdtype(a.dtype, np.number) else a
    info["min"] = float(np.nanmin(af))
    info["max"] = float(np.nanmax(af))
    if af.size >= 2:
        d = np.diff(af)
        d = d[np.isfinite(d)]
        info["step_median_abs"] = float(np.median(np.abs(d))) if d.size else float("nan")
        info["monotonic_inc"] = bool(np.all(d > 0)) if d.size else False
        info["monotonic_dec"] = bool(np.all(d < 0)) if d.size else False
    else:
        info["step_median_abs"] = float("nan")
        info["monotonic_inc"] = False
        info["monotonic_dec"] = False

    # duplicados (tolerancia exacta, para floats idealmente ya vienen exactos)
    try:
        info["dupes"] = int(a.size - np.unique(a).size)
    except Exception:
        info["dupes"] = -1

    # hashes: raw y redondeado (para detectar “solo ruido float”)
    if np.issubdtype(a.dtype, np.number):
        raw = a.astype("float64", copy=False)
        info["hash_raw"] = sha16(raw)
        info["hash_round6"] = sha16(np.round(raw, 6))
    else:
        info["hash_raw"] = sha16(a)
        info["hash_round6"] = info["hash_raw"]

    # muestra bordes
    info["head"] = np.array2string(af[:3], precision=6, separator=", ")
    info["tail"] = np.array2string(af[-3:], precision=6, separator=", ")
    return info

def print_info(file: str, lat_info: dict, lon_info: dict):
    print(
        f"\n[FILE] {file}\n"
        f"  LAT: n={lat_info['n']} min={lat_info['min']:.6f} max={lat_info['max']:.6f} "
        f"step~{lat_info['step_median_abs']:.6g} mono_inc={lat_info['monotonic_inc']} mono_dec={lat_info['monotonic_dec']} "
        f"dupes={lat_info['dupes']} dtype={lat_info['dtype']} hash_raw={lat_info['hash_raw']} hash_r6={lat_info['hash_round6']}\n"
        f"       head={lat_info['head']} tail={lat_info['tail']}\n"
        f"  LON: n={lon_info['n']} min={lon_info['min']:.6f} max={lon_info['max']:.6f} "
        f"step~{lon_info['step_median_abs']:.6g} mono_inc={lon_info['monotonic_inc']} mono_dec={lon_info['monotonic_dec']} "
        f"dupes={lon_info['dupes']} dtype={lon_info['dtype']} hash_raw={lon_info['hash_raw']} hash_r6={lon_info['hash_round6']}\n"
        f"       head={lon_info['head']} tail={lon_info['tail']}\n"
    )

def wrap_lon_to_180(lon: np.ndarray) -> np.ndarray:
    x = lon.astype("float64", copy=False)
    return ((x + 180.0) % 360.0) - 180.0

def show_diffs(base: np.ndarray, other: np.ndarray, tol: float = 0.0, max_show: int = 10):
    if base.size != other.size:
        print(f"  -> size differs: base={base.size} other={other.size}")
        return
    b = base.astype("float64", copy=False)
    o = other.astype("float64", copy=False)
    if tol == 0.0:
        mask = (b != o)
    else:
        mask = np.abs(b - o) > tol
    idx = np.where(mask)[0]
    if idx.size == 0:
        print("  -> arrays match under this criterion.")
        return
    print(f"  -> first {min(max_show, idx.size)} diffs (idx: base vs other):")
    for k in idx[:max_show]:
        print(f"     {int(k)}: {b[int(k)]:.10f} vs {o[int(k)]:.10f}")

def main():
    sources = get_ordered_sources()
    if not sources:
        raise SystemExit("No sources from get_ordered_sources(). Check historic/ directory and catalog.")

    base_lon = None
    base_lat = None
    base_name = None

    for p in sources:
        ds = open_ds(p)

        try:
            lat_name = pick_coord_name(ds, "latitude", "lat")
            lon_name = pick_coord_name(ds, "longitude", "lon")

            lat = np.asarray(ds[lat_name].values)
            lon = np.asarray(ds[lon_name].values)

            lat_info = coord_summary(lat_name, lat)
            lon_info = coord_summary(lon_name, lon)
            print_info(p.name, lat_info, lon_info)

            if base_lon is None:
                base_lon = lon
                base_lat = lat
                base_name = p.name
                continue

            # Comparación estricta (exacta) y “casi exacta”
            same_lon_exact = (lon_info["hash_raw"] == sha16(base_lon.astype("float64", copy=False)))
            same_lon_round = (lon_info["hash_round6"] == sha16(np.round(base_lon.astype("float64", copy=False), 6)))

            if not same_lon_exact:
                print(f"[MISMATCH] LON differs vs base={base_name}")
                print("  Exact diffs (tol=0):")
                show_diffs(base_lon, lon, tol=0.0)

                print("  Almost-equal diffs (tol=1e-10):")
                show_diffs(base_lon, lon, tol=1e-10)

                # Test rápido: ¿es solo convención 0..360 vs -180..180?
                lon_wrapped = wrap_lon_to_180(lon)
                base_wrapped = wrap_lon_to_180(base_lon)
                ok_if_wrap_sort = np.allclose(np.sort(lon_wrapped), np.sort(base_wrapped), atol=1e-10, rtol=0.0)
                print(f"  Heuristic: wrap to [-180,180] and sort matches base? {ok_if_wrap_sort}")

            # (Opcional) también comparar lat por si acaso
            if lat_info["hash_raw"] != sha16(base_lat.astype("float64", copy=False)):
                print(f"[MISMATCH] LAT differs vs base={base_name}")
                show_diffs(base_lat, lat, tol=0.0)

        finally:
            try:
                ds.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
