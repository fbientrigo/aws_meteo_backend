"""Utilities for preparing ERA5 data and running the Pangu weather pipeline."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Callable, Dict, Mapping, Optional, Sequence, Tuple

import numpy as np
import os
import subprocess
import sys
import xarray as xr
# import lib.xarray.xarray as xr


LEVELS_ORDER: Sequence[int] = (1000, 925, 850, 700, 600, 500, 400, 300, 250, 200, 150, 100, 50)
EXPECTED_SHAPE: Tuple[int, int] = (721, 1440)


def harmonize_era5(
    ds: xr.Dataset,
    *,
    is_pl: bool,
    enforce_shape: bool = True,
    expected: Tuple[int, int] = EXPECTED_SHAPE,
    select_expver: Optional[int] = 0,
    select_number: Optional[int] = 0,
    target_vertical: str = "pressure_level",
) -> xr.Dataset:
    out = ds
    rename: Dict[str, str] = {}

    if "valid_time" in out.coords and "time" not in out.coords:
        rename["valid_time"] = "time"
    if "forecast_time" in out.coords and "time" not in out.coords:
        rename["forecast_time"] = "time"
    if rename:
        out = out.rename(rename)
        rename = {}

    if is_pl:
        # Normalizar el nombre de la coordenada vertical
        if target_vertical not in out.coords:
            if "pressure_level" in out.coords:
                rename["pressure_level"] = target_vertical
            elif "level" in out.coords:
                rename["level"] = target_vertical
            elif "isobaricInhPa" in out.coords:
                # Caso típico de ECMWF / cfgrib
                rename["isobaricInhPa"] = target_vertical

        if rename:
            out = out.rename(rename)

    # expver / number
    if "expver" in out.sizes and select_expver is not None:
        out = out.isel(expver=select_expver).drop_vars("expver", errors="ignore")
    if "number" in out.sizes and select_number is not None:
        out = out.isel(number=select_number).drop_vars("number", errors="ignore")

    # lat descendente
    if "latitude" in out.coords:
        lat = out.latitude
        if float(lat[0]) < float(lat[-1]):
            out = out.reindex(latitude=lat[::-1])

    # lon en [0, 360)
    if "longitude" in out.coords:
        lon = out.longitude
        if float(lon.min()) < 0:
            out = out.assign_coords(longitude=(lon % 360))
        out = out.sortby("longitude")

    # chequeo de malla
    if enforce_shape and all(k in out.coords for k in ("latitude", "longitude")):
        if (out.sizes["latitude"], out.sizes["longitude"]) != tuple(expected):
            raise ValueError(
                "Malla no es %s; obtuviste %s"
                % (expected, (out.sizes["latitude"], out.sizes["longitude"]))
            )
    return out


def load_nc_for_pangu(
    surface_nc: str,
    pl_nc: str,
    *,
    expected_shape: Tuple[int, int] = EXPECTED_SHAPE,
    levels_order: Sequence[int] = LEVELS_ORDER,
) -> Tuple[xr.Dataset, xr.Dataset]:
    """Load surface and pressure-level ERA5 files and harmonize them."""
    ds_sfc = xr.open_dataset(surface_nc)
    ds_pl = xr.open_dataset(pl_nc)

    ds_sfc = harmonize_era5(ds_sfc, is_pl=False, enforce_shape=True, expected=expected_shape)
    ds_pl = harmonize_era5(ds_pl, is_pl=True, enforce_shape=True, expected=expected_shape)

    rename: Dict[str, str] = {}
    if "z" not in ds_pl and "geopotential" in ds_pl:
        rename["geopotential"] = "z"
    if "q" not in ds_pl and "specific_humidity" in ds_pl:
        rename["specific_humidity"] = "q"
    if rename:
        ds_pl = ds_pl.rename(rename)

    lev_name = "pressure_level" if "pressure_level" in ds_pl.coords else "level"
    ds_pl = ds_pl.sel({lev_name: list(levels_order)})
    return ds_sfc, ds_pl


def make_pangu_inputs(
    ds_sfc: xr.Dataset,
    ds_pl: xr.Dataset,
    *,
    out_surface: str = "input_data/input_surface.npy",
    out_upper: str = "input_data/input_upper.npy",
    expected_shape: Tuple[int, int] = EXPECTED_SHAPE,
) -> Tuple[np.ndarray, np.ndarray]:
    """Create the input tensors expected by Pangu and persist them as ``.npy`` files."""
    sfc_vars = ["msl", "u10", "v10", "t2m"]
    up_vars = ["z", "q", "t", "u", "v"]

    missing_sfc = [var for var in sfc_vars if var not in ds_sfc]
    missing_up = [var for var in up_vars if var not in ds_pl]
    if missing_sfc or missing_up:
        raise KeyError(
            f"Variables faltantes: superficie={missing_sfc} altura={missing_up}"
        )

    sfc_list = [ds_sfc[var].values for var in sfc_vars]
    arr_sfc = np.stack(sfc_list, axis=0).astype("float32")
    if arr_sfc.ndim == 4 and arr_sfc.shape[1] == 1:
        arr_sfc = arr_sfc.squeeze(axis=1)

    up_list = [ds_pl[var].values for var in up_vars]
    arr_up = np.stack(up_list, axis=0).astype("float32")
    if arr_up.ndim == 5 and arr_up.shape[1] == 1:
        arr_up = arr_up.squeeze(axis=1)

    if arr_sfc.shape[0] != 4 or arr_sfc.shape[-2:] != tuple(expected_shape):
        raise ValueError(f"surface {arr_sfc.shape}")
    if arr_up.shape[0] != 5 or arr_up.shape[1] != len(LEVELS_ORDER) or arr_up.shape[-2:] != tuple(expected_shape):
        raise ValueError(f"upper {arr_up.shape}")

    if np.isnan(arr_sfc).any() or np.isnan(arr_up).any():
        arr_sfc = np.nan_to_num(arr_sfc, nan=0.0)
        arr_up = np.nan_to_num(arr_up, nan=0.0)

    os.makedirs(os.path.dirname(out_surface) or ".", exist_ok=True)
    os.makedirs(os.path.dirname(out_upper) or ".", exist_ok=True)
    np.save(out_surface, arr_sfc)
    np.save(out_upper, arr_up)
    return arr_sfc, arr_up


def run_pangu_once(
    ds_sfc_in: xr.Dataset,
    ds_pl_in: xr.Dataset,
    *,
    input_surface_path: str = "input_data/input_surface.npy",
    input_upper_path: str = "input_data/input_upper.npy",
    output_surface_path: str = "output_data/output_surface.npy",
    output_upper_path: str = "output_data/output_upper.npy",
    inference_runner: Optional[Callable[[], None]] = None,
) -> Tuple[xr.Dataset, xr.Dataset, np.ndarray, np.ndarray]:
    """Run a single Pangu inference step using the provided datasets."""
    make_pangu_inputs(
        ds_sfc_in,
        ds_pl_in,
        out_surface=input_surface_path,
        out_upper=input_upper_path,
    )

    if inference_runner is None:
        try:
            subprocess.run([sys.executable, "inference_gpu.py"], check=True)
        except Exception:
            subprocess.run([sys.executable, "inference_cpu.py"], check=True)
    else:
        inference_runner()

    pred_sfc = np.load(output_surface_path)
    pred_up = np.load(output_upper_path)

    lat = ds_sfc_in.latitude.values.astype("float32")
    lon = ds_sfc_in.longitude.values.astype("float32")

    ds_pred_sfc = xr.Dataset(
        data_vars=dict(
            msl=(("latitude", "longitude"), pred_sfc[0]),
            u10=(("latitude", "longitude"), pred_sfc[1]),
            v10=(("latitude", "longitude"), pred_sfc[2]),
            t2m=(("latitude", "longitude"), pred_sfc[3]),
        ),
        coords=dict(latitude=lat, longitude=lon),
    )

    ds_pred_up = xr.Dataset(
        data_vars=dict(
            z=(("level", "latitude", "longitude"), pred_up[0]),
            q=(("level", "latitude", "longitude"), pred_up[1]),
            t=(("level", "latitude", "longitude"), pred_up[2]),
            u=(("level", "latitude", "longitude"), pred_up[3]),
            v=(("level", "latitude", "longitude"), pred_up[4]),
        ),
        coords=dict(level=list(LEVELS_ORDER), latitude=lat, longitude=lon),
    )

    return ds_pred_sfc, ds_pred_up, pred_sfc, pred_up


def _nanok(arr: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    data = np.asarray(arr, dtype=np.float32)
    mask = np.isfinite(data)
    return data, mask


def rmse(pred: np.ndarray, truth: np.ndarray, w: Optional[np.ndarray] = None) -> float:
    p, pm = _nanok(pred)
    t, tm = _nanok(truth)
    mask = pm & tm
    if not np.any(mask):
        return float("nan")
    err2 = (p[mask] - t[mask]) ** 2
    if w is None:
        return float(np.sqrt(np.mean(err2)))
    w_mask = np.broadcast_to(w, mask.shape)[mask]
    return float(np.sqrt(np.sum(err2 * w_mask) / np.sum(w_mask)))


def mae(pred: np.ndarray, truth: np.ndarray, w: Optional[np.ndarray] = None) -> float:
    p, pm = _nanok(pred)
    t, tm = _nanok(truth)
    mask = pm & tm
    if not np.any(mask):
        return float("nan")
    err = np.abs(p[mask] - t[mask])
    if w is None:
        return float(np.mean(err))
    w_mask = np.broadcast_to(w, mask.shape)[mask]
    return float(np.sum(err * w_mask) / np.sum(w_mask))


def compute_step_metrics(
    pred_sfc_arr: np.ndarray,
    pred_up_arr: np.ndarray,
    truth_sfc: xr.Dataset,
    truth_pl: xr.Dataset,
    lev_name: str,
    w: np.ndarray,
) -> Dict[str, float]:
    """Compute latitudinally weighted metrics for key variables."""
    truth_t2m = truth_sfc["t2m"].isel(time=0).astype("float32").values
    rmse_t2m = rmse(pred_sfc_arr[3], truth_t2m, w)
    mae_t2m = mae(pred_sfc_arr[3], truth_t2m, w)

    truth_msl_hpa = truth_sfc["msl"].isel(time=0).astype("float32").values / 100.0
    rmse_msl = rmse(pred_sfc_arr[0] / 100.0, truth_msl_hpa, w)
    mae_msl = mae(pred_sfc_arr[0] / 100.0, truth_msl_hpa, w)

    g = 9.80665
    idx500 = list(LEVELS_ORDER).index(500)
    truth_z500_gpm = (
        truth_pl["z"].isel(time=0, **{lev_name: idx500}).astype("float32").values / g
    )
    pred_z500_gpm = pred_up_arr[0, idx500] / g
    rmse_z500 = rmse(pred_z500_gpm, truth_z500_gpm, w)
    mae_z500 = mae(pred_z500_gpm, truth_z500_gpm, w)

    return {
        "rmse_t2m_K": rmse_t2m,
        "mae_t2m_K": mae_t2m,
        "rmse_msl_hPa": rmse_msl,
        "mae_msl_hPa": mae_msl,
        "rmse_z500_gpm": rmse_z500,
        "mae_z500_gpm": mae_z500,
    }


def lat_weights_from(ds_like: xr.Dataset) -> np.ndarray:
    lat = ds_like.latitude.values.astype("float32")
    return np.cos(np.deg2rad(lat))[:, None].astype("float32")


def ensure_time_coord(
    ds_sfc: xr.Dataset, ds_pl: xr.Dataset, time_value: np.datetime64
) -> Tuple[xr.Dataset, xr.Dataset]:
    time_array = np.array([np.datetime64(time_value)])
    if "time" not in ds_sfc.dims:
        ds_sfc = ds_sfc.expand_dims(time=time_array)
    if "time" not in ds_pl.dims:
        ds_pl = ds_pl.expand_dims(time=time_array)
    return ds_sfc, ds_pl


def iterative_rollout(
    base_dt: datetime,
    n_steps: int = 30,
    *,
    get_era5_truth: Callable[[datetime], Tuple[xr.Dataset, xr.Dataset, str]],
    lat_weights_func: Callable[[xr.Dataset], np.ndarray] = lat_weights_from,
    ensure_time_coord_func: Callable[[xr.Dataset, xr.Dataset, np.datetime64], Tuple[xr.Dataset, xr.Dataset]] = ensure_time_coord,
    run_once: Callable[[xr.Dataset, xr.Dataset], Tuple[xr.Dataset, xr.Dataset, np.ndarray, np.ndarray]] = run_pangu_once,
    make_inputs_fn: Callable[[xr.Dataset, xr.Dataset], Tuple[np.ndarray, np.ndarray]] = make_pangu_inputs,
    make_inputs_kwargs: Optional[Mapping[str, object]] = None,
) -> Dict[str, np.ndarray]:
    """Execute an auto-regressive Pangu rollout starting from ``base_dt``."""
    if n_steps <= 0:
        return {"base_dt": base_dt, "steps": np.array([], dtype=int), "times": np.array([], dtype="datetime64[ns]"), "metrics": {}}

    ds_sfc_t0, ds_pl_t0, lev_name = get_era5_truth(base_dt)
    weights = lat_weights_func(ds_sfc_t0)

    ds_cur_sfc = ds_sfc_t0
    ds_cur_pl = ds_pl_t0

    steps = []
    times = []
    metrics_list = []
    kwargs = dict(make_inputs_kwargs or {})

    for k in range(1, n_steps + 1):
        target_dt = base_dt + timedelta(hours=24 * k)

        ds_pred_sfc, ds_pred_up, pred_sfc_arr, pred_up_arr = run_once(ds_cur_sfc, ds_cur_pl)

        truth_sfc_k, truth_pl_k, lev_name_k = get_era5_truth(target_dt)
        metrics = compute_step_metrics(pred_sfc_arr, pred_up_arr, truth_sfc_k, truth_pl_k, lev_name_k, weights)
        metrics_list.append(metrics)
        steps.append(k)
        times.append(target_dt)

        ds_pred_sfc, ds_pred_up = ensure_time_coord_func(
            ds_pred_sfc, ds_pred_up, np.datetime64(target_dt)
        )

        make_inputs_fn(
            ds_pred_sfc,
            ds_pred_up,
            **kwargs,
        )

        ds_cur_sfc = ds_pred_sfc
        ds_cur_pl = ds_pred_up

    metrics_keys = metrics_list[0].keys() if metrics_list else []
    metrics_arrays = {
        key: np.array([m[key] for m in metrics_list], dtype=float)
        for key in metrics_keys
    }

    return {
        "base_dt": base_dt,
        "steps": np.array(steps, dtype=int),
        "times": np.array(times),
        "metrics": metrics_arrays,
    }


__all__ = [
    "LEVELS_ORDER",
    "EXPECTED_SHAPE",
    "harmonize_era5",
    "load_nc_for_pangu",
    "make_pangu_inputs",
    "run_pangu_once",
    "compute_step_metrics",
    "iterative_rollout",
    "lat_weights_from",
    "ensure_time_coord",
    "rmse",
    "mae",
]
