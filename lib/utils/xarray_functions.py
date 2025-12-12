"""Utility helpers for manipulating :mod:`xarray` objects.

This module gathers small routines that were previously embedded inside the
training notebooks.  They are kept here so they can be re-used from scripts and
unit tests while keeping the notebooks focused on the narrative pieces.
"""
from __future__ import annotations

from typing import Iterable, Tuple

import lib.utils.xarray_functions as xr

__all__ = [
    "_pick_var",
    "_ensure_celsius",
    "_pick_point_coords",
    "_assert_dims",
    "_shape_info",
]

_DEFAULT_SURFACE_VARS: Tuple[str, ...] = ("t2m_C", "t2m", "tp", "swvl1")


def _pick_var(ds: xr.Dataset, candidates: Iterable[str] | None = None) -> str:
    """Return the first available variable name from ``candidates``."""

    preferred = tuple(candidates) if candidates is not None else _DEFAULT_SURFACE_VARS
    for name in preferred:
        if name in ds.data_vars:
            return name
    # Fallback to the first variable declared in the dataset.
    try:
        return next(iter(ds.data_vars))
    except StopIteration as exc:  # pragma: no cover - guarding against misuse
        raise ValueError("Dataset does not contain any data variables") from exc


def _ensure_celsius(da: xr.DataArray) -> xr.DataArray:
    """Convert temperatures in Kelvin to degrees Celsius, preserving metadata."""

    units = str(da.attrs.get("units", "")).strip()
    if units.lower() in {"k", "kelvin"}:
        out = da - 273.15
        out.attrs.update(da.attrs)
        out.attrs["units"] = "°C"
        return out
    return da


def _pick_point_coords(
    ds: xr.Dataset, *, prefer_lat: float = -33.45, prefer_lon: float = -70.65
) -> tuple[str, str, float, float]:
    """Return the coordinate names and nearest values for a preferred location."""

    if "latitude" in ds.coords:
        lat_name = "latitude"
    elif "lat" in ds.coords:
        lat_name = "lat"
    else:  # pragma: no cover - defensive guard
        raise KeyError("Dataset does not provide a latitude coordinate")

    if "longitude" in ds.coords:
        lon_name = "longitude"
    elif "lon" in ds.coords:
        lon_name = "lon"
    else:  # pragma: no cover - defensive guard
        raise KeyError("Dataset does not provide a longitude coordinate")

    try:
        lat_val = float(ds[lat_name].sel({lat_name: prefer_lat}, method="nearest"))
        lon_val = float(ds[lon_name].sel({lon_name: prefer_lon}, method="nearest"))
    except Exception:
        # Fall back to the median of the coordinates when selection fails
        lat_val = float(ds[lat_name].median())
        lon_val = float(ds[lon_name].median())

    return lat_name, lon_name, lat_val, lon_val


def _assert_dims(
    da: xr.DataArray, required: Iterable[str] = ("time", "latitude", "longitude")
) -> None:
    """Ensure that ``da`` exposes all ``required`` dimensions."""

    missing = [dim for dim in required if dim not in da.dims]
    if missing:
        req = tuple(required)
        raise ValueError(
            f"La variable {da.name} no tiene dims {req}. Dims actuales: {da.dims}"
        )


def _shape_info(tag: str, obj: object) -> None:
    """Pretty-print the dimension/shape information of ``obj`` for debugging."""

    if hasattr(obj, "sizes"):
        print(f"[{tag}] dims: {dict(obj.sizes)}")
        return

    shape = getattr(obj, "shape", None)
    if shape is not None:
        print(f"[{tag}] shape: {tuple(shape)}")
    else:
        print(f"[{tag}] tipo: {type(obj)}")
