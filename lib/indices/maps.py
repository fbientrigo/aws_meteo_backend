"""Mapping helpers for climate indices notebooks."""
from __future__ import annotations

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import lib.utils.xarray_functions as xr

__all__ = [
    "to_2d_month_slice",
    "imshow_map",
    "area_mean_weighted",
]


def to_2d_month_slice(da: xr.DataArray, month_target: str) -> xr.DataArray:
    """Return the lat/lon slice corresponding to ``month_target``.

    The helper understands datasets that keep the temporal component either as a
    ``time`` coordinate (with timestamp values) or as a ``month`` dimension with
    integers.  The resulting array is squeezed so that only the spatial
    dimensions remain.
    """

    out = da
    if "time" in out.dims:
        try:
            out = out.sel(time=month_target)
        except Exception:
            times = pd.to_datetime(out.time.values).astype("datetime64[M]").astype(str)
            matches = np.where(times == month_target)[0]
            if matches.size:
                out = out.isel(time=int(matches[0]))
            else:
                raise ValueError(f"Mes {month_target} no está en 'time'")

    if "month" in out.dims:
        out = out.sel(month=int(month_target[-2:]))

    out = out.squeeze(drop=True)
    if set(out.dims) != {"latitude", "longitude"}:
        raise ValueError(
            f"Esperaba 2D lat/lon y obtuve dims {out.dims} con shape {tuple(out.shape)}"
        )
    return out


def imshow_map(
    ax: plt.Axes,
    da: xr.DataArray,
    title: str,
    *,
    vlim: float = 3.0,
    cmap: str = "RdBu_r",
) -> None:
    """Render a simple ``imshow`` map for ``da`` on ``ax``."""

    im = ax.imshow(
        da.values,
        origin="upper",
        vmin=-vlim,
        vmax=vlim,
        cmap=cmap,
        extent=[
            float(da.longitude.min()),
            float(da.longitude.max()),
            float(da.latitude.min()),
            float(da.latitude.max()),
        ],
    )
    ax.set_title(title)
    ax.set_xlabel("lon")
    ax.set_ylabel("lat")
    plt.colorbar(im, ax=ax, fraction=0.046, pad=0.02)


def area_mean_weighted(da: xr.DataArray, month_target: str) -> float:
    """Return the cosine-weighted spatial mean for ``month_target``."""

    slice_2d = to_2d_month_slice(da, month_target)
    weights = xr.DataArray(
        np.cos(np.deg2rad(slice_2d.latitude.astype("float32"))), dims=("latitude",)
    )
    return float(slice_2d.weighted(weights).mean(("latitude", "longitude")).values)
