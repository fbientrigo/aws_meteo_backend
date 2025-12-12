from datetime import datetime

import numpy as np
import pytest
import lib.utils.xarray_functions as xr

from aws_meteo_colab.pangu import (
    LEVELS_ORDER,
    compute_step_metrics,
    ensure_time_coord,
    harmonize_era5,
    iterative_rollout,
    lat_weights_from,
    make_pangu_inputs,
)


@pytest.fixture
def small_coords():
    lat = np.array([10.0, 0.0], dtype=np.float32)
    lon = np.array([0.0, 120.0, 240.0], dtype=np.float32)
    time = np.array([np.datetime64("2024-01-01T00")])
    return lat, lon, time


def make_surface_dataset(value: float, lat, lon, time):
    data = {
        "msl": (("time", "latitude", "longitude"), np.full((1, lat.size, lon.size), value, dtype=np.float32)),
        "u10": (("time", "latitude", "longitude"), np.full((1, lat.size, lon.size), value, dtype=np.float32)),
        "v10": (("time", "latitude", "longitude"), np.full((1, lat.size, lon.size), value, dtype=np.float32)),
        "t2m": (("time", "latitude", "longitude"), np.full((1, lat.size, lon.size), value, dtype=np.float32)),
    }
    coords = {"time": time, "latitude": lat, "longitude": lon}
    return xr.Dataset(data_vars=data, coords=coords)


def make_upper_dataset(value: float, lat, lon, time):
    shape = (1, len(LEVELS_ORDER), lat.size, lon.size)
    base = np.full(shape, value, dtype=np.float32)
    data = {
        "z": (("time", "level", "latitude", "longitude"), base * 9.80665),
        "q": (("time", "level", "latitude", "longitude"), base),
        "t": (("time", "level", "latitude", "longitude"), base),
        "u": (("time", "level", "latitude", "longitude"), base),
        "v": (("time", "level", "latitude", "longitude"), base),
    }
    coords = {"time": time, "level": list(LEVELS_ORDER), "latitude": lat, "longitude": lon}
    return xr.Dataset(data_vars=data, coords=coords)


def test_harmonize_era5_reorders_coords():
    lon = np.array([-120.0, 0.0, 120.0], dtype=np.float32)
    lat = np.array([-10.0, 0.0], dtype=np.float32)
    data = xr.Dataset(
        {
            "msl": (("latitude", "longitude"), np.zeros((lat.size, lon.size), dtype=np.float32)),
        },
        coords={"latitude": lat, "longitude": lon},
    )
    out = harmonize_era5(data, is_pl=False, enforce_shape=False)
    assert float(out.latitude[0]) > float(out.latitude[-1])
    assert np.all(out.longitude.values >= 0)
    assert np.all(np.diff(out.longitude.values) >= 0)


def test_make_pangu_inputs_handles_nan(tmp_path, small_coords):
    lat, lon, time = small_coords
    ds_sfc = make_surface_dataset(1.0, lat, lon, time)
    ds_pl = make_upper_dataset(1.0, lat, lon, time)
    ds_sfc["msl"].values[..., 0] = np.nan
    ds_pl["q"].values[..., 0] = np.nan

    surface_path = tmp_path / "surface.npy"
    upper_path = tmp_path / "upper.npy"
    arr_sfc, arr_up = make_pangu_inputs(
        ds_sfc,
        ds_pl,
        out_surface=str(surface_path),
        out_upper=str(upper_path),
        expected_shape=(lat.size, lon.size),
    )

    assert surface_path.exists() and upper_path.exists()
    assert arr_sfc.shape == (4, lat.size, lon.size)
    assert arr_up.shape == (5, len(LEVELS_ORDER), lat.size, lon.size)
    assert not np.isnan(arr_sfc).any()
    assert not np.isnan(arr_up).any()


def test_compute_step_metrics_matches_zero_error(small_coords):
    lat, lon, time = small_coords
    ds_sfc = make_surface_dataset(2.0, lat, lon, time)
    ds_pl = make_upper_dataset(2.0, lat, lon, time)

    pred_sfc = np.stack([ds_sfc[var].values[0] for var in ("msl", "u10", "v10", "t2m")])
    pred_up = np.stack([ds_pl[var].values[0] for var in ("z", "q", "t", "u", "v")])

    weights = np.ones((lat.size, lon.size), dtype=np.float32)
    metrics = compute_step_metrics(pred_sfc, pred_up, ds_sfc, ds_pl, "level", weights)
    assert all(pytest.approx(0.0, abs=1e-6) == v for v in metrics.values())


def test_iterative_rollout_autoregressive_flow(tmp_path, small_coords):
    lat, lon, time = small_coords
    base_dt = datetime(2024, 1, 1, 0)

    def get_truth(dt: datetime):
        delta = int((dt - base_dt).total_seconds() // (24 * 3600))
        value = float(delta)
        return (
            make_surface_dataset(value, lat, lon, time),
            make_upper_dataset(value, lat, lon, time),
            "level",
        )

    def run_once(ds_sfc_in: xr.Dataset, ds_pl_in: xr.Dataset):
        current = float(ds_sfc_in["msl"].values.mean())
        next_value = current + 1.0
        latitudes = ds_sfc_in.latitude.values.astype("float32")
        longitudes = ds_sfc_in.longitude.values.astype("float32")
        pred_sfc = np.full((4, latitudes.size, longitudes.size), next_value, dtype=np.float32)
        pred_up = np.full((5, len(LEVELS_ORDER), latitudes.size, longitudes.size), next_value, dtype=np.float32)
        pred_up[0] *= 9.80665

        ds_pred_sfc = xr.Dataset(
            {
                "msl": (("latitude", "longitude"), pred_sfc[0]),
                "u10": (("latitude", "longitude"), pred_sfc[1]),
                "v10": (("latitude", "longitude"), pred_sfc[2]),
                "t2m": (("latitude", "longitude"), pred_sfc[3]),
            },
            coords={"latitude": latitudes, "longitude": longitudes},
        )
        ds_pred_up = xr.Dataset(
            {
                "z": (("level", "latitude", "longitude"), pred_up[0]),
                "q": (("level", "latitude", "longitude"), pred_up[1]),
                "t": (("level", "latitude", "longitude"), pred_up[2]),
                "u": (("level", "latitude", "longitude"), pred_up[3]),
                "v": (("level", "latitude", "longitude"), pred_up[4]),
            },
            coords={"level": list(LEVELS_ORDER), "latitude": latitudes, "longitude": longitudes},
        )
        return ds_pred_sfc, ds_pred_up, pred_sfc, pred_up

    out = iterative_rollout(
        base_dt,
        n_steps=3,
        get_era5_truth=get_truth,
        lat_weights_func=lat_weights_from,
        ensure_time_coord_func=ensure_time_coord,
        run_once=run_once,
        make_inputs_fn=make_pangu_inputs,
        make_inputs_kwargs={
            "out_surface": str(tmp_path / "surface.npy"),
            "out_upper": str(tmp_path / "upper.npy"),
            "expected_shape": (lat.size, lon.size),
        },
    )

    assert list(out["steps"]) == [1, 2, 3]
    assert len(out["times"]) == 3
    for key, values in out["metrics"].items():
        assert all(pytest.approx(0.0, abs=1e-6) == v for v in values)
