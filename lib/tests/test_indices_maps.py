import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")
plt = pytest.importorskip("matplotlib.pyplot")
np = pytest.importorskip("numpy")
pd = pytest.importorskip("pandas")
xr = pytest.importorskip("xarray")

from aws_meteo_colab.indices import maps


def _sample_dataarray() -> xr.DataArray:
    time = pd.date_range("2020-01-01", periods=3, freq="MS")
    lat = xr.DataArray([-10.0, 0.0, 10.0], dims=("latitude",))
    lon = xr.DataArray([70.0, 71.0], dims=("longitude",))
    data = xr.DataArray(
        np.arange(time.size * lat.size * lon.size).reshape(time.size, lat.size, lon.size),
        coords={"time": time, "latitude": lat, "longitude": lon},
        dims=("time", "latitude", "longitude"),
        name="demo",
    )
    data.attrs["units"] = "K"
    return data


def test_to_2d_month_slice_extracts_month():
    da = _sample_dataarray()
    sl = maps.to_2d_month_slice(da, "2020-02")
    assert sl.dims == ("latitude", "longitude")
    np.testing.assert_array_equal(sl.latitude.values, da.latitude.values)
    np.testing.assert_array_equal(sl.longitude.values, da.longitude.values)
    np.testing.assert_array_equal(sl.values, da.sel(time="2020-02").values)


def test_to_2d_month_slice_with_month_dimension():
    month_data = _sample_dataarray().rename(time="month")
    month_data["month"] = [1, 2, 3]
    sl = maps.to_2d_month_slice(month_data, "2020-03")
    assert sl.dims == ("latitude", "longitude")


def test_area_mean_weighted_matches_manual_computation():
    da = _sample_dataarray()
    target = da.sel(time="2020-01")
    weights = xr.DataArray(np.cos(np.deg2rad(target.latitude)), dims=("latitude",))
    expected = float(target.weighted(weights).mean(("latitude", "longitude")).values)
    assert maps.area_mean_weighted(da, "2020-01") == pytest.approx(expected)


def test_imshow_map_adds_colorbar():
    da = maps.to_2d_month_slice(_sample_dataarray(), "2020-02")
    fig, ax = plt.subplots()
    maps.imshow_map(ax, da, "demo", vlim=5.0)
    # ``imshow_map`` adds a colorbar, so the figure ends with two axes.
    assert len(fig.axes) == 2
