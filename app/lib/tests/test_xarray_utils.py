import pytest

np = pytest.importorskip("numpy")
xr = pytest.importorskip("xarray")

from aws_meteo_colab import xarray_utils as xu


def test_pick_var_prefers_candidates():
    ds = xr.Dataset({
        "tp": ("time", np.arange(3)),
        "foo": ("time", np.zeros(3)),
    })
    assert xu._pick_var(ds) == "tp"
    assert xu._pick_var(ds, candidates=["foo", "bar"]) == "foo"


def test_pick_var_raises_on_empty_dataset():
    ds = xr.Dataset()
    with pytest.raises(ValueError):
        xu._pick_var(ds)


def test_ensure_celsius_converts_from_kelvin():
    da = xr.DataArray([273.15, 274.15], dims=("time",), attrs={"units": "K"})
    converted = xu._ensure_celsius(da)
    np.testing.assert_allclose(converted.values, [0.0, 1.0])
    assert converted.attrs["units"] == "°C"


def test_pick_point_coords_prefers_requested_location():
    lat = xr.DataArray(np.linspace(-40, -20, 5), dims=("latitude",))
    lon = xr.DataArray(np.linspace(-75, -60, 4), dims=("longitude",))
    temp = xr.DataArray(
        np.zeros((lat.size, lon.size)),
        coords={"latitude": lat, "longitude": lon},
        dims=("latitude", "longitude"),
    )
    ds = temp.to_dataset(name="t2m")
    lat_name, lon_name, lat_val, lon_val = xu._pick_point_coords(
        ds, prefer_lat=-30, prefer_lon=-70
    )
    assert lat_name == "latitude"
    assert lon_name == "longitude"
    assert lat_val == pytest.approx(-30.0, abs=2.5)
    assert lon_val == pytest.approx(-70.0, abs=4.0)


def test_assert_dims_detects_missing_dimensions():
    da = xr.DataArray(np.zeros((2, 3)), dims=("latitude", "longitude"), name="t2m")
    with pytest.raises(ValueError):
        xu._assert_dims(da)


def test_shape_info_reports_sizes(capsys):
    da = xr.DataArray(np.zeros((2, 3)), dims=("latitude", "longitude"))
    xu._shape_info("demo", da)
    captured = capsys.readouterr().out
    assert "dims" in captured
    assert "latitude" in captured
