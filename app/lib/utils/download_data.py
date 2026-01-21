import lib.utils.xarray_functions as xr
from pathlib import Path

def crop_demo():
    """Descarga un archivo de prueba (por ejemplo ERA5 sample) y lo recorta a la BBox de Chile."""
    url = "https://storage.ecmwf.europeanweather.cloud/public/sample-era5.nc"
    out = Path("data") / "era5_chile.nc"
    ds = xr.open_dataset(url).sel(
        longitude=slice(-75, -66),
        latitude=slice(-17, -56)
    )
    ds.to_netcdf(out)
    print("Guardado:", out)

if __name__ == "__main__":
    crop_demo()
