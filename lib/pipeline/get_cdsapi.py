import cdsapi, os

def retrieve_if_missing(dataset: str, req: dict, out_path: str):
    if os.path.exists(out_path):
        print("Existe:", out_path); return
    print("Descargando:", out_path)
    c = cdsapi.Client()
    c.retrieve(dataset, req, out_path)
    print("OK:", out_path)

def _req_single(dt):
    return {
        "product_type": "reanalysis",
        "variable": [
            "mean_sea_level_pressure",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "2m_temperature",
        ],
        "year": f"{dt:%Y}",
        "month": f"{dt:%m}",
        "day": f"{dt:%d}",
        "time": f"{dt:%H}:00",
        "format": "netcdf",
    }

def _req_pl(dt):
    return {
        "product_type": "reanalysis",
        "variable": [
            "geopotential",
            "specific_humidity",
            "temperature",
            "u_component_of_wind",
            "v_component_of_wind",
        ],
        "pressure_level": ["1000","925","850","700","600","500","400","300","250","200","150","100","50"],
        "year": f"{dt:%Y}",
        "month": f"{dt:%m}",
        "day": f"{dt:%d}",
        "time": f"{dt:%H}:00",
        "format": "netcdf",
    }
