# # pipeline/toy_run.py
# import numpy as np, pandas as pd
# from indices.core import IndicesConfig, index_bucket
# from extrapolation.naive_ci import extrapolate_last_k_with_ci

# def load_toy_daily(n_days=540, seed=42):
#     rng = np.random.default_rng(seed)
#     idx = pd.date_range("2023-01-01", periods=n_days, freq="D")
#     # Precip con estacionalidad simple y rachas secas/húmedas
#     seasonal = (np.sin(2*np.pi*(idx.dayofyear/365.25 - 0.2))+1.0)/2
#     precip = rng.gamma(shape=seasonal*1.5+0.1, scale=3.0).astype(float)
#     # Temperatura con estacionalidad
#     temp = 18 + 10*np.sin(2*np.pi*(idx.dayofyear/365.25)) + rng.normal(0,1.5,size=n_days)
#     return pd.Series(precip, idx, name="prec_mm"), pd.Series(temp, idx, name="temp_c")

# def run_toy(lat_deg=-33.5):
#     prec, temp = load_toy_daily()
#     cfg = IndicesConfig(spi_window_days=90, spei_window_days=90, sti_window_days=30)
#     bucket = index_bucket(prec, temp, cfg, lat_deg)
#     # Extrapolar SPI 14 días
#     spi_ci = extrapolate_last_k_with_ci(bucket["SPI"], horizon_days=14, k_window=30)
#     return bucket, spi_ci

# if __name__ == "__main__":
#     bucket, spi_ci = run_toy()
#     print(bucket.tail(3))
#     print(spi_ci.head(3))
