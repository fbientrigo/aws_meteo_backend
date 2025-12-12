# extrapolation/naive_ci.py
from __future__ import annotations
import numpy as np
import pandas as pd

def extrapolate_last_k_with_ci(series: pd.Series, horizon_days: int = 14,
                               k_window: int = 30, n_boot: int = 500,
                               seed: int = 0) -> pd.DataFrame:
    """Pronóstico naïve (promedio móvil k) + IC bootstrap (percentiles)."""
    rng = np.random.default_rng(seed)
    hist = series.dropna()
    if len(hist) < k_window+1:
        raise ValueError("Serie muy corta para el k_window especificado.")
    mean_k = hist.iloc[-k_window:].mean()
    res = hist.iloc[-k_window:] - mean_k
    fcst = np.full(horizon_days, mean_k, dtype=float)

    # bootstrap residual y suma acumulada (ruido blanco i.i.d.)
    sims = []
    for _ in range(n_boot):
        eps = rng.choice(res.values, size=horizon_days, replace=True)
        sims.append(fcst + eps)
    sims = np.stack(sims)  # (n_boot, H)

    q05 = np.percentile(sims, 5, axis=0)
    q50 = np.percentile(sims, 50, axis=0)
    q95 = np.percentile(sims, 95, axis=0)

    idx = pd.date_range(hist.index[-1] + pd.Timedelta(days=1), periods=horizon_days, freq="D")
    return pd.DataFrame({"p05": q05, "p50": q50, "p95": q95}, index=idx)
