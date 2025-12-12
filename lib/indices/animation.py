"""
Funciones de animaciónq que escribí en su momento, no se usan en el código actual.
Pero las dejo por si acaso queremos presentar animaciones en el futuro.
"""

import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import animation

def animate_field_chile(
    da: xr.DataArray,
    time_dim: str = "valid_time",
    out_path: str = "sti_chile.mp4",
    vlim: float = 3.0,
    fps: int = 6,
    dpi: int = 180
) -> str:
    """
    Anima un campo (time, lat, lon) recortado a Chile. Paleta simétrica [-vlim, vlim].
    """
    # Orden y shapes
    da = da.transpose(time_dim, "latitude", "longitude")
    lat = da["latitude"].values
    lon = da["longitude"].values
    times = da[time_dim].values
    data = da.values  # (F, L, M)

    # Figura
    plt.close('all')
    fig, ax = plt.subplots(dpi=dpi)
    extent = [lon.min(), lon.max(), lat.min(), lat.max()]

    img = ax.imshow(data[0], origin='upper', extent=extent,
                    vmin=-vlim, vmax=vlim, interpolation='nearest', aspect='auto')
    cbar = fig.colorbar(img, ax=ax, fraction=0.046, pad=0.04)
    cbar.set_label("STI [z-score]")

    ax.set_xlabel("Longitud [°]")
    ax.set_ylabel("Latitud [°]")

    def ttl(i):
        try:
            ts = np.datetime_as_string(times[i], unit='D')
        except Exception:
            ts = f"frame {i}"
        return f"STI — Chile — {ts}"

    ax.set_title(ttl(0))

    def update(i):
        img.set_data(data[i])
        ax.set_title(ttl(i))
        return [img]

    # Writer
    if out_path.lower().endswith('.mp4') and animation.writers.is_available('ffmpeg'):
        writer = animation.FFMpegWriter(fps=fps)
    else:
        if out_path.lower().endswith('.mp4'):
            print("Aviso: ffmpeg no disponible; exportando GIF.")
            out_path = out_path[:-4] + ".gif"
        writer = animation.PillowWriter(fps=fps)

    ani = animation.FuncAnimation(fig, update, frames=data.shape[0], interval=1000//fps, blit=True)
    ani.save(out_path, writer=writer, dpi=dpi)
    plt.close(fig)
    print("Animación guardada:", out_path)
    return out_path