import os
import subprocess
import sys
import shutil
from pathlib import Path
# import gdown   # <--- eliminada dependencia


REPO_URL = "https://github.com/198808xc/Pangu-Weather.git"
REPO_DIR = Path("Pangu-Weather")

MODEL_PATH = Path("pangu_weather_24.onnx")
MODEL_URL = "https://drive.google.com/file/d/1lweQlxcn9fG0zKNW8ne1Khr9ehRTI6HP/view?usp=share_link"


def clone_if_missing(repo_url: str = REPO_URL, repo_dir: Path = REPO_DIR):
    if not repo_dir.exists():
        print(f"Clonando repositorio desde {repo_url} ...")
        result = subprocess.run(
            ["git", "clone", "--quiet", repo_url, str(repo_dir)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            print("Error al clonar:", result.stderr)
            sys.exit(1)
        print("Repositorio clonado correctamente.")
    else:
        print(f"Repositorio ya existe en {repo_dir.resolve()}")


def ensure_dirs():
    for d in ["input_data", "output_data"]:
        path = Path(d)
        path.mkdir(parents=True, exist_ok=True)
        print(f"Carpeta lista: {path.resolve()}")


def download_weights_if_missing(model_path: Path = MODEL_PATH, url: str = MODEL_URL):
    """
    Descarga los pesos del modelo desde Google Drive usando gdown.
    """
    if model_path.exists():
        print(f"Los pesos ya existen: {model_path.resolve()}")
        return

    print(f"Descargando pesos desde Google Drive...")
    # gdown.download(url, str(model_path), fuzzy=True, quiet=False)
    print("ERROR: gdown ha sido eliminado. Por favor descarga el modelo manualmente de:")
    print(url)
    print(f"y guárdalo en: {model_path.resolve()}")
    sys.exit(1)


if __name__ == "__main__":
    clone_if_missing()
    ensure_dirs()
    download_weights_if_missing()
