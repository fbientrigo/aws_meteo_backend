from huggingface_hub import snapshot_download
from pathlib import Path

HF_MODELS = {
    "graphcast": "deepmind/graphcast",
    "fourcastnet": "nvidia/fourcastnet"
}

def download_all():
    for name, repo in HF_MODELS.items():
        target = Path("models") / name
        snapshot_download(repo_id=repo, local_dir=target, ignore_patterns="*.ckpt")  # excluye checkpoints pesados si no son necesarios
        print(f"{name} listo en {target}")

if __name__ == "__main__":
    download_all()
