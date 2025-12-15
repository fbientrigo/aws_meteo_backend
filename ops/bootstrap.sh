#!/bin/bash
set -euo pipefail

LOG=/var/log/aws-meteo-bootstrap.log
exec > >(tee -a "$LOG") 2>&1

LOCK=/var/lock/aws-meteo-bootstrap.lock
exec 200>"$LOCK"
flock -n 200 || { echo "[INFO] Bootstrap ya corriendo; salgo."; exit 0; }

APP_DIR="${AWS_METEO_DIR:-/opt/app/aws_meteo_backend}"
REQ_FILE="${APP_DIR}/api_requirements.txt"
VENV_DIR="${APP_DIR}/.venv"
MARKER_DIR="/var/lib/aws-meteo"
MARKER="${MARKER_DIR}/bootstrap.ok"

mkdir -p "${MARKER_DIR}"

if [ -f "${MARKER}" ]; then
  echo "[OK] Marker existe (${MARKER}). No reinstalo."
  exit 0
fi

if [ ! -f "${REQ_FILE}" ]; then
  echo "[FATAL] No existe ${REQ_FILE}"
  ls -la "${APP_DIR}" || true
  exit 1
fi

echo "[INFO] Creando venv en ${VENV_DIR}"
python3 -m venv "${VENV_DIR}"

echo "[INFO] Upgrading pip tooling"
"${VENV_DIR}/bin/python" -m pip install --upgrade pip setuptools wheel

echo "[INFO] Instalando requirements"
"${VENV_DIR}/bin/python" -m pip install --no-input --retries 5 --timeout 60 -r "${REQ_FILE}"

echo "[INFO] Smoke test: import main:app"
"${VENV_DIR}/bin/python" - <<'PY'
import importlib
m = importlib.import_module("main")
assert hasattr(m, "app"), "main.py no expone variable 'app' (FastAPI)"
print("OK: main:app importable")
PY

date > "${MARKER}"
echo "[OK] Bootstrap finalizado."
