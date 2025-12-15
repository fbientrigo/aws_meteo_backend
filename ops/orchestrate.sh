#!/bin/bash
set -euo pipefail

# Logs del orquestador
LOG=/var/log/aws-meteo-orchestrate.log
exec > >(tee -a "$LOG") 2>&1

# Lock global (evita dobles ejecuciones)
LOCK=/var/lock/aws-meteo-orchestrate.lock
exec 200>"$LOCK"
flock -n 200 || { echo "[INFO] Orquestador ya corriendo; salgo."; exit 0; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${SCRIPT_DIR}/lib.sh"
need_root

APP_DIR="${AWS_METEO_DIR:-/opt/app/aws_meteo_backend}"
USER_NAME="${AWS_METEO_USER:-ubuntu}"
ENV_DST="/etc/aws-meteo.env"
ENV_SRC="${REPO_DIR}/ops/env/aws-meteo.env.example"

echo "[INFO] APP_DIR=${APP_DIR}"
echo "[INFO] USER_NAME=${USER_NAME}"
echo "[INFO] REPO_DIR=${REPO_DIR}"

# 1) prerequisitos OS (mantenerlo acotado pero robusto)
export DEBIAN_FRONTEND=noninteractive
if command -v apt-get >/dev/null 2>&1; then
  wait_for_apt_lock
  retry apt-get update -y
  wait_for_apt_lock
  retry apt-get install -y --no-install-recommends \
    python3 python3-venv python3-pip \
    ca-certificates \
    build-essential python3-dev libffi-dev libssl-dev
fi

# 2) asegurar APP_DIR esperado
if [ ! -d "${APP_DIR}/.git" ]; then
  echo "[FATAL] No veo repo git en ${APP_DIR}. ¿Clonaste bien en User Data?"
  ls -la "$(dirname "${APP_DIR}")" || true
  exit 1
fi

# 3) env file opcional
if [ ! -f "${ENV_DST}" ] && [ -f "${ENV_SRC}" ]; then
  echo "[INFO] Copiando env example a ${ENV_DST}"
  cp "${ENV_SRC}" "${ENV_DST}"
  chmod 600 "${ENV_DST}"
fi

# 4) instalar systemd units desde repo
echo "[INFO] Instalando systemd units..."
cp "${APP_DIR}/ops/systemd/aws-meteo-bootstrap.service" /etc/systemd/system/aws-meteo-bootstrap.service
cp "${APP_DIR}/ops/systemd/aws-meteo-api.service"       /etc/systemd/system/aws-meteo-api.service

systemctl daemon-reload
systemctl enable aws-meteo-bootstrap.service aws-meteo-api.service

# 5) ejecutar bootstrap (deps) y esperar resultado
echo "[INFO] Iniciando bootstrap..."
systemctl start aws-meteo-bootstrap.service

echo "[INFO] Esperando bootstrap..."
for i in $(seq 1 180); do
  if systemctl is-active --quiet aws-meteo-bootstrap.service; then
    echo "[OK] Bootstrap activo (terminó bien)."
    break
  fi
  if systemctl is-failed --quiet aws-meteo-bootstrap.service; then
    echo "[FATAL] Bootstrap falló. Logs:"
    journalctl -u aws-meteo-bootstrap.service -b --no-pager -n 200 || true
    exit 1
  fi
  sleep 2
done

# 6) iniciar API
echo "[INFO] Iniciando API (uvicorn)..."
systemctl restart aws-meteo-api.service

echo "[OK] Orquestación completa."
echo "[HINT] Logs: journalctl -u aws-meteo-bootstrap.service -b | tail -n 200"
echo "[HINT] Logs: journalctl -u aws-meteo-api.service -b | tail -n 200"
