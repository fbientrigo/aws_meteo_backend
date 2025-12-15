#!/bin/bash
set -euo pipefail

APP_DIR="${AWS_METEO_DIR:-/opt/app/aws_meteo_backend}"
VENV_DIR="${APP_DIR}/.venv"

# Env opcional
ENV_FILE="/etc/aws-meteo.env"
if [ -f "${ENV_FILE}" ]; then
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
fi

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

exec "${VENV_DIR}/bin/uvicorn" main:app --host "${HOST}" --port "${PORT}"
