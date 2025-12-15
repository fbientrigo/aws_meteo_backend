#!/bin/bash
set -euo pipefail

retry() {
  local n=0 max="${RETRY_MAX:-12}" delay="${RETRY_DELAY:-2}"
  until "$@"; do
    n=$((n+1))
    if [ "$n" -ge "$max" ]; then
      echo "[FATAL] Falló tras $n intentos: $*"
      return 1
    fi
    echo "[WARN] Falló: $*  (reintento $n/$max en ${delay}s)"
    sleep "$delay"
    delay=$((delay*2))
    [ "$delay" -gt 30 ] && delay=30
  done
}

wait_for_apt_lock() {
  while fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1 \
     || fuser /var/lib/apt/lists/lock >/dev/null 2>&1 \
     || fuser /var/cache/apt/archives/lock >/dev/null 2>&1; do
    echo "[APT] Esperando locks de apt/dpkg..."
    sleep 2
  done
}

wait_for_dns() {
  retry getent hosts github.com >/dev/null
}

need_root() {
  if [ "${EUID:-$(id -u)}" -ne 0 ]; then
    echo "[FATAL] Esto debe ejecutarse como root"
    exit 1
  fi
}
