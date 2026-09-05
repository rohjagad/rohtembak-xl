#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# RohTembak (XL) - PRODUCTION container entrypoint (branch: main)
# -----------------------------------------------------------------------------
# Self-contained bootstrap for production deployments:
#   deps -> clone main (SEKALI) -> venv -> .env guard -> uvicorn (PID 1)
#
# TIDAK AUTO-UPDATE: perubahan di GitHub tidak otomatis masuk ke container.
# Update manual:
#   podman exec rohtembak bash -c "cd /opt/rohtembak && git pull --ff-only"
#   podman restart rohtembak
#
# Environment:
#   INSTALL_DIR          target dir                     (default /opt/rohtembak)
#   APP_PORT             uvicorn port                   (default 8000)
#   ROHTEMBAK_ENV_FILE   optional .env to copy over     (path inside container)
# =============================================================================

REPO_URL="https://github.com/rohjagad/rohtembak-xl"
REPO_BRANCH="main"
INSTALL_DIR="${INSTALL_DIR:-/opt/rohtembak}"
APP_PORT="${APP_PORT:-8000}"

log()  { echo -e "\033[1;32m[entrypoint]\033[0m $*"; }
warn() { echo -e "\033[1;33m[entrypoint]\033[0m $*"; }

# Retry dengan backoff untuk langkah yang rawan blip jaringan (apt/pip/git).
# PID 1 tidak boleh langsung mati gara-gara gangguan sesaat → restart-loop.
retry() {
    local max=4 wait=10 n=1
    until "$@"; do
        if [ "${n}" -ge "${max}" ]; then
            warn "Gagal permanen setelah ${n} percobaan: $*"
            return 1
        fi
        warn "Transient failure (${n}/${max}), coba lagi dalam ${wait}s: $*"
        sleep "${wait}"
        n=$((n + 1))
    done
    return 0
}

# --- 1. System dependencies ----------------------------------------------------
if ! command -v git >/dev/null 2>&1 || ! command -v python3 >/dev/null 2>&1 \
   || ! python3 -c "import venv" >/dev/null 2>&1 \
   || ! command -v curl >/dev/null 2>&1; then
    log "Installing system dependencies (git, python3-venv, curl)..."
    export DEBIAN_FRONTEND=noninteractive
    retry apt-get update -y >/dev/null
    retry apt-get install -y git python3 python3-venv python3-pip curl ca-certificates >/dev/null
fi

# --- 2. Source: clone once, NO auto-update --------------------------------------
# Container tidak pernah pull otomatis. Kode yang sudah ada di volume dipakai
# apa adanya sampai di-update manual (lihat header file ini).
if [ ! -f "${INSTALL_DIR}/main.py" ] || [ ! -d "${INSTALL_DIR}/.git" ]; then
    log "Fresh install - cloning ${REPO_URL} (branch ${REPO_BRANCH})..."
    # Jangan pernah menghapus isi volume. Clone ke temp, lalu copy kode masuk.
    # - Gagal jaringan -> exit SEBELUM menyentuh isi volume (.env/data aman)
    # - .git korup di volume tidak relevan (sejarah datang dari clone baru)
    mkdir -p "${INSTALL_DIR}"
    tmp="$(mktemp -d)"
    if ! retry git clone --depth 1 -b "${REPO_BRANCH}" "${REPO_URL}" "${tmp}/repo"; then
        rm -rf "${tmp}"
        warn "Clone gagal (jaringan?). Tidak ada data yang disentuh — coba lagi nanti."
        exit 1
    fi
    ENV_KEEP=""
    if [ -f "${INSTALL_DIR}/.env" ]; then
        cp "${INSTALL_DIR}/.env" "${tmp}/.env.keep"
        ENV_KEEP=1
    fi
    cp -a "${tmp}/repo/." "${INSTALL_DIR}/"
    if [ -n "${ENV_KEEP}" ]; then
        mv "${tmp}/.env.keep" "${INSTALL_DIR}/.env"
    fi
    rm -rf "${tmp}"
else
    log "Existing install found - NOT updating (auto-update disabled)."
fi
cd "${INSTALL_DIR}"

# --- 3. Virtualenv + Python dependencies -----------------------------------------
NEED_PIP=0
if [ ! -x "${INSTALL_DIR}/venv/bin/python" ]; then
    log "Creating virtualenv..."
    python3 -m venv venv
    NEED_PIP=1
else
    STAMP=".venv-requirements.sha"
    CUR="$(sha256sum requirements.txt 2>/dev/null | cut -d' ' -f1 || echo none)"
    PREV="$(cat "${STAMP}" 2>/dev/null || echo "")"
    if [ "${CUR}" != "${PREV}" ]; then
        NEED_PIP=1
    fi
fi
if [ "${NEED_PIP}" -eq 1 ]; then
    log "Installing Python dependencies..."
    retry venv/bin/pip install --upgrade pip --quiet
    retry venv/bin/pip install -r requirements.txt --quiet
    sha256sum requirements.txt | cut -d' ' -f1 > .venv-requirements.sha
fi

# --- 4. Configuration (.env) -----------------------------------------------------
if [ -n "${ROHTEMBAK_ENV_FILE:-}" ] && [ -f "${ROHTEMBAK_ENV_FILE}" ]; then
    log "Using env file: ${ROHTEMBAK_ENV_FILE}"
    cp "${ROHTEMBAK_ENV_FILE}" "${INSTALL_DIR}/.env"
else
    log "Keeping existing .env"
fi

# --- 5. Wait for real secrets (no crash-loop) ------------------------------------
if ! grep -qE "^BASE_CIAM_URL=.+" "${INSTALL_DIR}/.env" 2>/dev/null; then
    echo "[entrypoint] .env belum berisi secret (BASE_CIAM_URL kosong)."
    echo "[entrypoint] Letakkan .env asli, mis.: docker cp .env rohtembak:/opt/rohtembak/.env"
    echo "[entrypoint] lalu: docker restart rohtembak"
    echo "[entrypoint] Menunggu .env... (container tetap hidup, bukan crash-loop)"
    sleep infinity
fi

# --- 6. Run the app as PID 1 ------------------------------------------------------
log "Starting uvicorn on :${APP_PORT} ..."
cd "${INSTALL_DIR}"
exec "${INSTALL_DIR}/venv/bin/python" -m uvicorn main:app --host 0.0.0.0 --port "${APP_PORT}"
