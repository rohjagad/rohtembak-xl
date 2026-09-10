#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# RohTembak (XL) - emergency reset of admin credentials to admin/admin
# -----------------------------------------------------------------------------
# Recovery tool for when you get locked out (forgot or lost the admin
# username/password).
#
# NOTE: This is NOT part of entrypoint.sh. It must be run manually from a
# terminal / SSH session inside the container or on the server.
#
# It can be run from any working directory (e.g. your home dir) - no need
# to cd into the app folder first:
#
#   docker exec -it rohtembak bash /opt/rohtembak/reset-admin-credentials.sh
#   # or, if you copied it elsewhere:
#   sudo bash ~/reset-admin-credentials.sh
#
# It resets the first admin account to:
#   username : admin
#   password : admin
#
# User data is NOT touched (users, balances, XL accounts, transactions).
# =============================================================================

INSTALL_DIR="/opt/rohtembak"
VENV_PYTHON="${INSTALL_DIR}/venv/bin/python"

if [[ ! -x "${VENV_PYTHON}" ]]; then
    echo "[error] venv python not found at ${VENV_PYTHON}" >&2
    exit 1
fi

if [[ ! -f "${INSTALL_DIR}/main.py" ]]; then
    echo "[error] app not found at ${INSTALL_DIR}" >&2
    exit 1
fi

cd "${INSTALL_DIR}"

"${VENV_PYTHON}" - <<'PYEOF'
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv(Path(os.getcwd()) / ".env")
except Exception:
    pass

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/me-cli-web.db")
if not DATABASE_URL.startswith("sqlite:///"):
    print(f"[error] only sqlite is supported for reset (got: {DATABASE_URL})", file=sys.stderr)
    sys.exit(1)

db_path = DATABASE_URL[len("sqlite:///"):].rsplit("?", 1)[0]
db_path = db_path if db_path.startswith("/") else os.path.abspath(db_path)

if not os.path.isfile(db_path):
    print(f"[error] database not found at {db_path}", file=sys.stderr)
    sys.exit(1)

import sqlite3
# timeout=10: tunggu lock DB yang sedang dipakai uvicorn, jangan langsung gagal
conn = sqlite3.connect(db_path, timeout=10)
try:
    row = conn.execute(
        "SELECT id FROM users WHERE role='admin' ORDER BY id LIMIT 1"
    ).fetchone()
    if not row:
        print("[warn] no admin user found; nothing changed.")
        sys.exit(0)
    # Nama 'admin' bisa saja sudah di-register user lain — rename dulu biar
    # UPDATE username tidak kena UNIQUE constraint (script ini jalan justru
    # saat admin terkunci, jadi harus selalu berhasil).
    conn.execute(
        "UPDATE users SET username='admin~' || id WHERE username='admin' AND id<>?",
        (row[0],),
    )
    conn.execute(
        "UPDATE users SET username='admin', password='admin', password_hash='admin' WHERE id=?",
        (row[0],),
    )
    conn.commit()
    print(f"[ok] admin credentials reset to admin/admin (user id {row[0]}).")
finally:
    conn.close()
PYEOF

echo
echo "Done. Log in with:  username=admin  password=admin"
echo "Strongly recommended: change the password after logging in."
