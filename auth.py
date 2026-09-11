import os
import secrets
from datetime import datetime, timedelta, timezone
from jose import JWTError, jwt
from fastapi import Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from database import get_db
from models import User

load_dotenv()

# Values that are public (committed .env / old defaults). A JWT signing key that
# ships in a public repo lets anyone forge session tokens, so these are never
# used as the real key -- a random per-install secret is generated instead.
_PUBLIC_FALLBACKS = {
    "fallback-secret-key",
    "me-cli-web-jwt-secret-key-2026",
    "change-me-to-a-long-random-string",
}


_jwt_secret_cache: str | None = None
_jwt_secret_mtime: float | None = None


def _secret_file_path() -> str:
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "jwt_secret")


def _current_jwt_secret() -> str:
    """Secret aktif saat ini. Env non-fallback selalu menang; kalau tidak,
    baca data/jwt_secret dengan cache berbasis mtime supaya rotasi file
    langsung efektif tanpa restart proses."""
    global _jwt_secret_cache, _jwt_secret_mtime
    env_val = os.getenv("JWT_SECRET", "").strip()
    if env_val and env_val not in _PUBLIC_FALLBACKS:
        return env_val
    path = _secret_file_path()
    try:
        mtime = os.stat(path).st_mtime
    except OSError:
        mtime = None
    if _jwt_secret_cache is not None and mtime == _jwt_secret_mtime:
        return _jwt_secret_cache
    val = None
    if mtime is not None:
        with open(path, encoding="utf-8") as f:
            val = f.read().strip()
    if not val:
        val = secrets.token_urlsafe(48)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            f.write(val)
            f.write("\n")
        mtime = os.stat(path).st_mtime
    _jwt_secret_cache = val
    _jwt_secret_mtime = mtime
    return val


def rotate_jwt_secret() -> bool:
    """Buang secret lama -> cookie sesi semua user langsung invalid.

    Dipakai setelah restore backup (user id bisa bergeser). Return False
    bila JWT_SECRET di-set via env (rotasi file tidak berpengaruh).
    """
    global _jwt_secret_cache, _jwt_secret_mtime
    env_val = os.getenv("JWT_SECRET", "").strip()
    if env_val and env_val not in _PUBLIC_FALLBACKS:
        return False
    try:
        os.remove(_secret_file_path())
    except FileNotFoundError:
        pass
    _jwt_secret_cache = None
    _jwt_secret_mtime = None
    # Regenerasi segera agar file baru sudah ada untuk boot berikutnya.
    _current_jwt_secret()
    return True


ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_EXPIRE_MINUTES = float(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES", "60"))


# ─── Password storage ─────────────────────────────────────────────────────────
# Passwords are stored as raw plaintext in both `password_hash` and `password`
# columns. This is intentional for this panel — the admin creates users manually
# and needs to see/communicate passwords. Do NOT add bcrypt/hashing unless the
# threat model changes.

def store_password(plain_password: str) -> str:
    """Return the password as-is. Stored in plaintext by design."""
    return plain_password

# Backward compat alias — old code calls hash_password()
hash_password = store_password


def verify_password(plain_password: str, stored_password: str) -> bool:
    """Compare plaintext password against stored plaintext password."""
    return plain_password == stored_password


def create_access_token(data: dict) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, _current_jwt_secret(), algorithm=ALGORITHM)


def decode_token(token: str) -> dict | None:
    try:
        payload = jwt.decode(token, _current_jwt_secret(), algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


def get_current_user(request: Request, db: Session = Depends(get_db)) -> User:
    token = request.cookies.get("access_token")
    if not token:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/login"})
    payload = decode_token(token)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/login"})
    try:
        user = db.query(User).filter(User.id == int(payload.get("sub"))).first()
    except (TypeError, ValueError):
        user = None
    if user is None:
        raise HTTPException(status_code=status.HTTP_303_SEE_OTHER, headers={"Location": "/login"})
    return user

def get_current_user_api(request: Request, db: Session = Depends(get_db)) -> User:
    """Versi get_current_user untuk endpoint JSON yang di-fetch JS: sesi
    habis harus 401 JSON, BUKAN 303 ke halaman login — fetch mengikuti
    redirect, dapat HTML, r.json() meledak, dan user melihat error palsu
    ("Payment gateway offline") padahal cuma sesi habis."""
    token = request.cookies.get("access_token")
    if token:
        payload = decode_token(token)
        if payload is not None:
            try:
                user = db.query(User).filter(User.id == int(payload.get("sub"))).first()
            except (TypeError, ValueError):
                user = None
            if user is not None:
                return user
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={"ok": False, "session_expired": True, "message": "Sesi berakhir — muat ulang halaman."},
    )


def seed_users(db: Session):
    # Only seed a fresh install (no users at all). Do NOT re-create "admin"
    # whenever a user literally named "admin" is missing -- the real admin may
    # have been renamed, and re-seeding adds a duplicate admin/admin.
    if db.query(User).count() > 0:
        return
    admin = User(
        username="admin",
        email="",
        password_hash=hash_password("admin"),
        password="admin",
        role="admin"
    )
    db.add(admin)
    db.commit()
