import os
import io
import time
import json
import html as _html
import calendar
import random
import uuid
import asyncio
import threading
import zipfile
import re
import requests
from urllib.parse import quote as _urlquote
from contextlib import asynccontextmanager, redirect_stdout
from dotenv import load_dotenv

load_dotenv()

from fastapi import FastAPI, Request, Depends, Form, File, UploadFile, HTTPException, status
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles
from sqlalchemy import func, or_
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, joinedload
from jinja2 import Environment, FileSystemLoader

from database import init_db, get_db
from models import User, XLAccount, Balance, BalanceTransaction, FamilyFee, TopupTransaction, PackagePrice, XlFamily
from auth import (
    verify_password, create_access_token, decode_token,
    get_current_user, seed_users, hash_password, ACCESS_TOKEN_EXPIRE_MINUTES,
    rotate_jwt_secret
)

from datetime import datetime, timezone, timedelta
from app.client.ciam import get_otp as xl_get_otp, submit_otp as xl_submit_otp, get_new_token as xl_refresh_token
from app.client.encrypt import API_KEY, load_ax_fp, copy_shared_fp_to_user, remove_user_ax_fp, get_user_ax_fp, _safe_username
from app.client.engsel import login_info as xl_login_info, get_balance as xl_get_balance, get_transaction_history as xl_get_transactions, get_tiering_info as xl_get_tiering, send_api_request, get_family as xl_get_family, get_package as xl_get_package, get_addons as xl_get_addons
from app.menus.util import format_quota_byte
from app.type_dict import PaymentItem
from app.client import gopay

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates_dir = os.path.join(BASE_DIR, "templates")
jinja_env = Environment(loader=FileSystemLoader(templates_dir), auto_reload=True, autoescape=True)
WIB = timezone(timedelta(hours=7))


def _fmt_epoch_wib(ts):
    """Render a true unix epoch (UTC instant) as a WIB wall-clock in XL history format."""
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=WIB).strftime("%d %B %Y | %H:%M WIB")
    except (ValueError, OSError):
        return None


def _tgl_jam_wib(ts):
    """Split a true epoch into separate (tanggal, jam) WIB strings for the history table."""
    try:
        dt = datetime.fromtimestamp(int(ts), tz=WIB)
        return dt.strftime("%d %B %Y"), dt.strftime("%H:%M WIB")
    except (ValueError, OSError):
        return None, None


def _fmt_harga(value):
    """Normalize any price shape (int, '30000', 'IDR 30000', 'IDR30.000') to '30.000 IDR'."""
    if value is None:
        return None
    digits = "".join(ch for ch in str(value) if ch.isdigit())
    if not digits:
        return None
    try:
        n = int(digits)
    except ValueError:
        return None
    return "{:,.0f} IDR".format(n).replace(",", ".")


def _fmt_idr(n):
    """Format a number with dot thousand separators: 30000 -> '30.000'."""
    try:
        return "{:,.0f}".format(int(n)).replace(",", ".")
    except (ValueError, TypeError):
        return "0"


def _fmt_xl_ts(ts):
    """XL transaction timestamps are WIB wall-clock stored as if UTC (+7h ahead of the
    real instant). Subtract 7h first so the rendered WIB matches XL's formated_date."""
    if not ts:
        return None
    try:
        return _fmt_epoch_wib(int(ts) - 7 * 3600)
    except (ValueError, OSError):
        return None


def _fmt_xl_expiry(ts):
    """Quota/balance expiry epochs are true UTC; render in WIB unchanged."""
    if not ts:
        return None
    try:
        return datetime.fromtimestamp(int(ts), tz=WIB).strftime("%d %b %Y %H:%M WIB")
    except (ValueError, OSError):
        return None


def _fmt_wib(ts):
    if not ts:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(WIB).strftime("%Y-%m-%d %H:%M WIB")


def _parse_xl_dt(s):
    if not s:
        return 0
    s = str(s).strip()
    if s.endswith(" WIB"):
        s = s[:-4].rstrip()
    for fmt in ("%d %B %Y | %H:%M", "%d %b %Y | %H:%M", "%d %B %Y %H:%M", "%d %b %Y %H:%M"):
        try:
            return int(datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp())
        except ValueError:
            continue
    return 0


jinja_env.filters["datetimeformat"] = lambda ts: _fmt_xl_expiry(ts) or "—"
jinja_env.filters["quotabyte"] = format_quota_byte
jinja_env.filters["rupiah"] = lambda n: _fmt_idr(n)

API_DELAY = float(os.getenv("API_DELAY", "2.0"))
_XL_CALL_LIMIT = max(1, int(os.getenv("XL_CALL_LIMIT", "6")))

TOPUP_MIN_AMOUNT = int(os.getenv("TOPUP_MIN_AMOUNT", "5000"))
TOPUP_MAX_AMOUNT = int(os.getenv("TOPUP_MAX_AMOUNT", "1000000"))
TOPUP_FEE_MIN = int(os.getenv("TOPUP_FEE_MIN", "1"))
TOPUP_FEE_MAX = int(os.getenv("TOPUP_FEE_MAX", "250"))
TOPUP_QR_TTL_SECONDS = 5 * 60
TOPUP_CHECK_INTERVAL = int(os.getenv("TOPUP_CHECK_INTERVAL", "20"))
TOPUP_MAX_PENDING_PER_USER = 3
TOPUP_MANUAL_CHECK_COOLDOWN = 5 * 60
_APP_START_TS = time.time()
_reconcile_task = None

# Serializes every Balance read-modify-write in this process so concurrent
# requests cannot lose updates or drive a balance negative.
_balance_lock = threading.Lock()

# Fetch katalog admin harus serial — refresh token XL sekali pakai.
_catalog_fetch_lock = threading.Lock()

# Simple in-memory brute-force throttle for login/register endpoints.
_login_failures = {}
_LOGIN_WINDOW_SECONDS = 300
_LOGIN_MAX_FAILURES = 5


def _client_key(request: Request, extra: str = "") -> str:
    ip = request.client.host if request.client else "?"
    return f"{ip}:{extra}" if extra else ip


def _login_blocked(key: str) -> bool:
    rec = _login_failures.get(key)
    if not rec:
        return False
    if time.time() - rec[1] > _LOGIN_WINDOW_SECONDS:
        _login_failures.pop(key, None)
        return False
    return rec[0] >= _LOGIN_MAX_FAILURES


def _login_record_failure(key: str) -> None:
    now = time.time()
    rec = _login_failures.get(key)
    if not rec or now - rec[1] > _LOGIN_WINDOW_SECONDS:
        if len(_login_failures) > 10000:
            # Prune entri basi saja — clear() semua akan melepas lockout aktif
            # (attacker bisa sengaja membanjiri map buat reset rate-limit).
            for k in [k for k, v in list(_login_failures.items()) if v[1] < now - _LOGIN_WINDOW_SECONDS]:
                _login_failures.pop(k, None)
        _login_failures[key] = [1, now]
    else:
        rec[0] += 1


def _login_reset(key: str) -> None:
    _login_failures.pop(key, None)

import app.client.engsel as _engsel

_xl_sem = __import__("threading").BoundedSemaphore(_XL_CALL_LIMIT)
_engsel_send_api_request = _engsel.send_api_request


def _bounded_send_api_request(*args, **kwargs):
    """Rate-limit concurrent XL API calls so many users don't hammer/timeout."""
    with _xl_sem:
        return _engsel_send_api_request(*args, **kwargs)


_engsel.send_api_request = _bounded_send_api_request
send_api_request = _bounded_send_api_request


def _api_delay():
    time.sleep(API_DELAY)


_XL_TOKEN_CACHE = {}
_TOKEN_CACHE_FALLBACK_TTL = 540
REFRESH_WARN_SECONDS = 3 * 86400
_token_lock = __import__("threading").Lock()


def _persist_refresh_token(account_id, refresh_token, refresh_expires_at):
    """Save the latest (rotated) XL refresh token and its expiry to DB."""
    try:
        db = next(get_db())
        acct = db.query(XLAccount).filter(XLAccount.id == account_id).first()
        if not acct:
            return
        changed = False
        if refresh_token and refresh_token != acct.refresh_token:
            acct.refresh_token = refresh_token
            changed = True
        if refresh_expires_at and refresh_expires_at != acct.refresh_expires_at:
            acct.refresh_expires_at = int(refresh_expires_at)
            changed = True
        if changed:
            db.commit()
    except Exception as e:
        print(f"[persist_refresh] Error: {e}")
    finally:
        try:
            db.close()
        except Exception:
            pass


def _get_xl_tokens(active_xl, username=""):
    """Return cached XL access tokens; refresh once per token lifetime (~10 min).

    Access token expires_in is ~599s, refresh token ~88 days. Tokens are cached
    per XL account for the access-token lifetime so the (slow) refresh call is
    only made occasionally instead of on every request.

    XL rotates the refresh token on every refresh, so the newest token is saved
    back to DB (sliding ~88-day window) together with its refresh_expires_at.
    """
    if not active_xl or not active_xl.refresh_token:
        return None
    key = active_xl.subscriber_id or active_xl.id
    now = time.time()
    entry = _XL_TOKEN_CACHE.get(key)
    if isinstance(entry, dict) and not entry.get("refreshing") and entry.get("expires_at", 0) > now:
        return entry["tokens"]

    # Klaim tugas refresh (global lock hanya sesaat — JANGAN memegang lock
    # selama sleep + network call; itu pernah bikin seluruh panel stall).
    mine = False
    deadline = time.time() + 30
    while True:
        with _token_lock:
            entry = _XL_TOKEN_CACHE.get(key)
            if isinstance(entry, dict) and not entry.get("refreshing") and entry.get("expires_at", 0) > time.time():
                return entry["tokens"]  # thread lain selesai refresh duluan
            if isinstance(entry, dict) and entry.get("refreshing"):
                mine = False
            else:
                _XL_TOKEN_CACHE[key] = {"refreshing": True, "expires_at": 0}
                mine = True
        if mine:
            break
        if time.time() >= deadline:
            print(f"[_get_xl_tokens] timeout menunggu refresh akun {key}")
            return None
        time.sleep(0.5)

    try:
        _api_delay()
        xl_username = username or ""
        if not xl_username:
            try:
                xl_username = active_xl.user.username
            except Exception:
                print("[_get_xl_tokens] Could not load username from active_xl.user")
                return None
        tokens = xl_refresh_token(API_KEY, active_xl.refresh_token, xl_username)
        if not tokens:
            with _token_lock:
                cur = _XL_TOKEN_CACHE.get(key)
                if isinstance(cur, dict) and cur.get("refreshing"):
                    _XL_TOKEN_CACHE.pop(key, None)
            try:
                db2 = next(get_db())
                acct = db2.query(XLAccount).filter(XLAccount.id == active_xl.id).first()
                if acct:
                    acct.refresh_token = ""
                    db2.commit()
            except Exception as e:
                print(f"[clear_refresh] Error: {e}")
            finally:
                try:
                    db2.close()
                except Exception:
                    pass
            return None
        try:
            ttl = int(tokens.get("expires_in", 0)) - 30
        except (TypeError, ValueError):
            ttl = _TOKEN_CACHE_FALLBACK_TTL
        if ttl < 30:
            ttl = 30
        _persist_refresh_token(
            active_xl.id,
            tokens.get("refresh_token"),
            tokens.get("refresh_expires_in") and int(time.time()) + int(tokens["refresh_expires_in"]),
        )
        with _token_lock:
            _XL_TOKEN_CACHE[key] = {"tokens": tokens, "expires_at": time.time() + ttl}
        return tokens
    except Exception:
        # Lepas klaim refresh supaya caller berikutnya bisa mencoba lagi.
        with _token_lock:
            cur = _XL_TOKEN_CACHE.get(key)
            if isinstance(cur, dict) and cur.get("refreshing"):
                _XL_TOKEN_CACHE.pop(key, None)
        raise


def render(template_name: str, status_code: int = 200, context: dict | None = None, cache_control: str = "no-store"):
    template = jinja_env.get_template(template_name)
    html = template.render(**(context or {}))
    return HTMLResponse(html, status_code=status_code, headers={"Cache-Control": cache_control})


def render_template_to_string(template_name: str, context: dict | None = None) -> str:
    template = jinja_env.get_template(template_name)
    return template.render(**(context or {}))


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        import anyio
        limiter = anyio.to_thread.current_default_thread_limiter()
        limiter.total_tokens = 256
    except Exception:
        pass
    init_db()
    try:
        # Migrasi ringan: kolom baru di xl_families (create_all tidak ALTER)
        from sqlalchemy import text as _text
        _db0 = next(get_db())
        _cols = [r[1] for r in _db0.execute(_text("PRAGMA table_info(xl_families)"))]
        for _name, _ddl in (
            ("url_prefix", "VARCHAR(40) NOT NULL DEFAULT ''"),
            ("option_codes", "VARCHAR(255) NOT NULL DEFAULT ''"),
            ("qris_decoy", "BOOLEAN NOT NULL DEFAULT 0"),
            ("is_active", "BOOLEAN NOT NULL DEFAULT 1"),
        ):
            if _name not in _cols:
                _db0.execute(_text(f"ALTER TABLE xl_families ADD COLUMN {_name} {_ddl}"))
        _pkg_cols = [r[1] for r in _db0.execute(_text("PRAGMA table_info(package_prices)"))]
        for _name, _ddl in (
            ("decoy_qris", "VARCHAR(100) NOT NULL DEFAULT ''"),
            ("decoy_pulsa", "VARCHAR(100) NOT NULL DEFAULT ''"),
            ("fee_qris", "INTEGER DEFAULT NULL"),
            ("fee_pulsa", "INTEGER DEFAULT NULL"),
        ):
            if _name not in _pkg_cols:
                _db0.execute(_text(f"ALTER TABLE package_prices ADD COLUMN {_name} {_ddl}"))
        _db0.commit()
        _db0.close()
    except Exception as e:
        print(f"[lifespan] migrasi/index xl_families gagal: {e}")
    db = next(get_db())
    seed_users(db)
    existing_users = db.query(User).filter(User.role == "user").all()
    fp_dir = os.path.join(BASE_DIR, "data")
    for u in existing_users:
        old_fp = os.path.join(fp_dir, f"ax.fp.{u.id}")
        new_fp = os.path.join(fp_dir, f"ax.fp.{_safe_username(u.username)}")
        if os.path.exists(old_fp) and not os.path.exists(new_fp):
            try:
                os.rename(old_fp, new_fp)
            except OSError:
                pass
        copy_shared_fp_to_user(u.username)
    db.close()
    # Keep a strong reference: asyncio only weakly references tasks, so an
    # unreferenced sleeping task can be garbage-collected mid-loop.
    global _reconcile_task
    _reconcile_task = asyncio.create_task(_topup_reconcile_watch())
    threading.Thread(target=_autobackup_loop, daemon=True, name="autobackup").start()
    yield


app = FastAPI(title="RohTembak (XL)", lifespan=lifespan)

static_dir = os.path.join(BASE_DIR, "static")
os.makedirs(static_dir, exist_ok=True)
app.mount("/static", StaticFiles(directory=static_dir), name="static")


# ─── Error Pages ────────────────────────────────────────────────────────────

@app.exception_handler(404)
async def not_found(request: Request, exc):
    return render("error.html", context={
        "request": request,
        "code": 404,
        "message": "Halaman tidak ditemukan"
    }, status_code=404)


@app.exception_handler(500)
async def server_error(request: Request, exc):
    return render("error.html", context={
        "request": request,
        "code": 500,
        "message": "Terjadi kesalahan server"
    }, status_code=500)


def get_user_context(user: User, db: Session) -> dict:
    bal = db.query(Balance).filter(Balance.user_id == user.id).first()
    xl_accounts = db.query(XLAccount).options(joinedload(XLAccount.user)).filter(XLAccount.user_id == user.id).all()
    active_xl = next((x for x in xl_accounts if x.is_active), None)
    refresh_warning = None
    if active_xl and active_xl.refresh_expires_at:
        secs_left = int(active_xl.refresh_expires_at) - time.time()
        if secs_left <= REFRESH_WARN_SECONDS:
            refresh_warning = round(secs_left / 86400, 1)
    return {
        "request": None,
        "user": user,
        "balance": bal.balance if bal else 0,
        "xl_accounts": xl_accounts,
        "active_xl": active_xl,
        "refresh_warning": refresh_warning,
        "xl_count": len(xl_accounts),
    }


# ─── Root redirect ─────────────────────────────────────────────────────────

@app.get("/")
def root():
    return RedirectResponse(url="/login", status_code=303)


# ─── Login ─────────────────────────────────────────────────────────────────

def _redirect_authenticated(request: Request):
    """If a valid session cookie exists, bounce to the right dashboard."""
    token = request.cookies.get("access_token")
    if not token:
        return None
    payload = decode_token(token)
    if not payload:
        return None
    if payload.get("role") == "admin":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    return RedirectResponse(url="/user/dashboard", status_code=303)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    bounce = _redirect_authenticated(request)
    if bounce:
        return bounce
    return render("login.html", context={"request": request})


@app.post("/login")
def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    if not username.strip():
        return render("login.html", context={
            "request": request,
            "error": "Email atau username wajib diisi"
        }, status_code=400)
    username = username.strip().lower()
    attempt_key = _client_key(request, username)
    if _login_blocked(attempt_key):
        return render("login.html", context={
            "request": request,
            "error": "Terlalu banyak percobaan gagal. Coba lagi dalam beberapa menit."
        }, status_code=429)
    user = db.query(User).filter(
        (func.lower(User.username) == username) | (func.lower(User.email) == username)
    ).first()
    if not user or not verify_password(password, user.password_hash):
        _login_record_failure(attempt_key)
        return render("login.html", context={
            "request": request,
            "error": "Email/Username atau password salah"
        }, status_code=400)
    if user.role != "user":
        return render("login.html", context={
            "request": request,
            "error": "Akun admin tidak bisa login sebagai pengguna biasa"
        }, status_code=403)

    _login_reset(attempt_key)
    token = create_access_token({"sub": str(user.id), "role": user.role})
    resp = RedirectResponse(url="/user/dashboard", status_code=303)
    resp.set_cookie(key="access_token", value=token, httponly=True, samesite="lax", max_age=int(ACCESS_TOKEN_EXPIRE_MINUTES * 60))
    return resp


@app.get("/admin/login", response_class=HTMLResponse)
def admin_login_page(request: Request):
    bounce = _redirect_authenticated(request)
    if bounce:
        return bounce
    return render("admin_login.html", context={"request": request})


@app.post("/admin/login")
def admin_login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    if not username.strip():
        return render("admin_login.html", context={
            "request": request,
            "error": "Username wajib diisi"
        }, status_code=400)
    username = username.strip().lower()
    attempt_key = _client_key(request, username)
    if _login_blocked(attempt_key):
        return render("admin_login.html", context={
            "request": request,
            "error": "Terlalu banyak percobaan gagal. Coba lagi dalam beberapa menit."
        }, status_code=429)
    user = db.query(User).filter(func.lower(User.username) == username).first()
    if not user or not verify_password(password, user.password_hash):
        _login_record_failure(attempt_key)
        return render("admin_login.html", context={
            "request": request,
            "error": "Username atau password salah"
        }, status_code=400)
    if user.role != "admin":
        return render("admin_login.html", context={
            "request": request,
            "error": "Akun ini tidak memiliki akses admin"
        }, status_code=403)

    _login_reset(attempt_key)
    token = create_access_token({"sub": str(user.id), "role": user.role})
    resp = RedirectResponse(url="/admin/dashboard", status_code=303)
    resp.set_cookie(key="access_token", value=token, httponly=True, samesite="lax", max_age=int(ACCESS_TOKEN_EXPIRE_MINUTES * 60))
    return resp


@app.get("/logout")
def logout(request: Request):
    dest = "/login"
    payload = decode_token(request.cookies.get("access_token") or "")
    if payload and payload.get("role") == "admin":
        dest = "/admin/login"
    resp = RedirectResponse(url=dest, status_code=303)
    resp.delete_cookie("access_token")
    return resp


@app.get("/api/session")
def api_session(user: User = Depends(get_current_user)):
    return JSONResponse({"ok": True})


# ─── Admin Dashboard ────────────────────────────────────────────────────────

@app.get("/admin/dashboard", response_class=HTMLResponse)
def admin_home(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    first_login = verify_password("admin", user.password_hash)

    if gopay.is_configured():
        tok = gopay.token_status(timeout=8)
        if tok.get("ok"):
            qris_label = "Aktif · Token Valid"
        elif tok.get("http_status"):
            qris_label = "Aktif · Token Invalid"
        else:
            qris_label = "Aktif · Gateway Tak Dijangkau"
    else:
        qris_label = "Belum dikonfigurasi"

    up = max(0, int(time.time() - _APP_START_TS))
    days, rem = divmod(up, 86400)
    hours, rem2 = divmod(rem, 3600)
    minutes = rem2 // 60
    if days:
        uptime_label = f"{days}h {hours}j"
    elif hours:
        uptime_label = f"{hours}j {minutes}m"
    else:
        uptime_label = f"{minutes}m"

    db = next(get_db())
    try:
        day_start_utc = datetime.now(WIB).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).astimezone(timezone.utc)
        topup_today = db.query(func.coalesce(func.sum(BalanceTransaction.amount), 0)).filter(
            BalanceTransaction.type == "topup",
            BalanceTransaction.created_at >= day_start_utc,
        ).scalar() or 0
        # Real QRIS income includes the unique-code fee users actually paid;
        # the ledger only records the credited base amount.
        fee_today = db.query(func.coalesce(func.sum(TopupTransaction.fee), 0)).filter(
            TopupTransaction.status == "paid",
            TopupTransaction.paid_at.isnot(None),
            TopupTransaction.paid_at >= day_start_utc.replace(tzinfo=None),
        ).scalar() or 0
        topup_today += fee_today
        used_today = -(db.query(func.coalesce(func.sum(BalanceTransaction.amount), 0)).filter(
            BalanceTransaction.type == "purchase",
            BalanceTransaction.created_at >= day_start_utc,
        ).scalar() or 0)
    finally:
        db.close()

    return render("admin/home.html", context={
        "request": request,
        "user": user,
        "first_login": first_login,
        "qris_label": qris_label,
        "uptime_label": uptime_label,
        "topup_today": topup_today,
        "used_today": used_today,
    })


@app.get("/admin/users", response_class=HTMLResponse)
def admin_users_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    db = next(get_db())
    users = db.query(User).filter(User.role == "user").all()
    user_data = []
    for u in users:
        bal = db.query(Balance).filter(Balance.user_id == u.id).first()
        xl_count = db.query(XLAccount).filter(XLAccount.user_id == u.id).count()
        user_data.append({
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "balance": bal.balance if bal else 0,
            "xl_count": xl_count,
        })
    admin_bal = db.query(Balance).filter(Balance.user_id == user.id).first()
    db.close()
    first_login = verify_password("admin", user.password_hash)
    return render("admin/users.html", context={
        "request": request,
        "user": user,
        "users": user_data,
        "balance": admin_bal.balance if admin_bal else 0,
        "first_login": first_login,
    })


@app.get("/admin/credentials", response_class=HTMLResponse)
def admin_credentials_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    return render("admin/credentials.html", context={
        "request": request,
        "user": user,
        "mode": request.query_params.get("mode", "username"),
        "updated": request.query_params.get("updated"),
        "error": request.query_params.get("error"),
    })


@app.post("/admin/credentials")
def admin_credentials_update(
    mode: str = Form("username"),
    current_password: str = Form(...),
    new_username: str = Form(""),
    new_password: str = Form(""),
    confirm_password: str = Form(""),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if not verify_password(current_password, user.password_hash):
        return RedirectResponse(url="/admin/credentials?error=salah_password", status_code=303)
    if mode == "password":
        if new_password != confirm_password:
            return RedirectResponse(url="/admin/credentials?error=password_tidak_cocok&mode=password", status_code=303)
        if len(new_password) < 6:
            return RedirectResponse(url="/admin/credentials?error=password_pendek&mode=password", status_code=303)
        user.password_hash = hash_password(new_password)
        user.password = new_password
    else:
        new_username = new_username.strip().lower()
        if not new_username:
            return RedirectResponse(url="/admin/credentials?error=username_kosong", status_code=303)
        if db.query(User).filter(func.lower(User.username) == new_username.lower(), User.id != user.id).first():
            return RedirectResponse(url="/admin/credentials?error=username_dipakai", status_code=303)
        # NOTE: Not renaming ax.fp.{old_username} here is intentional.
        # The admin does not login as a user and does not use XL API directly,
        # so the per-user fingerprint is irrelevant for the admin role.
        user.username = new_username
    db.add(user)
    db.commit()
    return RedirectResponse(url=f"/admin/credentials?updated=1&mode={mode}", status_code=303)


def _decoy_json_body(cfg: dict) -> dict:
    return {
        "label": cfg.get("label") or "",
        "family_name": cfg.get("family_name") or "",
        "family_code": cfg.get("family_code") or "",
        "is_enterprise": bool(cfg.get("is_enterprise")),
        "migration_type": cfg.get("migration_type") or "NONE",
        "variant_code": cfg.get("variant_code") or "",
        "option_name": cfg.get("option_name") or "",
        "order": cfg.get("order") or 0,
        "price": cfg.get("price") or 0,
    }


def _list_decoys(payment_type: str) -> list[dict]:
    from app.service.decoy import list_decoy_names, load_decoy_config, decoy_label
    names = list_decoy_names(payment_type)
    if "default" not in names and load_decoy_config(payment_type) is not None:
        names.insert(0, "default")
    out = []
    for n in names:
        cfg = dict(load_decoy_config(payment_type, n) or {})
        cfg["name"] = n
        cfg["label"] = decoy_label(n, cfg)
        cfg["json_text"] = json.dumps(_decoy_json_body(cfg), indent=2, ensure_ascii=False)
        out.append(cfg)
    return out


def _decoy_file_entries() -> list[tuple[str, str]]:
    """(zip_path, file_path) untuk semua file decoy di decoy_data/ — backup
    ZIP membawa file apa adanya, bukan blob JSON."""
    from app.service.decoy import decoy_type_dir
    entries = []
    for ptype in ("qris", "balance"):
        dirpath = decoy_type_dir(ptype)
        if os.path.isdir(dirpath):
            for fn in sorted(os.listdir(dirpath)):
                if fn.endswith(".json"):
                    entries.append((f"decoys/{ptype}/{fn}", os.path.join(dirpath, fn)))
    return entries


def _wipe_decoys():
    """Hapus semua file decoy (qris & balance) + legacy decoy-default-*."""
    from app.service.decoy import decoy_type_dir, DECOY_DATA_DIR
    for ptype in ("qris", "balance"):
        dirpath = decoy_type_dir(ptype)
        if os.path.isdir(dirpath):
            for fn in os.listdir(dirpath):
                if fn.endswith(".json"):
                    try:
                        os.remove(os.path.join(dirpath, fn))
                    except OSError:
                        pass
        legacy = os.path.join(DECOY_DATA_DIR, f"decoy-default-{ptype}.json")
        if os.path.exists(legacy):
            try:
                os.remove(legacy)
            except OSError:
                pass


def _restore_decoys(decoys: dict) -> int:
    """Tulis config decoy dari backup ke decoy_data/. Return jumlah terpasang."""
    from app.service.decoy import save_decoy_config
    restored = 0
    for ptype in ("qris", "balance"):
        names = decoys.get(ptype)
        if not isinstance(names, dict):
            continue
        for name, cfg in names.items():
            name = str(name).strip()
            if not name or not re.fullmatch(r"[A-Za-z0-9_-]{1,60}", name):
                continue
            if not isinstance(cfg, dict):
                continue
            try:
                save_decoy_config(ptype, name, cfg)
                restored += 1
            except Exception as e:
                print(f"[restore-decoy] gagal tulis {ptype}/{name}: {e}")
    return restored


@app.get("/admin/decoys", response_class=HTMLResponse)
def admin_decoys_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    return render("admin/decoys.html", context={
        "request": request,
        "user": user,
        "decoys": {"qris": _list_decoys("qris"), "balance": _list_decoys("balance")},
        "error": request.query_params.get("error"),
        "updated": request.query_params.get("updated"),
    })


@app.get("/admin/decoys/form", response_class=HTMLResponse)
def admin_decoy_form_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    ptype = request.query_params.get("type", "")
    if ptype not in ("qris", "balance"):
        return RedirectResponse(url="/admin/decoys", status_code=303)
    mode = request.query_params.get("mode", "new")
    original = request.query_params.get("name", "")
    default_text = json.dumps(_decoy_json_body({}), indent=2, ensure_ascii=False)
    if mode == "edit":
        from app.service.decoy import load_decoy_config, decoy_label
        cfg = load_decoy_config(ptype, original) or {}
        if not cfg:
            return RedirectResponse(url="/admin/decoys", status_code=303)
        label = decoy_label(original, cfg)
        data_text = json.dumps(_decoy_json_body(cfg), indent=2, ensure_ascii=False)
    else:
        cfg = {}
        label = ""
        data_text = default_text
    return render("admin/decoy_form.html", context={
        "request": request,
        "user": user,
        "mode": "edit" if mode == "edit" else "new",
        "type": ptype,
        "original": original if mode == "edit" else "",
        "label": label,
        "data_text": data_text,
    })


@app.post("/admin/decoys/save")
def admin_decoy_save(
    payment_type: str = Form(...),
    name: str = Form(""),
    label: str = Form(""),
    data: str = Form(""),
    user: User = Depends(get_current_user),
):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    if payment_type not in ("qris", "balance"):
        return RedirectResponse(url="/admin/decoys", status_code=303)
    def _to_int(v) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0
    try:
        cfg = json.loads(data or "{}")
    except ValueError:
        return RedirectResponse(url="/admin/decoys?error=json", status_code=303)
    if not isinstance(cfg, dict):
        return RedirectResponse(url="/admin/decoys?error=json", status_code=303)
    config = {
        "label": label.strip()[:60],
        "family_name": str(cfg.get("family_name") or "").strip()[:200],
        "family_code": str(cfg.get("family_code") or "").strip()[:64],
        "is_enterprise": bool(cfg.get("is_enterprise")),
        "migration_type": str(cfg.get("migration_type") or "NONE").strip()[:20] or "NONE",
        "variant_code": str(cfg.get("variant_code") or "").strip()[:64],
        "option_name": str(cfg.get("option_name") or "").strip()[:200],
        "order": _to_int(cfg.get("order")),
        "price": _to_int(cfg.get("price")),
    }
    if not config["family_code"] or not config["variant_code"]:
        return RedirectResponse(url="/admin/decoys?error=field", status_code=303)
    from app.service.decoy import save_decoy_config, delete_decoy_config, decoy_slug
    def _clean(v: str) -> str:
        return re.sub(r"[^a-zA-Z0-9 _-]", "", (v or "").strip())[:60]
    raw_name = _clean(name)
    label = label.strip()[:60]
    if not label:
        return RedirectResponse(url="/admin/decoys?error=name", status_code=303)
    slug = decoy_slug(label)
    if not slug:
        return RedirectResponse(url="/admin/decoys?error=name", status_code=303)
    if raw_name and slug != raw_name:
        delete_decoy_config(payment_type, raw_name)
    save_decoy_config(payment_type, slug, config)
    return RedirectResponse(url="/admin/decoys?updated=1", status_code=303)


@app.post("/admin/decoys/delete")
def admin_decoy_delete(
    payment_type: str = Form(...),
    name: str = Form(...),
    user: User = Depends(get_current_user),
):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    if payment_type not in ("qris", "balance"):
        return RedirectResponse(url="/admin/decoys", status_code=303)
    from app.service.decoy import delete_decoy_config
    delete_decoy_config(payment_type, re.sub(r"[^a-zA-Z0-9 _-]", "", (name or "")).strip()[:60])
    return RedirectResponse(url="/admin/decoys?updated=1", status_code=303)


@app.get("/prices-xl", response_class=HTMLResponse)
def admin_prices_xl_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    db = next(get_db())
    try:
        overrides = db.query(PackagePrice).all()
    finally:
        db.close()
    ov_map = {}
    for ov in overrides:
        ov_map[(ov.family_key, ov.option_number)] = ov
    reg = _family_registry()
    fees = _get_all_family_fees()
    rows = []
    for fam, cfg in reg.items():
        row = {"key": fam, "label": cfg["label"], "pkgs": []}
        # Tabel paket KOSONG by default — baris = option codes yang ditambahkan
        # admin (+ override orphan). Nama paket dari snapshot (hasil Browse
        # Package) bila tersedia.
        snap = {it.get("number"): it for it in _load_catalog_snapshot(fam) if isinstance(it, dict)}
        nums = set(cfg["option_codes"]) | {n for (fk, n) in ov_map if fk == fam}
        for i, num in enumerate(sorted(nums), start=1):
            ov = ov_map.get((fam, num))
            info = snap.get(num) or {}
            label = info.get("name") or " ".join(x for x in (info.get("label"), info.get("size")) if x)
            name = label or f"Option #{num}"
            api_price = None
            if info.get("price") is not None:
                api_price = _fmt_harga(info["price"])
            if num not in snap:
                name += " (belum ada di snapshot — Browse dulu)"
            row["pkgs"].append({
                "number": num,
                "name": name,
                "api_price": api_price,
                "display": ov.display_price if ov else None,
                "rewrite": ov.rewrite_price if ov else None,
                "decoy_qris": (ov.decoy_qris or "") if ov else "",
                "decoy_pulsa": (ov.decoy_pulsa or "") if ov else "",
                "fee_pulsa": (ov.fee_pulsa if ov and ov.fee_pulsa is not None else fees.get(_fee_key(fam, "balance"), 0)),
                "fee_qris": (ov.fee_qris if ov and ov.fee_qris is not None else fees.get(_fee_key(fam, "qris"), 0)),
            })
        rows.append(row)
    admin_sess = _admin_xl_read()
    admin_acct_groups = []
    db2 = next(get_db())
    try:
        for u in db2.query(User).filter(User.role == "user").order_by(User.username).all():
            accts = [a for a in u.xl_accounts if a.phone_number]
            if accts:
                admin_acct_groups.append({
                    "username": u.username,
                    "accounts": [{
                        "id": a.id,
                        "label": a.label or f"Nomor {a.phone_number}",
                        "phone_number": a.phone_number,
                    } for a in accts],
                })
    finally:
        db2.close()
    return render("admin/prices_xl.html", context={
        "request": request,
        "user": user,
        "rows": rows,
        "admin_acct_groups": admin_acct_groups,
        "admin_active_account_id": (admin_sess or {}).get("account_id"),
        "family_codes": {k: v["family_code"] for k, v in reg.items()},
        "family_prefixes": {k: v["url_prefix"] for k, v in reg.items()},
        "family_options": {k: ",".join(str(x) for x in v["option_codes"]) for k, v in reg.items()},
        "family_actives": {k: v["is_active"] for k, v in reg.items()},
        "decoy_names_qris": [{"name": d["name"], "label": d["label"]} for d in _list_decoys("qris")],
        "decoy_names_pulsa": [{"name": d["name"], "label": d["label"]} for d in _list_decoys("balance")],
        "admin_xl_label": admin_sess.get("label") if admin_sess else None,
        "admin_xl_phone": admin_sess.get("phone_number") if admin_sess else None,
    })


@app.get("/prices-xl-custom", response_class=HTMLResponse)
def admin_prices_xl_custom_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    return render("admin/prices_xl_custom.html", context={
        "request": request,
        "user": user,
        "custom_buy": _custom_buy_read(),
    })


@app.post("/prices-xl/set")
def admin_prices_xl_set(
    family_key: str = Form(...),
    option_number: int = Form(...),
    old_option_number: int = Form(-1),
    display_price: str = Form(""),
    rewrite_price: str = Form(""),
    decoy_qris: str = Form(""),
    decoy_pulsa: str = Form(""),
    fee_pulsa: str = Form(""),
    fee_qris: str = Form(""),
    user: User = Depends(get_current_user),
):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    if family_key not in _family_registry() or option_number < 0:
        return RedirectResponse(url="/prices-xl", status_code=303)
    if family_key == "xcp" and option_number < 1:
        return RedirectResponse(url="/prices-xl", status_code=303)
    try:
        display = int(display_price) if display_price.strip() else None
        rewrite = int(rewrite_price) if rewrite_price.strip() else None
        fee_p = int(fee_pulsa) if fee_pulsa.strip() else None
        fee_q = int(fee_qris) if fee_qris.strip() else None
    except ValueError:
        return RedirectResponse(url="/prices-xl", status_code=303)
    if (display is not None and display < 0) or (rewrite is not None and rewrite < 0):
        return RedirectResponse(url="/prices-xl", status_code=303)
    if (fee_p is not None and fee_p < 0) or (fee_q is not None and fee_q < 0):
        return RedirectResponse(url="/prices-xl", status_code=303)
    def _clean_decoy(v: str, known: list) -> str:
        v = re.sub(r"[^a-zA-Z0-9 _-]", "", (v or "").strip())[:60]
        if v in ("none", ""):
            return ""
        if known and v not in known:
            return ""
        return v
    decoy_qris = _clean_decoy(decoy_qris, [d["name"] for d in _list_decoys("qris")])
    decoy_pulsa = _clean_decoy(decoy_pulsa, [d["name"] for d in _list_decoys("balance")])
    # XCP: memindahkan baris Alternatif (old ada di posisi tersimpan) =
    # mengubah posisi katalog alt tersebut; override ikut karena key-nya nomor
    # posisi. Orphan (old di luar posisi) cukup dinomori ulang — kalau nomor
    # barunya adalah posisi alt, override mengadopsi slot alt itu.
    if family_key == "xcp" and 0 <= old_option_number != option_number and old_option_number in _xcp_positions():
        if option_number in _xcp_positions():
            return RedirectResponse(url="/prices-xl", status_code=303)
        _save_xcp_positions(tuple(
            option_number if p == old_option_number else p for p in _xcp_positions()
        ))
    db = next(get_db())
    try:
        # Pindah posisi override (form urutan addon): old != new → pindahkan.
        if 0 <= old_option_number != option_number:
            src = db.query(PackagePrice).filter(
                PackagePrice.family_key == family_key,
                PackagePrice.option_number == old_option_number,
            ).first()
            if src:
                if (display is None and rewrite is None
                        and not decoy_qris and not decoy_pulsa
                        and fee_p is None and fee_q is None):
                    db.delete(src)
                else:
                    dst = db.query(PackagePrice).filter(
                        PackagePrice.family_key == family_key,
                        PackagePrice.option_number == option_number,
                    ).first()
                    if dst and dst.id != src.id:
                        db.delete(dst)
                        # flush agar DELETE dieksekusi sebelum UPDATE src
                        # menempati nomor yang sama (unique constraint)
                        db.flush()
                    src.option_number = option_number
                    src.display_price = display
                    src.rewrite_price = rewrite
                    src.decoy_qris = decoy_qris
                    src.decoy_pulsa = decoy_pulsa
                    src.fee_pulsa = fee_p
                    src.fee_qris = fee_q
                db.commit()
                return RedirectResponse(url="/prices-xl", status_code=303)
            # src tidak ada → jatuh ke logika upsert normal di bawah.
        row = db.query(PackagePrice).filter(
            PackagePrice.family_key == family_key,
            PackagePrice.option_number == option_number,
        ).first()
        if (display is None and rewrite is None
                and not decoy_qris and not decoy_pulsa
                and fee_p is None and fee_q is None):
            if row:
                db.delete(row)
        else:
            if not row:
                row = PackagePrice(family_key=family_key, option_number=option_number)
                db.add(row)
            row.display_price = display
            row.rewrite_price = rewrite
            row.decoy_qris = decoy_qris
            row.decoy_pulsa = decoy_pulsa
            row.fee_pulsa = fee_p
            row.fee_qris = fee_q
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/prices-xl", status_code=303)


@app.post("/prices-xl/family")
def admin_prices_xl_family(
    family_key: str = Form(""),
    label: str = Form(...),
    family_code: str = Form(""),
    url_prefix: str = Form(""),
    option_codes: str = Form(""),
    qris_decoy: str = Form(""),
    user: User = Depends(get_current_user),
):
    """Manage family group di registry.

    - Tanpa family_key = TAMBAH group baru: family_key & url_prefix
      di-generate otomatis (random short id), jadi admin tak
      perlu mengisi slug/prefix yang membingungkan.
    - Dengan family_key = EDIT family yang sudah ada (label, family code,
      url prefix, option codes).

    Label & family code diisi manual; halaman beli-paket otomatis merender
    family_key baru sebagai container baru.
    """
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    label = label.strip()[:100]
    family_code = family_code.strip()[:64]
    option_codes = re.sub(r"[^0-9,]", "", option_codes.strip())[:255]
    if not label:
        return RedirectResponse(url="/prices-xl", status_code=303)
    reg = _family_registry()
    family_key = family_key.strip().lower()[:20]
    editing = bool(family_key)
    if editing:
        if family_key not in reg:
            return RedirectResponse(url="/prices-xl", status_code=303)
        if not re.fullmatch(r"[a-z0-9_]+", family_key):
            return RedirectResponse(url="/prices-xl", status_code=303)
        prefix = re.sub(r"[^a-z0-9-]", "", url_prefix.strip().lower())[:40] or family_key
        existing = {k: v["url_prefix"] for k, v in reg.items()}
        if existing.get(family_key, prefix) != prefix and prefix in existing.values():
            return RedirectResponse(url="/prices-xl", status_code=303)
    else:
        if not family_code:
            return RedirectResponse(url="/prices-xl", status_code=303)
        family_key = "fam-" + uuid.uuid4().hex[:8]
        prefix = family_key
    db = next(get_db())
    try:
        row = db.query(XlFamily).filter(XlFamily.family_key == family_key).first()
        if not row:
            max_sort = db.query(func.coalesce(func.max(XlFamily.sort_order), -1)).scalar()
            row = XlFamily(family_key=family_key, sort_order=(max_sort or 0) + 1)
            db.add(row)
        row.label = label
        row.family_code = family_code
        row.url_prefix = prefix
        row.option_codes = option_codes
        row.qris_decoy = qris_decoy.strip() in ("1", "true", "on")
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/prices-xl", status_code=303)


@app.post("/prices-xl/family/toggle")
@app.get("/prices-xl/family/toggle")
def admin_prices_xl_family_toggle(
    request: Request,
    user: User = Depends(get_current_user),
):
    """Tampil/sembunyikan family di halaman beli-paket tanpa menghapusnya."""
    family_key = (request.query_params.get("key") or "").strip()
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    if not family_key:
        return RedirectResponse(url="/prices-xl", status_code=303)
    db = next(get_db())
    try:
        row = db.query(XlFamily).filter(XlFamily.family_key == family_key).first()
        if row:
            row.is_active = not row.is_active
            db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/prices-xl", status_code=303)


@app.post("/prices-xl/custom/save")
def admin_prices_xl_custom_save(
    label: str = Form(""),
    family_code: str = Form(""),
    user: User = Depends(get_current_user),
):
    """Simpan label tampilan group permanen Beli Paket Custom. family_code
    diisi hanya untuk backward-compat (mengganti seluruh pin lama dengan satu).
    """
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    cur = _custom_buy_read()
    pins = [family_code] if family_code else (cur.get("pins") or [])
    _custom_buy_write(label, pins)
    return RedirectResponse(url="/prices-xl-custom", status_code=303)


@app.post("/prices-xl/custom/pin/add")
def admin_prices_xl_custom_pin_add(
    label: str = Form(""),
    family_code: str = Form(""),
    user: User = Depends(get_current_user),
):
    """Pin family code baru dengan label opsional — user bisa browse paket
    keluarga ini tanpa mengetik kodenya (kode pin tidak pernah ditampilkan)."""
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    fc = _valid_custom_family_code(family_code)
    if not fc:
        return RedirectResponse(url="/prices-xl-custom?err=pin", status_code=303)
    cur = _custom_buy_read()
    pins = cur.get("pins") or []
    if fc not in {p["family_code"] for p in pins}:
        pins = pins + [{
            "label": str(label or "").strip()[:100] or cur.get("label") or CUSTOM_BUY_LABEL_DEFAULT,
            "family_code": fc,
        }]
    _custom_buy_write(cur.get("label") or CUSTOM_BUY_LABEL_DEFAULT, pins)
    return RedirectResponse(url="/prices-xl-custom", status_code=303)


@app.post("/prices-xl/custom/pin/delete")
def admin_prices_xl_custom_pin_delete(
    pin_index: int = Form(...),
    user: User = Depends(get_current_user),
):
    """Unpin (hapus) satu family code pin."""
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    cur = _custom_buy_read()
    pins = cur.get("pins") or []
    if 1 <= pin_index <= len(pins):
        pins = [p for i, p in enumerate(pins) if i + 1 != pin_index]
        _custom_buy_write(cur.get("label") or CUSTOM_BUY_LABEL_DEFAULT, pins)
    return RedirectResponse(url="/prices-xl-custom", status_code=303)


@app.post("/prices-xl/family/delete")
def admin_prices_xl_family_delete(
    family_key: str = Form(...),
    user: User = Depends(get_current_user),
):
    """Hapus satu group paket beserta semua turunannya (override harga/decoy,
    fee paket, snapshot katalog)."""
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    family_key = family_key.strip().lower()[:20]
    if not family_key or family_key not in _family_registry():
        return RedirectResponse(url="/prices-xl", status_code=303)
    db = next(get_db())
    try:
        db.query(XlFamily).filter(XlFamily.family_key == family_key).delete()
        db.query(PackagePrice).filter(PackagePrice.family_key == family_key).delete()
        db.query(FamilyFee).filter(FamilyFee.family_key.in_(
            (_fee_key(family_key, "balance"), _fee_key(family_key, "qris"))
        )).delete(synchronize_session=False)
        db.commit()
    finally:
        db.close()
    try:
        snap = _read_snapshot_file()
        if family_key in snap:
            del snap[family_key]
            _write_snapshot_file(snap)
    except Exception as e:
        print(f"[family-delete] snapshot cleanup gagal: {e}")
    return RedirectResponse(url="/prices-xl", status_code=303)


@app.post("/prices-xl/catalog/fetch")
def admin_prices_xl_catalog_fetch(user: User = Depends(get_current_user)):
    """Muat katalog semua family dari API XL pakai sesi admin (admin login XL).

    Refresh token XL berotasi per refresh dan hanya berlaku sekali — kalau
    dua fetch jalan barengan, yang kedua gagal. Serialisasi dengan lock.
    """
    if user.role != "admin":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    if not _admin_xl_read():
        return JSONResponse({"ok": False, "message": "Belum ada sesi XL dipilih. Pilih pengguna dulu."})
    with _catalog_fetch_lock:
        ok, msg = _admin_xl_fetch_catalog()
    return JSONResponse({"ok": ok, "message": msg})


@app.get("/prices-xl/family/{family_key}/browse")
def admin_prices_xl_family_browse_page(request: Request, family_key: str, user: User = Depends(get_current_user)):
    """Halaman Browse Package: isi katalog satu family langsung dari API XL
    (option #, nama, GB, harga API) via sesi admin. Sebelum browse, nama paket
    diambil dari snapshot bila tersedia."""
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    cfg = _family_registry().get(family_key)
    if not cfg:
        return RedirectResponse(url="/prices-xl", status_code=303)
    rows = None
    note = ""
    error = None
    if _admin_xl_read():
        with _catalog_fetch_lock:
            tokens = _admin_xl_tokens()
        if not tokens:
            _admin_xl_clear()
            error = "Sesi XL kedaluwarsa. Pilih ulang pengguna & nomornya."
        else:
            fam_code = cfg["family_code"]
            is_ent, mig = _family_api_params(fam_code)
            _api_delay()
            try:
                data = xl_get_family(API_KEY, tokens, fam_code, is_enterprise=is_ent, migration_type=mig)
            except Exception as e:
                data = None
                error = f"Gagal fetch katalog: {e}"
            if data:
                if fam_code == FAMILY_CODE_XTRA_COMBO:
                    _save_xcp_catalog(data)
                items = _build_registry_items(data, cfg["option_codes"])
                total = sum(len(v.get("package_options") or []) for v in (data.get("package_variants") or []))
                rows = [
                    {"number": it["number"],
                     "name": " ".join(x for x in (it.get("label"), it.get("size")) if x) or it.get("name") or "-",
                     "price": f"{it['price']:,}".replace(",", ".") if it.get("price") is not None else "-"}
                    for it in items
                ]
                if cfg["option_codes"] and total > len(items):
                    note = f"Menampilkan {len(items)} opsi terpilih dari {total} opsi katalog. Kosongkan Option codes di halaman Atur Paket XL untuk menampilkan semua."
            elif not error:
                error = "Katalog kosong / tidak ditemukan. Cek family code."
    else:
        error = "Belum ada sesi XL dipilih."
    selected = set(cfg["option_codes"])
    snap = {it.get("number"): it for it in _load_catalog_snapshot(family_key) if isinstance(it, dict)}
    return render("admin/browse_family.html", context={
        "request": request,
        "user": user,
        "fam_key": family_key,
        "label": cfg["label"],
        "rows": rows,
        "snap_rows": snap,
        "selected": selected,
        "note": note,
        "error": error,
        "admin_xl_label": (_admin_xl_read() or {}).get("label"),
        "admin_xl_phone": (_admin_xl_read() or {}).get("phone_number"),
    })


@app.post("/prices-xl/pkg/add")
def admin_prices_xl_pkg_add(
    family_key: str = Form(...),
    option_number: int = Form(...),
    display_price: str = Form(""),
    rewrite_price: str = Form(""),
    user: User = Depends(get_current_user),
):
    """Tambah paket ke group: pakai option code dari hasil Browse. Kode
    ditambahkan ke daftar tampil (option_codes) + override bila diisi."""
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    if family_key not in _family_registry() or option_number < 0:
        return RedirectResponse(url="/prices-xl", status_code=303)
    try:
        display = int(display_price) if display_price.strip() else None
        rewrite = int(rewrite_price) if rewrite_price.strip() else None
    except ValueError:
        return RedirectResponse(url="/prices-xl", status_code=303)
    if (display is not None and display < 0) or (rewrite is not None and rewrite < 0):
        return RedirectResponse(url="/prices-xl", status_code=303)
    _family_codes_add(family_key, option_number)
    if display is not None or rewrite is not None:
        db = next(get_db())
        try:
            row = db.query(PackagePrice).filter(
                PackagePrice.family_key == family_key,
                PackagePrice.option_number == option_number,
            ).first()
            if not row:
                row = PackagePrice(family_key=family_key, option_number=option_number)
                db.add(row)
            row.display_price = display
            row.rewrite_price = rewrite
            db.commit()
        finally:
            db.close()
    return RedirectResponse(url="/prices-xl", status_code=303)


@app.post("/prices-xl/pkg/delete")
def admin_prices_xl_pkg_delete(
    family_key: str = Form(...),
    option_number: int = Form(...),
    user: User = Depends(get_current_user),
):
    """Hapus paket dari group: buang override-nya dan keluarkan option code
    dari daftar tampil (option_codes)."""
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    db = next(get_db())
    try:
        row = db.query(PackagePrice).filter(
            PackagePrice.family_key == family_key,
            PackagePrice.option_number == option_number,
        ).first()
        if row:
            db.delete(row)
        _family_codes_remove(family_key, option_number)
        db.commit()
    finally:
        db.close()
    return RedirectResponse(url="/prices-xl", status_code=303)


def _family_codes_add(family_key, number):
    """Tambah nomor ke option_codes family (dedupe, urut)."""
    reg = _family_registry()
    cfg = reg.get(family_key)
    if not cfg:
        return
    codes = cfg["option_codes"]
    if number in codes:
        return
    codes = sorted(codes + [number])
    db = next(get_db())
    try:
        row = db.query(XlFamily).filter(XlFamily.family_key == family_key).first()
        if row:
            row.option_codes = ",".join(str(c) for c in codes)
            db.commit()
    finally:
        db.close()


def _family_codes_remove(family_key, number):
    """Keluarkan nomor dari option_codes family."""
    reg = _family_registry()
    cfg = reg.get(family_key)
    if not cfg or number not in cfg["option_codes"]:
        return
    codes = [c for c in cfg["option_codes"] if c != number]
    db = next(get_db())
    try:
        row = db.query(XlFamily).filter(XlFamily.family_key == family_key).first()
        if row:
            row.option_codes = ",".join(str(c) for c in codes)
            db.commit()
    finally:
        db.close()


# ─── Admin pilih sesi XL user (untuk fetch katalog) ─────────────────────────

@app.get("/prices-xl/login-xl", response_class=HTMLResponse)
def admin_prices_xl_login_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    sess = _admin_xl_read()
    accounts = []
    db = next(get_db())
    try:
        users = db.query(User).filter(User.role == "user").order_by(User.username).all()
        for u in users:
            accts = [a for a in u.xl_accounts if a.phone_number]
            if not accts:
                continue
            accounts.append({
                "username": u.username,
                "accounts": [{
                    "id": a.id,
                    "label": a.label or f"Nomor {a.phone_number}",
                    "phone_number": a.phone_number,
                    "is_active": a.is_active,
                    "has_token": bool(a.refresh_token),
                } for a in accts],
            })
    finally:
        db.close()
    return render("admin/login_xl.html", context={
        "request": request,
        "user": user,
        "accounts": accounts,
        "admin_xl_label": (sess or {}).get("label"),
        "admin_xl_phone": (sess or {}).get("phone_number"),
    })


@app.post("/prices-xl/login-xl/select")
def admin_prices_xl_login_select(
    account_id: int = Form(...),
    user: User = Depends(get_current_user),
):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    db = next(get_db())
    try:
        acct = db.query(XLAccount).filter(XLAccount.id == account_id).first()
        if not acct:
            return RedirectResponse(url="/prices-xl/login-xl", status_code=303)
        _admin_xl_write({
            "account_id": acct.id,
            "phone_number": acct.phone_number,
            "label": acct.label,
        })
    finally:
        db.close()
    return RedirectResponse(url="/prices-xl", status_code=303)


@app.post("/prices-xl/login-xl/logout")
def admin_prices_xl_logout(user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    _admin_xl_clear()
    return RedirectResponse(url="/prices-xl/login-xl", status_code=303)


@app.post("/admin/balance/add")
def admin_add_balance(
    user_id: int = Form(...),
    amount: int = Form(...),
    description: str = Form(""),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Jumlah harus positif")
    if not db.query(User).filter(User.id == user_id, User.role == "user").first():
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    with _balance_lock:
        bal = db.query(Balance).filter(Balance.user_id == user_id).first()
        if not bal:
            bal = Balance(user_id=user_id, balance=0)
            db.add(bal)
        bal.balance += amount

        trx = BalanceTransaction(
            user_id=user_id,
            amount=amount,
            type="topup",
            description=description or "Topup dari admin"
        )
        db.add(trx)
        db.commit()
    if _ab_read_state().get("notif_topup_admin"):
        u = db.query(User).filter(User.id == user_id).first()
        uname = u.username if u else f"id {user_id}"
        _notify(
            "🟢  " + _tg_bold("PENYESUAIAN SALDO") + "\n\n"
            "<blockquote>"
            + (
                _tg_field("User", _tg_esc(uname))
                + _tg_field("Nominal", f"+{_fmt_thousand(amount)} IDR")
                + _tg_field("Metode", "Admin")
            ).rstrip()
            + "</blockquote>\n"
            "<blockquote>"
            + _tg_field("Saldo", f"{_fmt_thousand(bal.balance)} IDR").rstrip()
            + "</blockquote>\n"
            "<blockquote>"
            + _tg_time_footer()
            + "</blockquote>"
        )
    return RedirectResponse(url="/admin/users", status_code=303)


def _admin_username(db: Session) -> str:
    admin = db.query(User).filter(User.role == "admin").first()
    return admin.username if admin else "admin"


@app.post("/admin/users/delete")
def admin_delete_user(
    user_id: int = Form(...),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if user_id == admin_user.id:
        raise HTTPException(status_code=400, detail="Tidak bisa menghapus akun sendiri")

    u = db.query(User).filter(User.id == user_id).first()
    if not u:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")
    if u.role != "user":
        raise HTTPException(status_code=400, detail="Hanya akun pengguna biasa yang bisa dihapus")
    pending_topups = db.query(TopupTransaction).filter(
        TopupTransaction.user_id == u.id,
        TopupTransaction.status.in_(("waiting", "pending"))
    ).count()
    if pending_topups:
        raise HTTPException(
            status_code=400,
            detail="User masih punya topup QRIS menunggu pembayaran. Tunggu kedaluwarsa dulu (maks ~6 menit)."
        )

    for acc in db.query(XLAccount).filter(XLAccount.user_id == u.id).all():
        _XL_TOKEN_CACHE.pop(acc.subscriber_id or acc.id, None)
    db.query(BalanceTransaction).filter(BalanceTransaction.user_id == u.id).delete()
    db.query(TopupTransaction).filter(TopupTransaction.user_id == u.id).delete()
    remove_user_ax_fp(u.username)
    db.delete(u)
    # Double-check terakhir: create-topup paralel bisa saja menyisipkan baris
    # pending baru setelah pengecekan awal. SQLite menserialisasi writer,
    # jadi cek di transaksi yang sama dengan delete ini menutup hampir semua
    # jendela race.
    if db.query(TopupTransaction).filter(
        TopupTransaction.user_id == u.id,
        TopupTransaction.status.in_(("waiting", "pending"))
    ).count():
        db.rollback()
        raise HTTPException(
            status_code=400,
            detail="User masih punya topup QRIS menunggu pembayaran. Tunggu kedaluwarsa dulu (maks ~6 menit)."
        )
    db.commit()
    return RedirectResponse(url="/admin/users", status_code=303)


@app.post("/admin/users/login-as")
def admin_login_as_user(
    user_id: int = Form(...),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    u = db.query(User).filter(User.id == user_id).first()
    if not u:
        raise HTTPException(status_code=404, detail="User tidak ditemukan")
    if u.role != "user":
        raise HTTPException(status_code=400, detail="Hanya bisa login sebagai pengguna biasa")

    token = create_access_token({"sub": str(u.id), "role": u.role})
    resp = RedirectResponse(url="/user/dashboard", status_code=303)
    resp.set_cookie(key="access_token", value=token, httponly=True, samesite="lax",
                    max_age=int(ACCESS_TOKEN_EXPIRE_MINUTES * 60))
    return resp


@app.post("/admin/balance/decrease")
def admin_decrease_balance(
    user_id: int = Form(...),
    amount: int = Form(...),
    description: str = Form(""),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if amount <= 0:
        raise HTTPException(status_code=400, detail="Jumlah harus positif")
    if not db.query(User).filter(User.id == user_id, User.role == "user").first():
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    with _balance_lock:
        bal = db.query(Balance).filter(Balance.user_id == user_id).first()
        if not bal or bal.balance <= 0:
            raise HTTPException(status_code=400, detail="Saldo tidak cukup")
        if amount > bal.balance:
            raise HTTPException(status_code=400, detail="Saldo tidak cukup")

        bal.balance -= amount
        db.add(BalanceTransaction(
            user_id=user_id,
            amount=-amount,
            type="decrease",
            description=description or "Kurangi oleh admin"
        ))
        db.commit()
    if _ab_read_state().get("notif_topup_admin"):
        u = db.query(User).filter(User.id == user_id).first()
        uname = u.username if u else f"id {user_id}"
        _notify(
            "🟢  " + _tg_bold("PENYESUAIAN SALDO") + "\n\n"
            "<blockquote>"
            + (
                _tg_field("User", _tg_esc(uname))
                + _tg_field("Nominal", f"-{_fmt_thousand(amount)} IDR")
                + _tg_field("Metode", "Admin")
            ).rstrip()
            + "</blockquote>\n"
            "<blockquote>"
            + _tg_field("Saldo", f"{_fmt_thousand(bal.balance)} IDR").rstrip()
            + "</blockquote>\n"
            "<blockquote>"
            + _tg_time_footer()
            + "</blockquote>"
        )
    return RedirectResponse(url="/admin/users", status_code=303)


@app.post("/admin/balance/set")
def admin_set_balance(
    user_id: int = Form(...),
    amount: int = Form(...),
    description: str = Form(""),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if amount < 0:
        raise HTTPException(status_code=400, detail="Jumlah tidak boleh negatif")
    if not db.query(User).filter(User.id == user_id, User.role == "user").first():
        raise HTTPException(status_code=404, detail="User tidak ditemukan")

    with _balance_lock:
        bal = db.query(Balance).filter(Balance.user_id == user_id).first()
        if not bal:
            bal = Balance(user_id=user_id, balance=0)
            db.add(bal)
        delta = amount - bal.balance
        bal.balance = amount
        if delta != 0:
            db.add(BalanceTransaction(
                user_id=user_id,
                amount=delta,
                type="set",
                description=description or "Penyesuaian saldo oleh admin"
            ))
        db.commit()
    if delta != 0 and _ab_read_state().get("notif_topup_admin"):
        u = db.query(User).filter(User.id == user_id).first()
        uname = u.username if u else f"id {user_id}"
        sign = "+" if delta > 0 else "-"
        _notify(
            "🟢  " + _tg_bold("PENYESUAIAN SALDO") + "\n\n"
            "<blockquote>"
            + (
                _tg_field("User", _tg_esc(uname))
                + _tg_field("Nominal", f"{sign}{_fmt_thousand(abs(delta))} IDR")
                + _tg_field("Metode", "Admin")
            ).rstrip()
            + "</blockquote>\n"
            "<blockquote>"
            + _tg_field("Saldo", f"{_fmt_thousand(bal.balance)} IDR").rstrip()
            + "</blockquote>\n"
            "<blockquote>"
            + _tg_time_footer()
            + "</blockquote>"
        )
    return RedirectResponse(url="/admin/users", status_code=303)


# ─── Admin Backup ───────────────────────────────────────────────────────────

@app.post("/admin/penghasilan/hapus")
def admin_penghasilan_hapus(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    db = next(get_db())
    db.query(BalanceTransaction).delete()
    db.commit()
    db.close()
    return RedirectResponse(url="/admin/penghasilan", status_code=303)


def _income_range_start(period: str):
    now_wib = datetime.now(WIB)
    if period == "today":
        start = now_wib.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        start = (now_wib - timedelta(days=now_wib.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
    elif period == "month":
        start = now_wib.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    else:
        start = now_wib - timedelta(days=3650)
    return start.astimezone(timezone.utc).replace(tzinfo=None)


@app.get("/admin/penghasilan", response_class=HTMLResponse)
def admin_penghasilan(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    db = next(get_db())
    metric = request.query_params.get("metric", "topup")
    if metric not in ("topup", "used"):
        metric = "topup"
    period = request.query_params.get("range", "today")
    trx_type = "topup" if metric == "topup" else "purchase"
    start = _income_range_start(period)
    rows = db.query(BalanceTransaction).filter(
        BalanceTransaction.type == trx_type,
        BalanceTransaction.created_at >= start,
    ).order_by(BalanceTransaction.created_at.desc()).all()

    # For topups, the real income is what the user PAID (base amount + unique
    # code fee). The ledger only stores the credited base, so match each row
    # to its TopupTransaction (written in the same commit, timestamps within
    # seconds) and add the fee back.
    paid_by_user = {}
    if metric == "topup":
        for t in db.query(TopupTransaction).filter(
            TopupTransaction.status == "paid",
            TopupTransaction.paid_at.isnot(None),
        ).all():
            paid_at = t.paid_at.replace(tzinfo=None) if t.paid_at.tzinfo else t.paid_at
            paid_by_user.setdefault(t.user_id, []).append((t, paid_at))

    def _match_fee(r):
        if not r.created_at:
            return 0
        best_diff = None
        fee = 0
        for t, paid_at in paid_by_user.get(r.user_id, []):
            diff = abs((paid_at - r.created_at).total_seconds())
            if diff <= 15 and (best_diff is None or diff < best_diff):
                best_diff = diff
                fee = t.fee or 0
        return fee

    total = 0
    details = []
    for r in rows:
        u = db.query(User).filter(User.id == r.user_id).first()
        amount = abs(r.amount) + (_match_fee(r) if metric == "topup" else 0)
        total += amount
        details.append({
            "ts": _fmt_wib(r.created_at) if r.created_at else "—",
            "username": u.username if u else f"user #{r.user_id}",
            "amount": amount,
        })
    db.close()
    return render("admin/penghasilan.html", context={
        "request": request, "user": user,
        "metric": metric,
        "period": period,
        "total": total,
        "details": details,
        "INCOME_METRICS": [("topup", "Total Topup"), ("used", "Total Dipakai")],
        "INCOME_RANGES": [("today", "Hari ini"), ("week", "Minggu ini"), ("month", "Bulan ini")],
    })


# ─── Admin: Pengaturan Pembayaran (QRIS gateway) ───────────────────────────


@app.get("/admin/payment")
def admin_payment_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    url, api_key = gopay.get_config()
    return render("admin/payment.html", context={
        "request": request,
        "user": user,
        "gateway_url": url,
        "gateway_api_key": api_key,
        "gopay_ready": bool(url and api_key),
    })


@app.post("/admin/payment/save")
def admin_payment_save(
    request: Request,
    endpoint: str = Form(""),
    api_key: str = Form(""),
    user: User = Depends(get_current_user),
):
    if user.role != "admin":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    endpoint = endpoint.strip()
    if endpoint and not endpoint.startswith(("http://", "https://")):
        return JSONResponse({"ok": False, "message": "Endpoint harus diawali http:// atau https://"}, status_code=400)
    try:
        gopay.set_config(endpoint, api_key)
    except Exception as e:
        return JSONResponse({"ok": False, "message": f"Gagal menyimpan: {e}"}, status_code=500)
    if not endpoint or not api_key.strip():
        msg = "Pengaturan disimpan. Gateway memakai fallback .env untuk nilai yang dikosongkan."
    else:
        msg = "Pengaturan pembayaran tersimpan."
    return JSONResponse({"ok": True, "message": msg})


@app.post("/admin/payment/test")
def admin_payment_test(
    request: Request,
    mode: str = Form(...),
    endpoint: str = Form(""),
    api_key: str = Form(""),
    user: User = Depends(get_current_user),
):
    if user.role != "admin":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    endpoint = endpoint.strip().rstrip("/")
    if not endpoint or not api_key.strip():
        return JSONResponse({"ok": False, "message": "Isi endpoint dan API key terlebih dahulu."})
    if not endpoint.startswith(("http://", "https://")):
        return JSONResponse({"ok": False, "message": "Endpoint harus diawali http:// atau https://"})

    res = gopay.token_status(endpoint, api_key)

    if res["http_status"] == 0:
        return JSONResponse({"ok": False, "message": res["error"]})
    if res["http_status"] in (401, 403):
        return JSONResponse({"ok": False, "message": "Endpoint terjangkau, tetapi API key ditolak gateway."})
    if res["http_status"] != 200 or not isinstance(res["data"], dict):
        return JSONResponse({
            "ok": False,
            "message": f"Endpoint merespons dengan HTTP {res['http_status']}, bukan respon gateway yang valid."
        })

    data = res["data"] or {}
    if mode == "token":
        info = data.get("data") or {}
        status_val = str(info.get("token_status") or "").lower()
        gw_msg = info.get("message") or ""
        if data.get("success") and status_val == "valid":
            return JSONResponse({
                "ok": True,
                "message": f"Token GoPay Merchant aktif.{(' ' + str(gw_msg)) if gw_msg else ''}"
            })
        return JSONResponse({
            "ok": False,
            "message": f"Token GoPay Merchant tidak valid / sesi hangus.{((' ' + str(gw_msg)) if gw_msg else '')} Jalankan 'node login.js' ulang di server gateway."
        })

    # mode == "api": reachability + API key accepted
    if data.get("success"):
        return JSONResponse({"ok": True, "message": "API gateway berjalan dan API key diterima."})
    # Gateway menaruh alasan gagal di beberapa tempat tergantung versinya:
    # top-level error/detail, data.message, atau data.data.token_status/message.
    inner = data.get("data") if isinstance(data.get("data"), dict) else {}
    err = (
        data.get("error")
        or data.get("detail")
        or inner.get("message")
        or inner.get("token_status")
        or "Respon tidak dikenal"
    )
    return JSONResponse({"ok": False, "message": f"Gateway merespons tapi gagal: {err}"})


@app.get("/admin/backup")
def admin_backup(admin_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    admin = db.query(User).filter(User.role == "admin").first()
    users = db.query(User).filter(User.role == "user").all()
    admin_data = {
        "username": admin.username if admin else "",
        "password": (admin.password or admin.password_hash) if admin else "",
        "email": admin.email if admin else "",
    }
    users_data = []
    xl_data = []
    for u in users:
        bal = db.query(Balance).filter(Balance.user_id == u.id).first()
        users_data.append({
            "username": u.username,
            "password": u.password or u.password_hash,
            "email": u.email,
            "saldo": bal.balance if bal else 0,
        })
        xls = db.query(XLAccount).filter(XLAccount.user_id == u.id).all()
        for x in xls:
            xl_data.append({
                "username": u.username,
                "phone_number": x.phone_number,
                "label": x.label,
                "refresh_token": x.refresh_token,
                "refresh_expires_at": x.refresh_expires_at,
                "subscriber_id": x.subscriber_id,
                "subscription_type": x.subscription_type,
                "is_active": bool(x.is_active),
            })
    fees = _get_all_family_fees()
    prices = _get_all_pkg_prices()
    zip_bytes = _build_backup_zip_bytes(admin_data, users_data, xl_data, fees, prices, users)
    resp = Response(content=zip_bytes, media_type="application/zip")
    resp.headers["Content-Disposition"] = 'attachment; filename="backup.zip"'
    return resp


def _collect_backup_settings() -> dict:
    """Kumpulkan pengaturan non-user untuk backup: QRIS gateway/API key,
    chat ID & token bot Telegram, jadwal auto backup, flag notif tele,
    dan posisi katalog Alternatif 1/2/3 XCP."""
    url, api_key = gopay.get_config()
    st = _ab_read_state()
    return {
        "qris_gateway": {"url": url, "api_key": api_key},
        "bot_tele": {
            "chat_id": (st.get("chat_id") or "").strip(),
            "token": (st.get("token") or "").strip(),
        },
        "autobackup": {
            "mode": st.get("mode") or "daily",
            "time": st.get("time") or "03:00",
            "weekday": st.get("weekday", 0),
            "monthday": st.get("monthday", 1),
        },
        "xcp_positions": {"alt1": _xcp_positions()[0], "alt2": _xcp_positions()[1], "alt3": _xcp_positions()[2]},
        "notif": {
            "topup_qris": bool(st.get("notif_topup_qris")),
            "topup_admin": bool(st.get("notif_topup_admin")),
            "purchase": bool(st.get("notif_purchase")),
        },
        "custom_buy": _custom_buy_read(),
    }


def _build_backup_zip_bytes(admin_data: dict, users_data: list, xl_data: list, fees: dict, prices: list, users: list) -> bytes:
    """Bangun backup.zip format manifest v3 — dipakai /admin/backup dan auto backup Telegram.

    Isi: users, xl_accounts, saldo, fee, harga paket, pengaturan, fingerprint per user,
    device.fp. CATATAN: database (*.db) dan riwayat topup TIDAK ikut — restore
    mengembalikan akun & saldo, bukan riwayat transaksi."""
    exported_at = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")
    ax_fp = _read_ax_fp_file()
    settings_data = _collect_backup_settings()
    decoy_files = _decoy_file_entries()
    decoy_count = len(decoy_files)

    entries: list[tuple[str, str]] = [
        ("manifest.json", json.dumps({
            "version": 3,
            "app": "RohTembak (XL)",
            "exported_at": exported_at,
            "counts": {"users": len(users_data), "xl_accounts": len(xl_data), "fees": len(fees), "prices": len(prices), "families": len(_family_registry()), "settings": 1, "decoys": decoy_count},
        }, indent=2, ensure_ascii=False)),
        ("admin.json", json.dumps(admin_data, indent=2, ensure_ascii=False)),
        ("users.json", json.dumps(users_data, indent=2, ensure_ascii=False)),
        ("xl_accounts.json", json.dumps(xl_data, indent=2, ensure_ascii=False)),
        ("fees.json", json.dumps(fees, indent=2, ensure_ascii=False)),
        ("prices.json", json.dumps(prices, indent=2, ensure_ascii=False)),
        ("families.json", json.dumps(_family_registry(), indent=2, ensure_ascii=False)),
        ("settings.json", json.dumps(settings_data, indent=2, ensure_ascii=False)),
    ]
    for zip_path, file_path in decoy_files:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                entries.append((zip_path, f.read()))
        except OSError:
            continue
    if not decoy_files:
        # Marker biar restore tahu backup ini memang tanpa decoy (bukan file lama)
        # dan decoy live ikut dihapus agar hasil identik dengan isi backup.
        entries.append(("decoys/.keep", ""))
    if ax_fp:
        entries.append(("device.fp", ax_fp))
    fp_dir = os.path.join(BASE_DIR, "data")
    for u in users:
        fp_path = os.path.join(fp_dir, f"ax.fp.{_safe_username(u.username)}")
        if os.path.exists(fp_path):
            try:
                with open(fp_path, "r", encoding="utf-8") as f:
                    fp_content = f.read().strip()
                if fp_content:
                    entries.append((f"fingerprints/{u.username}.fp", fp_content))
            except (OSError, UnicodeDecodeError):
                pass

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for ename, econtent in entries:
            zf.writestr(ename, econtent)
    buf.seek(0)
    return buf.getvalue()


# =============================================================================
# Auto Backup (Telegram Bot)
# -----------------------------------------------------------------------------
# Backup otomatis harian: seluruh data panel (.env, database, fingerprint)
# dikemas jadi zip lalu dikirim ke chat Telegram via bot (sendDocument).
# Konfigurasi minimal: Chat ID + API Key bot, disimpan di data/autobackup.json.
# =============================================================================

AUTOBACKUP_TIME = "03:00"  # WIB
TG_MAX_BYTES = 50 * 1024 * 1024  # batas upload dokumen Bot API
AUTOBACKUP_MAX_RETRIES = 3   # jumlah retry otomatis bila backup gagal
AUTOBACKUP_RETRY_MINUTES = 15  # jeda antar retry

_AUTOBACKUP_LOCK = threading.Lock()


def _ab_state_path() -> str:
    return os.path.join(BASE_DIR, "data", "autobackup.json")


_ab_state_lock = threading.Lock()


def _ab_read_state() -> dict:
    state = {
        "chat_id": "",
        "token": "",
        "mode": "daily",
        "time": "03:00",
        "weekday": 0,
        "monthday": 1,
        "next_run_ts": 0,
        "retry_left": 0,
        "notif_topup_qris": False,
        "notif_topup_admin": False,
        "notif_purchase": False,
        "last_run_at": "",
        "last_run_ok": None,
        "last_output": "",
    }
    with _ab_state_lock:
        try:
            with open(_ab_state_path(), "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                state.update({k: v for k, v in data.items() if k in state})
        except (OSError, ValueError):
            pass
    return state


def _ab_write_state(state: dict) -> None:
    with _ab_state_lock:
        os.makedirs(os.path.dirname(_ab_state_path()), exist_ok=True)
        with open(_ab_state_path(), "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)


def _ab_next_run_after(base_dt, st: dict):
    """Hitung waktu jadwal berikutnya dari base_dt sesuai mode (daily / weekly / monthly)."""
    mode = st.get("mode") or "daily"
    hh, mm = 3, 0
    try:
        parts = str(st.get("time") or "03:00").split(":")
        hh, mm = int(parts[0]), int(parts[1])
    except (ValueError, IndexError):
        pass
    try:
        wd = min(max(int(st.get("weekday") or 0), 0), 6)  # 0=Senin .. 6=Minggu
    except (TypeError, ValueError):
        wd = 0
    try:
        md = min(max(int(st.get("monthday") or 1), 1), 30)
    except (TypeError, ValueError):
        md = 1

    if mode == "weekly":
        cand = base_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
        cand += timedelta(days=(wd - cand.weekday()) % 7)
        if cand <= base_dt:
            cand += timedelta(days=7)
        return cand

    if mode == "monthly":
        y, mo = base_dt.year, base_dt.month

        def mk(y, mo):
            last = calendar.monthrange(y, mo)[1]
            return base_dt.replace(year=y, month=mo, day=min(md, last), hour=hh, minute=mm, second=0, microsecond=0)

        cand = mk(y, mo)
        if cand <= base_dt:
            mo += 1
            if mo > 12:
                mo, y = 1, y + 1
            cand = mk(y, mo)
        return cand

    # daily
    cand = base_dt.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if cand <= base_dt:
        cand += timedelta(days=1)
    return cand


def _ab_set_next_run(st: dict, base_dt=None) -> None:
    st["retry_left"] = 0
    st["next_run_ts"] = int(_ab_next_run_after(base_dt or datetime.now(WIB), st).timestamp())


def _telegram_send_text(text: str) -> tuple[bool, str]:
    """Kirim pesan teks ke chat yang terdaftar di pengaturan bot."""
    cfg = _ab_read_state()
    chat_id = (cfg.get("chat_id") or "").strip()
    token = (cfg.get("token") or "").strip()
    if not chat_id or not token:
        return False, "Chat ID / API key bot belum lengkap. Isi dulu di halaman Atur Bot Tele."
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            data={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=60,
        )
        try:
            body = r.json()
        except ValueError:
            body = {}
        if r.status_code == 200 and body.get("ok"):
            return True, "Pesan terkirim ke Telegram."
        return False, body.get("description") or f"HTTP {r.status_code} dari Telegram API."
    except requests.RequestException as e:
        return False, f"Gagal menghubungi Telegram: {e}"


def _notify(text: str) -> None:
    """Kirim notif teks via bot Telegram di background (tidak memblokir flow utama)."""
    def _run():
        try:
            ok, out = _telegram_send_text(text)
            if not ok:
                print(f"[notify] Gagal kirim notif: {out}")
        except Exception as e:
            print(f"[notify] Error: {e}")
    threading.Thread(target=_run, daemon=True).start()


def _tg_esc(v) -> str:
    return _html.escape(str(v), quote=False)


TG_LW = 14  # lebar kolom label agar titik dua semua baris sejajar vertikal


def _fmt_thousand(n) -> str:
    """Format angka dengan pemisah ribuan titik, mis. 10000 -> 10.000."""
    return f"{int(n):,}".replace(",", ".")


def _tg_bold(s: str) -> str:
    """Ubah huruf/digit ASCII jadi Unicode Bold Sans-Serif (judul lebih menonjol)."""
    caps = {chr(ord("A") + i): chr(0x1D5D4 + i) for i in range(26)}
    small = {chr(ord("a") + i): chr(0x1D5EE + i) for i in range(26)}
    digs = {chr(ord("0") + i): chr(0x1D7EC + i) for i in range(10)}
    return "".join(caps.get(c, small.get(c, digs.get(c, c))) for c in s)


def _tg_time_footer() -> str:
    """Footer Jam/Tanggal (WIB) untuk semua pesan notif Telegram."""
    now = datetime.now(WIB)
    return f"<code>{'Jam':<{TG_LW}}:</code> {now:%H:%M} WIB\n<code>{'Tanggal':<{TG_LW}}:</code> {now:%d/%m/%Y}"


def _tg_field(label: str, value: str) -> str:
    # label bold + monospace agar kolom titik dua sejajar; value teks biasa.
    return f"<code>{label:<{TG_LW}}:</code> {value}\n"


def _autobackup_zip_bytes() -> bytes:
    """Bangun backup.zip identik dengan Backup (ZIP) di dropdown admin (format manifest v3)."""
    db = next(get_db())
    try:
        admin = db.query(User).filter(User.role == "admin").first()
        users = db.query(User).filter(User.role == "user").all()
        admin_data = {
            "username": admin.username if admin else "",
            "password": (admin.password or admin.password_hash) if admin else "",
            "email": admin.email if admin else "",
        }
        users_data = []
        xl_data = []
        for u in users:
            bal = db.query(Balance).filter(Balance.user_id == u.id).first()
            users_data.append({
                "username": u.username,
                "password": u.password or u.password_hash,
                "email": u.email,
                "saldo": bal.balance if bal else 0,
            })
            xls = db.query(XLAccount).filter(XLAccount.user_id == u.id).all()
            for x in xls:
                xl_data.append({
                    "username": u.username,
                    "phone_number": x.phone_number,
                    "label": x.label,
                    "refresh_token": x.refresh_token,
                    "refresh_expires_at": x.refresh_expires_at,
                    "subscriber_id": x.subscriber_id,
                    "subscription_type": x.subscription_type,
                    "is_active": bool(x.is_active),
                })
        fees = _get_all_family_fees()
        prices = _get_all_pkg_prices()
        return _build_backup_zip_bytes(admin_data, users_data, xl_data, fees, prices, users)
    finally:
        db.close()


def _telegram_send_backup(trigger: str) -> tuple[bool, str]:
    cfg = _ab_read_state()
    chat_id = (cfg.get("chat_id") or "").strip()
    token = (cfg.get("token") or "").strip()
    if not chat_id or not token:
        return False, "Chat ID / API key bot belum lengkap. Isi dulu di halaman ini."

    ts = datetime.now(WIB).strftime("%Y%m%d-%H%M%S")
    fname = f"rohtembak-xl-backup-{ts}.zip"
    tmp_zip = os.path.join(BASE_DIR, "data", fname)
    ok, out = False, ""
    try:
        with open(tmp_zip, "wb") as f:
            f.write(_autobackup_zip_bytes())
        size = os.path.getsize(tmp_zip)
        if size > TG_MAX_BYTES:
            ok, out = False, f"Ukuran backup {size / 1048576:.1f} MB melebihi batas 50 MB Bot Telegram."
        else:
            try:
                with open(tmp_zip, "rb") as f:
                    r = requests.post(
                        f"https://api.telegram.org/bot{token}/sendDocument",
                        data={
                            "chat_id": chat_id,
                            "parse_mode": "HTML",
                            "caption": (
                                "🟢  " + _tg_bold("BACKUP BERHASIL") + "\n\n"
                                "<blockquote>"
                                + (
                                    _tg_field("Mode", "Otomatis" if trigger == "auto" else "Manual")
                                    + _tg_field("Ukuran", f"{size / 1048576:.2f} MB")
                                    + _tg_field("File", fname)
                                ).rstrip()
                                + "</blockquote>\n"
                                "<blockquote>"
                                + _tg_time_footer()
                                + "</blockquote>"
                            ),
                        },
                        files={"document": (fname, f)},
                        timeout=600,
                    )
                try:
                    body = r.json()
                except ValueError:
                    body = {}
                if r.status_code == 200 and body.get("ok"):
                    ok = True
                    out = f"Backup terkirim ke Telegram: {fname} ({size / 1048576:.2f} MB)."
                else:
                    out = body.get("description") or f"HTTP {r.status_code} dari Telegram API."
            except requests.RequestException as e:
                out = f"Gagal menghubungi Telegram: {e}"
    finally:
        try:
            os.remove(tmp_zip)
        except OSError:
            pass

    now_str = datetime.now(WIB).strftime("%Y-%m-%d %H:%M:%S WIB")
    cfg = _ab_read_state()
    cfg["last_run_at"] = now_str
    cfg["last_run_ok"] = ok
    cfg["last_output"] = out
    if ok:
        _ab_set_next_run(cfg)
    elif trigger == "auto" and int(cfg.get("retry_left") or 0) < AUTOBACKUP_MAX_RETRIES:
        cfg["retry_left"] = int(cfg.get("retry_left") or 0) + 1
        cfg["next_run_ts"] = int((datetime.now(WIB) + timedelta(minutes=AUTOBACKUP_RETRY_MINUTES)).timestamp())
    else:
        _ab_set_next_run(cfg)
    _ab_write_state(cfg)
    return ok, out


def _autobackup_loop():
    while True:
        try:
            cfg = _ab_read_state()
            if (cfg.get("chat_id") or "").strip() and (cfg.get("token") or "").strip():
                next_ts = int(cfg.get("next_run_ts") or 0)
                if not next_ts:
                    _ab_set_next_run(cfg)
                    _ab_write_state(cfg)
                elif datetime.now(WIB).timestamp() >= next_ts and _AUTOBACKUP_LOCK.acquire(blocking=False):
                    try:
                        _telegram_send_backup("auto")
                    finally:
                        _AUTOBACKUP_LOCK.release()
        except Exception:
            pass
        time.sleep(60)


@app.get("/admin/autobackup", response_class=HTMLResponse)
def admin_autobackup_page(request: Request, admin_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    st = _ab_read_state()
    configured = bool((st.get("chat_id") or "").strip() and (st.get("token") or "").strip())
    nts = int(st.get("next_run_ts") or 0)
    next_run_label = datetime.fromtimestamp(nts, tz=WIB).strftime("%d %B %Y %H:%M") if nts else ""
    return render("admin/autobackup.html", context={
        "request": request, "user": admin_user,
        "st": st, "configured": configured,
        "saved": request.query_params.get("saved") == "1",
        "next_run_label": next_run_label,
        "retry_left": int(st.get("retry_left") or 0),
        "retry_minutes": AUTOBACKUP_RETRY_MINUTES,
        "retry_max": AUTOBACKUP_MAX_RETRIES,
    })


@app.post("/admin/autobackup/settings")
def admin_autobackup_settings(
    request: Request,
    mode: str = Form(None),
    time_h: str = Form(None),
    time_m: str = Form(None),
    weekday: str = Form(None),
    monthday: str = Form(None),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    st = _ab_read_state()
    st["mode"] = mode if mode in ("daily", "weekly", "monthly") else "daily"
    hh, mm = 3, 0
    try:
        hh = min(max(int(time_h), 0), 23)
    except (TypeError, ValueError):
        pass
    try:
        mm = min(max(int(time_m), 0), 59)
    except (TypeError, ValueError):
        pass
    st["time"] = f"{hh:02d}:{mm:02d}"
    try:
        st["weekday"] = min(max(int(weekday), 0), 6)
    except (TypeError, ValueError):
        st["weekday"] = 0
    try:
        st["monthday"] = min(max(int(monthday), 1), 30)
    except (TypeError, ValueError):
        st["monthday"] = 1
    _ab_set_next_run(st)
    _ab_write_state(st)
    return RedirectResponse("/admin/autobackup?saved=1", status_code=303)


@app.get("/admin/bottele", response_class=HTMLResponse)
def admin_bottele_page(request: Request, admin_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    st = _ab_read_state()
    configured = bool((st.get("chat_id") or "").strip() and (st.get("token") or "").strip())
    return render("admin/bottele.html", context={
        "request": request, "user": admin_user,
        "st": st, "configured": configured,
        "saved": request.query_params.get("saved") == "1",
    })


@app.post("/admin/bottele/settings")
def admin_bottele_settings(
    request: Request,
    chat_id: str = Form(""),
    token: str = Form(""),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    st = _ab_read_state()
    st["chat_id"] = chat_id.strip()
    st["token"] = token.strip()
    _ab_write_state(st)
    return RedirectResponse("/admin/bottele?saved=1", status_code=303)


@app.post("/admin/bottele/test")
def admin_bottele_test(request: Request, admin_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    ok, out = _telegram_send_text("✅ <b>Tes berhasil!</b> Bot Telegram RohTembak (XL) terhubung.")
    return JSONResponse({"ok": ok, "output": out})


@app.get("/admin/notiftele", response_class=HTMLResponse)
def admin_notiftele_page(request: Request, admin_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    st = _ab_read_state()
    configured = bool((st.get("chat_id") or "").strip() and (st.get("token") or "").strip())
    return render("admin/notiftele.html", context={
        "request": request, "user": admin_user,
        "st": st, "configured": configured,
        "saved": request.query_params.get("saved") == "1",
    })


@app.post("/admin/notiftele/settings")
def admin_notiftele_settings(
    request: Request,
    topup_qris: str = Form(""),
    topup_admin: str = Form(""),
    purchase: str = Form(""),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    st = _ab_read_state()
    st["notif_topup_qris"] = topup_qris == "on"
    st["notif_topup_admin"] = topup_admin == "on"
    st["notif_purchase"] = purchase == "on"
    _ab_write_state(st)
    return RedirectResponse("/admin/notiftele?saved=1", status_code=303)


@app.post("/admin/autobackup/run")
def admin_autobackup_run(request: Request, admin_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")
    if not _AUTOBACKUP_LOCK.acquire(blocking=False):
        return JSONResponse({"ok": False, "output": "Backup lain sedang berjalan. Coba lagi nanti."})
    try:
        ok, out = _telegram_send_backup("manual")
    finally:
        _AUTOBACKUP_LOCK.release()
    return JSONResponse({"ok": ok, "output": out[-4000:]})


MAX_BACKUP_RESTORE_SIZE = 1 * 1024 * 1024


def _read_ax_fp_file() -> str:
    """Read the current device fingerprint (ax.fp) content, generating it if absent."""
    fp_path = os.path.join(BASE_DIR, "ax.fp")
    if not os.path.exists(fp_path):
        try:
            load_ax_fp()
        except Exception:
            return ""
    try:
        with open(fp_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except (OSError, UnicodeDecodeError):
        return ""


def _write_ax_fp_file(content: str) -> bool:
    """Persist the device fingerprint to ax.fp so restored refresh tokens match the device."""
    try:
        with open(os.path.join(BASE_DIR, "ax.fp"), "w", encoding="utf-8") as f:
            f.write(content)
        return True
    except OSError:
        return False


def _is_zip_bytes(raw_bytes: bytes) -> bool:
    return raw_bytes[:4] == b"PK\x03\x04"


def _norm_user_entry(entry) -> dict:
    return {
        "username": entry.get("username"),
        "password": entry.get("password"),
        "email": entry.get("email", ""),
        "saldo": entry.get("saldo", entry.get("balance", 0)),
    }


def _norm_backup(data: dict) -> dict:
    """Normalize full backup data into sections {admin, users, xl_accounts, fees, prices}."""
    users_raw = data.get("users") or []
    users = [_norm_user_entry(u) for u in users_raw if isinstance(u, dict)]
    xl_accounts = data.get("xl_accounts")
    if xl_accounts is None:
        xl_accounts = []
        for u in users_raw:
            if not isinstance(u, dict):
                continue
            for a in u.get("xl_accounts") or []:
                if isinstance(a, dict):
                    acc = dict(a)
                    acc.setdefault("username", u.get("username"))
                    xl_accounts.append(acc)
    return {
        "kind": "full",
        "version": data.get("version", 2),
        "admin": data.get("admin") if isinstance(data.get("admin"), dict) else None,
        "users": users,
        "xl_accounts": xl_accounts or None,
        "fees": data.get("fees") if isinstance(data.get("fees"), dict) else None,
        "prices": data.get("prices") if isinstance(data.get("prices"), list) else None,
        "families": data.get("families") if isinstance(data.get("families"), dict) else None,
        "decoys": data.get("decoys") if isinstance(data.get("decoys"), dict) else None,
        "settings": data.get("settings") if isinstance(data.get("settings"), dict) else None,
    }


def _parse_backup_json(data) -> dict:
    """Returns normalized sections from a single JSON file (v2 full or users-only)."""
    if isinstance(data, dict):
        if data.get("version") == 2 or "admin" in data or "fees" in data:
            if not isinstance(data.get("users"), list):
                raise ValueError("Format JSON tidak dikenali. Gunakan file hasil backup (field 'users').")
            return _norm_backup(data)
        users = data.get("users")
        if isinstance(users, list):
            return {"kind": "users", "version": 1, "admin": None,
                    "users": [_norm_user_entry(u) for u in users if isinstance(u, dict)],
                    "xl_accounts": None, "fees": None, "prices": None}
    if isinstance(data, list):
        return {"kind": "users", "version": 1, "admin": None,
                "users": [_norm_user_entry(u) for u in data if isinstance(u, dict)],
                "xl_accounts": None, "fees": None, "prices": None}
    raise ValueError("Format JSON tidak dikenali. Gunakan file hasil backup.")


_MAX_BACKUP_DECOMPRESSED = 20 * 1024 * 1024  # 20MB — cegah zip bomb


def _load_backup_v3(zf) -> dict | None:
    """Read v3 multi-file ZIP. Returns normalized sections or None if not v3."""
    try:
        total_uncompressed = sum(i.file_size for i in zf.infolist())
        if total_uncompressed > _MAX_BACKUP_DECOMPRESSED:
            raise ValueError("Backup terlalu besar saat diekstrak (zip bomb?).")
        manifest = json.loads(zf.read("manifest.json").decode("utf-8"))
    except (KeyError, json.JSONDecodeError, UnicodeDecodeError):
        return None
    if manifest.get("version") != 3:
        return None

    def read(name):
        try:
            return json.loads(zf.read(name).decode("utf-8"))
        except KeyError:
            return None
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ValueError(f"File {name} di dalam ZIP tidak valid.")

    device_fp = None
    try:
        raw_fp = zf.read("device.fp").decode("utf-8").strip()
        if raw_fp:
            device_fp = raw_fp
    except KeyError:
        pass
    except (UnicodeDecodeError, RuntimeError):
        raise ValueError("File device.fp di dalam ZIP tidak valid.")

    user_fingerprints = {}
    for name in zf.namelist():
        if name.startswith("fingerprints/") and name.endswith(".fp"):
            username = name[len("fingerprints/"):-3]
            try:
                fp_content = zf.read(name).decode("utf-8").strip()
                if fp_content:
                    user_fingerprints[username] = fp_content
            except (UnicodeDecodeError, RuntimeError):
                pass

    settings_raw = None
    try:
        settings_raw = json.loads(zf.read("settings.json").decode("utf-8"))
    except KeyError:
        pass
    except (json.JSONDecodeError, UnicodeDecodeError):
        raise ValueError("File settings.json di dalam ZIP tidak valid.")

    decoys_raw = {}
    for name in zf.namelist():
        if name.startswith("decoys/") and name.endswith(".json"):
            parts = name.split("/")
            if len(parts) != 3:
                continue
            ptype, fname = parts[1], parts[2][:-5]
            try:
                cfg = json.loads(zf.read(name).decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError, RuntimeError):
                continue
            if isinstance(cfg, dict):
                decoys_raw.setdefault(ptype, {})[fname] = cfg

    data = _norm_backup({
        "version": 3,
        "admin": read("admin.json"),
        "users": read("users.json"),
        "xl_accounts": read("xl_accounts.json"),
        "fees": read("fees.json"),
        "prices": read("prices.json"),
        "families": read("families.json"),
        "decoys": decoys_raw,
    })
    data["device_fp"] = device_fp
    data["user_fingerprints"] = user_fingerprints
    data["settings"] = settings_raw if isinstance(settings_raw, dict) else None
    return data


def _load_backup_data(raw_bytes: bytes, filename: str) -> dict:
    """Baca backup dari upload ZIP (manifest v3 / backup.json lama). Format
    .json / .txt tidak lagi diterima — decoy dibawa sebagai berkas .json
    terpisah, jadi restore harus selalu berupa ZIP."""
    name = (filename or "").lower()
    if name.endswith(".zip") or _is_zip_bytes(raw_bytes):
        try:
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                data = _load_backup_v3(zf)
                if data:
                    return data
                names = set(zf.namelist())
                json_names = [n for n in names if n.lower().endswith(".json")]
                if not json_names:
                    raise ValueError("ZIP tidak berisi file backup (manifest.json / backup.json).")
                target = "backup.json" if "backup.json" in names else sorted(json_names)[0]
                inner = zf.read(target).decode("utf-8")
        except (zipfile.BadZipFile, UnicodeDecodeError, RuntimeError, KeyError):
            raise ValueError("File ZIP tidak valid atau rusak.")
        try:
            return _parse_backup_json(json.loads(inner))
        except ValueError as e:
            raise ValueError(f"Isi ZIP tidak valid: {e}")
    raise ValueError("Hanya file backup ZIP (.zip) yang diterima — format .json / .txt tidak lagi didukung.")


def _validate_restore_settings(raw) -> dict | None:
    """Validasi section settings dari backup; None = tidak ada/tidak valid."""
    if not isinstance(raw, dict):
        return None
    out = {}

    qg = raw.get("qris_gateway")
    if isinstance(qg, dict):
        url = str(qg.get("url") or "").strip()
        if url and not url.startswith(("http://", "https://")):
            url = ""
        out["qris_gateway"] = {"url": url, "api_key": str(qg.get("api_key") or "").strip()}

    bt = raw.get("bot_tele")
    if isinstance(bt, dict):
        out["bot_tele"] = {
            "chat_id": str(bt.get("chat_id") or "").strip(),
            "token": str(bt.get("token") or "").strip(),
        }

    ab = raw.get("autobackup")
    if isinstance(ab, dict):
        mode = ab.get("mode") if ab.get("mode") in ("daily", "weekly", "monthly") else "daily"
        hh, mm = 3, 0
        try:
            parts = str(ab.get("time") or "03:00").split(":")
            hh, mm = int(parts[0]), int(parts[1])
        except (ValueError, IndexError):
            hh, mm = 3, 0
        hh = min(max(hh, 0), 23)   # clamp — jam invalid dari backup tidak boleh crash restore
        mm = min(max(mm, 0), 59)
        try:
            wd = min(max(int(ab.get("weekday")), 0), 6)
        except (TypeError, ValueError):
            wd = 0
        try:
            md = min(max(int(ab.get("monthday")), 1), 30)
        except (TypeError, ValueError):
            md = 1
        out["autobackup"] = {"mode": mode, "time": f"{hh:02d}:{mm:02d}", "weekday": wd, "monthday": md}

    nt = raw.get("notif")
    if isinstance(nt, dict):
        out["notif"] = {
            "topup_qris": bool(nt.get("topup_qris")),
            "topup_admin": bool(nt.get("topup_admin")),
            "purchase": bool(nt.get("purchase")),
        }

    xp = raw.get("xcp_positions")
    if isinstance(xp, dict):
        try:
            vals = tuple(int(xp[k]) for k in ("alt1", "alt2", "alt3"))
        except (TypeError, ValueError, KeyError):
            vals = None
        if vals and len(set(vals)) == 3 and all(v >= 1 for v in vals):
            out["xcp_positions"] = {"alt1": vals[0], "alt2": vals[1], "alt3": vals[2]}

    cb = raw.get("custom_buy")
    if isinstance(cb, dict):
        pins = _norm_custom_pins(cb.get("pins"), CUSTOM_BUY_LABEL_DEFAULT)
        if not pins:
            fc = _valid_custom_family_code(str(cb.get("family_code") or ""))
            if fc:
                pins = [{"label": CUSTOM_BUY_LABEL_DEFAULT, "family_code": fc}]
        label = str(cb.get("label") or "").strip()[:100]
        if label or pins:
            out["custom_buy"] = {
                "label": label or CUSTOM_BUY_LABEL_DEFAULT,
                "pins": pins,
            }

    return out or None


def _apply_restore_settings(valid_settings: dict) -> list:
    """Terapkan pengaturan dari backup. Return daftar label yang diterapkan."""
    applied = []

    qg = valid_settings.get("qris_gateway")
    if isinstance(qg, dict):
        try:
            gopay.set_config(qg.get("url") or "", qg.get("api_key") or "")
            applied.append("QRIS API & Key")
        except Exception as e:
            print(f"[restore-settings] gagal simpan QRIS gateway: {e}")

    st = _ab_read_state()
    touched_state = False

    bt = valid_settings.get("bot_tele")
    if isinstance(bt, dict):
        st["chat_id"] = bt.get("chat_id") or ""
        st["token"] = bt.get("token") or ""
        touched_state = True
        applied.append("Chat ID & Bot Telegram")

    ab = valid_settings.get("autobackup")
    if isinstance(ab, dict):
        st["mode"] = ab["mode"]
        st["time"] = ab["time"]
        st["weekday"] = ab["weekday"]
        st["monthday"] = ab["monthday"]
        _ab_set_next_run(st)
        touched_state = True
        applied.append("Jadwal Auto Backup")

    nt = valid_settings.get("notif")
    if isinstance(nt, dict):
        st["notif_topup_qris"] = bool(nt.get("topup_qris"))
        st["notif_topup_admin"] = bool(nt.get("topup_admin"))
        st["notif_purchase"] = bool(nt.get("purchase"))
        touched_state = True
        applied.append("Notif Telegram")

    xp = valid_settings.get("xcp_positions")
    if isinstance(xp, dict):
        if _save_xcp_positions((xp["alt1"], xp["alt2"], xp["alt3"])):
            applied.append("Posisi Katalog XCP")

    cb = valid_settings.get("custom_buy")
    if isinstance(cb, dict):
        _custom_buy_write(cb.get("label") or CUSTOM_BUY_LABEL_DEFAULT, cb.get("pins") or [])
        applied.append("Beli Paket Custom")

    if touched_state:
        _ab_write_state(st)
    return applied


def _validate_restore_entry(entry: dict) -> dict | None:
    username = str(entry.get("username") or "").strip().lower()
    password = str(entry.get("password") or "").strip()
    email = str(entry.get("email") or "").strip().lower()
    if not username or not password:
        return None
    if len(username) > 50 or len(password) > 255 or len(email) > 100:
        return None
    try:
        saldo = int(float(str(entry.get("saldo") or "0").replace(",", "").strip()))
    except (ValueError, TypeError):
        return None
    if saldo < 0:
        return None
    return {"username": username, "password": password, "email": email, "saldo": saldo}


def _restore_render(request: Request, user: User, result: dict | None = None, error: str | None = None):
    return render("admin/restore.html", context={
        "request": request,
        "user": user,
        "result": result,
        "error": error,
    })


def _resolve_restore_email(db: Session, existing: User | None, username: str, email: str) -> str:
    if email:
        owner = db.query(User).filter(func.lower(User.email) == email).first()
        if owner and (existing is None or owner.id != existing.id):
            email = ""
    if not email:
        if existing:
            return existing.email or ""
        candidate = f"{username}@restore.invalid"
        while db.query(User).filter(func.lower(User.email) == candidate).first():
            candidate = "_" + candidate
        return candidate
    return email


@app.get("/admin/restore", response_class=HTMLResponse)
def admin_restore_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "admin":
        return RedirectResponse(url="/user/dashboard", status_code=303)
    return _restore_render(request, user)


@app.post("/admin/restore")
async def admin_restore_upload(
    request: Request,
    backup_file: UploadFile = File(...),
    admin_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if admin_user.role != "admin":
        raise HTTPException(status_code=403, detail="Forbidden")

    raw_bytes = await backup_file.read()
    if not raw_bytes:
        return _restore_render(request, admin_user, error="File kosong. Pilih file backup yang benar.")
    if len(raw_bytes) > MAX_BACKUP_RESTORE_SIZE:
        return _restore_render(request, admin_user, error="Ukuran file terlalu besar (maksimal 1 MB).")
    try:
        data = _load_backup_data(raw_bytes, backup_file.filename or "")
    except ValueError as e:
        return _restore_render(request, admin_user, error=str(e))

    # 1. Validate everything BEFORE wiping so a bad file never destroys data
    valid_users = []
    seen = set()
    for entry in data["users"] or []:
        e = _validate_restore_entry(entry)
        if e is None or e["username"] in seen:
            continue
        seen.add(e["username"])
        valid_users.append(e)

    valid_xl = []
    xl_seen = set()
    for a in data["xl_accounts"] or []:
        if not isinstance(a, dict):
            continue
        username = str(a.get("username") or "").strip().lower()
        phone = str(a.get("phone_number") or "").strip()
        if not username or not phone or (username, phone) in xl_seen:
            continue
        xl_seen.add((username, phone))
        valid_xl.append(a)

    valid_fees = {}
    for key, fee in (data["fees"] or {}).items():
        if key in FAMILY_FEE_DEFAULTS and isinstance(fee, int) and not isinstance(fee, bool) and fee >= 0:
            valid_fees[key] = fee

    # Family dari backup ikut dianggap valid (families dipulihkan sebelum
    # commit) supaya override milik family custom tidak tertolak.
    known_fams = set(_family_registry()) | set(
        k for k, v in (data.get("families") or {}).items() if isinstance(v, dict)
    )
    valid_prices = []
    price_seen = set()
    for p in data["prices"] or []:
        if not isinstance(p, dict):
            continue
        fk = str(p.get("family_key") or "").strip()
        try:
            on = int(p.get("option_number"))
        except (TypeError, ValueError):
            continue
        if not fk or fk not in known_fams or on < 0 or (fk, on) in price_seen:
            continue
        dp = p.get("display_price")
        rp = p.get("rewrite_price")
        try:
            dp = int(dp) if dp is not None else None
            rp = int(rp) if rp is not None else None
        except (TypeError, ValueError):
            continue
        if dp is not None and dp < 0:
            continue
        if rp is not None and rp < 0:
            continue
        def _norm_fee(v):
            if v is None or isinstance(v, bool):
                return None
            try:
                v = int(v)
                return v if v >= 0 else None
            except (TypeError, ValueError):
                return None
        price_seen.add((fk, on))
        valid_prices.append({
            "family_key": fk,
            "option_number": on,
            "display_price": dp,
            "rewrite_price": rp,
            "decoy_qris": str(p.get("decoy_qris") or "")[:100],
            "decoy_pulsa": str(p.get("decoy_pulsa") or "")[:100],
            "fee_qris": _norm_fee(p.get("fee_qris")),
            "fee_pulsa": _norm_fee(p.get("fee_pulsa")),
        })

    valid_decoys = None
    raw_decoys = data.get("decoys")
    if isinstance(raw_decoys, dict):
        vd = {}
        for ptype in ("qris", "balance"):
            names = raw_decoys.get(ptype)
            if not isinstance(names, dict):
                continue
            cleaned = {}
            for name, cfg in names.items():
                name = str(name).strip()
                if name and re.fullmatch(r"[A-Za-z0-9_-]{1,60}", name) and isinstance(cfg, dict):
                    cleaned[name] = cfg
            if cleaned:
                vd[ptype] = cleaned
        # dict (mungkin kosong) = backup memang mengatur decoy (punya key) →
        # live decoy dihapus dulu biar hasil identik dengan isi backup.
        valid_decoys = vd

    valid_settings = _validate_restore_settings(data.get("settings"))
    legacy_backup = False
    if valid_settings is None and data.get("kind") == "full":
        # Backup lama (tanpa settings.json): reset pengaturan ke nilai netral,
        # bukan mempertahankan pengaturan milik mesin saat ini.
        legacy_backup = True
        valid_settings = {
            "qris_gateway": {"url": "", "api_key": ""},
            "bot_tele": {"chat_id": "", "token": ""},
            "autobackup": {"mode": "daily", "time": "03:00", "weekday": 0, "monthday": 1},
            "notif": {"topup_qris": False, "topup_admin": False, "purchase": False},
            # Backup lama tidak membawa posisi katalog XCP → reset ke default
            "xcp_positions": {"alt1": _XCP_POSITIONS_DEFAULT[0], "alt2": _XCP_POSITIONS_DEFAULT[1], "alt3": _XCP_POSITIONS_DEFAULT[2]},
        }

    admin_section = data.get("admin")
    a_username = str(admin_section.get("username") or "").strip().lower() if admin_section else ""
    a_password = str(admin_section.get("password") or "").strip() if admin_section else ""
    a_email = str(admin_section.get("email") or "").strip().lower() if admin_section else ""
    has_admin = bool(a_username and a_password)

    if not (has_admin or valid_users or valid_xl or valid_fees or valid_prices or valid_decoys or valid_settings is not None):
        return _restore_render(request, admin_user, error="Tidak ada data valid dalam file backup.")

    # 2. Wipe all existing data so the restore result is identical with the backup
    db.query(BalanceTransaction).delete(synchronize_session=False)
    db.query(TopupTransaction).delete(synchronize_session=False)
    db.query(Balance).delete(synchronize_session=False)
    db.query(XLAccount).delete(synchronize_session=False)
    db.query(User).delete(synchronize_session=False)
    db.query(FamilyFee).delete(synchronize_session=False)
    db.query(PackagePrice).delete(synchronize_session=False)
    db.expunge_all()
    with _token_lock:
        _XL_TOKEN_CACHE.clear()
    if valid_decoys is not None:
        _wipe_decoys()
    # account_id sesi kurasi menunjuk id XLAccount lama yang sudah terhapus
    # & nomor barunya bisa berbeda — paksa admin pilih ulang sesi.
    _admin_xl_clear()

    # 3. Apply the backup exactly
    users_by_name = {}
    reserved = set()
    if has_admin:
        admin = User(username=a_username, email=a_email,
                     password_hash=hash_password(a_password), password=a_password, role="admin")
        db.add(admin)
        users_by_name[a_username] = admin
        reserved.add(a_username)
        admin_msg = a_username
    else:
        admin = User(username="admin", email="",
                     password_hash=hash_password("admin"), password="admin", role="admin")
        db.add(admin)
        users_by_name["admin"] = admin
        reserved.add("admin")
        admin_msg = "admin/admin (default)"
    db.flush()

    # 3b. Restore device fingerprint (ax.fp) so restored refresh tokens match this device.
    #     Write shared fp FIRST so it's available as fallback for per-user copies.
    device_fp_ok = None
    if data.get("device_fp"):
        device_fp_ok = _write_ax_fp_file(data["device_fp"])

    users_restored = 0
    user_fingerprints = data.get("user_fingerprints") or {}
    for e in valid_users:
        if e["username"] in reserved:
            continue
        email = _resolve_restore_email(db, None, e["username"], e["email"])
        u = User(username=e["username"], email=email,
                 password_hash=hash_password(e["password"]), password=e["password"], role="user")
        db.add(u)
        db.flush()
        users_by_name[u.username] = u
        db.add(Balance(user_id=u.id, balance=e["saldo"]))
        if e["saldo"] > 0:
            db.add(BalanceTransaction(
                user_id=u.id,
                amount=e["saldo"],
                type="set",
                description="Restore dari backup"
            ))
        fp_content = user_fingerprints.get(e["username"])
        if fp_content:
            fp_dir = os.path.join(BASE_DIR, "data")
            os.makedirs(fp_dir, exist_ok=True)
            fp_path = os.path.join(fp_dir, f"ax.fp.{_safe_username(u.username)}")
            try:
                with open(fp_path, "w", encoding="utf-8") as f:
                    f.write(fp_content)
            except OSError:
                pass
        elif device_fp_ok:
            copy_shared_fp_to_user(u.username)
        users_restored += 1

    xl_restored = xl_skipped = 0
    for a in valid_xl:
        username = str(a.get("username") or "").strip().lower()
        owner = users_by_name.get(username)
        if not owner or owner.role != "user":
            xl_skipped += 1
            continue
        _r_exp = a.get("refresh_expires_at")
        try:
            _r_exp = int(_r_exp) if _r_exp is not None else None
        except (TypeError, ValueError):
            _r_exp = None
        db.add(XLAccount(
            user_id=owner.id,
            phone_number=str(a.get("phone_number") or "").strip(),
            label=str(a.get("label") or "")[:50],
            refresh_token=str(a.get("refresh_token") or ""),
            refresh_expires_at=_r_exp,
            subscriber_id=str(a.get("subscriber_id") or "")[:100],
            subscription_type=str(a.get("subscription_type") or "PREPAID")[:20],
            is_active=bool(a.get("is_active")),
        ))
        xl_restored += 1

    fee_restored = 0
    fee_source = valid_fees if valid_fees else FAMILY_FEE_DEFAULTS
    for key, fee in fee_source.items():
        db.add(FamilyFee(family_key=key, fee=fee))
        fee_restored += 1

    price_restored = 0
    for p in valid_prices:
        db.add(PackagePrice(
            family_key=p["family_key"],
            option_number=p["option_number"],
            display_price=p["display_price"],
            rewrite_price=p["rewrite_price"],
            decoy_qris=p.get("decoy_qris", ""),
            decoy_pulsa=p.get("decoy_pulsa", ""),
            fee_qris=p.get("fee_qris"),
            fee_pulsa=p.get("fee_pulsa"),
        ))
        price_restored += 1

    decoys_restored = _restore_decoys(valid_decoys) if valid_decoys else 0

    # Registry family dari backup (families.json) — dipulihkan persis.
    families_restored = 0
    for key, f in (data.get("families") or {}).items():
        if not isinstance(f, dict):
            continue
        row = db.query(XlFamily).filter(XlFamily.family_key == str(key)).first()
        if not row:
            row = XlFamily(family_key=str(key))
            db.add(row)
        row.label = str(f.get("label") or key)[:100]
        row.family_code = str(f.get("family_code") or "")[:64]
        row.url_prefix = str(f.get("url_prefix") or key)[:40]
        _oc = f.get("option_codes")
        if isinstance(_oc, list):
            _oc = ",".join(str(x) for x in _oc)
        row.option_codes = str(_oc or "")[:255]
        row.qris_decoy = bool(f.get("qris_decoy"))
        row.is_active = bool(f.get("is_active"))
        try:
            row.sort_order = int(f.get("sort") or 0)
        except (TypeError, ValueError):
            row.sort_order = 0
        families_restored += 1

    db.commit()

    settings_applied: list = []
    if valid_settings is not None:
        settings_applied = _apply_restore_settings(valid_settings)

    result = {
        "mode": "replace",
        "admin": admin_msg,
        "users_restored": users_restored,
        "xl_restored": xl_restored,
        "xl_skipped": xl_skipped,
        "fees_restored": fee_restored,
        "prices_restored": price_restored,
        "decoys_restored": decoys_restored,
        "families_restored": families_restored,
    }
    if valid_settings is not None:
        result["settings_applied"] = settings_applied
        if legacy_backup:
            result["settings_reset_default"] = True
    # Rotasi JWT secret: user id bisa bergeser setelah restore — cookie lama
    # yang masih menunjuk id lama HARUS mati seketika (anti cross-account).
    result["sessions_invalidated"] = rotate_jwt_secret()
    if device_fp_ok is not None:
        result["device_fp_restored"] = device_fp_ok
        if not device_fp_ok:
            result["device_fp_error"] = "Gagal menulis ax.fp. Cek izin folder aplikasi."
    return _restore_render(request, admin_user, result=result)


# ─── User Dashboard ─────────────────────────────────────────────────────────

@app.get("/user/dashboard", response_class=HTMLResponse)
def user_dashboard(request: Request, frag: int = 0, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()

    ctx["request"] = request
    if frag:
        html = render_template_to_string("user/_dashboard_xl_list.html", ctx)
        return JSONResponse({"ok": True, "html": html})
    return render("user/dashboard.html", context=ctx)


# ─── XL Number Management ──────────────────────────────────────────────────

@app.post("/user/xl/set-active")
def set_active_xl(
    xl_id: int = Form(...),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    db.query(XLAccount).filter(
        XLAccount.user_id == user.id, XLAccount.is_active == True
    ).update({"is_active": False})
    xl = db.query(XLAccount).filter(
        XLAccount.id == xl_id, XLAccount.user_id == user.id
    ).first()
    if xl:
        xl.is_active = True
        db.commit()
    return RedirectResponse(url="/user/dashboard", status_code=303)


@app.post("/user/xl/add")
def add_xl(
    request: Request,
    phone_number: str = Form(...),
    label: str = Form(""),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if (not phone_number.startswith("628") or len(phone_number) < 10 or len(phone_number) > 14
            or not phone_number.isdigit()):
        ctx = get_user_context(user, db)
        ctx.update({"request": request, "error": "Nomor tidak valid. Harus diawali 628 dan 10-14 digit"})
        return render("user/dashboard.html", context=ctx, status_code=400)

    xl_count = db.query(XLAccount).filter(XLAccount.user_id == user.id).count()
    if xl_count >= 10:
        ctx = get_user_context(user, db)
        ctx.update({"request": request, "error": "Maksimal 10 nomor XL per akun. Hapus nomor lama terlebih dahulu."})
        return render("user/dashboard.html", context=ctx, status_code=400)

    existing = db.query(XLAccount).filter(
        XLAccount.user_id == user.id,
        XLAccount.phone_number == phone_number
    ).first()
    if existing:
        ctx = get_user_context(user, db)
        ctx.update({"request": request, "error": "Nomor ini sudah terdaftar"})
        return render("user/dashboard.html", context=ctx, status_code=400)

    has_active = db.query(XLAccount).filter(
        XLAccount.user_id == user.id, XLAccount.is_active == True
    ).count() > 0

    xl = XLAccount(
        user_id=user.id,
        phone_number=phone_number,
        label=label,
        is_active=not has_active,
    )
    db.add(xl)
    db.commit()
    return RedirectResponse(url="/user/dashboard", status_code=303)


@app.post("/user/xl/remove")
def remove_xl(
    request: Request,
    xl_id: int = Form(...),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    xl = db.query(XLAccount).filter(
        XLAccount.id == xl_id, XLAccount.user_id == user.id
    ).first()
    if xl:
        was_active = xl.is_active
        _XL_TOKEN_CACHE.pop(xl.subscriber_id or xl.id, None)  # jangan pakai token basi
        db.delete(xl)
        if was_active:
            first_remaining = db.query(XLAccount).filter(
                XLAccount.user_id == user.id
            ).first()
            if first_remaining:
                first_remaining.is_active = True
        db.commit()
    return RedirectResponse(url="/user/dashboard", status_code=303)


# ─── XL OTP Login Flow ────────────────────────────────────────────────────

@app.get("/user/xl/otp/request", response_class=HTMLResponse)
def xl_otp_request_page(
    request: Request,
    xl_id: int = 0,
    phone: str = "",
    label: str = "",
    user: User = Depends(get_current_user),
):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    existing = None
    if xl_id:
        existing = db.query(XLAccount).filter(
            XLAccount.id == xl_id, XLAccount.user_id == user.id
        ).first()
    db.close()
    ctx["request"] = request
    ctx["existing"] = existing
    ctx["relogin"] = existing is not None
    if not existing:
        # Prefill dari link "Kirim ulang OTP" (alur nomor baru).
        ctx["phone_number"] = phone.strip()[:15]
        ctx["label"] = label.strip()[:50]
    return render("user/otp_request.html", context=ctx)


@app.post("/user/xl/otp/request")
def xl_otp_request(
    request: Request,
    phone_number: str = Form(...),
    label: str = Form(""),
    xl_id: int = Form(0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)

    ctx = get_user_context(user, db)

    if (not phone_number.startswith("628") or len(phone_number) < 10 or len(phone_number) > 14
            or not phone_number.isdigit()):
        ctx.update({"request": request, "error": "Nomor tidak valid. Harus diawali 628 dan 10-14 digit"})
        return render("user/otp_request.html", context=ctx, status_code=400)

    if xl_id:
        existing = db.query(XLAccount).filter(
            XLAccount.id == xl_id, XLAccount.user_id == user.id
        ).first()
        if not existing:
            ctx.update({"request": request, "error": "Nomor tidak ditemukan. Silakan daftarkan ulang."})
            return render("user/otp_request.html", context=ctx, status_code=400)
        if existing.phone_number != phone_number:
            ctx.update({"request": request, "error": "Nomor tidak cocok dengan catatan. Gunakan nomor sesuai daftar."})
            return render("user/otp_request.html", context=ctx, status_code=400)
    else:
        xl_count = db.query(XLAccount).filter(XLAccount.user_id == user.id).count()
        if xl_count >= 10:
            ctx.update({"request": request, "error": "Maksimal 10 nomor XL per akun. Hapus nomor lama terlebih dahulu."})
            return render("user/otp_request.html", context=ctx, status_code=400)

        existing = db.query(XLAccount).filter(
            XLAccount.user_id == user.id,
            XLAccount.phone_number == phone_number
        ).first()
        if existing:
            ctx.update({"request": request, "error": "Nomor ini sudah terdaftar. Gunakan tombol Re-OTP dari daftar nomor."})
            return render("user/otp_request.html", context=ctx, status_code=400)

    try:
        _api_delay()
        subscriber_id = xl_get_otp(phone_number, user.username)
    except Exception as e:
        ctx.update({"request": request, "error": f"Gagal mengirim OTP: {e}"})
        return render("user/otp_request.html", context=ctx, status_code=400)
    if subscriber_id is None:
        ctx.update({"request": request, "error": "Gagal mengirim OTP. Periksa nomor atau tunggu beberapa saat."})
        return render("user/otp_request.html", context=ctx, status_code=400)

    url = f"/user/xl/otp/submit?phone={_urlquote(str(phone_number))}&label={_urlquote(str(label))}&sid={_urlquote(str(subscriber_id))}"
    if xl_id:
        url += f"&xl_id={xl_id}"
    return RedirectResponse(url=url, status_code=303)


@app.get("/user/xl/otp/submit", response_class=HTMLResponse)
def xl_otp_submit_page(
    request: Request,
    phone: str = "",
    label: str = "",
    sid: str = "",
    xl_id: int = 0,
    user: User = Depends(get_current_user),
):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    if not phone:
        return RedirectResponse(url="/user/xl/otp/request", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    ctx.update({"request": request, "phone_number": phone, "label": label, "sid": sid, "xl_id": xl_id})
    return render("user/otp_submit.html", context=ctx)


@app.post("/user/xl/otp/submit")
def xl_otp_submit(
    request: Request,
    phone_number: str = Form(...),
    otp_code: str = Form(...),
    label: str = Form(""),
    subscriber_id: str = Form(""),
    xl_id: int = Form(0),
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)

    ctx = get_user_context(user, db)

    if not otp_code or len(otp_code) != 6 or not otp_code.isdigit():
        ctx.update({"request": request, "phone_number": phone_number, "label": label,
            "error": "Kode OTP harus 6 digit angka"})
        return render("user/otp_submit.html", context=ctx, status_code=400)

    _api_delay()
    tokens = xl_submit_otp(API_KEY, "SMS", phone_number, otp_code, user.username)
    if tokens is None:
        ctx.update({"request": request, "phone_number": phone_number, "label": label,
            "error": "Kode OTP salah atau sudah kadaluarsa"})
        return render("user/otp_submit.html", context=ctx, status_code=400)

    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    id_token = tokens.get("id_token", "")
    refresh_expires_at = None
    try:
        ref_exp = tokens.get("refresh_expires_in")
        if ref_exp:
            refresh_expires_at = int(time.time()) + int(ref_exp)
    except (TypeError, ValueError):
        refresh_expires_at = None

    _api_delay()
    profile = None
    try:
        profile = xl_login_info(API_KEY, {"access_token": access_token, "id_token": id_token})
    except Exception as e:
        print(f"[otp_submit] login_info gagal (akun tetap disimpan): {e}")
    subscription_type = "PREPAID"
    if profile and "subscription_type" in profile:
        subscription_type = profile["subscription_type"]

    if xl_id:
        target = db.query(XLAccount).filter(
            XLAccount.id == xl_id, XLAccount.user_id == user.id
        ).first()
    else:
        target = None

    if target:
        old_sid = target.subscriber_id
        _XL_TOKEN_CACHE.pop(old_sid or target.id, None)
        db.query(XLAccount).filter(
            XLAccount.user_id == user.id, XLAccount.is_active == True
        ).update({"is_active": False})
        target.phone_number = phone_number
        target.label = label
        target.refresh_token = refresh_token
        target.refresh_expires_at = refresh_expires_at
        target.subscriber_id = subscriber_id
        target.subscription_type = subscription_type
        target.is_active = True
        db.commit()
    else:
        has_active = db.query(XLAccount).filter(
            XLAccount.user_id == user.id, XLAccount.is_active == True
        ).count() > 0

        xl = XLAccount(
            user_id=user.id,
            phone_number=phone_number,
            label=label,
            refresh_token=refresh_token,
            refresh_expires_at=refresh_expires_at,
            subscriber_id=subscriber_id,
            subscription_type=subscription_type,
            is_active=not has_active,
        )
        db.add(xl)
        db.commit()

    return RedirectResponse(url="/user/dashboard", status_code=303)


# ─── Riwayat Transaksi ──────────────────────────────────────────────────────

@app.get("/user/detail-saldo-token", response_class=HTMLResponse)
def user_history(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    now_ts = time.time()
    history = []

    # Ambil SEMUA topup (tanpa limit) supaya dedupe BalanceTransaction di bawah
    # tidak menghilangkan topup paid yang "tua". Setiap topup QRIS yang dibayar
    # punya baris BalanceTransaction (dibuat bersamaan di _credit_topup); baris
    # saldo itu sudah diwakili baris kind="topup" status paid — jangan
    # ditampilkan dua kali. List akhir tetap dipotong ke 20 baris terbaru.
    topups = db.query(TopupTransaction).filter(
        TopupTransaction.user_id == user.id
    ).order_by(TopupTransaction.id.desc()).all()
    for t in topups:
        check_left = 0
        if t.last_checked_at is not None:
            last = t.last_checked_at
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            check_left = max(0, int(last.timestamp()) + TOPUP_MANUAL_CHECK_COOLDOWN - int(now_ts))
        created = t.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        # Keterangan + aksi ditentukan satu tempat: _topup_phase() — jangan
        # ubah di sini sendiri, supaya tidak melenceng dari pemetaan fase.
        keterangan, show_qris_link, show_check = _topup_phase(t)
        history.append({
            "kind": "topup",
            "id": t.id,
            "ts": _fmt_wib(t.created_at),
            "ts_sort": int(created.timestamp()),
            "amount": t.amount,
            "fee": t.fee,
            "total": t.total,
            "desc": keterangan,
            "status": t.status,
            "check_left": check_left,
            "show_qris_link": show_qris_link,
            "show_check": show_check,
        })

    transactions = db.query(BalanceTransaction).filter(
        BalanceTransaction.user_id == user.id
    ).order_by(BalanceTransaction.created_at.desc()).all()
    for bt in transactions:
        # Topup QRIS yang sudah terbayar sudah diwakili baris kind="topup"
        # di atas (status paid), jadi jangan ditampilkan dua kali.
        if bt.type == "topup" and "QRIS" in (bt.description or ""):
            continue
        created = bt.created_at
        if created.tzinfo is None:
            created = created.replace(tzinfo=timezone.utc)
        history.append({
            "kind": "saldo",
            "id": bt.id,
            "ts": _fmt_wib(bt.created_at),
            "ts_sort": int(created.timestamp()),
            "amount": bt.amount,
            "type": bt.type,
            "desc": bt.description or "—",
        })

    history.sort(key=lambda r: r["ts_sort"], reverse=True)
    history = history[:20]
    db.close()
    ctx.update({
        "request": request,
        "history": history,
        "topup_check_cooldown": TOPUP_MANUAL_CHECK_COOLDOWN,
    })
    return render("user/history.html", context=ctx)


def _build_history_rows(active_xl, user):
    xl_transactions = None
    tokens = None
    if active_xl and active_xl.refresh_token:
        try:
            _api_delay()
            tokens = _get_xl_tokens(active_xl)
            if tokens:
                _api_delay()
                xl_transactions = xl_get_transactions(API_KEY, tokens)
        except Exception as e:
            print(f"[xl_history] Error: {e}")

    qris_txs, qris_codes = _fetch_pending_qris(
        active_xl, tokens=tokens, transactions=xl_transactions
    )
    if xl_transactions and isinstance(xl_transactions, dict) and xl_transactions.get("list"):
        xl_transactions["list"] = [
            t for t in xl_transactions["list"]
            if (t.get("code") or "") not in qris_codes
        ]

    rows = []
    for q in qris_txs:
        expired = bool(q.get("expired"))
        te = int(q.get("ts_epoch") or 0)
        tgl, jam = _tgl_jam_wib(te) if te > 0 else (None, None)
        raw = q.get("created_at") or ""
        if tgl is None:
            if " | " in raw:
                tgl, jam = raw.split(" | ", 1)
            else:
                tgl, jam = (raw or "—"), "—"
        rows.append({
            "tgl": tgl,
            "jam": jam,
            "ts": f"{tgl} | {jam}",
            "sort": te,
            "paket": q.get("option_name") or "—",
            "harga": _fmt_harga(q.get("amount")) or "—",
            "status": "Expired" if expired else "Pending",
            "status_color": "red" if expired else "amber",
            "expires_ts": q.get("expires_ts") or 0,
            "expired": expired,
            "img": q.get("img") or "",
            "kind": "qris",
        })

    if xl_transactions and isinstance(xl_transactions, dict) and xl_transactions.get("list"):
        for trx in xl_transactions["list"]:
            s = (trx.get("status") or "").lower()
            if s in ("success", "finished"):
                color = "green"
            elif s in ("failed", "refund-success"):
                color = "red"
            else:
                color = "amber"
            ts_raw = trx.get("timestamp")
            if ts_raw:
                te = int(ts_raw) - 7 * 3600
            else:
                encoded = _parse_xl_dt(trx.get("formated_date") or "")
                te = encoded - 7 * 3600 if encoded else 0
            tgl, jam = _tgl_jam_wib(te) if te > 0 else (None, None)
            raw = trx.get("formated_date") or ""
            if tgl is None:
                if " | " in raw:
                    tgl, jam = raw.split(" | ", 1)
                else:
                    tgl, jam = (raw or "—"), "—"
            rows.append({
                "tgl": tgl,
                "jam": jam,
                "ts": f"{tgl} | {jam}",
                "sort": te,
                "paket": trx.get("title") or "—",
                "harga": _fmt_harga(trx.get("price") or trx.get("raw_price")) or "—",
                "status": trx.get("status") or "—",
                "status_color": color,
                "expires_ts": 0,
                "expired": False,
                "img": "",
                "kind": "xl",
            })

    rows.sort(key=lambda r: r["sort"], reverse=True)
    return rows, bool(rows)


@app.get("/user/xl/history", response_class=HTMLResponse)
def user_xl_history(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()

    active_xl = ctx.get("active_xl")
    if request.query_params.get("frag") == "1":
        rows, has_rows = _build_history_rows(active_xl, user)
        html = jinja_env.get_template("user/_history_data.html").render(
            active_xl=active_xl, rows=rows, has_rows=has_rows, error=None
        )
        return JSONResponse({"ok": True, "html": html}, headers={"Cache-Control": "no-store"})

    ctx.update({"request": request})
    return render("user/xl_history.html", context=ctx)


def _build_info_paket_data(active_xl):
    paket_list = None
    if active_xl and active_xl.refresh_token:
        try:
            _api_delay()
            tokens = _get_xl_tokens(active_xl)
            if tokens:
                id_token = tokens.get("id_token", "")
                path = "api/v8/packages/quota-details"
                payload = {"is_enterprise": False, "lang": "en", "family_member_id": ""}
                _api_delay()
                res = send_api_request(API_KEY, path, payload, id_token, "POST")
                if isinstance(res, dict) and res.get("status") == "SUCCESS":
                    paket_list = res["data"].get("quotas", [])
                    for q in paket_list:
                        pf = q.get("package_family") or {}
                        if not pf.get("package_family_code"):
                            code = FAMILY_CODE_BY_NAME.get(pf.get("name"))
                            if code:
                                pf["package_family_code"] = code
                                q["package_family"] = pf
        except Exception as e:
            print(f"[info_paket] Error: {e}")
    return paket_list


@app.get("/user/xl/info-paket", response_class=HTMLResponse)
def user_xl_info_paket(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()

    active_xl = ctx.get("active_xl")
    if request.query_params.get("frag") == "1":
        paket_list = _build_info_paket_data(active_xl)
        html = jinja_env.get_template("user/_info_paket_data.html").render(
            active_xl=active_xl, paket_list=paket_list
        )
        return JSONResponse({"ok": True, "html": html}, headers={"Cache-Control": "no-store"})

    ctx.update({"request": request})
    return render("user/info_paket.html", context=ctx)


# ─── Beli Paket ─────────────────────────────────────────────────────────────

FAMILY_CODE_XTRA_COMBO = "23b71540-8785-4abe-816d-e9b4efa48f95"
FAMILY_CODE_ADDON = "7658c955-a0b9-405f-bb17-de7f43d1a946"
FAMILY_CODE_ADDON_15 = "45c3a622-8c06-4bb1-8e56-bba1f3434600"
FAMILY_CODE_XTRA_CONFERENCE = "5dab52d5-6f02-4678-b72f-088396ceb113"

FAMILY_CODE_BY_NAME = {
    "Xtra Combo Plus": FAMILY_CODE_XTRA_COMBO,
    "Addon Xtra Combo Plus 10GB": FAMILY_CODE_ADDON,
    "Addon Xtra Combo Plus 15GB": FAMILY_CODE_ADDON_15,
    "Xtra Conference": FAMILY_CODE_XTRA_CONFERENCE,
}





def _build_registry_items(family_data, option_codes):
    """Builder generik untuk registry: semua opsi katalog (dengan filter
    option_codes bila di-set di admin). Ini sumber tunggal daftar paket
    untuk halaman beli-paket — 1 family = 1 group kartu.
    """
    items = []
    if not (family_data and family_data.get("package_variants")):
        return items
    item_number = 1
    for variant in family_data["package_variants"]:
        for option in variant["package_options"]:
            if not option_codes or item_number in option_codes:
                raw = option["name"]
                m = re.match(r'^(.+?)\s+(\d+\s*GB)$', raw, re.IGNORECASE)
                if m:
                    label, size = m.group(1), m.group(2).upper()
                else:
                    label, size = raw, ""
                items.append({
                    "number": item_number,
                    "label": label,
                    "size": size,
                    "name": option["name"],
                    "price": option["price"],
                    "variant_name": variant["name"],
                    "option_code": option["package_option_code"],
                })
            item_number += 1
    return items





def _catalog_snapshot_path():
    return os.path.join(BASE_DIR, "data", "catalog_options.json")


def _family_registry():
    """Registry semua family (termasuk non-aktif): family_key -> config dict.

    Sumber tunggal untuk routing, builder, label, prefix — halaman beli-paket
    adalah container yang merender registry ini.
    """
    db = next(get_db())
    try:
        rows = db.query(XlFamily).order_by(XlFamily.sort_order).all()
        reg = {}
        for r in rows:
            codes = []
            for part in re.sub(r"[\[\]]", " ", str(r.option_codes or "")).split(","):
                part = part.strip()
                if part:
                    try:
                        codes.append(int(part))
                    except ValueError:
                        pass
            reg[r.family_key] = {
                "label": r.label,
                "family_code": r.family_code,
                "url_prefix": r.url_prefix or r.family_key,
                "option_codes": codes,
                "qris_decoy": bool(r.qris_decoy),
                "is_active": bool(r.is_active),
                "sort": r.sort_order,
            }
        return reg
    finally:
        db.close()


def _active_families():
    """Registry family aktif saja, urut tampil."""
    reg = _family_registry()
    return {k: v for k, v in sorted(reg.items(), key=lambda kv: kv[1]["sort"]) if v["is_active"]}


def _family_key_by_prefix(prefix):
    for key, cfg in _family_registry().items():
        if cfg["url_prefix"] == prefix:
            return key
    return None


def _admin_xl_session_path():
    return os.path.join(BASE_DIR, "data", "admin_xl_session.json")


def _admin_xl_read():
    """Sesi kurasi: referensi ke akun XL milik user terpilih (bukan login
    OTP admin sendiri). Bisa kosong → admin belum pilih sesi."""
    try:
        with open(_admin_xl_session_path(), "r", encoding="utf-8") as f:
            d = json.load(f)
        return d if isinstance(d, dict) else None
    except (OSError, ValueError):
        return None


def _admin_xl_write(sess):
    path = _admin_xl_session_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(sess, f, ensure_ascii=False)
    except OSError as e:
        print(f"[admin-xl] gagal simpan sesi: {e}")


def _admin_xl_clear():
    try:
        os.remove(_admin_xl_session_path())
    except OSError:
        pass


# ─── Beli Paket Custom ───────────────────────────────────────────────────────
# Group paket permanen ("Beli Paket Custom"): bukan family di registry, tidak
# bisa dihapus admin, dan tersedia di halaman Beli Paket semua user. User
# mengetik family code sendiri (atau memakai pin dari admin); harga bisa
# di-rewrite user (rewrite harga XL AMAN, BUKAN BUG — hanya berlaku untuk
# group custom ini, bukan family biasa yang diatur admin).

CUSTOM_FAMILY_KEY = "custom"
CUSTOM_BUY_LABEL_DEFAULT = "Beli Paket Custom"


def _custom_buy_path():
    return os.path.join(BASE_DIR, "data", "custom_buy.json")


def _norm_custom_pins(raw, default_label: str = "") -> list:
    """Normalisasi daftar family code pin → [{"label", "family_code"}].
    Entri legacy bertipe string/pin tunggal dianggap family code dengan
    label default. Dedupe by family_code."""
    vals = raw if isinstance(raw, list) else []
    pins = []
    seen = set()
    for v in vals:
        if isinstance(v, dict):
            fc = _valid_custom_family_code(str(v.get("family_code") or ""))
            lbl = str(v.get("label") or default_label).strip()[:100]
        else:
            fc = _valid_custom_family_code(str(v or ""))
            lbl = str(default_label or "").strip()[:100]
        if fc and fc not in seen:
            pins.append({"label": lbl, "family_code": fc})
            seen.add(fc)
    return pins


def _custom_buy_read() -> dict:
    try:
        with open(_custom_buy_path(), "r", encoding="utf-8") as f:
            d = json.load(f)
        if not isinstance(d, dict):
            raise ValueError
    except (OSError, ValueError):
        d = {}
    pins = _norm_custom_pins(d.get("pins"), CUSTOM_BUY_LABEL_DEFAULT)
    old = _valid_custom_family_code(str(d.get("family_code") or ""))
    if not pins and old:
        pins = [{"label": CUSTOM_BUY_LABEL_DEFAULT, "family_code": old}]
    return {
        "label": str(d.get("label") or CUSTOM_BUY_LABEL_DEFAULT).strip()[:100] or CUSTOM_BUY_LABEL_DEFAULT,
        "pins": pins,
        # Backward-compat: family code pin pertama tetap diekspos sebagai family_code.
        "family_code": pins[0]["family_code"] if pins else "",
    }


def _custom_buy_write(label: str, pins: list):
    path = _custom_buy_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "label": str(label or CUSTOM_BUY_LABEL_DEFAULT).strip()[:100] or CUSTOM_BUY_LABEL_DEFAULT,
                "pins": _norm_custom_pins(pins, label or CUSTOM_BUY_LABEL_DEFAULT),
            }, f, ensure_ascii=False)
    except OSError as e:
        print(f"[custom-buy] gagal simpan: {e}")


def _valid_custom_family_code(fc: str) -> str:
    """Family code valid untuk custom (UID/UUID XL) — striping + cek bentuk."""
    fc = str(fc or "").strip()
    if not fc:
        return ""
    if not re.fullmatch(r"[A-Za-z0-9\-]{8,64}", fc):
        return ""
    return fc


def _admin_xl_tokens():
    """Token API XL dari akun XL user terpilih (bukan login admin sendiri).

    Admin memilih user + nomor yang sudah terdaftar; refresh token akun itulah
    yang dipakai. Refresh token XL berotasi tiap refresh — hasil terbaru
    disimpan balik ke baris akun user (lewat _get_xl_tokens)."""
    sess = _admin_xl_read()
    if not sess or not sess.get("account_id"):
        return None
    db = next(get_db())
    try:
        acct = db.query(XLAccount).filter(XLAccount.id == sess["account_id"]).first()
        if not acct or not acct.refresh_token:
            _admin_xl_clear()
            return None
        try:
            username = acct.user.username
        except Exception:
            username = ""
    finally:
        db.close()
    return _get_xl_tokens(acct, username)


def _admin_xl_fetch_catalog():
    """Fetch katalog semua family pakai sesi admin XL → perbarui snapshot.
    Return (ok, pesan)."""
    tokens = _admin_xl_tokens()
    if not tokens:
        _admin_xl_clear()
        return False, "Sesi XL tidak valid. Pilih pengguna dengan nomor XL yang aktif."
    results = []
    for fam_key, cfg in _family_registry().items():
        if not cfg["is_active"]:
            continue
        fam_code = cfg["family_code"]
        is_ent, mig = _family_api_params(fam_code)
        _api_delay()
        try:
            data = xl_get_family(API_KEY, tokens, fam_code, is_enterprise=is_ent, migration_type=mig)
        except Exception as e:
            results.append((fam_key, f"error: {e}"))
            continue
        if not data:
            results.append((fam_key, "kosong"))
            continue
        if fam_code == FAMILY_CODE_XTRA_COMBO:
            _save_xcp_catalog(data)
        cfg = _active_families().get(fam_key) or {}
        items = _build_registry_items(data, cfg.get("option_codes") or [])
        _save_catalog_snapshot(fam_key, items)
        results.append((fam_key, f"{len(items)} item"))
    ok = any(("error" not in msg and msg != "kosong") for _f, msg in results)
    return ok, "; ".join(f"{f}: {m}" for f, m in results)


_XCP_POSITIONS_DEFAULT = (25, 33, 35)


def _xcp_positions():
    """Posisi (option number) Alternatif 1/2/3 XCP = kolom option_codes di
    tabel xl_families (CSV, urutan = urutan alternatif). Dikelola admin di
    /prices-xl via kolom Option #."""
    codes = _family_registry().get("xcp", {}).get("option_codes") or []
    vals = tuple(codes[:3])
    if len(vals) == 3 and len(set(vals)) == 3 and all(v > 0 for v in vals):
        return vals
    return _XCP_POSITIONS_DEFAULT


def _save_xcp_positions(vals):
    """Simpan posisi alt 1/2/3 XCP ke kolom option_codes family xcp."""
    db = next(get_db())
    try:
        row = db.query(XlFamily).filter(XlFamily.family_key == "xcp").first()
        if not row:
            return False
        row.option_codes = ",".join(str(int(v)) for v in vals)
        db.commit()
        return True
    except Exception as e:
        print(f"[xcp-positions] gagal simpan: {e}")
        return False
    finally:
        db.close()


def _read_snapshot_file():
    try:
        with open(_catalog_snapshot_path(), "r", encoding="utf-8") as f:
            snap = json.load(f)
        return snap if isinstance(snap, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_snapshot_file(snap):
    path = _catalog_snapshot_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(snap, f, ensure_ascii=False)
    except OSError as e:
        print(f"[catalog-snapshot] gagal simpan: {e}")


def _save_catalog_snapshot(family_key, items):
    """Simpan daftar paket addon (number, label, size, harga API) dari fetch
    beli-paket terakhir — dipakai /prices-xl untuk render form.

    ponytail: penomoran addon posisional dari API XL (sama dengan alur
    pembelian). Snapshot regeneratif — hilang/katalog berubah = terisi ulang
    saat user browse. Batasi ke addon10/15; xcp lewat _save_xcp_catalog.
    """
    if family_key not in ("addon10", "addon15"):
        return
    snap = _read_snapshot_file()
    snap[family_key] = [
        {"number": it.get("number"), "label": it.get("label"),
         "size": it.get("size"), "price": it.get("price")}
        for it in items
    ]
    _write_snapshot_file(snap)


def _save_xcp_catalog(family_data):
    """Simpan daftar LENGKAP opsi XCP (semua posisi, tak difilter) — sumber
    nama untuk form urutan Alternatif 1/2/3 di /prices-xl."""
    if not (family_data and family_data.get("package_variants")):
        return
    opts = []
    number = 1
    for variant in family_data["package_variants"]:
        for option in variant.get("package_options") or []:
            opts.append({"number": number, "name": option.get("name"), "price": option.get("price")})
            number += 1
    snap = _read_snapshot_file()
    snap["xcp"] = opts
    _write_snapshot_file(snap)


def _load_catalog_snapshot(family_key):
    rows = _read_snapshot_file().get(family_key)
    return rows if isinstance(rows, list) else []


def _pkg_price_override(family_key, option_number):
    """Override harga per paket dari DB (display, rewrite) — NULL = ikut API.

    Admin mengatur ini di halaman /prices-xl. Sebelumnya nilai XCP alt 3
    di-hardcode; sekarang disimpan di tabel package_prices.
    BUKAN BUG — ini konfigurasi bisnis yang disengaja.
    """
    db = next(get_db())
    try:
        row = db.query(PackagePrice).filter(
            PackagePrice.family_key == family_key,
            PackagePrice.option_number == option_number,
        ).first()
        if not row:
            return None, None
        return row.display_price, row.rewrite_price
    finally:
        db.close()


def _pkg_decoy_override(family_key, option_number):
    """Decoy terpilih per paket dari DB (decoy_qris, decoy_pulsa).

    "" / "none" = tanpa decoy
    lain        = nama config decoy di decoy_data/{qris|balance}/
    """
    db = next(get_db())
    try:
        row = db.query(PackagePrice).filter(
            PackagePrice.family_key == family_key,
            PackagePrice.option_number == option_number,
        ).first()
        if not row:
            return "", ""
        return (row.decoy_qris or ""), (row.decoy_pulsa or "")
    finally:
        db.close()


def _get_all_pkg_prices() -> list:
    """Kumpulkan semua override harga paket untuk backup (prices.json)."""
    db = next(get_db())
    try:
        rows = db.query(PackagePrice).order_by(
            PackagePrice.family_key, PackagePrice.option_number
        ).all()
        return [
            {
                "family_key": r.family_key,
                "option_number": r.option_number,
                "display_price": r.display_price,
                "rewrite_price": r.rewrite_price,
                "decoy_qris": r.decoy_qris or "",
                "decoy_pulsa": r.decoy_pulsa or "",
                "fee_qris": r.fee_qris,
                "fee_pulsa": r.fee_pulsa,
            }
            for r in rows
        ]
    finally:
        db.close()





def _apply_pkg_list_price(family_key, items):
    """Apply display-price override to package list items (daftar paket).

    item.price (yang dilihat user di kartu) = display_price kalau di-set,
    selain itu tetap harga asli API. rewrite tidak dipakai di list.
    """
    for it in items:
        display, _ = _pkg_price_override(family_key, it.get("number"))
        if display is not None:
            it["price"] = display
    return items


def _apply_pkg_detail_price(family_key, detail):
    """Apply display+rewrite override to a detail dict.

    detail["price"]  = display_price (halaman detail & checkout menampilkan ini)
    detail["rewrite_price"] = rewrite_price (overwrite_amount saat settlement)
    """
    if not detail:
        return detail
    display, rewrite = _pkg_price_override(family_key, detail.get("number"))
    if display is not None:
        detail["price"] = display
    detail["rewrite_price"] = rewrite
    return detail


# ─── Beli Paket ──────────────────────────────────────────────────────────────


def _get_xl_info(active_xl):
    if not active_xl or not active_xl.refresh_token:
        return None
    try:
        _api_delay()
        tokens = _get_xl_tokens(active_xl)
        if not tokens:
            return None
        id_token = tokens.get("id_token", "")
        _api_delay()
        balance_data = xl_get_balance(API_KEY, id_token)
        if not balance_data:
            return None
        expiry_ts = balance_data.get("expired_at")
        expiry_date = datetime.fromtimestamp(expiry_ts, tz=WIB).strftime("%d %b %Y") if expiry_ts else None
        return {
            "main_credit": balance_data.get("remaining"),
            "expiry_date": expiry_date,
            "label": active_xl.label,
        }
    except Exception as e:
        print(f"[xl_info] Failed to fetch XL info: {e}")
        return None


@app.get("/user/xl/beli-paket", response_class=HTMLResponse)
def user_xl_beli_paket(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()

    # Sections untuk container beli-paket: 1 family aktif = 1 group kartu.
    # Family baru yang ditambahkan admin otomatis muncul di sini.
    sections = []
    for fam_key, cfg in _active_families().items():
        sections.append({"key": fam_key, "fam": fam_key, "title": cfg["label"]})

    ctx.update({
        "request": request,
        "family_name": _family_label("xcp"),
        "sections": sections,
        "url_prefixes": {k: v["url_prefix"] for k, v in _active_families().items()},
        "custom_buy": _custom_cfg(),
        "custom_url": "/user/xl/custom",
    })
    return render("user/beli_paket.html", context=ctx)


def _sse_event(event, obj):
    return "event: %s\ndata: %s\n\n" % (event, json.dumps(obj, ensure_ascii=False))


def _stream_beli_paket_events(active_xl, want, disconnected=None):
    error = False
    fetched_at = None
    xl_info = None
    fam_want = [f for f in _active_families() if f in want]
    need_tokens = bool(fam_want) or ("meta" in want)

    tokens = None
    if active_xl and active_xl.refresh_token and need_tokens:
        if disconnected and disconnected.is_set():
            return
        try:
            _api_delay()
            tokens = _get_xl_tokens(active_xl)
        except Exception as e:
            print(f"[beli-paket] Error: {e}")
            error = True
        if tokens:
            if "meta" in want:
                _api_delay()
                try:
                    balance_data = xl_get_balance(API_KEY, tokens.get("id_token", ""))
                    if balance_data:
                        expiry_ts = balance_data.get("expired_at")
                        expiry_date = datetime.fromtimestamp(expiry_ts, tz=WIB).strftime("%d %b %Y") if expiry_ts else None
                        xl_info = {
                            "main_credit": balance_data.get("remaining"),
                            "expiry_date": expiry_date,
                            "label": active_xl.label,
                        }
                except Exception as e:
                    print(f"[beli-paket] Error: {e}")
                    error = True
        else:
            error = True

    yield _sse_event("meta", {"xl_info": xl_info})

    if tokens:
        for f in fam_want:
            if disconnected and disconnected.is_set():
                return
            spec = _family_fetch_spec(f)
            if not spec:
                continue
            fam_code, builder, entry_key = spec
            is_ent, mig = _family_api_params(fam_code)
            _api_delay()
            ok = True
            result = []
            try:
                data = xl_get_family(API_KEY, tokens, fam_code, is_enterprise=is_ent, migration_type=mig)
                if fam_code == FAMILY_CODE_XTRA_COMBO:
                    _save_xcp_catalog(data)
                result = builder(data) if data else []
                result = _apply_pkg_list_price(f, result)
                if ok and result:
                    _save_catalog_snapshot(f, result)
            except Exception as e:
                print(f"[beli-paket] Error: {e}")
                ok = False
                error = True
            yield _sse_event("family", {"key": entry_key, "items": result, "ok": ok})
        fetched_at = time.time()

    yield _sse_event("done", {
        "error": error,
        "fetched_at": fetched_at,
        "last_refresh": datetime.now(WIB).strftime("%d %b %Y, %H:%M WIB"),
    })


@app.get("/user/xl/beli-paket/stream")
async def user_xl_beli_paket_stream(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"error": True}, status_code=403)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    want = {x for x in (request.query_params.get("families") or "").split(",") if x == "meta" or x in _active_families()}
    disconnected = threading.Event()
    async def _watch():
        # is_disconnected() = poll non-blocking. Loop + sleep supaya benar-
        # benar mendeteksi client pergi (kalau sekali panggil, langsung beres).
        while not await request.is_disconnected():
            await asyncio.sleep(1)
        disconnected.set()
    task = asyncio.create_task(_watch())

    async def _stream_guard():
        # Bungkus generator: cancel watcher tepat saat stream beneran selesai
        # (client pergi / server selesai) — bukan pas endpoint return.
        # Generator ini SYNC — iterasi pakai for biasa, lalu yield per chunk.
        try:
            for chunk in _stream_beli_paket_events(ctx.get("active_xl"), want, disconnected):
                yield chunk
        finally:
            disconnected.set()
            task.cancel()

    return StreamingResponse(
        _stream_guard(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


# ─── Beli Paket Custom (user) ─────────────────────────────────────────────────
# Group permanen (bukan family di registry, tak bisa dihapus). User mengetik
# family code bebas (atau memakai pin admin), lalu browse SEMUA opsi katalog,
# dan boleh me-rewrite harga. Rewrite harga paket XL AMAN, BUKAN BUG — disengaja
# dan hanya berlaku di alur custom ini (family biasa yang diatur admin tidak
# terpengaruh).

def _custom_cfg() -> dict:
    cb = _custom_buy_read()
    return {**cb, "family_key": CUSTOM_FAMILY_KEY}


def _parse_custom_rw(rw):
    """Parse rewrite harga dari query. Kosong/negatif/bukan angka -> None
    (pakai harga API). Berlaku HANYA untuk group custom."""
    rw = str(rw or "").strip()
    if not rw:
        return None
    try:
        v = int(rw)
    except ValueError:
        return None
    return v if v >= 0 else None


def _resolve_custom_fc(fc: str, pin: int = 0) -> str | None:
    """Family code untuk alur custom. pin=N memakai pin ke-N dari konfigurasi
    admin — kode pin TIDAK pernah dikirim ke klien, hanya label yang tampil.
    pin di luar jangkauan fallback ke pin pertama (ramah kalau admin unpin
    saat user masih memakai index lama)."""
    if pin:
        pins = _custom_buy_read().get("pins") or []
        fc = pins[pin - 1]["family_code"] if 1 <= pin <= len(pins) else (pins[0]["family_code"] if pins else "")
    fc = _valid_custom_family_code(fc)
    return fc


def _custom_fetch_family(tokens, family_code):
    """Katalog satu family code bebas (enterprise/migration dari API)."""
    is_ent, mig = _family_api_params(family_code)
    return xl_get_family(API_KEY, tokens, family_code, is_enterprise=is_ent, migration_type=mig)


def _custom_browse_items(tokens, family_code):
    """SEMUA opsi katalog (posisi 1..N), tak ada filter option_codes."""
    return _build_registry_items(_custom_fetch_family(tokens, family_code), [])


def _custom_item_and_detail(tokens, family_code, option_number):
    """(items, detail) untuk satu paket custom by family code bebas.

    item_price TETAP harga katalog API (XL menolak INVALID_PRICE kalau beda);
    rewrite hanya lewat overwrite_amount saat settle. Custom tidak punya
    override display/decoy admin — harga bersih dari API kecuali user rewrite.
    """
    family_data = _custom_fetch_family(tokens, family_code)
    if not (family_data and family_data.get("package_variants")):
        return None, None
    option_number_local = 1
    for variant in family_data["package_variants"]:
        for option in variant.get("package_options") or []:
            if option_number_local == option_number:
                _api_delay()
                pkg = xl_get_package(API_KEY, tokens, option["package_option_code"], family_code, variant["package_variant_code"])
                if not pkg:
                    return None, None
                detail = _build_pkg_detail(pkg, option, variant, f"custom-{option_number}", option_number)
                detail["rewrite_price"] = None
                items = [PaymentItem(
                    item_code=option["package_option_code"],
                    product_type="",
                    item_price=int(option["price"]),
                    item_name=f"{variant['name']} {option['name']}".strip(),
                    tax=0,
                    token_confirmation=pkg.get("token_confirmation", ""),
                )]
                return items, detail
            option_number_local += 1
    return None, None


def _custom_checkout_context(active_xl, user, detail, method, family_code, charge):
    db = next(get_db())
    try:
        bal = db.query(Balance).filter(Balance.user_id == user.id).first()
        balance = bal.balance if bal else 0
    finally:
        db.close()
    fee = _pkg_fee(CUSTOM_FAMILY_KEY, detail.get("number"), method)
    base_price = charge if charge is not None else (detail.get("price") or 0)
    price = int(base_price or 0)
    remaining = balance - fee
    return {
        "detail": detail,
        "method": method,
        "method_label": PAY_METHOD_LABELS.get(method, method),
        "balance": balance,
        "price": price,
        "base_price": int(base_price or 0),
        "decoy_extra": 0,
        "decoy_threshold": 0,
        "decoy_name": "",
        "fee": fee,
        "family_label": _family_label(CUSTOM_FAMILY_KEY),
        "remaining": remaining,
        "insufficient": remaining < 0,
        "decoy_pulsa_notice": False,
        "pay_url": f"/user/xl/custom/{detail.get('number')}/pay/{method}?fc={family_code}&rw="
                   + (str(charge) if charge is not None else ""),
        "back_url": f"/user/xl/custom/{detail.get('number')}/detail?fc={family_code}",
    }


@app.get("/user/xl/custom", response_class=HTMLResponse)
def user_xl_custom_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    ctx.update({
        "request": request,
        "custom_buy": _custom_cfg(),
    })
    return render("user/custom_beli_paket.html", context=ctx)


@app.get("/user/xl/custom/browse")
def user_xl_custom_browse(fc: str = "", pin: int = 0, user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "error": "Akses ditolak"}, status_code=403)
    fc = _resolve_custom_fc(fc, pin)
    if not fc:
        return JSONResponse({"ok": False, "error": "Family code tidak valid."})
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    active_xl = ctx.get("active_xl")
    if not (active_xl and active_xl.refresh_token):
        return JSONResponse({"ok": False, "error": "Belum ada nomor XL aktif."})
    try:
        _api_delay()
        tokens = _get_xl_tokens(active_xl)
        if not tokens:
            return JSONResponse({"ok": False, "error": "Gagal login XL."})
        _api_delay()
        items = _custom_browse_items(tokens, fc)
    except Exception as e:
        print(f"[custom-browse] Error: {e}")
        return JSONResponse({"ok": False, "error": "Katalog tidak ditemukan — cek family code."})
    if not items:
        return JSONResponse({"ok": False, "error": "Katalog kosong untuk family code ini."})
    return JSONResponse({"ok": True, "items": items})


@app.get("/user/xl/custom/detail")
def user_xl_custom_detail_json(fc: str = "", pin: int = 0, n: int = 0, user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "error": "Akses ditolak"}, status_code=403)
    fc = _resolve_custom_fc(fc, pin)
    if not fc or n < 1:
        return JSONResponse({"ok": False, "error": "Parameter tidak valid."})
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    active_xl = ctx.get("active_xl")
    if not (active_xl and active_xl.refresh_token):
        return JSONResponse({"ok": False, "error": "Belum ada nomor XL aktif."})
    try:
        _api_delay()
        tokens = _get_xl_tokens(active_xl)
        if not tokens:
            return JSONResponse({"ok": False, "error": "Gagal login XL."})
        _api_delay()
        items, detail = _custom_item_and_detail(tokens, fc, n)
    except Exception as e:
        print(f"[custom-detail] Error: {e}")
        return JSONResponse({"ok": False, "error": "Gagal memuat detail paket."})
    if not detail:
        return JSONResponse({"ok": False, "error": "Paket tidak ditemukan."})
    return JSONResponse({"ok": True, "detail": detail})


@app.get("/user/xl/custom/{n}/detail", response_class=HTMLResponse)
def user_xl_custom_detail_page(request: Request, n: int, fc: str = "", pin: int = 0,
                               user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    pin_mode = bool(pin)
    ctx.update({
        "request": request,
        "family": CUSTOM_FAMILY_KEY,
        "n": n,
        "pin_mode": pin_mode,
        "pin": pin,
        "fc": fc if not pin_mode else "",
        "custom_buy": _custom_cfg(),
    })
    if pin_mode:
        if not _resolve_custom_fc("", pin):
            return RedirectResponse(url="/user/xl/custom", status_code=303)
    elif not _valid_custom_family_code(fc):
        return RedirectResponse(url="/user/xl/custom", status_code=303)
    return render("user/custom_detail_paket.html", context=ctx)


@app.get("/user/xl/custom/{n}/checkout/{method}")
def user_xl_custom_checkout(request: Request, n: int, method: str, fc: str = "", pin: int = 0, rw: str = "",
                            user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    if method not in PAY_METHOD_LABELS:
        return RedirectResponse(url="/user/xl/custom", status_code=303)
    family_code = _resolve_custom_fc(fc, pin)
    if not family_code:
        return RedirectResponse(url="/user/xl/custom", status_code=303)
    back_qs = f"?pin={pin}" if pin else f"?fc={family_code}"
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    active_xl = ctx.get("active_xl")
    charge = _parse_custom_rw(rw)
    try:
        if not (active_xl and active_xl.refresh_token):
            return RedirectResponse(url=f"/user/xl/custom/{n}/detail{back_qs}", status_code=303)
        _api_delay()
        tokens = _get_xl_tokens(active_xl)
        items, detail = _custom_item_and_detail(tokens, family_code, n)
    except Exception as e:
        print(f"[custom-checkout] Error: {e}")
        items = detail = None
    if not detail:
        return RedirectResponse(url=f"/user/xl/custom/{n}/detail{back_qs}", status_code=303)
    if charge is not None:
        detail["rewrite_price"] = charge
    cc = _custom_checkout_context(active_xl, user, detail, method, family_code, charge)
    if pin:
        cc["pay_url"] = f"/user/xl/custom/{n}/pay/{method}?pin={pin}&rw=" + (str(charge) if charge is not None else "")
        cc["back_url"] = f"/user/xl/custom/{n}/detail?pin={pin}"
    ctx.update({"request": request, **cc})
    return render("user/checkout.html", context=ctx)


@app.post("/user/xl/custom/{n}/pay/{method}")
def user_xl_custom_pay(request: Request, n: int, method: str, fc: str = "", pin: int = 0, rw: str = "",
                       user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    if method not in PAY_METHOD_LABELS:
        return JSONResponse({"ok": False, "message": "Metode pembayaran tidak tersedia."}, status_code=400)
    family_code = _resolve_custom_fc(fc, pin)
    if not family_code:
        return JSONResponse({"ok": False, "message": "Paket tidak ditemukan."}, status_code=404)
    charge = _parse_custom_rw(rw)
    blocked = _panel_fee_precheck(user, CUSTOM_FAMILY_KEY, n, method)
    if blocked:
        return blocked
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    return _pay_with_fee(user, ctx,
                         lambda: _process_payment_custom(ctx.get("active_xl"), family_code, n, method, charge),
                         CUSTOM_FAMILY_KEY, n, method)


def _process_payment_custom(active_xl, family_code, option_number, method, charge):
    """Settle pembelian custom. Rewrite harga (charge) AMAN — BUKAN BUG —
    disengaja untuk group custom; tanpa decoy/fee admin."""
    pay_error = None
    pay_success = None
    detail = None
    pay_extra = {}
    _stdout_buf = io.StringIO()
    with redirect_stdout(_stdout_buf):
        if active_xl and active_xl.refresh_token:
            try:
                _api_delay()
                tokens = _get_xl_tokens(active_xl)
                items, detail = _custom_item_and_detail(tokens, family_code, option_number)
            except Exception as e:
                pay_error = f"Error fetching package: {e}"
                items = None
            if items and not pay_error:
                if charge is not None:
                    detail["rewrite_price"] = charge
                try:
                    if method == "balance":
                        from app.client.purchase.balance import settlement_balance as pay_balance
                        _api_delay()
                        res = _settle_with_decoy(pay_balance, tokens, items, detail, "balance", False)
                        if res and res.get("status") == "SUCCESS":
                            pay_success = "Pembelian berhasil! Silakan cek aplikasi MyXL."
                        else:
                            pay_error = f"Pembayaran gagal: {res.get('message', 'Unknown error') if res else 'No response'}"
                    elif method == "qris":
                        from app.client.purchase.qris import show_qris_payment
                        _api_delay()
                        qris_result = _settle_with_decoy(show_qris_payment, tokens, items, detail, "qris", False)
                        if qris_result:
                            qris_b64, _, qris_remaining = qris_result
                            pay_success = "QRIS berhasil dibuat. Silakan pindai kode QR untuk menyelesaikan pembayaran."
                            pay_extra["qris_b64"] = qris_b64
                            pay_extra["qris_remaining"] = int(qris_remaining or 0)
                        else:
                            pay_error = "Gagal membuat QRIS."
                    else:
                        pay_error = "Metode pembayaran tidak dikenal."
                except Exception as e:
                    pay_error = f"Error: {e}"
            else:
                pay_error = "Paket tidak ditemukan."
        else:
            pay_error = "Akun XL tidak aktif."
    pay_extra["terminal_output"] = _stdout_buf.getvalue()
    return detail, pay_error, pay_success, pay_extra


def _family_fetch_spec(fam_key):
    """(family_code, builder, entry_key) untuk stream beli-paket — dari registry.

    entry_key = family_key (container & cache di template kunci berdasar ini).
    Semua family pakai builder generik registry; option_codes (bila di-set)
    memfilter opsi katalog yang ditampilkan.
    """
    cfg = _active_families().get(fam_key)
    if not cfg:
        return None
    return cfg["family_code"], lambda data: _build_registry_items(data, cfg["option_codes"]), fam_key


def _family_api_params(family_code):
    """(is_enterprise, migration_type) per family — Xtra Conference butuh
    parameter khusus; family baru default ke (False, "NONE")."""
    if family_code == FAMILY_CODE_XTRA_CONFERENCE:
        return True, "PRIOH_TO_PRIO"
    return False, "NONE"


# ─── Detail Paket ───────────────────────────────────────────────────────────

def _build_pkg_detail(pkg, option, variant, url_id, number=None):
    po = pkg.get("package_option", {})
    pf = pkg.get("package_family", {})
    pv = pkg.get("package_detail_variant", {})
    return {
        "number": number,
        "url_id": url_id,
        "option_name": option["name"],
        "variant_name": variant["name"],
        "family_name": pf.get("name", ""),
        "price": option["price"],
        "validity": po.get("validity", ""),
        "point": po.get("point", 0),
        "plan_type": pf.get("plan_type", ""),
        "benefits": po.get("benefits", []),
        "tnc": po.get("tnc", ""),
        "option_code": option["package_option_code"],
        "token_confirmation": pkg.get("token_confirmation", ""),
        "payment_for": pf.get("payment_for", "BUY_PACKAGE"),
    }


@app.get("/user/xl/beli-paket/{family_prefix}-{option_number}/detail", response_class=HTMLResponse)
def user_xl_detail_paket(request: Request, family_prefix: str, option_number: int,
                         via: str = "", user: User = Depends(get_current_user)):
    """Detail generik — family_prefix adalah url_prefix family di registry
    (mis. "xcp", "addon10-xcp"). Family baru otomatis punya halaman detail."""
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    fam_key = _family_key_by_prefix(family_prefix)
    if not fam_key:
        return RedirectResponse(url="/user/xl/beli-paket", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    if via not in ("pulsa", "qris"):
        via = ""
    ctx.update({
        "request": request,
        "family": fam_key,
        "family_prefix": family_prefix,
        "n": option_number,
        "via": via,
    })
    return render("user/detail_paket.html", context=ctx)


def _family_detail(fam_key, option_number, active_xl, tokens=None):
    """Detail satu paket untuk family mana pun di registry (dipakai detail page
    & checkout). option_number = posisi di katalog API family tersebut."""
    cfg = _family_registry().get(fam_key)
    if not cfg:
        return None
    if not tokens:
        _api_delay()
        tokens = _get_xl_tokens(active_xl)
        if not tokens:
            return None
    fam_code = cfg["family_code"]
    is_ent, mig = _family_api_params(fam_code)
    _api_delay()
    family_data = xl_get_family(API_KEY, tokens, fam_code, is_enterprise=is_ent, migration_type=mig)
    if not (family_data and family_data.get("package_variants")):
        return None
    option_number_local = 1
    for variant in family_data["package_variants"]:
        for option in variant["package_options"]:
            if option_number_local == option_number:
                _api_delay()
                pkg = xl_get_package(API_KEY, tokens, option["package_option_code"], fam_code, variant["package_variant_code"])
                if pkg:
                    detail = _build_pkg_detail(pkg, option, variant, f"{cfg['url_prefix']}-{option_number}", option_number)
                    return _apply_pkg_detail_price(fam_key, detail)
                return None
            option_number_local += 1
    return None


def _stream_detail_events(family, option_number, active_xl, want, disconnected=None):
    """Single refresh-token stream for detail page: meta (banner) -> delay -> detail."""
    error = False
    want_meta = "meta" in want
    want_detail = "detail" in want
    xl_info = None
    detail = None

    tokens = None
    if active_xl and active_xl.refresh_token and (want_meta or want_detail):
        if disconnected and disconnected.is_set():
            return
        try:
            _api_delay()
            tokens = _get_xl_tokens(active_xl)
        except Exception as e:
            print(f"[detail-stream] Error: {e}")
            error = True
        if tokens:
            if want_meta:
                _api_delay()
                try:
                    balance_data = xl_get_balance(API_KEY, tokens.get("id_token", ""))
                    if balance_data:
                        expiry_ts = balance_data.get("expired_at")
                        expiry_date = datetime.fromtimestamp(expiry_ts, tz=WIB).strftime("%d %b %Y") if expiry_ts else None
                        xl_info = {
                            "main_credit": balance_data.get("remaining"),
                            "expiry_date": expiry_date,
                            "label": active_xl.label,
                        }
                except Exception as e:
                    print(f"[detail-stream] Error: {e}")
                    error = True
        else:
            error = True

    yield _sse_event("meta", {"xl_info": xl_info})

    if want_detail:
        if tokens and not (disconnected and disconnected.is_set()):
            _api_delay()
            try:
                detail = _family_detail(family, option_number, active_xl, tokens)
            except Exception as e:
                print(f"[detail-stream] {family}-{option_number} error: {e}")
                error = True
        yield _sse_event("detail", {"ok": bool(detail), "detail": detail})
    yield _sse_event("done", {"error": error})


@app.get("/user/xl/beli-paket/detail-stream")
async def user_xl_detail_stream(request: Request, family: str, n: int, user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "error": "forbidden"}, status_code=403)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    want = {x for x in (request.query_params.get("families") or "meta,detail").split(",") if x in ("meta", "detail")}
    disconnected = threading.Event()
    async def _watch():
        while not await request.is_disconnected():
            await asyncio.sleep(1)
        disconnected.set()
    task = asyncio.create_task(_watch())

    async def _stream_guard():
        try:
            for chunk in _stream_detail_events(family, n, ctx.get("active_xl"), want, disconnected):
                yield chunk
        finally:
            disconnected.set()
            task.cancel()

    return StreamingResponse(
        _stream_guard(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
    )


@app.get("/user/xl/banner-info")
def user_xl_banner_info(user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "xl_info": None}, status_code=403)
    db = next(get_db())
    ctx = get_user_context(user, db)
    active = ctx.get("active_xl")
    xl_info = _get_xl_info(active)
    db.close()
    token_ok = xl_info is not None
    if not token_ok and active and active.refresh_token:
        cached = _XL_TOKEN_CACHE.get(active.subscriber_id or active.id)
        token_ok = bool(cached and cached.get("expires_at", 0) > time.time())
    return JSONResponse(
        {"ok": True, "xl_info": xl_info, "token_ok": token_ok},
        headers={"Cache-Control": "no-store"},
    )


# ─── Payment Routes ─────────────────────────────────────────────────────────

def _get_family_items_and_detail(fam_key, option_number, active_xl, tokens=None):
    """Payment items + detail untuk family mana pun di registry (settlement).

    item_price TETAP harga katalog API (XL menolak INVALID_PRICE kalau beda);
    rewrite hanya lewat overwrite_amount saat settle.
    """
    cfg = _family_registry().get(fam_key)
    if not cfg:
        return None, None
    if not tokens:
        _api_delay()
        tokens = _get_xl_tokens(active_xl)
        if not tokens:
            return None, None
    fam_code = cfg["family_code"]
    is_ent, mig = _family_api_params(fam_code)
    _api_delay()
    family_data = xl_get_family(API_KEY, tokens, fam_code, is_enterprise=is_ent, migration_type=mig)
    if not family_data:
        return None, None
    option_number_local = 1
    for variant in family_data["package_variants"]:
        for option in variant["package_options"]:
            if option_number_local == option_number:
                _api_delay()
                pkg = xl_get_package(API_KEY, tokens, option["package_option_code"], fam_code, variant["package_variant_code"])
                if not pkg:
                    return None, None
                detail = _build_pkg_detail(pkg, option, variant, f"{cfg['url_prefix']}-{option_number}", option_number)
                detail = _apply_pkg_detail_price(fam_key, detail)
                price = int(option["price"])
                items = [PaymentItem(
                    item_code=option["package_option_code"],
                    product_type="",
                    item_price=price,
                    item_name=f"{variant['name']} {option['name']}".strip(),
                    tax=0,
                    token_confirmation=pkg.get("token_confirmation", ""),
                )]
                return items, detail
            option_number_local += 1
    return None, None

def _append_decoy_item(items, tokens, payment_type="balance", name="default"):
    from app.service.decoy import build_decoy_item
    _api_delay()
    decoy_item = build_decoy_item(API_KEY, tokens, payment_type, name)
    if not decoy_item or not decoy_item["item_code"]:
        return items, None
    return items + [decoy_item], int(decoy_item["item_price"] or 0)


def _parse_bizz_total(error_msg):
    msg = str(error_msg or "")
    for token in ("=", "valid amount is "):
        if token in msg:
            try:
                return int(msg.split(token)[1].strip())
            except (ValueError, IndexError):
                continue
    return None


def _settle_with_decoy(pay_fn, tokens, items, detail, method, use_decoy, decoy_name="default"):
    # rewrite_price (dari /prices-xl) menang atas display/api — ini jumlah
    # yang benar-benar ditagih; item_price PaymentItem TETAP harga asli API.
    charge = detail.get("rewrite_price")
    if charge is None:
        charge = detail["price"]
    if not use_decoy:
        return pay_fn(API_KEY, tokens, items, detail["payment_for"], False, overwrite_amount=charge)

    items_with_decoy, decoy_price = _append_decoy_item(items, tokens, method, decoy_name)
    if decoy_price is None:
        raise ValueError("Gagal memuat paket decoy.")
    overwrite_amount = int(charge or 0) + decoy_price

    if method == "qris":
        return pay_fn(API_KEY, tokens, items_with_decoy, "SHARE_PACKAGE", False, overwrite_amount=overwrite_amount, token_confirmation_idx=1)

    res = pay_fn(API_KEY, tokens, items_with_decoy, "🤫", False, overwrite_amount=overwrite_amount, token_confirmation_idx=1)
    if isinstance(res, dict) and res.get("status") != "SUCCESS":
        msg = str(res.get("message", ""))
        if "Bizz-err.Amount.Total" in msg or "valid amount is" in msg:
            valid_amount = _parse_bizz_total(msg)
            if valid_amount is not None:
                print(f"Adjusted total amount to: {valid_amount}")
                res = pay_fn(API_KEY, tokens, items_with_decoy, "🤫", False, overwrite_amount=valid_amount, token_confirmation_idx=-1)
    return res


def _process_payment(active_xl, fam_key, option_number, method):
    pay_error = None
    pay_success = None
    detail = None
    pay_extra = {}
    # Decoy murni per-package: override pada /prices-xl menentukan satu-satunya
    # decoy untuk metode ini. Kosong/none = tanpa decoy.
    dq, dp = _pkg_decoy_override(fam_key, option_number)
    href = dq if method == "qris" else dp
    use_decoy = bool(href and href != "none")
    decoy_name = href if use_decoy else "default"
    _stdout_buf = io.StringIO()
    with redirect_stdout(_stdout_buf):
        if active_xl and active_xl.refresh_token:
            try:
                items, detail = _get_family_items_and_detail(fam_key, option_number, active_xl)
            except Exception as e:
                pay_error = f"Error fetching package: {e}"
                items = None
            if items and not pay_error:
                try:
                    _api_delay()
                    tokens = _get_xl_tokens(active_xl)
                    if method == "balance":
                        from app.client.purchase.balance import settlement_balance as pay_balance
                        _api_delay()
                        res = _settle_with_decoy(pay_balance, tokens, items, detail, "balance", use_decoy, decoy_name)
                        if res and res.get("status") == "SUCCESS":
                            pay_success = "Pembelian berhasil! Silakan cek aplikasi MyXL."
                        else:
                            pay_error = f"Pembayaran gagal: {res.get('message', 'Unknown error') if res else 'No response'}"
                    elif method == "qris":
                        from app.client.purchase.qris import show_qris_payment
                        _api_delay()
                        qris_result = _settle_with_decoy(show_qris_payment, tokens, items, detail, "qris", use_decoy, decoy_name)
                        if qris_result:
                            qris_b64, _, qris_remaining = qris_result
                            pay_success = "QRIS berhasil dibuat. Silakan pindai kode QR untuk menyelesaikan pembayaran."
                            pay_extra["qris_b64"] = qris_b64
                            pay_extra["qris_remaining"] = int(qris_remaining or 0)
                        else:
                            pay_error = "Gagal membuat QRIS."
                    else:
                        pay_error = "Metode pembayaran tidak dikenal."
                except Exception as e:
                    pay_error = f"Error: {e}"
            else:
                pay_error = "Paket tidak ditemukan."
        else:
            pay_error = "Akun XL tidak aktif."
    pay_extra["terminal_output"] = _stdout_buf.getvalue()
    return detail, pay_error, pay_success, pay_extra


def _deduct_token_balance(user, amount, description):
    """Deduct the panel-saldo consumption fee; returns new balance or None when insufficient."""
    db = next(get_db())
    try:
        with _balance_lock:
            bal = db.query(Balance).filter(Balance.user_id == user.id).first()
            if not bal or bal.balance < amount:
                return None
            bal.balance -= amount
            db.add(BalanceTransaction(user_id=user.id, amount=-amount, type="purchase", description=description))
            db.commit()
            return bal.balance
    finally:
        db.close()


def _refund_token_balance(user, amount, description):
    """Kembalikan biaya konsumsi panel saat pembelian gagal (mirror of deduct)."""
    db = next(get_db())
    try:
        with _balance_lock:
            bal = db.query(Balance).filter(Balance.user_id == user.id).first()
            if not bal:
                bal = Balance(user_id=user.id, balance=0)
                db.add(bal)
            bal.balance += amount
            db.add(BalanceTransaction(user_id=user.id, amount=amount, type="refund", description=description))
            db.commit()
            return bal.balance
    finally:
        db.close()


PAY_METHOD_LABELS = {
    "balance": "Pulsa XL",
    "qris": "QRIS",
}

FAMILY_FEE_DEFAULTS = {
    "xcp:balance": 2000,
    "xcp:qris": 2000,
    "addon10:balance": 1000,
    "addon10:qris": 1000,
    "addon15:balance": 1000,
    "addon15:qris": 1000,
    "xtraconf:balance": 1000,
    "xtraconf:qris": 1000,
}

def _family_label(family_key):
    if family_key == CUSTOM_FAMILY_KEY:
        return _custom_buy_read().get("label") or CUSTOM_BUY_LABEL_DEFAULT
    return _family_registry().get(family_key, {}).get("label", family_key)


def _fee_key(family_key, method):
    return f"{family_key}:{method}"


def _get_family_fee(family_key):
    db = next(get_db())
    try:
        row = db.query(FamilyFee).filter(FamilyFee.family_key == family_key).first()
        return row.fee if row else FAMILY_FEE_DEFAULTS.get(family_key, 0)
    finally:
        db.close()


def _pkg_fee(family_key, option_number, method):
    """Fee saldo panel per-package; fallback ke fee family bila belum di-override."""
    if option_number is not None:
        db = next(get_db())
        try:
            row = db.query(PackagePrice).filter(
                PackagePrice.family_key == family_key,
                PackagePrice.option_number == option_number,
            ).first()
            if row is not None:
                val = row.fee_qris if method == "qris" else row.fee_pulsa
                if val is not None:
                    return val
        finally:
            db.close()
    return _get_family_fee(_fee_key(family_key, method))


def _set_family_fee(family_key, fee):
    db = next(get_db())
    try:
        row = db.query(FamilyFee).filter(FamilyFee.family_key == family_key).first()
        if row:
            row.fee = fee
        else:
            db.add(FamilyFee(family_key=family_key, fee=fee))
        db.commit()
    finally:
        db.close()


def _get_all_family_fees():
    db = next(get_db())
    try:
        rows = db.query(FamilyFee).all()
        stored = {r.family_key: r.fee for r in rows}
        return {k: stored.get(k, v) for k, v in FAMILY_FEE_DEFAULTS.items()}
    finally:
        db.close()


def _checkout_detail(active_xl, fetch_fn):
    if not (active_xl and active_xl.refresh_token):
        return None
    try:
        return fetch_fn()
    except Exception as e:
        # XL blip / refresh gagal -> balik ke halaman detail dengan pesan,
        # bukan 500 mentah di halaman checkout.
        print(f"[checkout_detail] Error: {e}")
        return None


_decoy_price_cache: dict = {}
# Cache harga decoy mengikuti masa sesi login (ACCESS_TOKEN_EXPIRE_MINUTES),
# selaras dengan TTL cache paket di sisi browser (570 detik utk 9.5 menit).
_DECOY_PRICE_TTL = int(float(ACCESS_TOKEN_EXPIRE_MINUTES) * 60)


def _qris_decoy_price(active_xl=None, name="default"):
    """Harga decoy QRIS yang dipakai di settlement (live dari API).

    Checkout menampilkan harga ini agar total QRIS di layar == total yang
    benar-benar ditagih. Hanya harga hasil fetch live yang di-cache; nilai
    fallback config TIDAK di-cache supaya begitu ada akun XL aktif,
    checkout berikutnya langsung memakai harga live.
    """
    cache_key = f"qris:{name}"
    entry = _decoy_price_cache.get(cache_key)
    if entry and entry[1] > time.time():
        return entry[0]
    try:
        from app.service.decoy import build_decoy_item, load_decoy_config
        config = load_decoy_config("qris", name) or {}
        price = int(config.get("price") or 0)
        if active_xl and active_xl.refresh_token:
            _api_delay()
            tokens = _get_xl_tokens(active_xl)
            if tokens:
                _api_delay()
                item = build_decoy_item(API_KEY, tokens, "qris", name)
                if item:
                    price = int(item["item_price"] or 0)
                    _decoy_price_cache[cache_key] = (price, time.time() + _DECOY_PRICE_TTL)
                    return price
    except Exception:
        pass
    return price


def _checkout_context(active_xl, user, detail, method, family_key, option_number=None):
    db = next(get_db())
    try:
        bal = db.query(Balance).filter(Balance.user_id == user.id).first()
        balance = bal.balance if bal else 0
    finally:
        db.close()
    fee = _pkg_fee(family_key, option_number, method)
    remaining = balance - fee
    # Decoy murni per-package (override /prices-xl). Kosong/none = tanpa decoy.
    decoy = False
    decoy_name = "default"
    if option_number is not None:
        dq, dp = _pkg_decoy_override(family_key, option_number)
        href = dq if method == "qris" else dp
        if href and href != "none":
            decoy = True
            decoy_name = href
    # Checkout menampilkan harga yang BENAR-BENAR ditagih: rewrite_price kalau
    # di-set di /prices-xl, selain itu display_price, lalu harga API.
    # Decoy QRIS ditampilkan sebagai baris terpisah ("biaya decoy") supaya
    # user lihat bahwa ada penambah harga.
    base_price = detail.get("rewrite_price")
    if base_price is None:
        base_price = detail.get("price") or 0
    decoy_extra = 0
    decoy_threshold = 0
    if decoy:
        if method == "qris":
            decoy_extra = _qris_decoy_price(active_xl, decoy_name)
        elif method == "balance":
            from app.service.decoy import load_decoy_config
            decoy_threshold = int((load_decoy_config("balance", decoy_name) or {}).get("price") or 0)
    price = int(base_price or 0) + int(decoy_extra or 0)
    return {
        "detail": detail,
        "method": method,
        "method_label": PAY_METHOD_LABELS.get(method, method),
        "balance": balance,
        "price": price,
        "base_price": int(base_price or 0),
        "decoy_extra": int(decoy_extra or 0),
        "decoy_threshold": int(decoy_threshold or 0),
        "decoy_name": decoy_name if decoy else "",
        "fee": fee,
        "family_label": _family_label(family_key),
        "remaining": remaining,
        "insufficient": remaining < 0,
        # Family decoy via pulsa (balance): item decoy (bundle) sengaja dibuat
        # gagal, jadi pulsa nomor harus DI BAWAH harga decoy — kalau lebih,
        # decoy ikut terpotong sungguhan. QRIS tidak berlaku (harga tampil
        # sudah termasuk).
        "decoy_pulsa_notice": decoy and method == "balance",
        "pay_url": f"/user/xl/beli-paket/{detail.get('url_id')}/pay/{method}",
    }


@app.get("/user/xl/beli-paket/{family_prefix}-{option_number}/checkout/{method}")
def checkout_paket(request: Request, family_prefix: str, option_number: int, method: str,
                   user: User = Depends(get_current_user)):
    """Checkout generik — resolve family dari url_prefix di registry."""
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    if method not in PAY_METHOD_LABELS:
        return RedirectResponse(url="/user/xl/beli-paket", status_code=303)
    fam_key = _family_key_by_prefix(family_prefix)
    if not fam_key:
        return RedirectResponse(url="/user/xl/beli-paket", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    active_xl = ctx.get("active_xl")
    detail = _checkout_detail(active_xl, lambda: _family_detail(fam_key, option_number, active_xl))
    if not detail:
        return RedirectResponse(url=f"/user/xl/beli-paket/{family_prefix}-{option_number}/detail", status_code=303)
    cc = _checkout_context(active_xl, user, detail, method, fam_key, option_number)
    ctx.update({"request": request, **cc})
    return render("user/checkout.html", context=ctx)

def _panel_fee_precheck(user, family_key: str, option_number: int, method: str):
    """Return an error response when panel saldo cannot cover the fee; else None.

    Called before the XL API call so an underfunded user never reaches purchase.
    The authoritative re-check happens in _deduct_token_balance at settle time.
    """
    fee = _pkg_fee(family_key, option_number, method)
    db = next(get_db())
    try:
        bal = db.query(Balance).filter(Balance.user_id == user.id).first()
        if not bal or bal.balance < fee:
            return JSONResponse({
                "ok": False,
                "message": f"Saldo panel tidak cukup. Butuh {_fmt_idr(fee)} IDR, saldo kamu {_fmt_idr(bal.balance if bal else 0)} IDR."
            }, status_code=400)
        return None
    finally:
        db.close()

@app.post("/user/xl/beli-paket/{family_prefix}-{option_number}/pay/{method}")
def pay_paket(request: Request, family_prefix: str, option_number: int, method: str,
              user: User = Depends(get_current_user)):
    """Pay generik — family dari url_prefix registry; decoy murni per-package
    (override set di /prices-xl per paket, bukan per family)."""
    if user.role != "user":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    if method not in PAY_METHOD_LABELS:
        return JSONResponse({"ok": False, "message": "Metode pembayaran tidak tersedia."}, status_code=400)
    fam_key = _family_key_by_prefix(family_prefix)
    if not fam_key:
        return JSONResponse({"ok": False, "message": "Paket tidak ditemukan."}, status_code=404)
    blocked = _panel_fee_precheck(user, fam_key, option_number, method)
    if blocked:
        return blocked
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    return _pay_with_fee(user, ctx,
                         lambda: _process_payment(ctx.get("active_xl"), fam_key, option_number, method),
                         fam_key, option_number, method)

def _qris_png_data_uri(qris_b64):
    import base64 as _b64
    import io as _io
    import qrcode as _qrcode
    try:
        data = _b64.urlsafe_b64decode(qris_b64.encode()).decode()
        qr = _qrcode.QRCode(error_correction=_qrcode.constants.ERROR_CORRECT_M)
        qr.add_data(data)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        buf = _io.BytesIO()
        img.save(buf, format="PNG")
        return "data:image/png;base64," + _b64.b64encode(buf.getvalue()).decode()
    except Exception:
        return None


def _fetch_pending_qris(active_xl, tokens=None, transactions=None):
    import base64 as _b64
    qris_txs = []
    matched_codes = set()
    if not active_xl or not active_xl.refresh_token:
        return qris_txs, matched_codes
    try:
        if tokens is None:
            _api_delay()
            tokens = _get_xl_tokens(active_xl)
            if not tokens:
                return qris_txs, matched_codes
        if transactions is None:
            _api_delay()
            hist = xl_get_transactions(API_KEY, tokens) or {}
        else:
            hist = transactions or {}
        for trx in hist.get("list", []):
            pm = (trx.get("payment_method") or "").upper()
            st = (trx.get("status") or "").upper()
            if "QRIS" not in pm or st not in ("READY", "PENDING", "WAITING_PAYMENT", "ONGOING", "PROCESS"):
                continue
            code = trx.get("code") or ""
            if not code or code in matched_codes:
                continue
            matched_codes.add(code)
            payload = {"transaction_id": code, "is_enterprise": False, "lang": "en", "status": ""}
            _api_delay()
            res = send_api_request(API_KEY, "payments/api/v8/pending-detail", payload, tokens["id_token"], "POST")
            if not isinstance(res, dict) or res.get("status") != "SUCCESS":
                continue
            detail = res.get("data") or {}
            qr_raw = detail.get("qr_code")
            if not qr_raw:
                continue
            qris_b64 = _b64.urlsafe_b64encode(qr_raw.encode()).decode()
            remaining = int(detail.get("remaining_time") or 0)
            expires_ts = int(time.time()) + remaining if remaining > 0 else 0
            st_detail = (detail.get("status") or "").upper()
            pay_st = (trx.get("payment_status") or "").upper()
            expired = remaining <= 0 or st_detail == "EXPIRED" or pay_st == "EXPIRED"
            raw_ts = trx.get("timestamp")
            qris_txs.append({
                "transaction_id": detail.get("payment_id") or code,
                "option_name": trx.get("title") or trx.get("product_name") or "Paket",
                "amount": trx.get("raw_price") or 0,
                "status": trx.get("status"),
                "created_at": detail.get("formated_date") or trx.get("formated_date") or "",
                "ts_epoch": (int(raw_ts) - 7 * 3600) if raw_ts else 0,
                "expires_ts": expires_ts,
                "expired": expired,
                "img": _qris_png_data_uri(qris_b64),
                "qris_b64": qris_b64,
            })
    except Exception as e:
        print(f"[fetch_pending_qris] Error: {e}")
    return qris_txs, matched_codes


def _pay_with_fee(user, ctx, run_purchase, family_key, option_number, method):
    """Bayar biaya konsumsi panel SEBELUM purchase XL, refund kalau gagal.

    Menutup celah: dua order konkuren yang sama-sama lolos precheck tidak
    lagi bisa mengantre paket tanpa fee — saldo sudah terpotong di depan.
    """
    fee = _pkg_fee(family_key, option_number, method)
    desc = f"Konsumsi saldo panel {_family_label(family_key)} via {PAY_METHOD_LABELS.get(method, method)}"
    if _deduct_token_balance(user, fee, desc) is None:
        return JSONResponse({
            "ok": False,
            "message": f"Saldo panel tidak cukup untuk biaya konsumsi ({_fmt_idr(fee)} IDR). Topup dulu ya."
        }, status_code=400)
    detail, pay_error, pay_success, pay_extra = run_purchase()
    if not pay_success:
        _refund_token_balance(user, fee, f"Refund {desc} (pembelian gagal)")
    return _pay_response(user, detail, pay_error, pay_success, method, family_key, option_number, pay_extra,
                         phone_number=getattr(ctx.get("active_xl"), "phone_number", "") or "",
                         fee_charged=True)


def _pay_response(user, detail, pay_error, pay_success, method, family_key, option_number=None, pay_extra=None, phone_number="", fee_charged=False):
    new_balance = None
    if pay_success:
        fee = _pkg_fee(family_key, option_number, method)
        if not fee_charged:
            new_balance = _deduct_token_balance(
                user,
                fee,
                f"Konsumsi saldo panel {_family_label(family_key)} via {PAY_METHOD_LABELS.get(method, method)}"
            )
            if new_balance is None:
                return JSONResponse({
                    "ok": False,
                    "message": f"Saldo panel tidak cukup untuk biaya konsumsi ({_fmt_idr(fee)} IDR). Topup dulu ya."
                })
        if new_balance is None:
            # Jalur fee_charged (dipotong sebelum purchase): ambil saldo segar
            # untuk notif — kalau tidak, NameError saat notif_purchase aktif.
            dbn = next(get_db())
            try:
                bn = dbn.query(Balance).filter(Balance.user_id == user.id).first()
                new_balance = bn.balance if bn else 0
            finally:
                dbn.close()
        if _ab_read_state().get("notif_purchase"):
            option_name = (detail or {}).get("option_name") or ""
            family_label = _family_label(family_key)
            if option_name and family_label.lower() not in option_name.lower():
                pkg_name = f"{family_label} - {option_name}"
            else:
                pkg_name = option_name or family_label
            pkg_number = (detail or {}).get("number")
            if pkg_number:
                pkg_name = f"{pkg_name} (#{pkg_number})"
            metode_label = {"balance": "via Pulsa XL", "qris": "via QRIS XL"}.get(method, method)
            _notify(
                "🟢  " + _tg_bold("PEMBELIAN PAKET") + "\n\n"
                "<blockquote>"
                + (
                    _tg_field("User", _tg_esc(user.username))
                    + _tg_field("Nomor", _tg_esc(phone_number) if phone_number else "-")
                    + _tg_field("Biaya admin", f"{_fmt_thousand(fee)} IDR")
                    + _tg_field("Metode", metode_label)
                    + _tg_field("Paket", _tg_esc(pkg_name))
                ).rstrip()
                + "</blockquote>\n"
                "<blockquote>"
                + _tg_field("Saldo", f"{_fmt_thousand(new_balance)} IDR").rstrip()
                + "</blockquote>\n"
                "<blockquote>"
                + _tg_time_footer()
                + "</blockquote>"
            )
        resp = {"ok": True, "message": pay_success, "deducted": fee, "new_balance": new_balance}
        qris_b64 = (pay_extra or {}).get("qris_b64")
        if qris_b64:
            resp["qris_img"] = _qris_png_data_uri(qris_b64)
            remaining = int((pay_extra or {}).get("qris_remaining") or 0)
            resp["qris_expires_ts"] = int(time.time()) + remaining if remaining > 0 else 0
        terminal_output = (pay_extra or {}).get("terminal_output", "")
        if terminal_output:
            resp["terminal_output"] = terminal_output
        return JSONResponse(resp)
    resp = {"ok": False, "message": pay_error or "Pembayaran gagal."}
    terminal_output = (pay_extra or {}).get("terminal_output", "")
    if terminal_output:
        resp["terminal_output"] = terminal_output
    return JSONResponse(resp)


# ─── Topup Saldo (QRIS via GoPay gateway) ──────────────────────────────────


def _topup_expiry_epoch(topup) -> int:
    dt = topup.expires_at
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _topup_phase(topup):
    """Fase tampilan satu baris topup QRIS + aksi yang relevan.

    Status DB mengikuti fase 1:1 — tidak ada status yang bermakna ganda:
      waiting -> pending -> expired | paid

    status DB | keterangan          | Lihat QRIS | Cek Pembayaran
    ----------|---------------------|------------|---------------
    waiting   | Menunggu Pembayaran | ya         | ya
    pending   | Pending             | tidak      | ya
    expired   | Kadaluarsa          | tidak      | tidak
    paid      | Topup Berhasil      | tidak      | tidak

    Return (keterangan, show_qris_link, show_check).
    """
    if topup.status == "waiting":
        return "Menunggu Pembayaran", True, True
    if topup.status == "pending":
        return "Pending", False, True
    if topup.status == "paid":
        return "Topup Berhasil", False, False
    return "Kadaluarsa", False, False


_topup_credit_lock = threading.Lock()


def _credit_topup(db: Session, topup: TopupTransaction):
    """Credit a paid topup exactly once (atomic pending/expired → paid flip)."""
    with _topup_credit_lock:
        # Atomic guard: UPDATE hanya matches while the row is still unpaid.
        # Jangan percaya atribut objek sesi ini (identity map bisa stale
        # ketika path lain baru saja commit 'paid' untuk baris yang sama).
        updated = db.query(TopupTransaction).filter(
            TopupTransaction.id == topup.id,
            TopupTransaction.status.in_(("waiting", "pending", "expired")),
        ).update({
            "status": "paid",
            "paid_at": datetime.now(timezone.utc),
        }, synchronize_session=False)
        db.commit()
        if not updated:
            return None
        with _balance_lock:
            bal = db.query(Balance).filter(Balance.user_id == topup.user_id).first()
            if not bal:
                bal = Balance(user_id=topup.user_id, balance=0)
                db.add(bal)
            bal.balance += topup.amount
            db.add(BalanceTransaction(
                user_id=topup.user_id,
                amount=topup.amount,
                type="topup",
                description=f"Topup saldo via QRIS ({_fmt_idr(topup.total)} IDR, termasuk biaya admin {_fmt_idr(topup.fee)} IDR)"
            ))
            db.commit()
        if _ab_read_state().get("notif_topup_qris"):
            u = db.query(User).filter(User.id == topup.user_id).first()
            uname = u.username if u else f"id {topup.user_id}"
            _notify(
                "🟢  " + _tg_bold("TOPUP QRIS") + "\n\n"
                "<blockquote>"
                + (
                    _tg_field("User", _tg_esc(uname))
                    + _tg_field("Nominal", f"+{_fmt_thousand(topup.amount)} IDR")
                    + _tg_field("Metode", "QRIS")
                ).rstrip()
                + "</blockquote>\n"
                "<blockquote>"
                + _tg_field("Saldo", f"{_fmt_thousand(bal.balance)} IDR").rstrip()
                + "</blockquote>\n"
                "<blockquote>"
                + _tg_time_footer()
                + "</blockquote>"
            )
        return bal.balance


def _check_and_settle_topup(db: Session, topup: TopupTransaction) -> dict:
    """Ask the gateway about one topup; settle (credit/mark expired) accordingly."""
    res = gopay.check_payment(topup.total, topup.trx_id)
    if not res.get("success"):
        # Gateway offline / belum dikonfigurasi: jangan jatuh ke tebakan
        # "belum ada pembayaran" yang menyesatkan. Laporkan kegagalan
        # eksplisit; baris tetap di fase sekarang, sweep berikutnya akan
        # mencoba lagi (tidak dikunci ke pending selagi gateway mati).
        return {"ok": False, "status": "unknown",
                "message": res.get("error") or "Gagal menghubungi gateway QRIS. Coba lagi nanti."}
    if res.get("paid"):
        new_balance = _credit_topup(db, topup)
        if new_balance is not None:
            return {"ok": True, "status": "paid", "new_balance": new_balance,
                    "credited": topup.amount,
                    "message": "Pembayaran diterima! Saldo sudah ditambahkan."}
        return {"ok": True, "status": "paid", "credited": topup.amount,
                "message": "Pembayaran sudah dikonfirmasi sebelumnya."}
    now_ts = time.time()
    exp_ts = _topup_expiry_epoch(topup)
    if now_ts > exp_ts:
        # QRIS sudah kedaluwarsa: transisi HANYA ke pending. Kunci
        # pending -> expired (setelah 24 jam sejak kedaluwarsa) adalah
        # satu-satunya tanggung jawab _reconcile_pending_topups — jangan
        # pernah set status expired langsung dari sini, supaya state machine
        # punya satu aturan per transisi. Pembayaran terlambat tetap bisa
        # dikredit kapan pun lewat _credit_topup.
        # Conditional update: jangan pernah menimpa 'paid' yang di-commit
        # path lain secara konkuren (stale attribute protection).
        marked = db.query(TopupTransaction).filter(
            TopupTransaction.id == topup.id,
            TopupTransaction.status.in_(("waiting", "pending")),
        ).update({"status": "pending"}, synchronize_session=False)
        db.commit()
        if not marked:
            fresh = db.query(TopupTransaction).filter(
                TopupTransaction.id == topup.id
            ).first()
            st = fresh.status if fresh else "waiting"
            if st == "paid":
                return {"ok": True, "status": "paid",
                        "credited": topup.amount,
                        "message": "Pembayaran sudah dikonfirmasi sebelumnya."}
            return {"ok": True, "status": st,
                    "message": "Belum ada pembayaran yang terdeteksi."}
        return {"ok": True, "status": "pending",
                "message": "Belum ada pembayaran yang terdeteksi."}
    return {"ok": True, "status": "waiting",
            "message": "Belum ada pembayaran yang terdeteksi."}


def _reconcile_pending_topups():
    """Background sweep — hemat panggilan gateway (anti-ban).

    Lifecycle status: waiting -> pending -> expired | paid.
    - Baris waiting TIDAK dipolling selama QR masih berlaku. Tepat setelah
      kedaluwarsa: satu panggilan /check-payment per baris untuk menangkap
      pembayaran yang sudah masuk; kalau tidak dibayar -> pending.
    - Baris pending dikunci jadi expired setelah 24 jam sejak kedaluwarsa
      (tanpa cek lagi).
    - Sebelum lewat masa berlaku, deteksi hanya lewat Cek Pembayaran manual.
    """
    if not gopay.is_configured():
        return
    db = next(get_db())
    try:
        cutoff = datetime.now(timezone.utc)
        if cutoff.tzinfo is not None:
            cutoff = cutoff.replace(tzinfo=None)
        # Baru lewat masa berlaku: cek sekali (deteksi pembayaran yang masuk).
        rows = db.query(TopupTransaction).filter(
            TopupTransaction.status == "waiting",
            TopupTransaction.expires_at <= cutoff,
        ).all()
        db.expire_all()  # refresh identity-map objects; jangan percaya atribut stale
        for row in rows:
            try:
                _check_and_settle_topup(db, row)
            except Exception as e:
                db.rollback()  # session rusak -> jangan biarkan sisa batch ikut gagal
                print(f"[topup-reconcile] id={row.id} Error: {e}")
        # Lewat 24 jam sejak kedaluwarsa: kunci jadi expired, tanpa cek lagi.
        expired_cutoff = cutoff - timedelta(hours=24)
        db.query(TopupTransaction).filter(
            TopupTransaction.status == "pending",
            TopupTransaction.expires_at <= expired_cutoff,
        ).update({"status": "expired"}, synchronize_session=False)
        db.commit()
    finally:
        db.close()


async def _topup_reconcile_watch():
    while True:
        await asyncio.sleep(TOPUP_CHECK_INTERVAL)
        try:
            await asyncio.to_thread(_reconcile_pending_topups)
        except Exception as e:
            print(f"[topup-reconcile] Error: {e}")


@app.get("/user/topup")
def topup_page(request: Request, user: User = Depends(get_current_user)):
    if user.role != "user":
        return RedirectResponse(url="/admin/dashboard", status_code=303)
    db = next(get_db())
    ctx = get_user_context(user, db)
    db.close()
    ctx.update({
        "request": request,
        "topup_min": TOPUP_MIN_AMOUNT,
        "topup_max": TOPUP_MAX_AMOUNT,
        "topup_fee_min": TOPUP_FEE_MIN,
        "topup_fee_max": TOPUP_FEE_MAX,
        "topup_check_cooldown": TOPUP_MANUAL_CHECK_COOLDOWN,
        "gopay_ready": gopay.is_configured(),
    })
    return render("user/topup.html", context=ctx)


@app.post("/user/topup/create")
def topup_create(amount: int = Form(...), user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    if not gopay.is_configured():
        return JSONResponse({"ok": False, "message": "Topup QRIS belum tersedia. Coba lagi nanti."}, status_code=503)
    if amount < TOPUP_MIN_AMOUNT or amount > TOPUP_MAX_AMOUNT:
        return JSONResponse({
            "ok": False,
            "message": f"Nominal topup harus antara {_fmt_idr(TOPUP_MIN_AMOUNT)} dan {_fmt_idr(TOPUP_MAX_AMOUNT)} IDR."
        }, status_code=400)

    db = next(get_db())
    try:
        pending_count = db.query(TopupTransaction).filter(
            TopupTransaction.user_id == user.id,
            TopupTransaction.status.in_(("waiting", "pending"))
        ).count()
        if pending_count >= TOPUP_MAX_PENDING_PER_USER:
            return JSONResponse({
                "ok": False,
                "message": f"Kamu masih punya {pending_count} topup menunggu pembayaran. Selesaikan atau tunggu kedaluwarsa dulu."
            }, status_code=429)

        total = amount + TOPUP_FEE_MIN  # lower bound sanity check only

        # The gateway matches payments by nominal only, so every pending QRIS
        # must have a unique total. A random unique-code fee (1..250 IDR) is
        # added on top of the amount; retry until the total is free.
        row = None
        for _ in range(60):
            fee = random.randint(TOPUP_FEE_MIN, TOPUP_FEE_MAX)
            total = amount + fee
            clash = db.query(TopupTransaction).filter(
                TopupTransaction.status.in_(("waiting", "pending")),
                TopupTransaction.total == total
            ).first()
            if clash:
                continue
            row = TopupTransaction(
                user_id=user.id,
                amount=amount,
                fee=fee,
                total=total,
                trx_id=f"pending-{uuid.uuid4().hex}",
                status="waiting",
                expires_at=datetime.fromtimestamp(
                    int(time.time()) + TOPUP_QR_TTL_SECONDS, tz=timezone.utc
                ),
            )
            db.add(row)
            try:
                db.commit()
            except IntegrityError:
                # Unique index uq_topup_pending_total: another request claimed
                # the same total concurrently. Roll back and try another fee.
                db.rollback()
                continue
            break
        if row is None:
            return JSONResponse({
                "ok": False,
                "message": "Semua kode unik sedang terpakai. Coba lagi beberapa menit."
            }, status_code=409)

        # Guard race hapus-user: kalau admin menghapus user ini tepat ketika
        # kita membuat QRIS, jangan serahkan QRIS hidup ke akun yang sudah
        # tiada — buang barisnya dan tolak.
        if not db.query(User).filter(User.id == user.id).first():
            db.delete(row)
            db.commit()
            return JSONResponse({
                "ok": False,
                "message": "Akun tidak ditemukan."
            }, status_code=403)

        res = gopay.create_qris(total)
        if not res.get("success"):
            db.delete(row)
            db.commit()
            return JSONResponse({
                "ok": False,
                "message": res.get("error") or "Gagal membuat QRIS. Coba lagi."
            }, status_code=502)

        data = res.get("data") or {}
        trx_id = data.get("trx_id") or ""
        qris_id = str(data.get("qris_id") or "")
        if not trx_id or not qris_id:
            db.delete(row)
            db.commit()
            return JSONResponse({
                "ok": False,
                "message": "Respon gateway tidak lengkap. Coba lagi."
            }, status_code=502)

        expires_ts = int(time.time()) + TOPUP_QR_TTL_SECONDS
        try:
            parsed = datetime.fromisoformat(str(data.get("expires_at")).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            expires_ts = int(parsed.timestamp())
        except (ValueError, TypeError, AttributeError):
            pass

        row.trx_id = trx_id
        row.qris_id = qris_id
        row.expires_at = datetime.fromtimestamp(expires_ts, tz=timezone.utc)
        db.add(row)
        try:
            db.commit()
        except IntegrityError:
            # trx_id duplikat dari gateway — buang barisnya, jangan tinggalkan
            # pending yatim yang menahan slot unique-fee.
            db.rollback()
            db.delete(row)
            db.commit()
            return JSONResponse({
                "ok": False,
                "message": "Gateway mengirim QRIS duplikat. Coba lagi."
            }, status_code=502)

        return JSONResponse({
            "ok": True,
            "id": row.id,
            "expires_ts": expires_ts,
            "amount": amount,
            "fee": row.fee,
            "total": total,
        })
    finally:
        db.close()


@app.get("/user/topup/qr/{topup_id}")
def topup_qr_image(topup_id: int, user: User = Depends(get_current_user)):
    """Fetch the QRIS PNG live from the payment gateway (no DB storage).

    Gateway harus online: kalau tidak, QR tidak bisa ditampilkan dan user
    tidak bisa membayar — precaution agar tidak ada pembayaran saat PG down.
    """
    if user.role != "user":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    if not gopay.is_configured():
        return JSONResponse({"ok": False, "message": "Topup QRIS belum tersedia."}, status_code=503)

    db = next(get_db())
    try:
        row = db.query(TopupTransaction).filter(
            TopupTransaction.id == topup_id,
            TopupTransaction.user_id == user.id
        ).first()
        if not row:
            return JSONResponse({"ok": False, "message": "Transaksi topup tidak ditemukan."}, status_code=404)
        if row.status == "paid":
            return JSONResponse({"ok": False, "message": "Pembayaran sudah dikonfirmasi."})
        if not row.qris_id:
            return JSONResponse({"ok": False, "message": "QRIS belum dibuat."}, status_code=404)

        res = gopay.get_qris_image(row.qris_id)
        if not res.get("ok"):
            return JSONResponse({
                "ok": False,
                "message": res.get("error") or "Gagal mengambil QRIS dari gateway. Coba lagi."
            }, status_code=502)

        import base64 as _b64
        return JSONResponse({
            "ok": True,
            "qr_img": "data:image/png;base64," + _b64.b64encode(res["data"]).decode(),
            "expires_ts": _topup_expiry_epoch(row),
        })
    finally:
        db.close()


@app.post("/user/topup/check")
def topup_check(topup_id: int = Form(...), user: User = Depends(get_current_user)):
    if user.role != "user":
        return JSONResponse({"ok": False, "message": "Akses ditolak"}, status_code=403)
    if not gopay.is_configured():
        return JSONResponse({"ok": False, "message": "Topup QRIS belum tersedia."}, status_code=503)

    db = next(get_db())
    try:
        row = db.query(TopupTransaction).filter(
            TopupTransaction.id == topup_id,
            TopupTransaction.user_id == user.id
        ).first()
        if not row:
            return JSONResponse({"ok": False, "message": "Transaksi topup tidak ditemukan."}, status_code=404)
        if row.status == "paid":
            bal = db.query(Balance).filter(Balance.user_id == user.id).first()
            return JSONResponse({
                "ok": True, "status": "paid",
                "new_balance": bal.balance if bal else 0,
                "credited": row.amount,
                "message": "Pembayaran sudah dikonfirmasi sebelumnya."
            })
        # Grace manual: baris unpaid (termasuk yang sudah kedaluwarsa) boleh
        # dicek ulang manual dengan cooldown 5 menit antar klik. Auto-sweep
        # hanya menyentuh baris pending, jadi ini satu-satunya jalan bagi
        # pembayaran yang terlambat masuk setelah kedaluwarsa.
        #
        # Klaim cooldown ATOMIK: dua request paralel (dobel-klik / 2 tab)
        # hanya satu yang lolos — sisanya dapat sisa cooldown.
        now_dt = datetime.now(timezone.utc)
        cooldown_cutoff = (now_dt - timedelta(seconds=TOPUP_MANUAL_CHECK_COOLDOWN)).replace(tzinfo=None)
        claimed = db.query(TopupTransaction).filter(
            TopupTransaction.id == row.id,
            or_(
                TopupTransaction.last_checked_at.is_(None),
                TopupTransaction.last_checked_at <= cooldown_cutoff,
            ),
        ).update({"last_checked_at": now_dt.replace(tzinfo=None)}, synchronize_session=False)
        db.commit()
        if not claimed:
            db.expire_all()
            fresh_last = row.last_checked_at
            left = TOPUP_MANUAL_CHECK_COOLDOWN
            if fresh_last is not None:
                if fresh_last.tzinfo is None:
                    fresh_last = fresh_last.replace(tzinfo=timezone.utc)
                # min 1 detik: kalau sisa persis 0 (tepat di batas), client jangan
                # di-lock 5 menit penuh gara-gara fallback cooldown di JS.
                left = max(1, int(fresh_last.timestamp()) + TOPUP_MANUAL_CHECK_COOLDOWN - int(time.time()))
            return JSONResponse({
                "ok": False, "cooldown": left,
                "message": f"Tunggu {left // 60}m {left % 60}s sebelum cek pembayaran lagi."
            })
        result = _check_and_settle_topup(db, row)
        result["cooldown"] = TOPUP_MANUAL_CHECK_COOLDOWN
        return JSONResponse(result)
    finally:
        db.close()


# ─── Register ───────────────────────────────────────────────────────────────

@app.get("/register", response_class=HTMLResponse)
def register_page(request: Request):
    return render("register.html", context={"request": request})


@app.post("/register")
def register(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    db: Session = Depends(get_db),
):
    if not email.strip():
        return render("register.html", context={
            "request": request,
            "error": "Email wajib diisi"
        }, status_code=400)
    if not username.strip():
        return render("register.html", context={
            "request": request,
            "error": "Username wajib diisi"
        }, status_code=400)
    if len(password) < 6:
        return render("register.html", context={
            "request": request,
            "error": "Password minimal 6 karakter"
        }, status_code=400)
    username = username.strip().lower()
    email = email.strip().lower()
    if not re.fullmatch(r"[a-z0-9]+", username):
        return render("register.html", context={
            "request": request,
            "error": "Username hanya boleh huruf dan angka (tanpa spasi/simbol)"
        }, status_code=400)
    if len(username) < 3 or len(username) > 50:
        return render("register.html", context={
            "request": request,
            "error": "Username harus 3-50 karakter"
        }, status_code=400)
    if len(email) > 100 or not re.fullmatch(
        # Local part: segmen alnum (boleh _, %, +, -) dipisah titik tunggal.
        # Domain: label alnum (hyphen hanya di tengah) dipisah titik tunggal,
        # TLD huruf 2+.
        r"[a-z0-9_%+-]+(?:\.[a-z0-9_%+-]+)*@"
        r"(?:[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z]{2,}",
        email
    ):
        return render("register.html", context={
            "request": request,
            "error": "Format email tidak valid"
        }, status_code=400)
    attempt_key = _client_key(request)
    if _login_blocked(attempt_key):
        return render("register.html", context={
            "request": request,
            "error": "Terlalu banyak percobaan. Coba lagi dalam beberapa menit."
        }, status_code=429)
    if username == _admin_username(db).lower():
        _login_record_failure(attempt_key)
        return render("register.html", context={
            "request": request,
            "error": "Username admin tidak boleh digunakan"
        }, status_code=400)
    existing = db.query(User).filter(
        func.lower(User.username) == username
    ).first()
    if existing:
        _login_record_failure(attempt_key)
        return render("register.html", context={
            "request": request,
            "error": "Username atau email sudah terdaftar"
        }, status_code=400)
    existing = db.query(User).filter(
        func.lower(User.email) == email
    ).first()
    if existing:
        _login_record_failure(attempt_key)
        return render("register.html", context={
            "request": request,
            "error": "Username atau email sudah terdaftar"
        }, status_code=400)

    user = User(
        username=username,
        email=email,
        password_hash=hash_password(password),
        password=password,
        role="user"
    )
    db.add(user)
    db.flush()
    db.add(Balance(user_id=user.id, balance=0))
    db.commit()
    _login_reset(attempt_key)
    get_user_ax_fp(user.username)

    token = create_access_token({"sub": str(user.id), "role": "user"})
    resp = RedirectResponse(url="/user/dashboard", status_code=303)
    resp.set_cookie(key="access_token", value=token, httponly=True, samesite="lax", max_age=int(ACCESS_TOKEN_EXPIRE_MINUTES * 60))
    return resp


# ─── Run ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
