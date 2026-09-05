# RohTembak (XL)

Web UI untuk mengelola akun XL / paket XL (beli paket, info paket, riwayat, pembayaran QRIS) berbasis FastAPI.

## Fitur

- Login & registrasi pengguna (admin / user)
- Tambah & kelola akun XL
- Beli paket (Xtra Combo Plus, Addon, Xtra Conference) via XL API
- Info paket, pemakaian kuota, status perpanjangan otomatis
- Riwayat transaksi + pembayaran QRIS dengan rekonsiliasi otomatis
- QRIS duplikat otomatis dibersihkan (dedup by `transaction_id` / `qris_b64`)
- Konkurensi aman (semaphore + thread pool) untuk panggilan XL API

## Struktur

```
.
├── main.py               # FastAPI app (routes, middleware, reconciliation)
├── auth.py               # JWT auth + seed admin
├── database.py           # SQLAlchemy engine + init
├── models.py             # ORM models
├── app/                  # XL API client (engsel, ciam, purchase, crypto)
├── templates/            # Jinja2 templates
├── static/               # CSS / JS
├── requirements.txt
├── entrypoint.sh             # PRODUCTION Docker entrypoint (branch main)
├── docker-compose.yml    # production container
├── .env                  # konfigurasi kredensial XL API (sudah terisi)
└── .env.example          # template konfigurasi
```

## Quick start (Docker, production)

```bash
git clone https://github.com/rohjagad/rohtembak-xl.git
cd rohtembak-xl
docker compose up -d
```

Buka `http://localhost:8000` — login `admin` / `admin`.

> Pada host cgroup v2, jika container gagal start, tambahkan `cgroupns: host` pada service di `docker-compose.yml`.

## Deployment notes

> `entrypoint.sh` bisa dipakai sebagai entrypoint container maupun installer bare-metal.
> Bare metal: script menjalankan uvicorn di foreground — jalankan via `tmux`/`screen`
> atau systemd bila ingin tetap hidup setelah SSH logout.
>
> Repo ini private: pada deploy fresh, buat repo public sementara agar clone /
> raw.githubusercontent bisa diakses, lalu kembalikan ke private setelah jalan.
>
> Docker Compose vs Swarm: volume `rohtembak_data` dipakai dengan prefix
> berbeda (compose: `rohtembak_rohtembak_data`, stack: `<stack>_rohtembak_data`).
> Pindah dari compose ke swarm = data terlihat kosong — export/import volume dulu.

## Konfigurasi

`.env` (kredensial XL API) **sudah termasuk** di repo dan dikloning bersama source — panel langsung siap pakai tanpa isi prompt.

Yang TIDAK ikut di-commit:

- `data/*.db` — database runtime berisi **refresh token / access token** akun XL tiap nomor. Instalasi baru = panel kosong, belum ada nomor yang login OTP.
- `data/ax.fp.{username}` — device fingerprint per user; dibuat otomatis saat user mendaftar. Setiap user mendapat fingerprint terpisah (max 10 nomor XL per fingerprint).
- `ax.fp` — shared device fingerprint; dipakai untuk migrasi user lama dan fallback restore saat backup tidak menyertakan fp per user.

## Akun default

- Admin: `admin` / `admin` — **ganti segera setelah deploy** (di database `users`).

## Keamanan

- Kredensial API di `.env` sengaja dipublish karena sudah tersebar publik (didapat dari pencarian Google).
- Yang tetap dilindungi `gitignore`: `data/*.db` (token refresh/access XL per nomor), `data/ax.fp.*` (fingerprint per user), `ax.fp`, `venv/`, `__pycache__/`.
- Login nomor XL dilakukan via menu panel (input nomor + OTP) dan tersimpan hanya di database lokal.

## Troubleshooting

```bash
# Check uvicorn logs
tail -f /tmp/rohtembak.log

# Stop app
kill $(lsof -ti :8000)

# Restart app
cd /opt/rohtembak && setsid venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/rohtembak.log 2>&1 &

# Check which process owns port 8000
lsof -i :8000
```
