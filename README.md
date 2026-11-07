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
├── entrypoint.sh         # PRODUCTION Docker entrypoint (branch main)
├── docker-compose.yml    # production container (Docker, Debian 12)
├── data/                 # runtime DB + fingerprint (gitignored, dibuat otomatis)
├── .env                  # konfigurasi kredensial XL API (sudah terisi)
└── .env.example          # template konfigurasi
```

## Quick start (Docker, production)

```bash
git clone https://github.com/rohjagad/rohtembak-xl.git
cd rohtembak-xl
docker compose up -d
```

Buka `http://localhost:8600` — login `admin` / `admin`.

> Compose memakai named volume `rohtembak-xl-v1-install` untuk code + venv + `.env`, dan bind mount `./data` untuk data runtime (DB + fingerprint).
> Bootstrap (`entrypoint.sh`, apt, git clone, pip) berjalan sekali saat volume install masih kosong. Kalau bootstrap gagal, container tetap hidup dan error terlihat di `docker logs`.
> Target deploy: Docker pada Debian 12 mutable (SELinux tidak aktif), jadi bind mount tidak memakai relabel `:Z`.
> Jika suatu saat di-deploy di host SELinux enforcing (Fedora/Silverblue), tambahkan `:Z` pada bind mount di `docker-compose.yml`.

## Update aplikasi

Perubahan di GitHub TIDAK masuk otomatis. Update manual setelah `git push`:

```bash
docker exec rohtembak-xl-v1 bash -c "cd /opt/rohtembak-xl && git pull --ff-only"
docker restart rohtembak-xl-v1
```

Swarm: `docker exec <container> bash -c "cd /opt/rohtembak-xl && git pull --ff-only"` lalu `docker service update --force rohtembak-xl-v1`.

> Repo private: set remote ber-token SEKALI agar `git pull` tetap jalan
> setelah repo dikembalikan private:
> `docker exec rohtembak-xl-v1 bash -c "cd /opt/rohtembak-xl && git remote set-url origin https://<PAT>@github.com/rohjagad/rohtembak-xl"`

## Deployment notes

> `entrypoint.sh` bisa dipakai sebagai entrypoint container maupun installer bare-metal.
> Bare metal: script menjalankan uvicorn di foreground — jalankan via `tmux`/`screen`
> atau systemd bila ingin tetap hidup setelah SSH logout.
>
> Repo ini private: pada deploy fresh, buat repo public sementara agar clone /
> raw.githubusercontent bisa diakses, lalu kembalikan ke private setelah jalan.
>
> Docker Compose memakai named volume `rohtembak-xl-v1-install` untuk install
> (code/venv/.env) dan bind mount `./data` untuk data runtime.
> Swarm memakai named volume `rohtembak-xl-v1-data` (`<stack>_rohtembak-xl-v1-data`),
> jadi pindah compose ↔ swarm butuh copy data dulu.
> Compose tidak butuh `entrypoint.sh` di host: script diunduh otomatis ke volume install.

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

Docker (target deploy):

```bash
# Container logs
docker logs -f rohtembak-xl-v1

# Restart container
docker restart rohtembak-xl-v1

# Cek port mapping (host 8600 -> container 8000)
docker port rohtembak-xl-v1

# Cek status/restart count
docker inspect -f 'status={{.State.Status}} restarts={{.RestartCount}} oom={{.State.OOMKilled}}' rohtembak-xl-v1
```

Kalau container `Up` tapi aplikasi belum jalan, cek pesan bootstrap di log
(DNS ke `deb.debian.org`/`github.com`/`pypi.org`, jam server, atau disk penuh).

Bare metal:

```bash
# Check uvicorn logs
tail -f /tmp/rohtembak-xl.log

# Stop app
kill $(lsof -ti :8000)

# Restart app
cd /opt/rohtembak-xl && setsid venv/bin/python -m uvicorn main:app --host 0.0.0.0 --port 8000 > /tmp/rohtembak-xl.log 2>&1 &

# Check which process owns port 8000
lsof -i :8000
```
