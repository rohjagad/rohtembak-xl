import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./data/me-cli-web.db")

if DATABASE_URL.startswith("sqlite:///"):
    _db_path = DATABASE_URL[len("sqlite:///"):].rsplit("?", 1)[0]
    _db_dir = os.path.dirname(os.path.abspath(_db_path))
    os.makedirs(_db_dir, exist_ok=True)

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=engine)
    with engine.connect() as conn:
        cols = [row[1] for row in conn.execute(__import__("sqlalchemy").text("PRAGMA table_info(users)"))]
        if "password" not in cols:
            conn.execute(__import__("sqlalchemy").text("ALTER TABLE users ADD COLUMN password VARCHAR(255) DEFAULT ''"))
            conn.commit()
        xl_cols = [row[1] for row in conn.execute(__import__("sqlalchemy").text("PRAGMA table_info(xl_accounts)"))]
        if "refresh_expires_at" not in xl_cols:
            conn.execute(__import__("sqlalchemy").text("ALTER TABLE xl_accounts ADD COLUMN refresh_expires_at INTEGER DEFAULT NULL"))
            conn.commit()
        topup_cols = [row[1] for row in conn.execute(__import__("sqlalchemy").text("PRAGMA table_info(topup_transactions)"))]
        if "last_checked_at" not in topup_cols:
            conn.execute(__import__("sqlalchemy").text("ALTER TABLE topup_transactions ADD COLUMN last_checked_at DATETIME DEFAULT NULL"))
            conn.commit()
        bt_cols = [row[1] for row in conn.execute(__import__("sqlalchemy").text("PRAGMA table_info(balance_transactions)"))]
        if "topup_id" not in bt_cols:
            conn.execute(__import__("sqlalchemy").text("ALTER TABLE balance_transactions ADD COLUMN topup_id INTEGER DEFAULT NULL"))
            conn.commit()
        # Migrasi status topup ke lifecycle 4 fase: waiting -> pending -> expired | paid.
        # Sebelumnya status 'expired' bermakna ganda (5 mnt-24 jam vs >=24 jam).
        conn.execute(__import__("sqlalchemy").text(
            "UPDATE topup_transactions SET status='waiting' "
            "WHERE status='pending' AND expires_at > datetime('now')"
        ))
        conn.execute(__import__("sqlalchemy").text(
            "UPDATE topup_transactions SET status='pending' "
            "WHERE status='expired' AND datetime(expires_at, '+24 hours') > datetime('now')"
        ))
        conn.commit()
        idx = conn.execute(__import__("sqlalchemy").text(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='uq_topup_pending_total'"
        )).fetchone()
        # Selalu drop + recreate (idempotent & aman): definisi index bisa
        # berubah antar versi (mis. migrasi status topup 4 fase), dan
        # create_all() tidak pernah mengubah index yang sudah ada. Overhead
        # drop/recreate per boot sangat kecil; kalau definisinya sudah sama,
        # hasilnya index yang identik.
        if idx:
            try:
                conn.execute(__import__("sqlalchemy").text(
                    "DROP INDEX uq_topup_pending_total"
                ))
            except Exception as e:
                print(f"[init_db] warning: gagal drop index uq_topup_pending_total: {e}")
        try:
            conn.execute(__import__("sqlalchemy").text(
                "CREATE UNIQUE INDEX uq_topup_pending_total ON topup_transactions (total) "
                "WHERE status IN ('waiting', 'pending')"
            ))
            conn.commit()
        except Exception as e:
            print(f"[init_db] warning: gagal membuat index uq_topup_pending_total: {e}")
