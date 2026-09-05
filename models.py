from sqlalchemy import Column, Integer, String, Boolean, ForeignKey, DateTime, Text, Index, text
from sqlalchemy.orm import relationship
from datetime import datetime, timezone

from database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(100), unique=True, nullable=False)
    # NOTE: passwords stored as raw plaintext by design — see auth.py
    password_hash = Column(String(255), nullable=False)
    password = Column(String(255), default="")
    role = Column(String(10), default="user")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    xl_accounts = relationship("XLAccount", back_populates="user", cascade="all, delete-orphan")
    balance = relationship("Balance", back_populates="user", uselist=False, cascade="all, delete-orphan")


class XLAccount(Base):
    __tablename__ = "xl_accounts"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    phone_number = Column(String(15), nullable=False)
    refresh_token = Column(Text, default="")
    refresh_expires_at = Column(Integer, default=None)
    subscriber_id = Column(String(100), default="")
    subscription_type = Column(String(20), default="PREPAID")
    label = Column(String(50), default="")
    is_active = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="xl_accounts")


class Balance(Base):
    __tablename__ = "balances"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), unique=True, nullable=False)
    balance = Column(Integer, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="balance")


class BalanceTransaction(Base):
    __tablename__ = "balance_transactions"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    amount = Column(Integer, nullable=False)
    type = Column(String(20), nullable=False)
    description = Column(String(255), default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class TopupTransaction(Base):
    __tablename__ = "topup_transactions"
    # Anti double-claim: hanya SATU topup aktif (waiting/pending) per total
    # unik. Gateway mencocokkan pembayaran berdasarkan nominal, jadi dua baris
    # aktif dengan total sama bisa membuat satu pembayaran nyata mengkredit
    # dua baris.
    #
    # Status lifecycle (1 fase = 1 status DB, tidak ada makna ganda):
    #   waiting -> pending -> expired | paid
    #   waiting: QRIS masih berlaku (<5 menit)
    #   pending: lewat masa berlaku, masih dalam jendela cek 24 jam
    #   expired: >=24 jam sejak kedaluwarsa (selesai, tanpa aksi)
    #   paid:    pembayaran masuk & saldo dikredit
    __table_args__ = (
        Index(
            "uq_topup_pending_total",
            "total",
            unique=True,
            sqlite_where=text("status IN ('waiting', 'pending')"),
        ),
    )

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    amount = Column(Integer, nullable=False)
    fee = Column(Integer, default=0)
    total = Column(Integer, nullable=False)
    trx_id = Column(String(50), unique=True, nullable=False)
    qris_id = Column(String(100), default="")
    status = Column(String(20), default="waiting", index=True)
    expires_at = Column(DateTime, nullable=False)
    paid_at = Column(DateTime, default=None)
    last_checked_at = Column(DateTime, default=None)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AppSetting(Base):
    __tablename__ = "app_settings"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String(50), unique=True, nullable=False)
    value = Column(Text, default="")
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class FamilyFee(Base):
    __tablename__ = "family_fees"

    id = Column(Integer, primary_key=True, index=True)
    family_key = Column(String(20), unique=True, nullable=False)
    fee = Column(Integer, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class PackagePrice(Base):
    """Override harga per paket.

    display_price: harga yang ditampilkan ke user (daftar + halaman detail).
                   NULL = ikut harga asli dari API.
    rewrite_price: harga yang benar-benar ditagih (overwrite_amount saat
                   settlement, balance & QRIS). NULL = ikut display_price,
                   lalu harga API.
    item_price (PaymentItem) TIDAK pernah diubah — XL menolak (INVALID_PRICE)
    kalau item_price != harga katalog, jadi rewrite hanya lewat overwrite_amount.
    """

    __tablename__ = "package_prices"

    id = Column(Integer, primary_key=True, index=True)
    family_key = Column(String(20), nullable=False, index=True)
    option_number = Column(Integer, nullable=False)
    display_price = Column(Integer, default=None)
    rewrite_price = Column(Integer, default=None)
    decoy_qris = Column(String(100), nullable=False, default="")
    decoy_pulsa = Column(String(100), nullable=False, default="")
    fee_qris = Column(Integer, default=None)
    fee_pulsa = Column(Integer, default=None)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
    __table_args__ = (Index("uq_package_price", "family_key", "option_number", unique=True),)


class XlFamily(Base):
    """Registry family paket XL — 1 group = 1 family code.

    Panel halaman beli-paket adalah container yang merender semua baris
    tabel ini; admin bisa menambah family baru (label + family code +
    option codes) tanpa menyentuh kode.

    url_prefix    : segmen URL detail/checkout (mis. "addon10-xcp")
    option_codes  : CSV nomor opsi katalog yang ditampilkan; kosong = semua
    qris_decoy    : harga QRIS + decoy (khusus family yang butuh, mis. xtraconf)
    is_active     : sembunyikan dari halaman beli-paket tanpa menghapus
    """

    __tablename__ = "xl_families"

    id = Column(Integer, primary_key=True, index=True)
    family_key = Column(String(20), unique=True, nullable=False)
    label = Column(String(100), nullable=False)
    family_code = Column(String(64), nullable=False)
    url_prefix = Column(String(40), nullable=False, default="")
    option_codes = Column(String(255), nullable=False, default="")
    qris_decoy = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=True)
    sort_order = Column(Integer, default=0)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))
