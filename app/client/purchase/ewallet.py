import json
import uuid
import time
import requests

from datetime import datetime, timezone

from app.client.engsel import BASE_API_URL, UA, intercept_page, send_api_request
from app.client.encrypt import API_KEY, decrypt_xdata, encryptsign_xdata, java_like_timestamp, get_x_signature_payment

# Metode e-wallet multipayment XL (referensi: me-cli-sunset purchase/ewallet.py).
# DANA/OVO butuh wallet_number (08xx); SHOPEEPAY/GOPAY tidak.
EWALLET_TYPES = ("DANA", "SHOPEEPAY", "GOPAY", "OVO")
EWALLET_NEEDS_NUMBER = ("DANA", "OVO")


def validate_wallet(wallet_type: str, wallet_number: str):
    """Return pesan error atau None bila valid."""
    if wallet_type not in EWALLET_TYPES:
        return "Metode e-wallet tidak dikenal."
    if wallet_type in EWALLET_NEEDS_NUMBER:
        wn = (wallet_number or "").strip()
        if not (wn.startswith("08") and wn.isdigit() and 10 <= len(wn) <= 13):
            return "Nomor e-wallet tidak valid — harus diawali 08 dan 10-13 digit."
    return None


def settlement_ewallet(
    api_key: str,
    tokens: dict,
    items: list,
    payment_for: str,
    ask_overwrite: bool = False,
    overwrite_amount: int = -1,
    token_confirmation_idx: int = 0,
    wallet_type: str = "",
    wallet_number: str = "",
):
    """ask_overwrite ada hanya agar kompatibel dengan pemanggilan generik
    _settle_with_decoy (panel selalu kirim overwrite_amount; tidak ada input
    interaktif)."""
    """Settlement multipayment e-wallet. Return dict respons XL.

    UNKNOWN (bukan None) saat koneksi/respon tidak terbaca — hasil tidak
    diketahui bukan kegagalan bersih (settlement bisa sudah terbentuk).
    """
    token_confirmation = items[token_confirmation_idx]["token_confirmation"]
    payment_targets = ""
    for item in items:
        if payment_targets != "":
            payment_targets += ";"
        payment_targets += item["item_code"]

    amount_int = int(overwrite_amount or 0)

    intercept_page(api_key, tokens, items[0]["item_code"], False)

    # Get payment methods
    payment_path = "payments/api/v8/payment-methods-option"
    payment_payload = {
        "payment_type": "PURCHASE",
        "is_enterprise": False,
        "payment_target": items[token_confirmation_idx]["item_code"],
        "lang": "en",
        "is_referral": False,
        "token_confirmation": token_confirmation
    }

    print("Getting payment methods...")
    payment_res = send_api_request(api_key, payment_path, payment_payload, tokens["id_token"], "POST")
    if not isinstance(payment_res, dict) or payment_res.get("status") != "SUCCESS":
        print("Failed to fetch payment methods.")
        print(f"Error: {payment_res}")
        return {"status": "FAILED", "message": "Gagal mengambil metode pembayaran XL."}

    pay_data = payment_res.get("data") or {}
    token_payment = pay_data.get("token_payment")
    ts_to_sign = pay_data.get("timestamp")
    if not token_payment or not ts_to_sign:
        print("Payment methods tidak lengkap (token_payment/timestamp kosong).")
        return {"status": "FAILED", "message": "Respon XL tidak lengkap — coba lagi."}

    # Settlement request
    path = "payments/api/v8/settlement-multipayment/ewallet"
    settlement_payload = {
        "akrab": {
            "akrab_members": [],
            "akrab_parent_alias": "",
            "members": []
        },
        "can_trigger_rating": False,
        "total_discount": 0,
        "coupon": "",
        "payment_for": payment_for,
        "topup_number": "",
        "is_enterprise": False,
        "autobuy": {
            "is_using_autobuy": False,
            "activated_autobuy_code": "",
            "autobuy_threshold_setting": {
                "label": "",
                "type": "",
                "value": 0
            }
        },
        "cc_payment_type": "",
        "access_token": tokens["access_token"],
        "is_myxl_wallet": False,
        "wallet_number": wallet_number or "",
        "additional_data": {},
        "total_amount": amount_int,
        "total_fee": 0,
        "is_use_point": False,
        "lang": "en",
        "items": items,
        "verification_token": token_payment,
        "payment_method": wallet_type,
        "timestamp": int(time.time())
    }

    encrypted_payload = encryptsign_xdata(
        api_key=api_key,
        method="POST",
        path=path,
        id_token=tokens["id_token"],
        payload=settlement_payload
    )

    xtime = int(encrypted_payload["encrypted_body"]["xtime"])
    sig_time_sec = (xtime // 1000)
    x_requested_at = datetime.fromtimestamp(sig_time_sec, tz=timezone.utc).astimezone()
    settlement_payload["timestamp"] = ts_to_sign

    body = encrypted_payload["encrypted_body"]
    x_sig = get_x_signature_payment(
        api_key,
        tokens["access_token"],
        ts_to_sign,
        payment_targets,
        token_payment,
        wallet_type,
        payment_for,
        path
    )

    headers = {
        "host": BASE_API_URL.replace("https://", ""),
        "content-type": "application/json; charset=utf-8",
        "user-agent": UA,
        "x-api-key": API_KEY,
        "authorization": f"Bearer {tokens['id_token']}",
        "x-hv": "v3",
        "x-signature-time": str(sig_time_sec),
        "x-signature": x_sig,
        "x-request-id": str(uuid.uuid4()),
        "x-request-at": java_like_timestamp(x_requested_at),
        "x-version-app": "8.9.0",
    }

    url = f"{BASE_API_URL}/{path}"
    print("Sending settlement request...")
    try:
        resp = requests.post(url, headers=headers, data=json.dumps(body), timeout=30)
    except requests.RequestException as e:
        # UNKNOWN, bukan None — settlement bisa sudah terbentuk di XL.
        print(f"[settlement-ewallet] network error: {e}")
        return {"status": "UNKNOWN", "message": "Koneksi ke XL terputus saat memproses."}

    try:
        decrypted_body = decrypt_xdata(api_key, json.loads(resp.text))
        if not isinstance(decrypted_body, dict):
            return {"status": "UNKNOWN", "message": "Respon XL tidak terbaca."}
        if decrypted_body.get("status") == "SUCCESS":
            # Deeplink belum selalu ada — dump bentuk data sekali untuk
            # memetakan key yang benar (deeplink/redirect_url/dll).
            data = decrypted_body.get("data") or {}
            print(f"[settlement-ewallet] data keys: {sorted(data.keys())}")
        return decrypted_body
    except Exception as e:
        print("[decrypt err]", e)
        return {"status": "UNKNOWN", "message": "Respon XL tidak terbaca."}
