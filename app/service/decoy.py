# Decoy package management
import json
import os
import re

from app.client.engsel import get_family, get_package
from app.type_dict import PaymentItem

DECOY_DATA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "decoy_data"
)

DECOY_TYPES = ("qris", "balance")


def decoy_slug(raw: str) -> str:
    """Flatten a label into a file-friendly slug: lowercase, no spaces/symbols.
    "Tiktok 2GB" -> "tiktok2gb"."""
    return re.sub(r"[^a-zA-Z0-9]", "", (raw or "").lower())[:60]


def decoy_label(name: str, cfg: dict | None) -> str:
    """Display label for a decoy — stored in the config JSON, falls back to
    the slug/filename when absent."""
    cfg = cfg or {}
    return (cfg.get("label") or "").strip() or name


def decoy_type_dir(payment_type: str) -> str:
    return os.path.join(DECOY_DATA_DIR, payment_type)


def decoy_config_path(payment_type: str, name: str = "default") -> str:
    path = os.path.join(decoy_type_dir(payment_type), f"{name}.json")
    if os.path.exists(path):
        return path
    # Fallback legacy: decoy_data/decoy-default-{type}.json
    if name == "default":
        legacy = os.path.join(DECOY_DATA_DIR, f"decoy-default-{payment_type}.json")
        if os.path.exists(legacy):
            return legacy
    return path


def list_decoy_names(payment_type: str) -> list[str]:
    names = []
    dirpath = decoy_type_dir(payment_type)
    if os.path.isdir(dirpath):
        for fn in sorted(os.listdir(dirpath)):
            if fn.endswith(".json"):
                names.append(fn[:-5])
    return names


def load_decoy_config(payment_type: str, name: str = "default") -> dict | None:
    path = decoy_config_path(payment_type, name)
    if not os.path.exists(path):
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_decoy_config(payment_type: str, name: str, config: dict) -> str:
    dirpath = decoy_type_dir(payment_type)
    os.makedirs(dirpath, exist_ok=True)
    path = os.path.join(dirpath, f"{name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)
    return path


def delete_decoy_config(payment_type: str, name: str) -> bool:
    # Hapus config default juga: bisa legacy decoy-default-{type}.json maupun
    # decoy_data/{type}/default.json. Default sekarang "biasa" — boleh dihapus.
    path = decoy_config_path(payment_type, name)
    if path and os.path.exists(path):
        os.remove(path)
        return True
    # Berkas legacy mungkin sudah hilang tapi file default.json masih ada.
    if name == "default":
        alt = os.path.join(decoy_type_dir(payment_type), "default.json")
        if alt != path and os.path.exists(alt):
            os.remove(alt)
            return True
    return False


def resolve_decoy_package(api_key: str, tokens: dict, config: dict) -> dict | None:
    family_data = get_family(
        api_key,
        tokens,
        config["family_code"],
        config.get("is_enterprise"),
        config.get("migration_type"),
    )
    if not family_data:
        return None

    option_code = None
    for variant in family_data["package_variants"]:
        if variant["package_variant_code"] != config["variant_code"]:
            continue
        for option in variant["package_options"]:
            if option["order"] == config.get("order"):
                option_code = option["package_option_code"]
                break
        break

    if option_code is None:
        return None

    return get_package(api_key, tokens, option_code, config["family_code"], config["variant_code"])


def build_decoy_item(api_key: str, tokens: dict, payment_type: str = "balance", name: str = "default") -> PaymentItem | None:
    config = load_decoy_config(payment_type, name)
    if not config:
        return None

    package_detail = resolve_decoy_package(api_key, tokens, config)
    if not package_detail:
        return None

    option = package_detail.get("package_option", {})
    return PaymentItem(
        item_code=option.get("package_option_code", ""),
        product_type="",
        item_price=option.get("price", config.get("price", 0)),
        item_name=option.get("name", ""),
        tax=0,
        token_confirmation=package_detail.get("token_confirmation", ""),
    )