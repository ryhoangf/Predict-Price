from __future__ import annotations

import json
import re
from typing import Any

import pandas as pd


BAD_AMBIGUOUS_STORAGE_GB = {"0", "1", "4", "8", "12"}
GENERIC_STORAGE_RE = re.compile(r"^\s*[A-Za-z]+\s+\d+\s*(?:GB|TB)\s*$", re.IGNORECASE)
HIGH_END_1TB_TOKENS = {"pro", "ultra", "max", "fold"}
GENERIC_MODEL_NAMES = {
    "android",
    "iphone",
    "galaxy",
    "pixel",
    "xperia",
    "aquos",
    "redmi",
    "xiaomi",
    "oppo",
    "sony",
    "samsung",
    "motorola",
}

_FAMILY_PATTERNS: dict[str, re.Pattern[str]] = {
    "iphone": re.compile(r"\biphone\s*(?:se\s*\d*|\d{1,2}\s*(?:pro\s*max|pro|max|plus|mini|s)?)?\b", re.IGNORECASE),
    "galaxy": re.compile(r"\bgalaxy\s+(?:z\s*)?(?:s|a|m|note|fold|flip)?\s*\d{1,2}\w*|\bgalaxy\s+(?:note|fold|flip|ultra)\b", re.IGNORECASE),
    "pixel": re.compile(r"\bpixel\s+\d{1,2}\w*\b", re.IGNORECASE),
    "xperia": re.compile(r"\bxperia\s+[a-z0-9ivx]+\b", re.IGNORECASE),
    "aquos": re.compile(r"\baquos\s+[a-z0-9]+\b", re.IGNORECASE),
    "redmi": re.compile(r"\bredmi\s+(?:note\s*)?\d{1,2}\w*\b", re.IGNORECASE),
    "poco": re.compile(r"\bpoco\s+[fxmc]\s*\d+\w*\b", re.IGNORECASE),
    "xiaomi": re.compile(r"\bxiaomi\s+\d{1,2}\w*\b|\bmi\s+\d{1,2}\w*\b", re.IGNORECASE),
    "oppo": re.compile(r"\b(?:oppo\s+)?(?:reno|find)\s*\d+\w*\b|\boppo\s+a\s*\d+\w*\b", re.IGNORECASE),
    "huawei": re.compile(r"\b(?:huawei\s+)?(?:mate|nova|enjoy|p)\s*\d+\w*\b", re.IGNORECASE),
    "motorola": re.compile(r"\b(?:motorola\s+)?(?:moto|edge|razr)\s+[a-z]?\s*\d+\w*\b", re.IGNORECASE),
    "asus": re.compile(r"\b(?:zenfone|rog\s+phone)\s*\d+\w*\b", re.IGNORECASE),
    "realme": re.compile(r"\brealme\s+\d+\w*\b|\bgt\s+\d+\w*\b", re.IGNORECASE),
}


def present(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return str(value).strip() != ""


def normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def compact_identity_text(*values: Any, limit: int = 1400) -> str:
    text = normalize_text(" ".join(str(v) for v in values if present(v)))
    for marker in (" #", "ご覧いただき", "ご覧頂き", "よろしくお願い", "注意事項"):
        idx = text.find(marker)
        if idx > 0:
            text = text[:idx]
    return text[:limit].strip()


def model_family_hits(text: Any) -> set[str]:
    text_value = normalize_text(text)
    if not text_value:
        return set()
    return {name for name, pattern in _FAMILY_PATTERNS.items() if pattern.search(text_value)}


def is_mixed_model_text(text: Any) -> bool:
    return len(model_family_hits(text)) >= 2


def _json_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not present(raw):
        return {}
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def has_explicit_storage_marker(text: Any, storage: Any) -> bool:
    if not present(storage):
        return True
    storage_s = str(storage).strip()
    text_s = normalize_text(text).lower()
    if storage_s == "1024":
        return bool(re.search(r"\b(?:1024\s*gb|1\s*tb)\b", text_s, re.IGNORECASE))
    return bool(re.search(rf"\b{re.escape(storage_s)}\s*(?:gb|ギガ)\b", text_s, re.IGNORECASE))


def is_generic_identity_row(row: dict[str, Any]) -> bool:
    model_line = normalize_text(row.get("model_line"))
    model_number = normalize_text(row.get("model_number"))
    standard_name = normalize_text(row.get("standard_name") or row.get("name"))
    brand = normalize_text(row.get("brand"))
    if not model_line or not model_number:
        return True
    if brand and standard_name and GENERIC_STORAGE_RE.match(standard_name):
        return True
    if standard_name.strip().lower() in GENERIC_MODEL_NAMES:
        return True
    return False


def is_suspicious_future_or_unknown_model(row: dict[str, Any]) -> bool:
    model_line = normalize_text(row.get("model_line")).lower()
    model_number = normalize_text(row.get("model_number")).lower()
    standard_name = normalize_text(row.get("standard_name") or row.get("name")).lower()

    if model_line == "iphone" and model_number:
        m = re.match(r"(\d{1,2})", model_number)
        if m and int(m.group(1)) > 17:
            return True
    if model_line == "pixel" and model_number:
        m = re.match(r"(\d{1,2})", model_number)
        if m and int(m.group(1)) > 10:
            return True
    if model_line == "galaxy" and model_number:
        m = re.search(r"\bs\s*(\d{1,2})", model_number)
        if m and int(m.group(1)) > 26:
            return True
    if re.search(r"\b(?:iphone\s*1[89]|pixel\s*1[1-9]|galaxy\s*s\s*2[7-9])\b", standard_name):
        return True
    return False


def identity_quality_reason(row: dict[str, Any]) -> str | None:
    title_text = compact_identity_text(row.get("name_raw"), row.get("name"))
    title_families = model_family_hits(title_text)
    description_text = compact_identity_text(row.get("description"))
    text_value = title_text or description_text
    if len(title_families) >= 2:
        return "mixed_model_families"
    if not title_families and is_mixed_model_text(description_text):
        return "mixed_model_families"

    if is_generic_identity_row(row):
        return "generic_brand_storage_without_model"

    if is_suspicious_future_or_unknown_model(row):
        return "suspicious_future_or_unknown_model"

    model_number = normalize_text(row.get("model_number"))
    if model_number.isdigit():
        try:
            model_year = int(model_number)
        except ValueError:
            model_year = 0
        if 2020 <= model_year <= 2030:
            return "suspicious_year_model_number"

    specs = _json_dict(row.get("base_specs"))
    storage = normalize_text(specs.get("storage"))
    ram = normalize_text(specs.get("ram"))
    if storage in BAD_AMBIGUOUS_STORAGE_GB:
        if storage == ram or not has_explicit_storage_marker(text_value, storage):
            return "ambiguous_storage_value"
    if storage == "1024":
        identity_text = normalize_text(
            " ".join(
                str(row.get(k) or "")
                for k in ("standard_name", "name", "model_line", "model_number", "variant")
            )
        ).lower()
        if not any(token in identity_text.split() for token in HIGH_END_1TB_TOKENS):
            return "suspicious_1tb_storage"
    if storage.isdigit():
        try:
            storage_gb = int(storage)
        except ValueError:
            storage_gb = 0
        if storage_gb >= 2048:
            return "suspicious_large_storage"

    return None


def split_identity_quality_gate(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        return df, pd.DataFrame()

    checked = df.copy()
    checked["_identity_quality_reason"] = checked.apply(
        lambda row: identity_quality_reason(row.to_dict()),
        axis=1,
    )
    review_mask = checked["_identity_quality_reason"].notna()
    review_df = checked[review_mask].copy()
    keep_df = checked[~review_mask].copy()

    if not review_df.empty:
        review_df["product_match_confidence"] = 0.0
        review_df["product_match_reason"] = review_df["_identity_quality_reason"]

    return (
        keep_df.drop(columns=["_identity_quality_reason"], errors="ignore"),
        review_df.drop(columns=["_identity_quality_reason"], errors="ignore"),
    )
