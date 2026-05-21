"""Runtime knobs for buyee_http (mirrors buyee.jp/config.json + env overrides)."""

from __future__ import annotations

import os
from pathlib import Path

_PREDICTPRICE_DIR = Path(__file__).resolve().parent.parent
_BUYEE_JP_DIR = _PREDICTPRICE_DIR.parent.parent / "buyee.jp"

BASE_DIR = _PREDICTPRICE_DIR
BASE_URL = "https://buyee.jp"

# --- Proxy keys file (default: predictprice/proxy_keys.txt, else buyee.jp) ---
_default_keys = _PREDICTPRICE_DIR / "proxy_keys.txt"
_fallback_keys = _BUYEE_JP_DIR / "proxy_keys.txt"
PROXY_KEYS_FILE = Path(
    os.getenv(
        "PROXY_KEYS_FILE",
        str(_default_keys if _default_keys.is_file() else _fallback_keys),
    )
).resolve()

USE_PROXY: bool = os.getenv("USE_PROXY", "1") not in ("0", "false", "False", "")

# Base URL get.php — key/nhamang/tinhthanh gửi qua query params (khớp config.py cũ).
PROXY_XOAY_API_BASE: str = os.getenv(
    "PROXY_XOAY_API_URL",
    "https://proxyxoay.shop/api/get.php",
).split("?", 1)[0].strip()
# Giữ tên cũ cho buyee.jp-style .format(key=...) nếu ai đó vẫn dùng template đủ query.
PROXY_API_URL: str = os.getenv(
    "PROXY_XOAY_API_URL_TEMPLATE",
    "https://proxyxoay.shop/api/get.php?key={key}&nhamang=random&tinhthanh=0&whitelist=",
)
PROXY_XOAY_NHAMANG = os.getenv("PROXY_XOAY_NHAMANG", "random").strip()
PROXY_XOAY_TINHTHANH = os.getenv("PROXY_XOAY_TINHTHANH", "0").strip()
PROXY_XOAY_WHITELIST = os.getenv("PROXY_XOAY_WHITELIST", "").strip()

PROXY_MIN_LIFETIME_SEC: int = int(os.getenv("PROXY_MIN_LIFETIME_SEC", "60"))
PROXY_FAILURE_PAUSE_SEC: int = int(os.getenv("PROXY_FAILURE_PAUSE_SEC", "20"))
PROXY_MAX_RETRIES: int = int(os.getenv("PROXY_MAX_RETRIES", "3"))

# Legacy single key (fallback when file empty)
PROXY_XOAY_KEY = os.getenv("PROXY_XOAY_KEY", "").strip()
PROXY_XOAY_KEYS_RAW = os.getenv("PROXY_XOAY_KEYS", "").strip()

# --- HTTP ---
REQUEST_TIMEOUT: int = int(os.getenv("BUYEE_REQUEST_TIMEOUT_SEC", "20"))
USER_AGENT: str = os.getenv(
    "BUYEE_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
)
DEFAULT_HEADERS: dict[str, str] = {
    "User-Agent": USER_AGENT,
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}
BAD_STATUSES: set[int] = {202, 403, 429, 503}

BUYEE_CURL_IMPERSONATE = os.getenv("BUYEE_CURL_IMPERSONATE", "chrome").strip() or "chrome"

# --- WAF / Playwright ---
WAF_SEED_ITEM_IDS: list[str] = [
    x.strip()
    for x in os.getenv(
        "BUYEE_WAF_SEED_ITEM_IDS",
        "75721385d5c1201c811f5dc57ff15bd2,"
        "fae5da6ba4238f7c5e68589c318c4f25,"
        "bac9d51f4475f6592cb6ee3c04bceab4",
    ).split(",")
    if x.strip()
]
WAF_BROWSER_TIMEOUT_SEC: int = int(os.getenv("BUYEE_WAF_BROWSER_TIMEOUT_SEC", "60"))
WAF_HEADLESS: bool = os.getenv("BUYEE_WAF_HEADLESS", "1") not in ("0", "false", "False", "")
BUYEE_WAF_WARMUP: bool = os.getenv("BUYEE_WAF_WARMUP", "1") not in (
    "0",
    "false",
    "False",
    "",
)

# Listing/detail pacing (buyee.jp defaults)
LISTER_DELAY_SEC: float = float(os.getenv("BUYEE_LISTER_DELAY", "3.0"))
WORKER_DELAY_SEC: float = float(os.getenv("BUYEE_WORKER_DELAY", "0.5"))
