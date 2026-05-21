import os
import re
import json
import threading
import requests
import time
import random
from pathlib import Path
from urllib.parse import quote, urljoin, urlparse, urlunparse
from dotenv import load_dotenv

try:
    from curl_cffi import requests as curl_requests

    _CURL_CFFI_AVAILABLE = True
except ImportError:
    curl_requests = None  # type: ignore
    _CURL_CFFI_AVAILABLE = False

_CONFIG_DIR = Path(__file__).resolve().parent
# Spark thường copy config.py vào /tmp/spark-.../userFiles — không có .env cạnh đó;
# trong container app mount tại /opt/spark/apps/predictprice → luôn thử .env ở đó.
_DOCKER_PREDICTPRICE_ENV = Path("/opt/spark/apps/predictprice/.env")
for _env_path in (_CONFIG_DIR / ".env", _DOCKER_PREDICTPRICE_ENV):
    if _env_path.is_file():
        load_dotenv(_env_path)
if not (_CONFIG_DIR / ".env").is_file() and not _DOCKER_PREDICTPRICE_ENV.is_file():
    load_dotenv()

# Cookie từ trình duyệt đã vào được buyee.jp (DevTools → Application → Cookie), dán nguyên chuỗi "name=value; ...".
# Cần khi server trả AWS WAF / thách thức JS (requests không có JS → không có dl.m-goodsTable).
BUYEE_COOKIE = os.getenv("BUYEE_COOKIE", "").strip()

# TLS: Python requests ≠ fingerprint Chrome → AWS WAF hay trả 202. curl-cffi bắt chước Chrome.
# Để trống BUYEE_HTTP_CLIENT và đã pip install curl-cffi → tự dùng curl. BUYEE_HTTP_CLIENT=requests để tắt.
BUYEE_HTTP_CLIENT = os.getenv("BUYEE_HTTP_CLIENT", "").strip().lower()
# Mặc định chrome136: AWS WAF Buyee thường trả 202 với chrome120/chrome131/edge101 cùng proxy datacenter.
BUYEE_CURL_IMPERSONATE = os.getenv("BUYEE_CURL_IMPERSONATE", "chrome136").strip() or "chrome136"
# Với curl-cffi: mặc định KHÔNG gửi User-Agent tùy chỉnh (TLS impersonate đã khớp UA+fingerprint).
BUYEE_FORCE_USER_AGENT = os.getenv("BUYEE_FORCE_USER_AGENT", "").strip().lower() in (
    "1",
    "true",
    "yes",
)
# Qua proxy: cookie copy từ trình duyệt máy bạn thường sai ngữ cảnh IP → WAF challenge. IWR của bạn không gửi cookie vẫn 200.
BUYEE_SEND_COOKIE = os.getenv("BUYEE_SEND_COOKIE", "1").strip().lower() not in (
    "0",
    "false",
    "no",
    "off",
)

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "ivaluate_datalake")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "raw_items")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3000")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "ivaluate")

def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        raw = (os.getenv(name, str(default)) or str(default)).strip()
        v = int(raw)
        return max(minimum, v)
    except ValueError:
        return max(minimum, default)


MAX_PAGES_MERCARI = _env_int("MAX_PAGES_MERCARI", 1)
MAX_PAGES_RAKUMA = _env_int("MAX_PAGES_RAKUMA", 1)
MAX_PAGES_YAHOO = _env_int("MAX_PAGES_YAHOO", 1)

WORKER_MONGO_URI = os.getenv(
    "WORKER_MONGO_URI",
    "mongodb://da-mongo:27017/",
)

# --- Buyee.jp-style multi-key proxy (one key per detail thread) ---
from buyee_http.proxy_manager import ProxyAssignments, WorkerProxy, load_keys
from buyee_http import settings as buyee_settings
from buyee_http.http_client import FetchError, fetch_response as _buyee_fetch_response
from buyee_http.thread_context import bind_thread_worker_proxy, get_thread_worker_proxy

PROXY_XOAY_KEY = os.getenv("PROXY_XOAY_KEY", "").strip()
PROXY_XOAY_KEYS: list[str] = load_keys()
PROXY_KEYS_FILE = buyee_settings.PROXY_KEYS_FILE
USE_PROXY = buyee_settings.USE_PROXY
PROXY_XOAY_API_URL = buyee_settings.PROXY_API_URL
PROXY_XOAY_NHAMANG = buyee_settings.PROXY_XOAY_NHAMANG
PROXY_XOAY_TINHTHANH = buyee_settings.PROXY_XOAY_TINHTHANH
PROXY_XOAY_WHITELIST = buyee_settings.PROXY_XOAY_WHITELIST
PROXY_FAILURE_PAUSE_SEC = buyee_settings.PROXY_FAILURE_PAUSE_SEC
PROXY_MAX_RETRIES = buyee_settings.PROXY_MAX_RETRIES

_keys_n = len(PROXY_XOAY_KEYS)
_raw_detail_workers = _env_int("DETAIL_FETCH_MAX_WORKERS", 6, minimum=1)
DETAIL_FETCH_MAX_WORKERS = min(_raw_detail_workers, _keys_n) if _keys_n else _raw_detail_workers

_assignments: ProxyAssignments | None = None
_assignments_lock = threading.Lock()
_pool_ready = False


class BuyeeHttpResponse:
    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text


def get_proxy_assignments() -> ProxyAssignments | None:
    return _assignments


def proxy_for_thread_index(idx: int) -> WorkerProxy | None:
    if _assignments is None:
        return None
    return _assignments.for_worker(idx)


def listing_worker_proxy() -> WorkerProxy | None:
    return proxy_for_thread_index(0)


def init_buyee_http_pool(warm_waf: bool | None = None) -> int:
    """Warm proxy API + optional WAF cookies (buyee.jp run.py startup)."""
    global _assignments, _pool_ready
    if not USE_PROXY:
        _pool_ready = True
        return 0
    keys = load_keys()
    if not keys:
        print("[buyee_http] WARNING: no proxy keys — direct fetch (WAF likely)")
        _pool_ready = True
        return 0
    with _assignments_lock:
        if _assignments is not None and _pool_ready:
            return len(_assignments.workers)
        _assignments = ProxyAssignments(keys)
        ok = _assignments.warm_up_all()
        do_waf = buyee_settings.BUYEE_WAF_WARMUP if warm_waf is None else warm_waf
        if do_waf:
            n_detail = min(DETAIL_FETCH_MAX_WORKERS, len(_assignments))
            waf_ok = _assignments.warm_waf_all(n_detail)
            print(
                f"[buyee_http] keys={len(keys)} proxy_ok={ok}/{len(keys)} "
                f"waf_ok={waf_ok}/{n_detail} detail_workers={DETAIL_FETCH_MAX_WORKERS}"
            )
        else:
            print(
                f"[buyee_http] keys={len(keys)} proxy_ok={ok}/{len(keys)} "
                f"(WAF warmup skipped) detail_workers={DETAIL_FETCH_MAX_WORKERS}"
            )
        _pool_ready = True
        return ok


def ensure_buyee_http_pool(warm_waf: bool | None = None) -> None:
    if not _pool_ready:
        init_buyee_http_pool(warm_waf=warm_waf)


def invalidate_rotating_proxy(worker_proxy: WorkerProxy | None = None) -> None:
    """Rotate proxy for one worker slot, or all slots if worker_proxy is None."""
    if worker_proxy is not None:
        worker_proxy.get(force_new=True)
        return
    if _assignments:
        for w in _assignments.workers:
            w.get(force_new=True)


def proxy_xoay_last_diagnostic() -> dict | None:
    """Legacy shim for debug.py."""
    return None


def _xoay_proxies_dict(force_refresh: bool = False):
    """Legacy shim: first listing worker proxy as requests dict."""
    wp = listing_worker_proxy()
    if wp is None:
        return None
    if force_refresh:
        wp.get(force_new=True)
    p = wp.get()
    return p.as_requests_dict() if p else None


# Proxy HTTP tĩnh (PROXY_FALLBACK_LIST) đã tắt — dùng buyee_http multi-key pool.
# # PROXY_FALLBACK_RAW = os.getenv("PROXY_FALLBACK_LIST", "").strip()
# # PROXY_LIST = [u.strip() for u in PROXY_FALLBACK_RAW.split(",") if u.strip()]


def _env_connect_read_timeout(name: str, default: tuple[float, float]) -> tuple[float, float]:
    """Mặc định rộng hơn cho Buyee qua proxy (tránh read quá ngắn → connection closed)."""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        if "," in raw:
            a, b = raw.split(",", 1)
            return (float(a.strip()), float(b.strip()))
        s = float(raw)
        return (s, s)
    except ValueError:
        return default


# BUYEE_PROXY_TIMEOUT / BUYEE_DIRECT_TIMEOUT: "connect,read" (giây). Qua proxy + curl-cffi connect dễ >15s.
# Mặc định (30, 90) khớp debug.py khi dùng curl-cffi — tránh timeout sớm so với bản debug.
PROXY_TIMEOUT = _env_connect_read_timeout("BUYEE_PROXY_TIMEOUT", (30.0, 90.0))
DIRECT_TIMEOUT = _env_connect_read_timeout("BUYEE_DIRECT_TIMEOUT", (10.0, 45.0))


def _resolve_worker_proxy(worker_proxy: WorkerProxy | None = None) -> WorkerProxy | None:
    if worker_proxy is not None:
        return worker_proxy
    wp = get_thread_worker_proxy()
    if wp is not None:
        return wp
    if PROXY_XOAY_KEYS and USE_PROXY:
        return listing_worker_proxy()
    return None


def fetch(
    url: str,
    headers: dict,
    worker_proxy: WorkerProxy | None = None,
    timeout_proxy=None,
    timeout_direct=None,
):
    """
    Buyee.jp-style fetch: per-thread WorkerProxy + WAF cookies + curl_cffi retry.
    Returns BuyeeHttpResponse(status_code, text) or None on failure.
    """
    del timeout_proxy, timeout_direct  # buyee_http uses REQUEST_TIMEOUT from settings
    ensure_buyee_http_pool()
    wp = _resolve_worker_proxy(worker_proxy)
    try:
        status, text = _buyee_fetch_response(url, wp, extra_headers=headers)
        return BuyeeHttpResponse(status, text)
    except FetchError:
        return None
    except Exception:
        return None


def fetch_with_session(
    session,
    url: str,
    headers: dict,
    worker_proxy: WorkerProxy | None = None,
    timeout_proxy=None,
    timeout_direct=None,
):
    """Same as fetch(); session kept for API compat (WAF cookies live on WorkerProxy)."""
    del session
    return fetch(
        url,
        headers,
        worker_proxy=worker_proxy,
        timeout_proxy=timeout_proxy,
        timeout_direct=timeout_direct,
    )


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/110.0.0.0 Safari/537.36"
)
HEADERS_BASE = {"User-Agent": USER_AGENT}
USER_AGENT_PS_IWR = (
    "Mozilla/5.0 (Windows NT 10.0; Microsoft Windows 10.0.26200; en-US) "
    "PowerShell/7.4.10"
)


def prefer_curl_cffi_for_buyee() -> bool:
    """True → dùng curl-cffi (JA3/TLS giống trình duyệt), tránh WAF 202 với requests thuần."""
    if not _CURL_CFFI_AVAILABLE:
        return False
    if BUYEE_HTTP_CLIENT in ("requests", "urllib", "std"):
        return False
    if BUYEE_HTTP_CLIENT in ("curl_cffi", "curl", "curlcffi", "1", "yes", "true"):
        return True
    return True


def create_buyee_session():
    """Session dùng cho Buyee: curl-cffi (khuyến nghị) hoặc requests."""
    if prefer_curl_cffi_for_buyee():
        return curl_requests.Session(impersonate=BUYEE_CURL_IMPERSONATE)
    s = requests.Session()
    s.trust_env = False
    return s


def _buyee_http_get(url: str, **kwargs):
    if prefer_curl_cffi_for_buyee():
        kwargs.setdefault("impersonate", BUYEE_CURL_IMPERSONATE)
        return curl_requests.get(url, **kwargs)
    return requests.get(url, **kwargs)


def buyee_minimal_headers_powershell() -> dict:
    """Gần IWR mặc định — chỉ khi buộc dùng requests và vẫn 202."""
    h = {"User-Agent": USER_AGENT_PS_IWR, "Accept": "*/*"}
    if BUYEE_COOKIE and BUYEE_SEND_COOKIE:
        h["Cookie"] = BUYEE_COOKIE
    return h


def buyee_bare_headers_like_iwr(send_cookie: bool | None = None) -> dict:
    """
    Gần Invoke-WebRequest tối đa: chỉ Accept */*, không Referer / Accept-Language.
    Với curl-cffi không ghi User-Agent (TLS impersonate tự gắn).
    """
    h = {"Accept": "*/*"}
    if not prefer_curl_cffi_for_buyee() or BUYEE_FORCE_USER_AGENT:
        h["User-Agent"] = USER_AGENT_PS_IWR
    use_ck = BUYEE_SEND_COOKIE if send_cookie is None else send_cookie
    if BUYEE_COOKIE and use_ck:
        h["Cookie"] = BUYEE_COOKIE
    return h


def buyee_page_headers(referer: str, send_cookie: bool | None = None) -> dict:
    """
    Header cho Buyee. Với curl-cffi + impersonate, tuyệt đối không ghi đè User-Agent kiểu Chrome/110
    (TLS lại là Chrome 120) — WAF hay trả 202. Để curl tự gắn UA khớp fingerprint.
    send_cookie: None → theo BUYEE_SEND_COOKIE; False → không gửi Cookie (giống IWR không -Headers).
    """
    h = {
        "Referer": referer,
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,"
            "image/avif,image/webp,*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9,ja;q=0.8",
    }
    if not prefer_curl_cffi_for_buyee() or BUYEE_FORCE_USER_AGENT:
        h = {**HEADERS_BASE, **h}
    use_ck = BUYEE_SEND_COOKIE if send_cookie is None else send_cookie
    if BUYEE_COOKIE and use_ck:
        h["Cookie"] = BUYEE_COOKIE
    return h


def response_looks_like_buyee_waf_challenge(resp) -> bool:
    """Body là trang thách thức AWS WAF (không có markup trang sản phẩm)."""
    if resp is None:
        return True
    try:
        sc = getattr(resp, "status_code", None)
        if sc == 202:
            return True
    except Exception:
        pass
    try:
        text = resp.text or ""
    except Exception:
        return True
    low = text[:240000].lower()
    if "gokuprops" in low or "window.awswaf" in low:
        return True
    if "challenge" in low and "awswaf" in low:
        return True
    return False


def looks_like_buyee_condition_label(label: str) -> bool:
    """Nhận diện dòng condition (EN/JP), tránh nhầm Bidder rating / shipping."""
    if not label:
        return False
    t = " ".join(str(label).split())
    low = t.lower()
    skip = (
        "bidder",
        "international shipping",
        "early finish",
        "automatic extension",
        "auction id",
        "number of bids",
        "highest bidder",
    )
    if any(x in low for x in skip):
        return False
    if "item condition" in low:
        return True
    if low.strip() == "condition":
        return True
    for jp in ("商品の状態", "コンディション", "状態"):
        if jp in t:
            return True
    return False


REFERERS = {
    "mercari": "https://buyee.jp/mercari/search",
    "rakuma":  "https://buyee.jp/rakuma/search",
    "yahoo":   "https://buyee.jp/item/search/category"
}

DELAY = (
    float(buyee_settings.LISTER_DELAY_SEC),
    float(buyee_settings.LISTER_DELAY_SEC) + 1.0,
)
LISTING_DELAY_SEC = buyee_settings.LISTER_DELAY_SEC
WORKER_DELAY_SEC = buyee_settings.WORKER_DELAY_SEC

ENDPOINTS = {
    "mercari_iframe": "https://buyee.jp/mercari/search",
    "mercari_category_id": os.getenv("MERCARI_CATEGORY_ID", "859"),
    "rakuma_search": "https://buyee.jp/rakuma/search",
    "rakuma_category_id": os.getenv("RAKUMA_CATEGORY_ID", "668"),
    "yahoo_base": "https://buyee.jp/item/search/category",
    "yahoo_category_id": os.getenv("YAHOO_CATEGORY_ID", "2084317598"),
}


def normalize_link(href: str) -> str:
    """
    Xóa '/undefined' nếu có & ghép domain buyee.jp
    """
    try:
        if not href:
            return None
        return urljoin("https://buyee.jp", str(href).replace("/undefined", ""))
    except Exception:
        return None

def _fetch_result_ok(result) -> bool:
    """Coi kết quả fetch chi tiết là hợp lệ để không retry (None/str/tuple)."""
    if result is None:
        return False
    if isinstance(result, (tuple, list)):
        return any(
            x is not None and (not isinstance(x, str) or str(x).strip())
            for x in result
        )
    if isinstance(result, str):
        return bool(result.strip())
    return True


def safe_fetch_with_retry(
    fetch_func,
    url,
    max_retries=1,
    invalidate_proxy_on_retry: bool = False,
):
    """
    Wrapper an toàn với retry logic. fetch_func có thể trả về str, tuple, hoặc None.
    invalidate_proxy_on_retry: trước mỗi lần thử lại, xóa cache proxy xoay để API cấp IP mới.
    """
    if not url:
        return None

    for attempt in range(max_retries + 1):
        try:
            result = fetch_func(url)
            if _fetch_result_ok(result):
                return result
            if attempt < max_retries:
                if invalidate_proxy_on_retry:
                    invalidate_rotating_proxy(get_thread_worker_proxy())
                time.sleep(random.uniform(0.3, 0.8))
        except Exception:
            if attempt == max_retries:
                return None
            if invalidate_proxy_on_retry:
                invalidate_rotating_proxy(get_thread_worker_proxy())
            time.sleep(random.uniform(0.3, 0.8))

    return None
