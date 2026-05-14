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

MAX_PAGES_MERCARI = 1
MAX_PAGES_RAKUMA = 1
MAX_PAGES_YAHOO = 1

WORKER_MONGO_URI = os.getenv(
    "WORKER_MONGO_URI",
    "mongodb://da-mongo:27017/",
)
# Proxy xoay (proxyxoay.shop) — chỉ dùng HTTP từ trường proxyhttp. Đặt PROXY_XOAY_KEY trong env.
PROXY_XOAY_KEY = os.getenv("PROXY_XOAY_KEY", "").strip()
PROXY_XOAY_API_URL = os.getenv(
    "PROXY_XOAY_API_URL", "https://proxyxoay.shop/api/get.php"
)
PROXY_XOAY_NHAMANG = os.getenv("PROXY_XOAY_NHAMANG", "random")
PROXY_XOAY_TINHTHANH = os.getenv("PROXY_XOAY_TINHTHANH", "0")
PROXY_XOAY_WHITELIST = os.getenv("PROXY_XOAY_WHITELIST", "")
# Giới hạn thời gian giữ 1 proxy trong cache (proxy thường chết ~15–20p; mặc định làm mới sớm hơn).
PROXY_XOAY_MAX_CACHE_SECONDS = float(
    os.getenv("PROXY_XOAY_MAX_CACHE_SECONDS", "840").strip() or "840"
)
# Phần TTL API bỏ đi trước khi coi hết hạn (0.2–0.5 khuyến nghị).
PROXY_XOAY_TTL_MARGIN_RATIO = float(
    os.getenv("PROXY_XOAY_TTL_MARGIN_RATIO", "0.28").strip() or "0.28"
)
# Khi API không trả message hoặc không parse được số giây — không giả định 20p (1200s).
PROXY_XOAY_TTL_FALLBACK_SECONDS = int(
    os.getenv("PROXY_XOAY_TTL_FALLBACK_SECONDS", "600").strip() or "600"
)
# Gọi API get.php bằng POST trước (một số môi trường GET query bị chặn / khác cache).
PROXY_XOAY_API_PREFER_POST = (
    os.getenv("PROXY_XOAY_API_PREFER_POST", "").strip().lower() in ("1", "true", "yes")
)

_xoay_lock = threading.Lock()
_xoay_cache = {"urls": None, "expires": 0.0}
# Lần gọi get.php gần nhất (khi không ra proxy — debug / hỗ trợ user).
_proxy_xoay_last_diagnostic: dict | None = None


def proxy_xoay_last_diagnostic() -> dict | None:
    """Copy dict chẩn đoán lần fetch proxyxoay gần nhất (None nếu thành công hoặc chưa gọi)."""
    with _xoay_lock:
        d = _proxy_xoay_last_diagnostic
        return dict(d) if d else None


def _xoay_diag_set(payload: dict):
    global _proxy_xoay_last_diagnostic
    with _xoay_lock:
        _proxy_xoay_last_diagnostic = payload


def _xoay_diag_clear():
    global _proxy_xoay_last_diagnostic
    with _xoay_lock:
        _proxy_xoay_last_diagnostic = None


def _parse_proxy_xoay_field(raw: str):
    """Parse host:port:user:pass (user/pass có thể rỗng). VD API: 42.x.x.x:10836::"""
    if not raw:
        return None
    s = str(raw).strip().strip("'\"")
    for prefix in ("http://", "https://"):
        if s.lower().startswith(prefix):
            s = s[len(prefix) :].split("/")[0].strip()
    m = re.fullmatch(r"([^:@\s/]+):(\d+)(?::([^:]*):([^:]*))?", s)
    if not m:
        return None
    host, port, user, pw = m.group(1), m.group(2), m.group(3) or "", m.group(4) or ""
    if not port.isdigit() or not (1 <= int(port) <= 65535):
        return None
    return host, port, user, pw


def _xoay_api_params():
    """
    Đúng doc proxyxoay: key, nhamang, tinhthanh; whitelist chỉ gửi khi có giá trị
    (một số bản PHP coi whitelist= rỗng khác với không gửi key).
    """
    p = {
        "key": PROXY_XOAY_KEY,
        "nhamang": PROXY_XOAY_NHAMANG.strip(),
        "tinhthanh": PROXY_XOAY_TINHTHANH.strip(),
    }
    wl = (PROXY_XOAY_WHITELIST or "").strip()
    if wl:
        p["whitelist"] = wl
    return p


def _proxyhttp_from_payload(data: dict) -> str:
    """Lấy chuỗi host:port:mật_khẩu từ JSON — thử nhiều key (PHP đôi khi đổi hoa thường)."""
    if not data or not isinstance(data, dict):
        return ""
    for k, v in data.items():
        lk = str(k).lower().replace("_", "")
        if lk == "proxyhttp" and v is not None:
            return str(v).strip()
    return ""


def _build_http_proxy_url(host: str, port: str, user: str, password: str) -> str:
    if user or password:
        auth = f"{quote(user, safe='')}:{quote(password, safe='')}@"
    else:
        auth = ""
    return f"http://{auth}{host}:{port}"


def _xoay_ttl_from_message(message: str) -> int:
    """
    Bóc số giây còn sống từ message (VD: 'proxy nay se die sau 1777s').
    Không khớp → PROXY_XOAY_TTL_FALLBACK_SECONDS (mặc định 10 phút, an toàn hơn 20p).
    """
    if not message:
        return max(60, min(PROXY_XOAY_TTL_FALLBACK_SECONDS, 86400))
    msg = str(message)
    patterns = [
        r"sau\s+(\d+)\s*s(?:\b|ec|\.)",
        r"die\s+sau\s*(\d+)\s*(?:s|sec|giây|giay)?\b",
        r"(\d+)\s*(?:s|sec|seconds?|giây|giay)\b",
        r"(\d+)\s*(?:phút|phut)\b",
        r"(?:after|in)\s+(\d+)\s*(?:min|minutes)\b",
    ]
    for i, pat in enumerate(patterns):
        m = re.search(pat, msg, flags=re.I)
        if not m:
            continue
        n = int(m.group(1))
        if i == 3:
            n *= 60
        elif i == 4:
            n *= 60
        return max(120, min(n, 86400))
    return max(60, min(PROXY_XOAY_TTL_FALLBACK_SECONDS, 86400))


def _xoay_cache_valid_until(api_message: str) -> float:
    """Thời điểm unix hết hạn cache: theo API nhưng cắt margin + trần PROXY_XOAY_MAX_CACHE_SECONDS."""
    api_ttl = _xoay_ttl_from_message(api_message)
    ratio = PROXY_XOAY_TTL_MARGIN_RATIO
    ratio = max(0.05, min(ratio, 0.6))
    usable = float(api_ttl) * (1.0 - ratio)
    cap = max(120.0, PROXY_XOAY_MAX_CACHE_SECONDS)
    seconds = max(60.0, min(usable, cap))
    return time.time() + seconds


def _xoay_preview_text(s: str, max_len: int = 200) -> str:
    if not s:
        return ""
    s = " ".join(s.split())
    if len(s) <= max_len:
        return s
    return s[: max_len - 3] + "..."


def _xoay_parse_api_response(r: requests.Response | None) -> tuple[dict | None, str]:
    """
    Parse body JSON object từ get.php. Trả (dict, "") khi parse được;
    (None, lý_do) khi lỗi HTTP / không phải JSON object.
    """
    if r is None:
        return None, "no_response"
    http_sc = getattr(r, "status_code", 0)
    try:
        r.raise_for_status()
    except requests.HTTPError:
        return None, f"http_error_status_{http_sc}"
    text = (r.text or "").strip().lstrip("\ufeff")
    if not text:
        return None, f"empty_body_http_{http_sc}"
    if text[0] == "<":
        return None, f"html_not_json:{_xoay_preview_text(text, 180)}"
    if text[0] == "[":
        return None, f"json_array_not_object:{_xoay_preview_text(text, 120)}"
    if not text.startswith("{"):
        return None, f"not_json_object:{_xoay_preview_text(text, 120)}"
    try:
        data = json.loads(text)
    except (json.JSONDecodeError, ValueError) as e:
        return None, f"json_decode:{e}:{_xoay_preview_text(text, 160)}"
    if not isinstance(data, dict):
        return None, "json_top_level_not_object"
    return data, ""


def _xoay_cooldown_sleep_seconds(message: str) -> float | None:
    """VD API: 'Con 21s moi co the doi proxy' → chờ ~21s (+buffer)."""
    if not message:
        return None
    m = re.search(r"(\d+)\s*s", str(message), flags=re.I)
    if not m:
        return None
    return float(min(max(int(m.group(1)) + 2, 3), 120))


def _xoay_fetch_from_api():
    """Gọi https://proxyxoay.shop/api/get.php (GET hoặc POST) — field proxyhttp, status=100."""
    if not PROXY_XOAY_KEY:
        return None
    _xoay_diag_clear()
    params = _xoay_api_params()
    api_headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        ),
    }

    timeout = (8, 30)

    def _finalize_from_data(data: dict, http_sc: int, via: str) -> dict | None:
        try:
            st = int(float(str(data.get("status", 0) or "0").strip() or "0"))
        except ValueError:
            _xoay_diag_set(
                {
                    "via": via,
                    "http_status": http_sc,
                    "error": "api_status_not_numeric",
                    "json_keys": list(data.keys())[:25],
                }
            )
            return None
        msg = str(data.get("message", "") or "")[:800]
        if st != 100:
            _xoay_diag_set(
                {
                    "via": via,
                    "http_status": http_sc,
                    "api_status": st,
                    "api_message": msg,
                    "json_keys": list(data.keys())[:25],
                }
            )
            return None
        raw_http = _proxyhttp_from_payload(data)
        if not raw_http:
            _xoay_diag_set(
                {
                    "via": via,
                    "http_status": http_sc,
                    "api_status": st,
                    "error": "missing_proxyhttp_in_json",
                    "json_keys": list(data.keys())[:25],
                }
            )
            return None
        parts = _parse_proxy_xoay_field(raw_http)
        if not parts:
            _xoay_diag_set(
                {
                    "via": via,
                    "http_status": http_sc,
                    "api_status": st,
                    "error": "proxyhttp_unparseable",
                    "proxyhttp_preview": _xoay_preview_text(raw_http, 120),
                }
            )
            return None
        host, port, user, pw = parts
        proxy_url = _build_http_proxy_url(host, port, user, pw)
        expires = _xoay_cache_valid_until(str(data.get("message", "")))
        proxies = {"http": proxy_url, "https": proxy_url}
        with _xoay_lock:
            _xoay_cache["urls"] = proxies
            _xoay_cache["expires"] = expires
        _xoay_diag_clear()
        return proxies

    max_cooldown_rounds = int(
        os.getenv("PROXY_XOAY_COOLDOWN_RETRIES", "4").strip() or "4"
    )
    max_cooldown_rounds = max(1, min(max_cooldown_rounds, 8))

    try:
        for round_i in range(max_cooldown_rounds):
            with requests.Session() as sess:
                sess.trust_env = False

                def _do_get():
                    return sess.get(
                        PROXY_XOAY_API_URL,
                        params=params,
                        headers=api_headers,
                        timeout=timeout,
                    )

                def _do_post():
                    h = {
                        **api_headers,
                        "Content-Type": "application/x-www-form-urlencoded",
                    }
                    return sess.post(
                        PROXY_XOAY_API_URL,
                        data=params,
                        headers=h,
                        timeout=timeout,
                    )

                if PROXY_XOAY_API_PREFER_POST:
                    attempts = [("POST", _do_post), ("GET", _do_get)]
                else:
                    attempts = [("GET", _do_get), ("POST", _do_post)]

                for via, fn in attempts:
                    try:
                        r = fn()
                    except requests.RequestException as e:
                        _xoay_diag_set(
                            {
                                "via": via,
                                "error": "request_exception",
                                "detail": f"{type(e).__name__}: {e}",
                            }
                        )
                        continue
                    http_sc = getattr(r, "status_code", 0)
                    data, parse_err = _xoay_parse_api_response(r)
                    if data is None:
                        _xoay_diag_set(
                            {
                                "via": via,
                                "http_status": http_sc,
                                "parse_error": parse_err,
                                "body_preview": _xoay_preview_text(r.text or "", 220),
                            }
                        )
                        continue
                    out = _finalize_from_data(data, http_sc, via)
                    if out:
                        return out

            d = proxy_xoay_last_diagnostic()
            if (
                d
                and d.get("api_status") == 101
                and round_i + 1 < max_cooldown_rounds
            ):
                msg = str(d.get("api_message", ""))
                sec = _xoay_cooldown_sleep_seconds(msg)
                time.sleep(sec if sec is not None else 25.0)
                continue
            break
    except requests.RequestException as e:
        _xoay_diag_set(
            {"error": "session_request_exception", "detail": f"{type(e).__name__}: {e}"}
        )
        return None
    except Exception as e:
        _xoay_diag_set({"error": "unexpected", "detail": f"{type(e).__name__}: {e}"})
        return None
    return None


def _xoay_proxies_dict(force_refresh: bool = False):
    if not PROXY_XOAY_KEY:
        return None
    now = time.time()
    with _xoay_lock:
        if (
            not force_refresh
            and _xoay_cache["urls"]
            and now < _xoay_cache["expires"]
        ):
            return dict(_xoay_cache["urls"])
    try:
        return _xoay_fetch_from_api()
    except Exception:
        return None


def _xoay_invalidate():
    with _xoay_lock:
        _xoay_cache["urls"] = None
        _xoay_cache["expires"] = 0.0


def invalidate_rotating_proxy():
    """Bắt proxyxoay cache làm mới lần GET tiếp theo (sau lỗi mạng / body rỗng / WAF)."""
    _xoay_invalidate()


# Proxy HTTP tĩnh (PROXY_FALLBACK_LIST) đã tắt — chỉ dùng proxy xoay khi có PROXY_XOAY_KEY.
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


def _get_via_proxies(fn, url: str, headers: dict, t_proxy, t_direct):
    """
    fn = requests.get hoặc session.get.
    Có PROXY_XOAY_KEY: chỉ đi qua proxy xoay (không proxy tĩnh, không direct).
    Không có key: gọi trực tiếp (dev).
    """
    if PROXY_XOAY_KEY:
        for force_refresh in (False, True):
            proxies = _xoay_proxies_dict(force_refresh=force_refresh)
            if not proxies:
                break
            try:
                r = fn(url, headers=headers, proxies=proxies, timeout=t_proxy)
                if 200 <= r.status_code < 300:
                    if r.status_code == 202:
                        _xoay_invalidate()
                        continue
                    return r
                _xoay_invalidate()
            except Exception:
                _xoay_invalidate()
        return None

    try:
        r = fn(url, headers=headers, timeout=t_direct)
        if 200 <= r.status_code < 300 and r.status_code != 202:
            return r
    except Exception:
        pass
    return None


USER_AGENT   = (
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

DELAY = (1.0, 2.0)

ENDPOINTS = {
    "mercari_iframe":      "https://buyee.jp/mercari/search",
    "mercari_category_id": "859",
    "rakuma_search":       "https://buyee.jp/rakuma/search",
    "rakuma_category_id":  "668",
    "yahoo_base":          "https://buyee.jp/item/search/category",
    "yahoo_category_id":   "2084317598",
}

def fetch(
    url: str,
    headers: dict,
    timeout_proxy=None,
    timeout_direct=None,
):
    """
    Thử fetch: proxy xoay (nếu có key) → proxy fallback trong env (nếu có) → direct.
    timeout_proxy / timeout_direct: tuple (connect, read) hoặc số giây; None = mặc định module.
    """
    try:
        t_proxy = timeout_proxy if timeout_proxy is not None else PROXY_TIMEOUT
        t_direct = timeout_direct if timeout_direct is not None else DIRECT_TIMEOUT
        return _get_via_proxies(_buyee_http_get, url, headers, t_proxy, t_direct)
    except Exception:
        return None


def fetch_with_session(
    session,
    url: str,
    headers: dict,
    timeout_proxy=None,
    timeout_direct=None,
):
    """
    Giống fetch() nhưng dùng Session để giữ cookie (trang item → iframe).
    `session` nên từ create_buyee_session() để TLS khớp khi bật curl-cffi.
    """
    try:
        t_proxy = timeout_proxy if timeout_proxy is not None else PROXY_TIMEOUT
        t_direct = timeout_direct if timeout_direct is not None else DIRECT_TIMEOUT
        return _get_via_proxies(session.get, url, headers, t_proxy, t_direct)
    except Exception:
        return None


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
                if invalidate_proxy_on_retry and PROXY_XOAY_KEY:
                    invalidate_rotating_proxy()
                time.sleep(random.uniform(0.3, 0.8))
        except Exception:
            if attempt == max_retries:
                return None
            if invalidate_proxy_on_retry and PROXY_XOAY_KEY:
                invalidate_rotating_proxy()
            time.sleep(random.uniform(0.3, 0.8))

    return None
