import os
import itertools
import requests
import time
import random
from urllib.parse import urljoin
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.getenv("MONGO_DB_NAME", "ivaluate_datalake")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "raw_items")

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3000")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DB = os.getenv("MYSQL_DB", "ivaluate")

MAX_PAGES_MERCARI = 40
MAX_PAGES_RAKUMA = 80
MAX_PAGES_YAHOO = 40

WORKER_MONGO_URI = "mongodb://da-mongo:27017/"


PROXY_LIST  = [
    "http://119.3.113.150:9094",
    "http://32.223.6.94:80",
    "http://159.65.128.194:1080", 
    "http://123.30.154.171:7777", 
    "http://51.81.245.3:17981",
    # "http://23.247.136.254:80",
    # "http://154.194.12.10:80",
    # "http://206.238.237.253:80",
    # "http://103.160.204.104:80",
]
PROXY_CYCLE = itertools.cycle(PROXY_LIST)

USER_AGENT   = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/110.0.0.0 Safari/537.36"
)
HEADERS_BASE = {"User-Agent": USER_AGENT}

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

def fetch(url: str, headers: dict, timeout: int = 15):
    """
    Thử fetch qua proxy, nếu thất bại thì direct.
    """
    proxy = next(PROXY_CYCLE)
    try:
        r = requests.get(url, headers=headers, proxies={"http": proxy}, timeout=timeout)
        if r.status_code == 200:
            return r
    except:
        pass
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r
    except:
        pass
    return None

def normalize_link(href: str) -> str:
    """
    Xóa '/undefined' nếu có & ghép domain buyee.jp
    """
    return urljoin("https://buyee.jp", href.replace("/undefined", ""))

# Hàm helper chung cho retry logic
def safe_fetch_with_retry(fetch_func, url, max_retries=1):  # ✅ Giảm xuống 1 retry
    """
    Wrapper an toàn với retry logic
    """
    if not url:
        return None
    
    for attempt in range(max_retries + 1):
        try:
            result = fetch_func(url)
            if result:
                return result
            if attempt < max_retries:
                time.sleep(random.uniform(0.3, 0.8))  # ✅ Giảm delay
        except Exception:
            if attempt == max_retries:
                return None
            time.sleep(random.uniform(0.3, 0.8))
    
    return None
