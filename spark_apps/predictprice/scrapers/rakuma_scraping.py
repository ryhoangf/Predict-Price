import os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
from scrapers.detail_parallel import map_urls_parallel
from scrapers.listing_df_cleanup import prepare_listing_dataframe
import pandas as pd
import random, time
from bs4 import BeautifulSoup


def _rakuma_condition_from_soup(soup: BeautifulSoup):
    try:
        dl = soup.select_one("dl.attrContainer__detail")
        scope = dl or soup
        for a in scope.select("a[href*='condition=']"):
            t = a.get_text(strip=True)
            if t:
                return t
        return None
    except Exception:
        return None


def _rakuma_description_path(url: str) -> str | None:
    """.../rakuma/item/xyz → .../rakuma/item/description/xyz"""
    try:
        marker = "/rakuma/item/"
        if marker not in url:
            return None
        rest = url.split(marker, 1)[1]
        base = rest.split("?", 1)[0].strip("/")
        if not base or "/" in base:
            return None
        return f"https://buyee.jp/rakuma/item/description/{base}"
    except Exception:
        return None


def _rakuma_explanation_from_soup(item_url: str, soup: BeautifulSoup):
    try:
        content_div = soup.select_one("div.itemDetail__content")
        if content_div:
            t = content_div.get_text(separator="\n", strip=True)
            if t:
                return t
        desc_url = _rakuma_description_path(item_url)
        if not desc_url:
            return None
        if "lang=en" not in desc_url:
            desc_url += "&lang=en" if "?" in desc_url else "?lang=en"
        desc_resp = config.fetch(desc_url, config.buyee_bare_headers_like_iwr())
        if not desc_resp or config.response_looks_like_buyee_waf_challenge(desc_resp):
            desc_resp = config.fetch(
                desc_url, config.buyee_page_headers(referer=item_url)
            )
        if not desc_resp or config.response_looks_like_buyee_waf_challenge(desc_resp):
            return None
        desc_soup = BeautifulSoup(desc_resp.text, "html.parser")
        p_desc = desc_soup.select_one("p.m-itemDetail__content")
        if p_desc:
            t = p_desc.get_text(separator="\n", strip=True)
            if t:
                return t
        body = desc_soup.find("body")
        if body:
            for tag in body.find_all(["script", "style", "noscript"]):
                try:
                    tag.decompose()
                except Exception:
                    pass
            t = body.get_text(separator="\n", strip=True)
            if t:
                return t
        return None
    except Exception:
        return None


def fetch_rakuma_item_detail(url: str):
    """Một GET trang item (+ tối đa 1 GET description) → (condition, explanation)."""
    if not url:
        return (None, None)
    try:
        u = url
        if "lang=en" not in u:
            u += "&lang=en" if "?" in u else "?lang=en"
        hdr = config.buyee_page_headers(referer=config.REFERERS["rakuma"])
        resp = config.fetch(u, hdr)
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            resp = config.fetch(u, config.buyee_bare_headers_like_iwr())
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return (None, None)
        soup = BeautifulSoup(resp.text, "html.parser")
        return (
            _rakuma_condition_from_soup(soup),
            _rakuma_explanation_from_soup(u, soup),
        )
    except Exception:
        return (None, None)


def scrape_rakuma(end_page: int) -> pd.DataFrame:
    headers = config.buyee_page_headers(referer=config.REFERERS["rakuma"])
    links, names, prices = [], [], []

    for page in range(1, end_page + 1):
        url = (
            f"{config.ENDPOINTS['rakuma_search']}?"
            f"lang=en&category_id={config.ENDPOINTS['rakuma_category_id']}&page={page}"
        )
        print(f"→ [Rakuma] Fetching page {page}/{end_page}")
        try:
            resp = config.fetch(url, headers)
            if not resp:
                print("   [!] skip")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            ul = soup.find("ul", class_="item-lists")
            items = ul.find_all("li", class_="list", recursive=False) if ul else []
            print(f"   → Found {len(items)} items")

            for it in items:
                try:
                    a = it.find("a", href=True)
                    href = a["href"] if a else None
                    links.append(config.normalize_link(href) if href else None)
                    n = it.find("h2", class_="name")
                    p = it.find("p", class_="price")
                    names.append(n.get_text(strip=True) if n else None)
                    prices.append(p.get_text(strip=True) if p else None)
                except Exception:
                    links.append(None)
                    names.append(None)
                    prices.append(None)
        except Exception:
            print("   [!] page error, skip")
            continue

        time.sleep(random.uniform(*config.DELAY))

    df = pd.DataFrame({"link": links, "name": names, "price": prices})
    df = prepare_listing_dataframe(df, "Rakuma")
    if df.empty:
        print("   [!] Rakuma: no rows with valid link after cleanup")
        return df

    print(
        f"   → Fetching details for {len(df)} items "
        f"(parallel workers={config.DETAIL_FETCH_MAX_WORKERS})..."
    )
    try:

        def _one(u):
            return config.safe_fetch_with_retry(
                fetch_rakuma_item_detail,
                u,
                max_retries=2,
                invalidate_proxy_on_retry=True,
            )

        t_detail = time.perf_counter()
        pairs = map_urls_parallel(
            df["link"].tolist(), _one, config.DETAIL_FETCH_MAX_WORKERS
        )
        print(
            f"   [Rakuma] [timing] detail_fetch={time.perf_counter() - t_detail:.1f}s"
        )
        df["condition"] = [
            (p[0] if isinstance(p, (tuple, list)) and len(p) >= 1 else None)
            for p in pairs
        ]
        df["explanation"] = [
            (p[1] if isinstance(p, (tuple, list)) and len(p) >= 2 else None)
            for p in pairs
        ]
    except Exception:
        df["condition"] = None
        df["explanation"] = None

    return df


def get_item_condition_rakuma(url: str) -> str:
    c, _ = fetch_rakuma_item_detail(url)
    return c


def get_item_explanation_rakuma(url: str) -> str:
    _, e = fetch_rakuma_item_detail(url)
    return e
