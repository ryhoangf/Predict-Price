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


def _mercari_condition_from_soup(soup: BeautifulSoup):
    try:
        condition_tag = soup.select_one("dl.m-goodsTable a[href*='condition=']")
        if condition_tag:
            t = condition_tag.get_text(strip=True)
            return t if t else None
        dl = soup.find("dl", class_="m-goodsTable")
        if not dl:
            return None
        for dt in dl.find_all("dt"):
            text = dt.get_text(strip=True).lower()
            if "condition" in text or "状態" in text:
                dd = dt.find_next_sibling("dd")
                if dd:
                    t = dd.get_text(strip=True)
                    return t if t else None
        return None
    except Exception:
        return None


def _mercari_explanation_from_soup(item_url: str, soup: BeautifulSoup):
    try:
        section = soup.find(
            "section", class_="m-itemDetail", id="shopping_item_detail_container"
        )
        if not section:
            section = soup.find("section", class_="m-itemDetail")
        if not section:
            return None
        iframe = section.find("iframe")
        if not iframe:
            return None
        iframe_src = iframe.get("data-src") or iframe.get("src") or ""
        if not str(iframe_src).strip():
            return None
        iframe_src = str(iframe_src).split("#googtrans")[0].split("#")[0].strip()
        iframe_url = (
            f"https://buyee.jp{iframe_src}"
            if iframe_src.startswith("/")
            else iframe_src
        )
        iframe_resp = config.fetch(iframe_url, config.buyee_bare_headers_like_iwr())
        if not iframe_resp or config.response_looks_like_buyee_waf_challenge(
            iframe_resp
        ):
            iframe_resp = config.fetch(
                iframe_url, config.buyee_page_headers(referer=item_url)
            )
        if not iframe_resp or config.response_looks_like_buyee_waf_challenge(
            iframe_resp
        ):
            return None
        iframe_soup = BeautifulSoup(iframe_resp.text, "html.parser")
        p_desc = iframe_soup.select_one("p.m-itemDetail__content")
        if p_desc:
            t = p_desc.get_text(separator="\n", strip=True)
            if t:
                return t
        if iframe_soup.body:
            for tag in iframe_soup.body.find_all(["script", "style", "noscript"]):
                try:
                    tag.decompose()
                except Exception:
                    pass
            t = iframe_soup.body.get_text(separator="\n", strip=True)
            if t:
                return t
        t = iframe_soup.get_text(separator="\n", strip=True)
        return t if t else None
    except Exception:
        return None


def fetch_mercari_item_detail(url: str):
    """Một GET trang item → (condition, explanation)."""
    if not url:
        return (None, None)
    try:
        u = url
        if "lang=en" not in u:
            u += "&lang=en" if "?" in u else "?lang=en"
        hdr = config.buyee_page_headers(referer=config.REFERERS["mercari"])
        resp = config.fetch(u, hdr)
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            resp = config.fetch(u, config.buyee_bare_headers_like_iwr())
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return (None, None)
        soup = BeautifulSoup(resp.text, "html.parser")
        return (
            _mercari_condition_from_soup(soup),
            _mercari_explanation_from_soup(u, soup),
        )
    except Exception:
        return (None, None)


def scrape_mercari(end_page: int) -> pd.DataFrame:
    headers = config.buyee_page_headers(referer=config.REFERERS["mercari"])
    links, names, prices = [], [], []

    for page in range(1, end_page + 1):
        iframe_url = (
            f"{config.ENDPOINTS['mercari_iframe']}?"
            f"limit=100&lang=en&page={page}"
            "&searchType=filter"
            "&order-sort=desc-created_time"
            f"&category_id={config.ENDPOINTS['mercari_category_id']}"
        )
        print(f"→ [Mercari] Fetching page {page}/{end_page}")
        try:
            resp = config.fetch(iframe_url, headers)
            if not resp:
                print("   [!] skip")
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("li", class_="list")
            print(f"   → Found {len(items)} items")

            for it in items:
                try:
                    a = it.find("a", href=True)
                    links.append(config.normalize_link(a["href"]) if a else None)
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
    df = prepare_listing_dataframe(df, "Mercari")
    if df.empty:
        print("   [!] Mercari: no rows with valid link after cleanup")
        return df

    print(
        f"   → Fetching details for {len(df)} items "
        f"(parallel workers={config.DETAIL_FETCH_MAX_WORKERS})..."
    )
    try:

        def _one(u):
            return config.safe_fetch_with_retry(
                fetch_mercari_item_detail,
                u,
                max_retries=2,
                invalidate_proxy_on_retry=True,
            )

        t_detail = time.perf_counter()
        urls = df["link"].tolist()
        pairs = map_urls_parallel(urls, _one, config.DETAIL_FETCH_MAX_WORKERS)
        print(
            f"   [Mercari] [timing] detail_fetch={time.perf_counter() - t_detail:.1f}s"
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


def get_item_condition_mercari(url: str) -> str:
    c, _ = fetch_mercari_item_detail(url)
    return c


def get_item_explanation_mercari(url: str) -> str:
    _, e = fetch_mercari_item_detail(url)
    return e
