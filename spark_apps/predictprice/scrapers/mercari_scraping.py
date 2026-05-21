import os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
from scrapers.scrape_pipeline import run_two_phase_scrape
from scrapers.listing_df_cleanup import prepare_listing_dataframe
import pandas as pd
import time
from bs4 import BeautifulSoup


def _listing_worker():
    return config.listing_worker_proxy()


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
        wp = config.get_thread_worker_proxy()
        iframe_resp = config.fetch(
            iframe_url, config.buyee_bare_headers_like_iwr(), worker_proxy=wp
        )
        if not iframe_resp or config.response_looks_like_buyee_waf_challenge(
            iframe_resp
        ):
            iframe_resp = config.fetch(
                iframe_url,
                config.buyee_page_headers(referer=item_url),
                worker_proxy=wp,
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
    if not url:
        return (None, None)
    try:
        u = url
        if "lang=en" not in u:
            u += "&lang=en" if "?" in u else "?lang=en"
        hdr = config.buyee_page_headers(referer=config.REFERERS["mercari"])
        resp = config.fetch(u, hdr, worker_proxy=config.get_thread_worker_proxy())
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return (None, None)
        soup = BeautifulSoup(resp.text, "html.parser")
        return (
            _mercari_condition_from_soup(soup),
            _mercari_explanation_from_soup(u, soup),
        )
    except Exception:
        return (None, None)


def _scrape_mercari_listing(end_page: int) -> pd.DataFrame:
    headers = config.buyee_page_headers(referer=config.REFERERS["mercari"])
    links, names, prices = [], [], []
    wp = _listing_worker()

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
            resp = config.fetch(iframe_url, headers, worker_proxy=wp)
            if not resp:
                err = config.last_fetch_error()
                print(f"   [!] fetch failed page {page}{f' | {err}' if err else ''}")
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

        time.sleep(config.LISTING_DELAY_SEC)

    df = pd.DataFrame({"link": links, "name": names, "price": prices})
    return prepare_listing_dataframe(df, "Mercari")


def scrape_mercari(end_page: int) -> pd.DataFrame:
    return run_two_phase_scrape(
        "mercari",
        lambda: _scrape_mercari_listing(end_page),
        fetch_mercari_item_detail,
    )


def get_item_condition_mercari(url: str) -> str:
    c, _ = fetch_mercari_item_detail(url)
    return c


def get_item_explanation_mercari(url: str) -> str:
    _, e = fetch_mercari_item_detail(url)
    return e
