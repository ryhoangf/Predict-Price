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
        desc_resp = config.fetch(
            desc_url,
            config.buyee_bare_headers_like_iwr(),
            worker_proxy=config.get_thread_worker_proxy(),
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
    if not url:
        return (None, None)
    try:
        u = url
        if "lang=en" not in u:
            u += "&lang=en" if "?" in u else "?lang=en"
        hdr = config.buyee_page_headers(referer=config.REFERERS["rakuma"])
        resp = config.fetch(u, hdr, worker_proxy=config.get_thread_worker_proxy())
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return (None, None)
        soup = BeautifulSoup(resp.text, "html.parser")
        return (
            _rakuma_condition_from_soup(soup),
            _rakuma_explanation_from_soup(u, soup),
        )
    except Exception:
        return (None, None)


def _scrape_rakuma_listing(end_page: int) -> pd.DataFrame:
    headers = config.buyee_page_headers(referer=config.REFERERS["rakuma"])
    links, names, prices = [], [], []
    wp = _listing_worker()

    for page in range(1, end_page + 1):
        url = (
            f"{config.ENDPOINTS['rakuma_search']}?"
            f"lang=en&category_id={config.ENDPOINTS['rakuma_category_id']}&page={page}"
        )
        print(f"→ [Rakuma] Fetching page {page}/{end_page}")
        try:
            resp = config.fetch(url, headers, worker_proxy=wp)
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

        time.sleep(config.LISTING_DELAY_SEC)

    df = pd.DataFrame({"link": links, "name": names, "price": prices})
    return prepare_listing_dataframe(df, "Rakuma")


def scrape_rakuma(end_page: int) -> pd.DataFrame:
    return run_two_phase_scrape(
        "rakuma",
        lambda: _scrape_rakuma_listing(end_page),
        fetch_rakuma_item_detail,
    )


def get_item_condition_rakuma(url: str) -> str:
    c, _ = fetch_rakuma_item_detail(url)
    return c


def get_item_explanation_rakuma(url: str) -> str:
    _, e = fetch_rakuma_item_detail(url)
    return e
