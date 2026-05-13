import os, sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
import pandas as pd
import random, time
from bs4 import BeautifulSoup


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

    print(f"   → Fetching details for {len(df)} items...")
    try:
        df["condition"] = df["link"].apply(
            lambda u: config.safe_fetch_with_retry(
                get_item_condition_rakuma,
                u,
                max_retries=2,
                invalidate_proxy_on_retry=True,
            )
        )
        df["explanation"] = df["link"].apply(
            lambda u: config.safe_fetch_with_retry(
                get_item_explanation_rakuma,
                u,
                max_retries=2,
                invalidate_proxy_on_retry=True,
            )
        )
    except Exception:
        df["condition"] = None
        df["explanation"] = None

    return df


def get_item_condition_rakuma(url: str) -> str:
    try:
        if not url:
            return None
        if "lang=en" not in url:
            url += "&lang=en" if "?" in url else "?lang=en"

        hdr = config.buyee_page_headers(referer=config.REFERERS["rakuma"])
        resp = config.fetch(url, hdr)
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            resp = config.fetch(url, config.buyee_bare_headers_like_iwr())
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
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


def get_item_explanation_rakuma(url: str) -> str:
    try:
        if not url:
            return None
        if "lang=en" not in url:
            url += "&lang=en" if "?" in url else "?lang=en"

        hdr = config.buyee_page_headers(referer=config.REFERERS["rakuma"])
        resp = config.fetch(url, hdr)
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            resp = config.fetch(url, config.buyee_bare_headers_like_iwr())
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        content_div = soup.select_one("div.itemDetail__content")
        if content_div:
            t = content_div.get_text(separator="\n", strip=True)
            if t:
                return t

        desc_url = _rakuma_description_path(url)
        if not desc_url:
            return None
        if "lang=en" not in desc_url:
            desc_url += "&lang=en" if "?" in desc_url else "?lang=en"

        desc_resp = config.fetch(desc_url, config.buyee_bare_headers_like_iwr())
        if not desc_resp or config.response_looks_like_buyee_waf_challenge(desc_resp):
            desc_resp = config.fetch(desc_url, config.buyee_page_headers(referer=url))
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
