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
from urllib.parse import urljoin


def _listing_worker():
    return config.listing_worker_proxy()


def _scrape_yahooauction_listing(end_page: int) -> pd.DataFrame:
    headers = config.buyee_page_headers(referer=config.REFERERS["yahoo"])
    all_items = []
    wp = _listing_worker()

    for page in range(1, end_page + 1):
        url = (
            f"{config.ENDPOINTS['yahoo_base']}/"
            f"{config.ENDPOINTS['yahoo_category_id']}?page={page}"
        )
        print(f"→ [YahooAuction] Fetching page {page}/{end_page}")
        try:
            resp = config.fetch(url, headers, worker_proxy=wp)
            if not resp:
                print("   [!] no response, skip")
                continue
            if config.response_looks_like_buyee_waf_challenge(resp):
                print("   [!] WAF Challenge detected. Skipping page.")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("li", class_="itemCard")
            print(f"   → Found {len(items)} items")

            for it in items:
                try:
                    a = it.find("a", href=True)
                    href = a["href"] if a else None
                    link = urljoin("https://buyee.jp", href) if href else None
                    if not link:
                        continue
                    n = it.find("div", class_="itemCard__itemName")
                    name = n.get_text(strip=True) if n else None
                    price_info = extract_price_from_card(it)
                    if not price_info or not price_info.get("price"):
                        continue
                    all_items.append(
                        {"link": link, "name": name, "price": price_info["price"]}
                    )
                except Exception:
                    continue
        except Exception:
            print("   [!] page error, skip")
            continue

        time.sleep(config.LISTING_DELAY_SEC)

    df = pd.DataFrame(all_items)
    return prepare_listing_dataframe(df, "YahooAuction")


def scrape_yahooauction(end_page: int) -> pd.DataFrame:
    df = run_two_phase_scrape(
        "yahooauction",
        lambda: _scrape_yahooauction_listing(end_page),
        get_item_details_yahooauction,
    )
    if df is None or df.empty:
        print("   ❌ No valid items found")
    return df


def extract_price_from_card(item_card) -> dict:
    result = {"price": None, "price_type": None, "listing_type": None}
    try:
        fleamarket_div = item_card.find("div", class_="itemCard__fleamarket")
        result["listing_type"] = "fleamarket" if fleamarket_div else "auction"

        price_details = item_card.find("div", class_="g-priceDetails")
        if not price_details:
            return result

        buyout_price = fixed_price = current_price = None
        for item in price_details.find_all("li", class_="g-priceDetails__item"):
            title_span = item.find("span", class_="g-title")
            price_span = item.find("span", class_="g-price")
            if not title_span or not price_span:
                continue
            title = title_span.get_text(strip=True)
            price = price_span.get_text(strip=True)
            if "Buyout Price" in title or "即決価格" in title:
                buyout_price = price
            elif (
                "Price" in title
                and "Current" not in title
                and "Buyout" not in title
            ):
                fixed_price = price
            elif "Current Price" in title or "現在価格" in title:
                current_price = price

        if buyout_price:
            result["price"] = buyout_price
            result["price_type"] = "buyout"
        elif fixed_price:
            result["price"] = fixed_price
            result["price_type"] = "fixed"
        elif current_price and result["listing_type"] == "fleamarket":
            result["price"] = current_price
            result["price_type"] = "fixed"
        else:
            result["price"] = None
            result["price_type"] = "current_bid_skip"
    except Exception:
        pass
    return result


def _yahoo_list_name_text(name_div):
    try:
        if not name_div:
            return ""
        for tip in name_div.select(".tooltip-auc"):
            try:
                tip.decompose()
            except Exception:
                pass
        return " ".join(name_div.get_text(separator=" ", strip=True).split())
    except Exception:
        return ""


def _clean_multiline_text(raw: str) -> str | None:
    try:
        if not raw or not str(raw).strip():
            return None
        lines = [line.strip() for line in str(raw).split("\n") if line.strip()]
        return "\n".join(lines) if lines else None
    except Exception:
        return None


def get_item_details_yahooauction(url: str):
    if not url:
        return None
    try:
        if "lang=en" not in url:
            url += "&lang=en" if "?" in url else "?lang=en"

        wp = config.get_thread_worker_proxy()
        headers = config.buyee_page_headers(referer=config.REFERERS["yahoo"])
        resp = config.fetch(url, headers, worker_proxy=wp)
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            resp = config.fetch(
                url,
                config.buyee_bare_headers_like_iwr(),
                worker_proxy=wp,
            )
        if not resp or config.response_looks_like_buyee_waf_challenge(resp):
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        condition = None
        explanation = None

        for li in soup.find_all("li", class_="itemDetail__list"):
            try:
                name_div = li.find("div", class_="itemDetail__listName")
                if name_div and config.looks_like_buyee_condition_label(
                    _yahoo_list_name_text(name_div)
                ):
                    val_div = li.find("div", class_="itemDetail__listValue")
                    if val_div:
                        condition = val_div.get_text(strip=True)
                        break
            except Exception:
                continue

        desc_section = soup.find(
            "section", id="itemDescription"
        ) or soup.find("section", class_="itemDescription")

        if desc_section:
            try:
                iframe = desc_section.find("iframe")
                if iframe:
                    iframe_src = iframe.get("data-src") or iframe.get("src", "")
                    iframe_src = str(iframe_src).split("#")[0].strip()
                    if iframe_src:
                        iframe_url = urljoin("https://buyee.jp", iframe_src)
                        iframe_resp = config.fetch(
                            iframe_url,
                            config.buyee_bare_headers_like_iwr(),
                            worker_proxy=wp,
                        )
                        if (
                            not iframe_resp
                            or config.response_looks_like_buyee_waf_challenge(
                                iframe_resp
                            )
                        ):
                            iframe_resp = config.fetch(
                                iframe_url,
                                config.buyee_page_headers(referer=url),
                                worker_proxy=wp,
                            )
                        if iframe_resp:
                            iframe_soup = BeautifulSoup(
                                iframe_resp.text, "html.parser"
                            )
                            p_desc = iframe_soup.select_one(
                                "p.m-itemDetail__content"
                            )
                            if p_desc:
                                explanation = _clean_multiline_text(
                                    p_desc.get_text(separator="\n", strip=True)
                                )
                            if not explanation:
                                body = iframe_soup.find("body")
                                if body:
                                    for tag in body.find_all(
                                        ["script", "style", "noscript"]
                                    ):
                                        try:
                                            tag.decompose()
                                        except Exception:
                                            pass
                                raw_text = (
                                    body.get_text(separator="\n", strip=True)
                                    if body
                                    else iframe_soup.get_text(
                                        separator="\n", strip=True
                                    )
                                )
                                explanation = _clean_multiline_text(raw_text)
            except Exception:
                pass

        if condition or explanation:
            return (condition, explanation)
        return None
    except Exception:
        return None
