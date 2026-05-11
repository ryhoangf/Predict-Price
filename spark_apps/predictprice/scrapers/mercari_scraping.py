import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
import pandas as pd
import random, time
from bs4 import BeautifulSoup

def scrape_mercari(end_page: int) -> pd.DataFrame:
    headers = {**config.HEADERS_BASE, "Referer": config.REFERERS["mercari"]}
    links, names, prices = [], [], []

    for page in range(1, end_page+1):
        iframe_url = (
            f"{config.ENDPOINTS['mercari_iframe']}?"
            f"limit=100&lang=en&page={page}"
            "&searchType=filter"
            "&order-sort=desc-created_time"
            f"&category_id={config.ENDPOINTS['mercari_category_id']}"
        )
        print(f"→ [Mercari] Fetching page {page}/{end_page}")
        resp = config.fetch(iframe_url, headers)
        if not resp:
            print("   [!] skip")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.find_all("li", class_="list")
        print(f"   → Found {len(items)} items")

        for it in items:
            a = it.find("a", href=True)
            links.append(config.normalize_link(a["href"]) if a else None)

            n = it.find("h2", class_="name")
            p = it.find("p", class_="price")
            names.append(n.get_text(strip=True)  if n else None)
            prices.append(p.get_text(strip=True) if p else None)

        time.sleep(random.uniform(*config.DELAY))

    df = pd.DataFrame({"link": links, "name": names, "price": prices})
    
    # Tuần tự với retry logic - ổn định hơn cho dữ liệu lớn
    print(f"   → Fetching details for {len(df)} items...")
    df["condition"] = df["link"].apply(lambda u: config.safe_fetch_with_retry(get_item_condition_mercari, u))
    df["explanation"] = df["link"].apply(lambda u: config.safe_fetch_with_retry(get_item_explanation_mercari, u))
    
    return df

def get_item_condition_mercari(url: str) -> str:
    resp = config.fetch(url, config.HEADERS_BASE)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    dl = soup.find("dl", class_="m-goodsTable")
    if not dl:
        return None
    for dt in dl.find_all("dt"):
        if dt.get_text(strip=True) == "Item Condition":
            dd = dt.find_next_sibling("dd")
            return dd.get_text(strip=True) if dd else None
    return None

def get_item_explanation_mercari(url: str) -> str:
    resp = config.fetch(url, config.HEADERS_BASE)
    if not resp:
        return None
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # 1. Tìm iframe chứa mô tả
    section = soup.find("section", class_="m-itemDetail", id="shopping_item_detail_container")
    if not section:
        return None
    
    iframe = section.find("iframe")
    if not iframe or not iframe.get("src"):
        return None
    
    iframe_src = iframe["src"]
    
    # Dọn dẹp URL của iframe (bỏ đuôi googtrans đi để lấy gốc)
    if "#googtrans" in iframe_src:
        iframe_src = iframe_src.split("#googtrans")[0]
    
    if iframe_src.startswith("/"):
        iframe_url = f"https://buyee.jp{iframe_src}"
    else:
        iframe_url = iframe_src
    
    # 2. Fetch nội dung BÊN TRONG iframe
    iframe_resp = config.fetch(iframe_url, config.HEADERS_BASE)
    if not iframe_resp:
        return None
    
    iframe_soup = BeautifulSoup(iframe_resp.text, "html.parser")
    
    # 3. Bóc tách nội dung thật theo cấu trúc DOM bạn vừa tìm ra
    desc_section = iframe_soup.find("section", id="item-description")
    if desc_section:
        return desc_section.get_text(separator=" ", strip=True)
        
    fallback_div = iframe_soup.find("div", id="js_inline_content")
    if fallback_div:
        return fallback_div.get_text(separator=" ", strip=True)
    
    # Phương án dự phòng cuối cùng: Lấy toàn bộ chữ trong iframe
    explanation = iframe_soup.get_text(separator=" ", strip=True)
    return explanation if explanation else None
