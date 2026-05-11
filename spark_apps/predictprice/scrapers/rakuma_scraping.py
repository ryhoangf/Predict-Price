import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
import pandas as pd
import random, time
from bs4 import BeautifulSoup

def scrape_rakuma(end_page: int) -> pd.DataFrame:
    headers = {**config.HEADERS_BASE, "Referer": config.REFERERS["rakuma"]}
    links, names, prices = [], [], []

    for page in range(1, end_page+1):
        url = (
            f"{config.ENDPOINTS['rakuma_search']}?"
            f"lang=en&category_id={config.ENDPOINTS['rakuma_category_id']}&page={page}"
        )
        print(f"→ [Rakuma] Fetching page {page}/{end_page}")
        resp = config.fetch(url, headers)
        if not resp:
            print("   [!] skip")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        ul = soup.find("ul", class_="item-lists")
        items = ul.find_all("li", class_="list", recursive=False) if ul else []
        print(f"   → Found {len(items)} items")

        for it in items:
            a = it.find("a", href=True)
            href = a["href"] if a else None
            links.append(config.normalize_link(href) if href else None)

            n = it.find("h2", class_="name")
            p = it.find("p",  class_="price")
            names.append(n.get_text(strip=True)  if n else None)
            prices.append(p.get_text(strip=True) if p else None)

        time.sleep(random.uniform(*config.DELAY))

    df = pd.DataFrame({"link": links, "name": names, "price": prices})
    
    # Tuần tự với retry logic
    print(f"   → Fetching details for {len(df)} items...")
    df["condition"] = df["link"].apply(lambda u: config.safe_fetch_with_retry(get_item_condition_rakuma, u))
    df["explanation"] = df["link"].apply(lambda u: config.safe_fetch_with_retry(get_item_explanation_rakuma, u))
    
    return df

def get_item_condition_rakuma(url: str) -> str:
    resp = config.fetch(url, config.HEADERS_BASE)
    if not resp:
        return None
    soup = BeautifulSoup(resp.text, "html.parser")
    for dl in soup.find_all("dl", class_="attrContainer__detail"):
        for dt in dl.find_all("dt"):
            if dt.get_text(strip=True) == "Item Condition":
                dd = dt.find_next_sibling("dd")
                if dd:
                    a = dd.find("a")
                    return a.get_text(strip=True) if a else dd.get_text(strip=True)
    return None

def get_item_explanation_rakuma(url: str) -> str:
    resp = config.fetch(url, config.HEADERS_BASE)
    if not resp:
        return None
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    section = soup.find("section", class_="rakuma__itemDetail")
    if not section:
        return None
    
    content_div = section.find("div", class_="itemDetail__content")
    if not content_div:
        return None
    
    explanation = content_div.get_text(separator=" ", strip=True)
    
    return explanation if explanation else None
