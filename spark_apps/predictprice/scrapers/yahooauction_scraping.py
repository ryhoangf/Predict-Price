import os, sys
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config as config
import pandas as pd
import random, time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def scrape_yahooauction(end_page: int) -> pd.DataFrame:
    """
    Scrape Yahoo Auction với logic phân biệt Auction vs Fixed Price
    """
    headers = {**config.HEADERS_BASE, "Referer": config.REFERERS["yahoo"]}
    
    all_items = []

    for page in range(1, end_page+1):
        url = (
            f"{config.ENDPOINTS['yahoo_base']}/"
            f"{config.ENDPOINTS['yahoo_category_id']}?page={page}"
        )
        print(f"→ [YahooAuction] Fetching page {page}/{end_page}")
        resp = config.fetch(url, headers)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        items = soup.find_all("li", class_="itemCard")
        print(f"   → Found {len(items)} items")

        for it in items:
            a = it.find("a", href=True)
            href = a["href"] if a else None
            link = urljoin("https://buyee.jp", href) if href else None
            
            if not link:
                continue

            n = it.find("div", class_="itemCard__itemName")
            name = n.get_text(strip=True) if n else None
            
            # ===== ANALYZE PRICE STRUCTURE =====
            price_info = extract_price_from_card(it)
            
            if not price_info or not price_info['price']:
                continue
            
            # ✅ CHỈ LƯU CÁC FIELD CHUẨN (giống Mercari & Rakuma)
            all_items.append({
                'link': link,
                'name': name,
                'price': price_info['price']
                # ✅ Drop price_type và listing_type để match với 2 scrapers khác
            })

        time.sleep(random.uniform(*config.DELAY))

    df = pd.DataFrame(all_items)
    
    if df.empty:
        print("   ❌ No valid items found")
        return df
    
    # ===== FETCH DETAILS =====
    print(f"\n   → Fetching details for {len(df)} items...")
    df["condition"] = df["link"].apply(lambda u: config.safe_fetch_with_retry(get_item_condition_yahooauction, u))
    df["explanation"] = df["link"].apply(lambda u: config.safe_fetch_with_retry(get_item_explanation_yahooauction, u))
    
    # ✅ CONSISTENT OUTPUT: link, name, price, condition, explanation
    return df


def extract_price_from_card(item_card) -> dict:
    """
    Phân tích cấu trúc giá từ itemCard
    """
    result = {
        'price': None,
        'price_type': None,
        'listing_type': None
    }
    
    # ===== CHECK LISTING TYPE =====
    fleamarket_div = item_card.find("div", class_="itemCard__fleamarket")
    if fleamarket_div:
        result['listing_type'] = 'fleamarket'
    else:
        result['listing_type'] = 'auction'
    
    # ===== FIND ALL PRICE ITEMS =====
    price_details = item_card.find("div", class_="g-priceDetails")
    if not price_details:
        return result
    
    price_items = price_details.find_all("li", class_="g-priceDetails__item")
    
    buyout_price = None
    fixed_price = None
    current_price = None
    
    for item in price_items:
        title_span = item.find("span", class_="g-title")
        price_span = item.find("span", class_="g-price")
        
        if not title_span or not price_span:
            continue
        
        title = title_span.get_text(strip=True)
        price = price_span.get_text(strip=True)
        
        if "Buyout Price" in title or "即決価格" in title:
            buyout_price = price
        elif "Price" in title and "Current" not in title and "Buyout" not in title:
            fixed_price = price
        elif "Current Price" in title or "現在価格" in title:
            current_price = price
    
    # ===== DECISION LOGIC =====
    if buyout_price:
        result['price'] = buyout_price
        result['price_type'] = 'buyout'
    elif fixed_price:
        result['price'] = fixed_price
        result['price_type'] = 'fixed'
    elif current_price and result['listing_type'] == 'fleamarket':
        result['price'] = current_price
        result['price_type'] = 'fixed'
    else:
        result['price'] = None
        result['price_type'] = 'current_bid_skip'
    
    return result


def get_item_condition_yahooauction(url: str) -> str:
    """
    Lấy condition từ <div class="itemDetail__listValue">
    """
    resp = config.fetch(url, config.HEADERS_BASE)
    if not resp:
        return None
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Tìm div chứa "Item Condition" trong label
    labels = soup.find_all("div", class_="itemDetail__listLabel")
    for label in labels:
        if "Item Condition" in label.get_text(strip=True) or "Condition" in label.get_text(strip=True):
            value_div = label.find_next_sibling("div", class_="itemDetail__listValue")
            if value_div:
                return value_div.get_text(strip=True)
    
    # Fallback
    for div in soup.find_all("div", class_="itemDetail__listValue"):
        prev_div = div.find_previous_sibling("div")
        if prev_div and "condition" in prev_div.get_text(strip=True).lower():
            return div.get_text(strip=True)
    
    return None


def get_item_explanation_yahooauction(url: str) -> str:
    """
    Lấy explanation (nội dung tiếng Nhật) từ iframe trong section itemDescription
    """
    resp = config.fetch(url, config.HEADERS_BASE)
    if not resp:
        return None
    
    soup = BeautifulSoup(resp.text, "html.parser")
    
    # Tìm section với id="itemDescription"
    desc_section = soup.find("section", id="itemDescription")
    if not desc_section:
        desc_section = soup.find("section", class_="itemDescription")
    
    if desc_section:
        iframe = desc_section.find("iframe", src=True)
        if iframe:
            iframe_src = iframe.get("src")
            if iframe_src:
                iframe_src = iframe_src.split("#")[0]
                
                if iframe_src.startswith("/"):
                    iframe_url = urljoin("https://buyee.jp", iframe_src)
                else:
                    iframe_url = iframe_src
                
                iframe_resp = config.fetch(iframe_url, config.HEADERS_BASE)
                if iframe_resp:
                    iframe_soup = BeautifulSoup(iframe_resp.text, "html.parser")
                    body = iframe_soup.find("body")
                    if body:
                        explanation = body.get_text(separator=" ", strip=True)
                        explanation = " ".join(explanation.split())
                        
                        if explanation and len(explanation) > 5:
                            return explanation
    
    # Fallback
    fallback_ids = ["item-description", "auction_item_description", "item_description"]
    for section_id in fallback_ids:
        section = soup.find("section", id=section_id)
        if section:
            div = section.find("div")
            if div:
                explanation = div.get_text(separator=" ", strip=True)
                explanation = " ".join(explanation.split())
                if explanation and len(explanation) > 5:
                    return explanation
    
    return None
