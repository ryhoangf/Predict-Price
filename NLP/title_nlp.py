import spacy
from spacy.matcher import PhraseMatcher
import re
import pandas as pd
import unicodedata

class PhoneInfoExtractor:
    def __init__(self):
        self.nlp = spacy.load("ja_core_news_sm")
        self.phrase_matcher = PhraseMatcher(self.nlp.vocab, attr="LOWER")
        self._setup_brand_matcher()
    
    def preprocess_text(self, text):
        if not isinstance(text, str):
            return ""
        
        #Chuẩn hóa Unicode (vd: １４→ 14)
        text = unicodedata.normalize('NFKC', text)
        
        #Loại bỏ ký tự trang trí
        text = re.sub(r'[★☆♪♡●◆■□◇△▲▼►◄※⚠️‼️]', ' ', text)
        
        #Loại bỏ emoji
        emoji_pattern = re.compile(
            "["
            u"\U0001F600-\U0001F64F" 
            u"\U0001F300-\U0001F5FF"
            u"\U0001F680-\U0001F6FF"
            u"\U0001F1E0-\U0001F1FF"
            u"\U00002702-\U000027B0"
            u"\U000024C2-\U0001F251"
            "]+", flags=re.UNICODE
        )
        text = emoji_pattern.sub(r' ', text)
        
        #Loại bỏ URL
        text = re.sub(r'http\S+|www\S+', '', text)
        
        #xóa từ khóa spam
        spam_keywords = [
            r'即日発送', r'送料無料', r'在庫あり', r'限定',
            r'セール中', r'SALE', r'おまけ付き', r'期間限定',
            r'箱付き', r'コード\d+本付き', r'画面保護フィルム付き',
            r'外箱痛みあり', r'アウトレット', r'残債なし',
            r'判定[○〇×]', r'管理番号\w+', r'SYS', r'\[721\]'
        ]
        
        for keyword in spam_keywords:
            text = re.sub(keyword, ' ', text, flags=re.IGNORECASE)

        #xóa metadata 【】
        text = re.sub(r'【[^】]*】', ' ', text)
        
        #Chuẩn hóa khoảng trắng
        text = text.replace('　', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        
        #Chuẩn hóa tên model
        text = re.sub(r'\biphone\b', 'iPhone', text, flags=re.IGNORECASE)
        text = re.sub(r'\bgalaxy\b', 'Galaxy', text, flags=re.IGNORECASE)
        text = re.sub(r'\bpixel\b', 'Pixel', text, flags=re.IGNORECASE)
        text = re.sub(r'\bxperia\b', 'Xperia', text, flags=re.IGNORECASE)
        
        return text
    
    def _setup_brand_matcher(self):
        """Setup PhraseMatcher cho brands"""
        brands = [
            "Apple", "アップル", "Samsung", "サムスン",
            "Google", "グーグル", "Sony", "ソニー",
            "Xiaomi", "シャオミ", "OPPO", "オッポ",
            "Realme", "リアルミー", "OUKITEL", "オウキテル",
            "Motorola", "モトローラ", "ASUS", "エイスース",
            "Huawei", "ファーウェイ", "SHARP", "シャープ"
        ]
        
        patterns = [self.nlp.make_doc(brand) for brand in brands]
        self.phrase_matcher.add("BRAND", patterns)
        
        self.brand_normalize = {
            'apple': 'Apple', 'アップル': 'Apple',
            'samsung': 'Samsung', 'サムスン': 'Samsung',
            'google': 'Google', 'グーグル': 'Google',
            'sony': 'Sony', 'ソニー': 'Sony',
            'xiaomi': 'Xiaomi', 'シャオミ': 'Xiaomi',
            'oppo': 'OPPO', 'オッポ': 'OPPO',
            'realme': 'Realme', 'リアルミー': 'Realme',
            'oukitel': 'OUKITEL', 'オウキテル': 'OUKITEL',
            'motorola': 'Motorola', 'モトローラ': 'Motorola',
            'asus': 'ASUS', 'エイスース': 'ASUS',
            'huawei': 'Huawei', 'ファーウェイ': 'Huawei',
            'sharp': 'SHARP', 'シャープ': 'SHARP'
        }
    
    def extract_brand(self, text):
        doc = self.nlp(text)
        matches = self.phrase_matcher(doc)
        
        if matches:
            match_id, start, end = matches[0]
            brand_text = doc[start:end].text
            return self.brand_normalize.get(brand_text.lower(), brand_text)
        
        return None
    
    def extract_model_info(self, text):
        """Trích xuất Model Line và Model Number"""
        model_line = None
        model_number = None
        
        # iPhone patterns
        iphone_match = re.search(r'iPhone\s*(\d+|SE|XR|XS|X)', text, re.IGNORECASE)
        if iphone_match:
            model_line = "iPhone"
            model_number = iphone_match.group(1)
        
        # Galaxy patterns
        galaxy_match = re.search(r'Galaxy\s*([A-Z]*\s*\d+)', text, re.IGNORECASE)
        if galaxy_match:
            model_line = "Galaxy"
            model_number = galaxy_match.group(1).strip()
        
        # Pixel patterns
        pixel_match = re.search(r'Pixel\s*(\d+[a-zA-Z]*)', text, re.IGNORECASE)
        if pixel_match:
            model_line = "Pixel"
            model_number = pixel_match.group(1)
        
        # Xperia patterns
        xperia_match = re.search(r'Xperia\s*([A-Z0-9\s]+?)(?:\s|　|$|SO-|SOG|XQ-)', text, re.IGNORECASE)
        if xperia_match:
            model_line = "Xperia"
            model_number = xperia_match.group(1).strip()
        
        # Redmi patterns
        redmi_match = re.search(r'Redmi\s*(Note\s*\d+[a-zA-Z]*|\d+[a-zA-Z]*)', text, re.IGNORECASE)
        if redmi_match:
            model_line = "Redmi"
            model_number = redmi_match.group(1).strip()
        
        # Realme patterns
        realme_match = re.search(r'Realme\s*(\d+[a-zA-Z]*)', text, re.IGNORECASE)
        if realme_match:
            model_line = "Realme"
            model_number = realme_match.group(1)
        
        # AQUOS patterns
        aquos_match = re.search(r'AQUOS\s*([a-zA-Z0-9\s]+?)(?:\s|　|$|SH-)', text, re.IGNORECASE)
        if aquos_match:
            model_line = "AQUOS"
            model_number = aquos_match.group(1).strip()
        
        return model_line, model_number
    
    def extract_variant(self, text):
        variants = []
        
        if re.search(r'\bPro\b', text, re.IGNORECASE):
            variants.append('Pro')
        if re.search(r'\bMax\b', text, re.IGNORECASE):
            variants.append('Max')
        if re.search(r'\bUltra\b', text, re.IGNORECASE):
            variants.append('Ultra')
        if re.search(r'\bPlus\b', text, re.IGNORECASE):
            variants.append('Plus')
        if re.search(r'\bmini\b', text, re.IGNORECASE):
            variants.append('mini')
        
        if 'プロ' in text:
            variants.append('Pro')
        if 'マックス' in text:
            variants.append('Max')
        
        return ' '.join(list(dict.fromkeys(variants))) if variants else None
    
    def extract_capacity(self, text):
        capacity_matches = re.findall(r'(\d+)\s*(GB|gb|TB|tb|ギガ|テラ)', text)
        
        if not capacity_matches:
            return None
        
        capacities = []
        for number, unit in capacity_matches:
            number = int(number)
            unit = unit.upper()
            if unit in ['ギガ']:
                unit = 'GB'
            elif unit in ['テラ']:
                unit = 'TB'
            
            gb_value = number * 1024 if unit == 'TB' else number
            capacities.append((gb_value, f"{number}{unit}"))
        
        if len(capacities) == 1:
            return capacities[0][1]
        
        storage_candidates = [cap for cap in capacities if cap[0] >= 32]
        
        if storage_candidates:
            max_storage = max(storage_candidates, key=lambda x: x[0])
            return max_storage[1]
        
        max_capacity = max(capacities, key=lambda x: x[0])
        return max_capacity[1]
    
    def extract_ram(self, text):
        capacity_matches = re.findall(r'(\d+)\s*(GB|gb|ギガ)', text)
        
        if not capacity_matches:
            return None
        
        capacities = []
        for number, unit in capacity_matches:
            number = int(number)
            capacities.append((number, f"{number}GB"))
        
        if len(capacities) == 1:
            if capacities[0][0] <= 24:
                return capacities[0][1]
            else:
                return None
        
        ram_candidates = [cap for cap in capacities if cap[0] <= 24]
        
        if ram_candidates:
            min_ram = min(ram_candidates, key=lambda x: x[0])
            return min_ram[1]
        
        return None
    
    def extract_color(self, text):
        colors = {
            'シルバー': 'Silver', 'ゴールド': 'Gold', 'ブラック': 'Black',
            'ホワイト': 'White', 'ブルー': 'Blue', 'レッド': 'Red',
            'グリーン': 'Green', 'ピンク': 'Pink', 'パープル': 'Purple',
            'silver': 'Silver', 'gold': 'Gold', 'black': 'Black',
            'white': 'White', 'blue': 'Blue', 'red': 'Red',
            'midnight': 'Midnight', 'starlight': 'Starlight',
            'sea': 'Sea', 'シー': 'Sea'
        }
        
        for jp_color, en_color in colors.items():
            if jp_color.lower() in text.lower():
                return en_color
        return None
    
    def extract_all_info(self, text):
        #Preprocessing
        original_text = text
        text = self.preprocess_text(text)
        
        #Extract với rules
        brand = self.extract_brand(text)
        model_line, model_number = self.extract_model_info(text)
        
        #Brand inference (nếu không tìm thấy brand)
        if not brand and model_line:
            brand_inference = {
                'iPhone': 'Apple', 'Pixel': 'Google', 'Galaxy': 'Samsung',
                'Xperia': 'Sony', 'Redmi': 'Xiaomi', 'AQUOS': 'SHARP',
                'Realme': 'Realme', 'Huawei P': 'Huawei'
            }
            brand = brand_inference.get(model_line)
        
        #Extract các features khác
        variant = self.extract_variant(text)
        ram = self.extract_ram(text)
        capacity = self.extract_capacity(text)
        color = self.extract_color(text)
        
        return {
            'original_title': original_text,
            'preprocessed_title': text,
            'brand': brand,
            'model_line': model_line,
            'model_number': model_number,
            'variant': variant,
            'ram': ram,
            'capacity': capacity,
            'color': color
        }
    
    def process_dataframe(self, df, title_column='title'):
        results = []
        for idx, row in df.iterrows():
            title = row[title_column]
            extracted = self.extract_all_info(title)
            result = {**row.to_dict(), **extracted}
            results.append(result)
            
            if (idx + 1) % 20 == 0:
                print(f"Processed {idx + 1}/{len(df)} records...")
        
        return pd.DataFrame(results)

def main():
    extractor = PhoneInfoExtractor()
    
    print("Đọc từ all_items.csv")
    df = pd.read_csv('book_data/all_items.csv')
    print(f"Tổng số records: {len(df)}\n")
    
    results = extractor.process_dataframe(df, title_column='name')
    
    results.to_csv('book_data/extracted_phone_info.csv', index=False, encoding='utf-8-sig')
    print("\Saved to book_data/extracted_phone_info.csv")
    
    essential_columns = [
        'preprocessed_title', 'brand', 'model_line', 'model_number',
        'variant', 'ram', 'capacity', 'color', 'condition'
    ]
    
    results_essential = results[essential_columns]
    results_essential.to_csv('book_data/phone_features.csv', index=False, encoding='utf-8-sig')
    print("Saved to book_data/phone_features.csv")

    print("\n20 kết quả đầu tiên")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 50)
    print(results_essential.head(20))


if __name__ == "__main__":
    main()
