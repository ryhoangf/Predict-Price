import pandas as pd
import re
import unicodedata
import json
import os
from flashtext import KeywordProcessor

_DECOR_RE = re.compile(r'[★☆♪♡●◆■□◇△▲▼►◄※⚠️‼️【】]')
_EMOJI_RE = re.compile(
    "[" "\U0001F600-\U0001F64F" "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF" "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0" "\U000024C2-\U0001F251" "]+",
    flags=re.UNICODE,
)
_URL_RE = re.compile(r'http\S+|www\S+')
_WS_RE = re.compile(r'\s+')
_BRAND_FIX_RE = (
    (re.compile(r'\biphone\b', re.IGNORECASE), 'iPhone'),
    (re.compile(r'\bgalaxy\b', re.IGNORECASE), 'Galaxy'),
    (re.compile(r'\bpixel\b', re.IGNORECASE), 'Pixel'),
    (re.compile(r'\bxperia\b', re.IGNORECASE), 'Xperia'),
)

class PhoneInfoExtractor:
    def __init__(self, config_path='nlp_config.json'):
        """Khởi tạo Extractor, load cấu hình từ JSON và nạp vào FlashText"""
        self.config = self._load_config(config_path)
        
        # Khởi tạo các KeywordProcessor thay cho SpaCy (Siêu nhẹ, siêu nhanh)
        self.brand_processor = KeywordProcessor(case_sensitive=False)
        self.color_processor = KeywordProcessor(case_sensitive=False)
        self.variant_processor = KeywordProcessor(case_sensitive=False)
        
        self._setup_processors()
        spam = self.config.get('spam_keywords', [])
        self._spam_res = [re.compile(kw, re.IGNORECASE) for kw in spam]

    def _load_config(self, config_path):
        """Đọc file JSON chứa từ điển"""
        if not os.path.exists(config_path):
            # Thử NLP/config/ (cũ)
            nlp_dir = os.path.dirname(os.path.abspath(__file__))
            candidate1 = os.path.join(nlp_dir, 'config', 'nlp_config.json')
            # Thử predictprice/config/ (đúng)
            candidate2 = os.path.join(os.path.dirname(nlp_dir), 'config', 'nlp_config.json')
            if os.path.exists(candidate2):
                config_path = candidate2
            elif os.path.exists(candidate1):
                config_path = candidate1
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Lỗi đọc file cấu hình NLP ({e}). Đang dùng dict rỗng.")
            return {"brands": {}, "colors": {}, "variants": {}, "spam_keywords": []}

    def _setup_processors(self):
        """Nạp từ khóa từ JSON vào thuật toán cây của FlashText"""
        # 1. Nạp Brand
        for main_word, aliases in self.config.get('brands', {}).items():
            for alias in aliases:
                self.brand_processor.add_keyword(alias, main_word)
                
        # 2. Nạp Color
        for main_word, aliases in self.config.get('colors', {}).items():
            for alias in aliases:
                self.color_processor.add_keyword(alias, main_word)
                
        # 3. Nạp Variant (Pro, Max, Ultra...)
        for main_word, aliases in self.config.get('variants', {}).items():
            for alias in aliases:
                self.variant_processor.add_keyword(alias, main_word)

    def preprocess_text(self, text):
        if not isinstance(text, str):
            return ""
        
        # Chuẩn hóa Unicode
        text = unicodedata.normalize('NFKC', text)

        text = _DECOR_RE.sub(' ', text)
        text = _EMOJI_RE.sub(' ', text)
        text = _URL_RE.sub('', text)

        for cre in self._spam_res:
            text = cre.sub(' ', text)

        text = text.replace('　', ' ')
        text = _WS_RE.sub(' ', text).strip()

        for cre, repl in _BRAND_FIX_RE:
            text = cre.sub(repl, text)
        
        return text

    def extract_dict_features(self, text):
        """Dùng FlashText quét text 1 lần để lấy Brand, Color, Variant"""
        brands = self.brand_processor.extract_keywords(text)
        colors = self.color_processor.extract_keywords(text)
        variants = self.variant_processor.extract_keywords(text)
        
        # FlashText sẽ bắt các biến thể dài trước (VD: "Pro Max" sẽ được bắt thay vì tách rời "Pro" và "Max")
        extracted = {
            'brand': brands[0] if brands else None,
            'color': colors[0] if colors else None,
            'variant': ' '.join(list(dict.fromkeys(variants))) if variants else None
        }
        
        # Xử lý trường hợp hiếm khi chuỗi có cả "Pro Max" và "Pro" ở 2 nơi khác nhau
        if extracted['variant']:
            if 'Pro Max' in extracted['variant'] and 'Pro' in extracted['variant']:
                extracted['variant'] = extracted['variant'].replace('Pro Max', '').replace('Pro', 'Pro Max').strip()
                # Dọn dẹp khoảng trắng thừa nếu có
                extracted['variant'] = re.sub(r'\s+', ' ', extracted['variant']).strip()
                
        return extracted

    def extract_model_info(self, text):
        """Regex xử lý động cho Model Line và Number"""
        model_line, model_number = None, None
        
        if match := re.search(r'iPhone\s*(\d+|SE\d*|XR|XS|X)', text, re.IGNORECASE):
            model_line, model_number = "iPhone", match.group(1)
        elif match := re.search(r'Galaxy\s*([A-Z]*\s*\d+)', text, re.IGNORECASE):
            model_line, model_number = "Galaxy", match.group(1).strip()
        elif match := re.search(r'Pixel\s*(\d+[a-zA-Z]*)', text, re.IGNORECASE):
            model_line, model_number = "Pixel", match.group(1)
        elif match := re.search(r'Xperia\s*([A-Z0-9\s]+?)(?:\s|　|$|SO-|SOG|XQ-)', text, re.IGNORECASE):
            model_line, model_number = "Xperia", match.group(1).strip()
        elif match := re.search(r'Redmi\s*(Note\s*\d+[a-zA-Z]*|\d+[a-zA-Z]*)', text, re.IGNORECASE):
            model_line, model_number = "Redmi", match.group(1).strip()
        elif match := re.search(r'AQUOS\s*([a-zA-Z0-9\s]+?)(?:\s|　|$|SH-)', text, re.IGNORECASE):
            model_line, model_number = "AQUOS", match.group(1).strip()
            
        return model_line, model_number

    def extract_capacity(self, text):
        capacity_matches = re.findall(r'(\d+)\s*(GB|gb|TB|tb|ギガ|テラ)', text)
        if not capacity_matches: return None
        
        capacities = []
        for number, unit in capacity_matches:
            number = int(number)
            unit = 'GB' if unit.upper() in ['GB', 'ギガ'] else 'TB'
            gb_value = number * 1024 if unit == 'TB' else number
            capacities.append((gb_value, f"{number}{unit}"))
            
        if len(capacities) == 1: return capacities[0][1]
        
        storage_candidates = [cap for cap in capacities if cap[0] >= 32]
        if storage_candidates:
            return max(storage_candidates, key=lambda x: x[0])[1]
        return max(capacities, key=lambda x: x[0])[1]

    def extract_ram(self, text):
        capacity_matches = re.findall(r'(\d+)\s*(GB|gb|ギガ)', text)
        if not capacity_matches: return None
        
        capacities = [(int(num), f"{num}GB") for num, unit in capacity_matches]
        if len(capacities) == 1 and capacities[0][0] <= 24:
            return capacities[0][1]
            
        ram_candidates = [cap for cap in capacities if cap[0] <= 24]
        if ram_candidates:
            return min(ram_candidates, key=lambda x: x[0])[1]
        return None

    def extract_all_info(self, text):
        original_text = text
        preprocessed_text = self.preprocess_text(text)
        
        # 1. Quét từ điển siêu tốc (Brand, Color, Variant)
        dict_features = self.extract_dict_features(preprocessed_text)
        
        # 2. Dùng Regex cho các số liệu động (Model, Ram, Capacity)
        model_line, model_number = self.extract_model_info(preprocessed_text)
        ram = self.extract_ram(preprocessed_text)
        capacity = self.extract_capacity(preprocessed_text)
        
        # 3. Suy luận Brand nếu tiêu đề bị khuyết
        brand = dict_features['brand']
        if not brand and model_line:
            brand_inference = {'iPhone': 'Apple', 'Pixel': 'Google', 'Galaxy': 'Samsung', 'AQUOS': 'SHARP', 'Redmi': 'Xiaomi', 'Xperia': 'Sony'}
            brand = brand_inference.get(model_line)

        return {
            'original_title': original_text,
            'preprocessed_title': preprocessed_text,
            'brand': brand,
            'model_line': model_line,
            'model_number': model_number,
            'variant': dict_features['variant'],
            'color': dict_features['color'],
            'ram': ram,
            'capacity': capacity
        }

    @staticmethod
    def _cell_to_title_str(v):
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return ""
        return str(v)

    def process_dataframe(self, df, title_column='name'):
        if title_column not in df.columns:
            titles = pd.Series([""] * len(df), index=df.index)
        else:
            titles = df[title_column].map(self._cell_to_title_str)

        n = len(df)
        feats = []
        for i in range(n):
            feats.append(self.extract_all_info(titles.iat[i]))
            if (i + 1) % 1000 == 0:
                print(f"Processed {i + 1}/{n} records...")

        feat_df = pd.DataFrame(feats, index=df.index)
        out = df.copy()
        for c in feat_df.columns:
            out[c] = feat_df[c].values
        return out

def main():
    # Chú ý đường dẫn file JSON cấu hình
    extractor = PhoneInfoExtractor(config_path='nlp_config.json')
    
    print("Đọc từ all_items.csv")
    try:
        df = pd.read_csv('book_data/all_items.csv')
        print(f"Tổng số records: {len(df)}\n")
        
        # Trích xuất từ cột 'name' theo dữ liệu mẫu của bạn
        results = extractor.process_dataframe(df, title_column='name')
        
        results.to_csv('book_data/extracted_phone_info.csv', index=False, encoding='utf-8-sig')
        print("Saved to book_data/extracted_phone_info.csv")
        
        essential_columns = [
            'preprocessed_title', 'brand', 'model_line', 'model_number',
            'variant', 'ram', 'capacity', 'color'
        ]
        
        # Chỉ filter những cột tồn tại trong results để tránh lỗi KeyError
        results_essential = results[[c for c in essential_columns if c in results.columns]]
        results_essential.to_csv('book_data/phone_features.csv', index=False, encoding='utf-8-sig')
        print("Saved to book_data/phone_features.csv")

        print("\n20 kết quả đầu tiên:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 200)
        print(results_essential.head(20))
        
    except FileNotFoundError:
        print("⚠️ Không tìm thấy file 'book_data/all_items.csv'.")

if __name__ == "__main__":
    main()