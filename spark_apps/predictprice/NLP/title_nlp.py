import functools
import pandas as pd
import re
import unicodedata
import json
import os
from typing import Any

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
    (re.compile(r'\bpoco\b', re.IGNORECASE), 'POCO'),
    (re.compile(r'\bmoto\b', re.IGNORECASE), 'Moto'),
    (re.compile(r'\bzenfone\b', re.IGNORECASE), 'Zenfone'),
    (re.compile(r'\brealme\b', re.IGNORECASE), 'Realme'),
)
_MODEL_BRAND_OVERRIDES = {
    "iPhone": "Apple",
    "Galaxy": "Samsung",
    "Pixel": "Google",
    "Xperia": "Sony",
    "AQUOS": "SHARP",
    "Redmi": "Xiaomi",
    "Reno": "OPPO",
    "Find": "OPPO",
    "A": "OPPO",
    "Mate": "Huawei",
    "P": "Huawei",
    "nova": "Huawei",
    "Enjoy": "Huawei",
    "POCO": "Xiaomi",
    "Xiaomi": "Xiaomi",
    "Mi": "Xiaomi",
    "Moto": "Motorola",
    "Edge": "Motorola",
    "Razr": "Motorola",
    "Zenfone": "ASUS",
    "ROG Phone": "ASUS",
    "Realme": "Realme",
    "GT": "Realme",
}
_BRAND_CANONICAL = {
    "apple": "Apple",
    "samsung": "Samsung",
    "google": "Google",
    "sony": "Sony",
    "xiaomi": "Xiaomi",
    "oppo": "OPPO",
    "sharp": "SHARP",
    "huawei": "Huawei",
    "motorola": "Motorola",
    "asus": "ASUS",
    "realme": "Realme",
}
_SKIP_BRAND_ALIASES = {"op"}
_NON_BRAND_WORDS = {
    "black", "white", "red", "green", "blue", "yellow", "orange", "purple",
    "pink", "gold", "silver", "gray", "grey", "graphite", "starlight",
    "midnight", "titanium", "natural", "cream", "lavender", "mint",
}
_IPHONE_13_STORAGE_GB = {"128", "256", "512", "1024"}
_COMMON_STORAGE_GB = {"16", "32", "64", "128", "256", "512", "1024", "2048"}

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
            canonical = _BRAND_CANONICAL.get(str(main_word).strip().lower(), main_word)
            for alias in aliases:
                if str(alias).strip().lower() in _SKIP_BRAND_ALIASES:
                    continue
                self.brand_processor.add_keyword(alias, canonical)
                
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
        if variants and "Pro Max" in variants:
            variant_str = "Pro Max"
        elif variants:
            variant_str = " ".join(dict.fromkeys(variants))
        else:
            variant_str = None

        extracted = {
            'brand': brands[0] if brands else None,
            'color': colors[0] if colors else None,
            'variant': variant_str,
        }
        return extracted

    def extract_model_info(self, text):
        """Regex xử lý động cho Model Line và Number"""
        model_line, model_number = None, None
        
        if match := re.search(r'iPhone\s*(\d{1,2})\s*mini\b', text, re.IGNORECASE):
            model_line, model_number = "iPhone", f"{match.group(1)} mini"
        elif match := re.search(
            r'iPhone\s*(SE\s*3|SE\s*2|SE3|SE2|SE)\b',
            text,
            re.IGNORECASE,
        ):
            raw = match.group(1).upper().replace(" ", "")
            model_line, model_number = "iPhone", raw
        elif match := re.search(r'iPhone\s*(\d{1,2}|XR|XS|X)\b', text, re.IGNORECASE):
            model_line, model_number = "iPhone", match.group(1)
        elif match := re.search(
            r'Galaxy\s*Z\s*(Flip|Fold)\s*(\d+)|Galaxy\s*Z\s*(Flip|Fold)(\d+)',
            text,
            re.IGNORECASE,
        ):
            kind = (match.group(1) or match.group(3)).title()
            num = match.group(2) or match.group(4)
            model_line, model_number = "Galaxy", f"Z {kind} {num}"
        elif match := re.search(
            r'Galaxy\s+(Note\s*\d+\s*(?:Ultra|\+)?|S\d+\s*(?:Ultra|\+|FE)?|A\s*\d+\w*|M\s*\d+)',
            text,
            re.IGNORECASE,
        ):
            model_line, model_number = "Galaxy", match.group(1).strip()
        elif match := re.search(r'Galaxy\s*([A-Z]*\s*\d+\w*)', text, re.IGNORECASE):
            model_line, model_number = "Galaxy", match.group(1).strip()
        elif match := re.search(r'Pixel\s*(\d+[a-zA-Z]*)', text, re.IGNORECASE):
            model_line, model_number = "Pixel", match.group(1)
        elif match := re.search(r'Xperia\s*(\d+\s*(?:[IVX]+|[A-Z])?|\d+[A-Z]?|[A-Z]\d+)', text, re.IGNORECASE):
            model_line, model_number = "Xperia", match.group(1).strip()
        elif match := re.search(r'Xperia\s*([A-Z0-9\s]+?)(?:\s|　|$|SO-|SOG|XQ-)', text, re.IGNORECASE):
            model_line, model_number = "Xperia", match.group(1).strip()
        elif match := re.search(r'Redmi\s*(Note\s*\d+[a-zA-Z]*|\d+[a-zA-Z]*)', text, re.IGNORECASE):
            model_line, model_number = "Redmi", match.group(1).strip()
        elif match := re.search(r'POCO\s*(F|X|M|C)\s*(\d+)\s*(Pro|Ultra|GT)?', text, re.IGNORECASE):
            suffix = (match.group(3) or "").strip().title()
            model_line = "POCO"
            model_number = f"{match.group(1).upper()}{match.group(2)} {suffix}".strip()
        elif match := re.search(r'\bMi\s*(\d+)\s*(Lite|Pro|Ultra|T)?\b', text, re.IGNORECASE):
            suffix = (match.group(2) or "").strip().title()
            model_line = "Mi"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'\bXiaomi\s*(\d+)\s*(Lite|Pro|Ultra|T)?\b', text, re.IGNORECASE):
            suffix = (match.group(2) or "").strip().title()
            model_line = "Xiaomi"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'AQUOS\s*([a-zA-Z0-9\s]+?)(?:\s|　|$|SH-)', text, re.IGNORECASE):
            model_line, model_number = "AQUOS", match.group(1).strip()
        elif match := re.search(
            r'(?i)(?:oppo\s+)?reno\s*(\d+)\s*([a-z])?\b|(?:oppo\s+)?reno(\d+)([a-z])\b',
            text,
        ):
            num = match.group(1) or match.group(3)
            letter = (match.group(2) or match.group(4) or "").upper()
            model_line = "Reno"
            model_number = f"{num} {letter}".strip() if letter else num
        elif match := re.search(r'(?i)(?:oppo\s+)?find\s*([x]?\d+)\s*([a-z])?\b', text):
            model_line = "Find"
            suffix = (match.group(2) or "").upper()
            model_number = f"{match.group(1)} {suffix}".strip() if suffix else match.group(1)
        elif match := re.search(r'(?i)oppo\s+a\s*(\d+)s\b', text):
            model_line, model_number = "A", f"{match.group(1)}s"
        elif match := re.search(r'(?i)oppo\s+a\s*(\d+)([sx])\b', text):
            model_line, model_number = "A", f"{match.group(1)}{match.group(2).upper()}"
        elif match := re.search(r'(?i)oppo\s+a\s*(\d+)\b', text):
            model_line, model_number = "A", match.group(1)
        elif match := re.search(
            r'(?i)(?:huawei\s+)?mate\s*(\d+)\s*(pro|lite|rs)?',
            text,
        ):
            suffix = (match.group(2) or "").strip()
            model_line = "Mate"
            model_number = f"{match.group(1)} {suffix.title()}".strip() if suffix else match.group(1)
        elif match := re.search(
            r'(?i)(?:\bhuawei\s+)?\bp\s*(\d+)\s*(pro|lite|plus)?\b',
            text,
        ):
            suffix = (match.group(2) or "").strip()
            model_line = "P"
            model_number = f"{match.group(1)} {suffix.title()}".strip() if suffix else match.group(1)
        elif match := re.search(r'(?i)nova\s*(\d+[a-z]*)', text):
            model_line, model_number = "nova", match.group(1).strip()
        elif match := re.search(r'(?i)(?:huawei\s+)?enjoy\s*(\d+)', text):
            model_line, model_number = "Enjoy", match.group(1)
        elif match := re.search(r'(?i)moto\s*g\s*(\d+)\s*(power|play|plus|stylus)?', text):
            suffix = (match.group(2) or "").strip().title()
            model_line = "Moto"
            model_number = f"G{match.group(1)} {suffix}".strip()
        elif match := re.search(r'(?i)motorola\s+edge\s*(\d+)\s*(pro|neo|fusion|ultra)?', text):
            suffix = (match.group(2) or "").strip().title()
            model_line = "Edge"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'(?i)edge\s*(\d+)\s*(pro|neo|fusion|ultra)?', text):
            suffix = (match.group(2) or "").strip().title()
            model_line = "Edge"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'(?i)(?:motorola\s+)?razr\s*(\d+)\s*(ultra)?', text):
            suffix = (match.group(1) or "").strip()
            extra = (match.group(2) or "").strip().title()
            model_line = "Razr"
            model_number = f"{suffix} {extra}".strip()
        elif match := re.search(r'(?i)zenfone\s*(\d+)\s*(ultra)?', text):
            suffix = (match.group(2) or "").strip().title()
            model_line = "Zenfone"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'(?i)rog\s*phone\s*(\d+)\s*(pro|ultimate)?', text):
            suffix = (match.group(2) or "").strip().title()
            model_line = "ROG Phone"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'(?i)realme\s*gt\s*(\d+)\s*(neo|pro)?', text):
            suffix = (match.group(2) or "").strip().title()
            model_line = "GT"
            model_number = f"{match.group(1)} {suffix}".strip()
        elif match := re.search(r'(?i)realme\s*(\d+)\s*(pro|plus|x|i)?', text):
            suffix = (match.group(2) or "").strip().upper()
            model_line = "Realme"
            model_number = f"{match.group(1)} {suffix}".strip()

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
            
        storage_candidates = [cap for cap in capacities if cap[0] >= 32]
        if storage_candidates:
            return max(storage_candidates, key=lambda x: x[0])[1]
        return None

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

    def normalize_brand_for_model(self, brand, model_line):
        override = _MODEL_BRAND_OVERRIDES.get(model_line)
        if override:
            return override
        if not brand:
            return None
        brand_str = str(brand).strip()
        if brand_str.lower() in _NON_BRAND_WORDS:
            return None
        return brand_str

    def filter_capacity_for_model(self, model_line, model_number, capacity):
        if not capacity:
            return None
        storage_gb = _normalize_spec_gb(capacity)
        model_number_lc = str(model_number or "").strip().lower()
        if model_line == "iPhone" and model_number_lc.startswith("13"):
            if storage_gb not in _IPHONE_13_STORAGE_GB:
                return None
        return capacity

    def extract_all_info(self, text):
        original_text = text
        preprocessed_text = self.preprocess_text(text)
        
        # 1. Quét từ điển siêu tốc (Brand, Color, Variant)
        dict_features = self.extract_dict_features(preprocessed_text)
        
        # 2. Dùng Regex cho các số liệu động (Model, Ram, Capacity)
        model_line, model_number = self.extract_model_info(preprocessed_text)
        ram = self.extract_ram(preprocessed_text)
        capacity = self.extract_capacity(preprocessed_text)
        capacity = self.filter_capacity_for_model(model_line, model_number, capacity)
        
        # 3. Suy luận Brand nếu tiêu đề bị khuyết
        brand = self.normalize_brand_for_model(dict_features['brand'], model_line)
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


# --- Catalog identity: products.name, model_series, base_specs (dùng trong etl.py) ---

_VARIANT_ONLY = frozenset(
    {"pro", "max", "lite", "plus", "ultra", "mini", "se", "fe", "neo", "prime", "rs"}
)


def _present(val: Any) -> bool:
    if val is None:
        return False
    try:
        if pd.isna(val):
            return False
    except (TypeError, ValueError):
        pass
    if isinstance(val, str) and not val.strip():
        return False
    return True


def _variant_without_redundancy(variant: Any, model_line: Any, model_number: Any) -> str | None:
    if not _present(variant):
        return None
    base_tokens = {
        t.lower()
        for t in f"{model_line or ''} {model_number or ''}".split()
        if t
    }
    kept = [t for t in str(variant).split() if t.lower() not in base_tokens]
    return " ".join(kept).strip() or None


def build_standard_name(row: dict) -> str | None:
    """Tên catalog (products.name): model + variant; brand/specs tách cột MySQL."""
    if not _present(row.get("model_line")):
        return None

    variant = _variant_without_redundancy(
        row.get("variant"), row.get("model_line"), row.get("model_number")
    )
    parts = [row.get("model_line"), row.get("model_number"), variant]
    name = " ".join(str(p) for p in parts if _present(p)).strip()
    if not name:
        return None

    tokens = name.lower().split()
    if all(t in _VARIANT_ONLY for t in tokens):
        return None
    if len(tokens) == 1 and tokens[0] in _VARIANT_ONLY:
        return None
    return name


def _normalize_spec_gb(value: Any) -> str | None:
    if not _present(value):
        return None
    s = str(value).strip().upper().replace(" ", "")
    m_tb = re.match(r"^(\d+(?:\.\d+)?)TB$", s)
    if m_tb:
        return str(int(round(float(m_tb.group(1)) * 1024)))
    m_gb = re.match(r"^(\d+(?:\.\d+)?)GB?$", s)
    if m_gb:
        return str(int(round(float(m_gb.group(1)))))
    m_num = re.match(r"^(\d+(?:\.\d+)?)$", s)
    if m_num:
        return str(int(round(float(m_num.group(1)))))
    return None


def _is_iphone_row(row: dict) -> bool:
    model_line = str(row.get("model_line") or "").strip().lower()
    if model_line == "iphone":
        return True
    base_name = build_standard_name(row) or ""
    return base_name.lower().startswith("iphone ")


def _is_iphone_13_row(row: dict) -> bool:
    if str(row.get("model_line") or "").strip().lower() == "iphone":
        return str(row.get("model_number") or "").strip().lower().startswith("13")
    base_name = build_standard_name(row) or ""
    return base_name.lower().startswith("iphone 13")


def _specs_from_row(row: dict) -> tuple[str | None, str | None]:
    if _present(row.get("capacity")):
        storage_val = row.get("capacity")
    elif _present(row.get("name_raw")):
        storage_val = None
    else:
        storage_val = row.get("storage")
    storage = _normalize_spec_gb(storage_val)
    ram = _normalize_spec_gb(row.get("ram"))
    if storage and storage not in _COMMON_STORAGE_GB:
        storage = None
    if ram:
        try:
            ram_gb = int(ram)
        except (TypeError, ValueError):
            ram_gb = 0
        if ram_gb <= 0 or ram_gb > 24:
            ram = None
    if storage and ram and storage == ram:
        ram = None
    if _is_iphone_13_row(row) and storage not in _IPHONE_13_STORAGE_GB:
        storage = None
    if _is_iphone_row(row):
        ram = None
    return storage, ram


def _format_storage_label(storage_gb: str | None) -> str | None:
    if not storage_gb:
        return None
    try:
        gb = int(storage_gb)
    except (TypeError, ValueError):
        return None
    if gb <= 0:
        return None
    if gb >= 1024 and gb % 1024 == 0:
        return f"{gb // 1024}TB"
    return f"{gb}GB"


def _format_ram_label(ram_gb: str | None) -> str | None:
    if not ram_gb:
        return None
    try:
        gb = int(ram_gb)
    except (TypeError, ValueError):
        return None
    if gb <= 0:
        return None
    return f"{gb}GB RAM"


def build_product_display_name(row: dict) -> str | None:
    """Catalog display name: model identity plus known storage/RAM suffix."""
    base_name = build_standard_name(row)
    if not base_name:
        return None
    storage, ram = _specs_from_row(row)
    suffixes = [x for x in (_format_storage_label(storage), _format_ram_label(ram)) if x]
    if not suffixes:
        return base_name
    base_lc = base_name.lower()
    kept = [s for s in suffixes if s.lower() not in base_lc]
    return " ".join([base_name, *kept]).strip()


def build_product_identity_key(row: dict) -> str | None:
    """Stable product key: brand + model identity + normalized storage/RAM."""
    base_name = build_standard_name(row)
    if not base_name:
        return None
    brand = str(row.get("brand") or "").strip().lower()
    storage, ram = _specs_from_row(row)
    return "|".join(
        [
            brand,
            re.sub(r"\s+", " ", base_name).strip().lower(),
            storage or "",
            ram or "",
        ]
    )


def build_model_series(row: dict) -> str:
    parts = [row.get("model_line"), row.get("model_number")]
    return " ".join(str(p) for p in parts if _present(p)).strip()


@functools.lru_cache(maxsize=1)
def _default_phone_extractor() -> PhoneInfoExtractor:
    return PhoneInfoExtractor()


def resolve_product_ml_identity(
    product: dict[str, Any],
    *,
    extractor: PhoneInfoExtractor | None = None,
) -> dict[str, str]:
    """
    Parse products.name → model_line, model_number, variant cho SmartPricePredictor.
    model_series trong MySQL chỉ là thế hệ (iPhone 15); variant nằm trong name.
    """
    name = str(product.get("name") or "").strip()
    if name:
        ext = extractor or _default_phone_extractor()
        feats = ext.extract_all_info(name)
        model_line = feats.get("model_line")
        if _present(model_line):
            variant = _variant_without_redundancy(
                feats.get("variant"), model_line, feats.get("model_number")
            )
            return {
                "model_line": str(model_line).strip(),
                "model_number": str(feats.get("model_number") or "").strip(),
                "variant": str(variant or "").strip(),
            }

    fallback = str(product.get("model_series") or product.get("name") or "").strip()
    return {"model_line": fallback, "model_number": "", "variant": ""}


def _parse_base_specs_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if not _present(raw):
        return {}
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def product_identity_key_from_product_row(product: dict[str, Any]) -> str | None:
    """Identity key for existing MySQL products, including legacy model-only names."""
    specs = _parse_base_specs_dict(product.get("base_specs"))
    ext = _default_phone_extractor()
    row = row_from_mongo_doc(
        {
            "name": product.get("name") or product.get("model_series") or "",
            "brand": product.get("brand"),
            "storage": specs.get("storage"),
            "ram": specs.get("ram"),
        },
        ext,
    )
    row["name_raw"] = None
    if not _present(row.get("brand")) and _present(product.get("brand")):
        row["brand"] = product.get("brand")
    return build_product_identity_key(row)


def build_base_specs(row: dict) -> str:
    storage, ram = _specs_from_row(row)
    return json.dumps({"storage": storage, "ram": ram})


def row_from_nlp_features(feats: dict, *, name_raw: str | None = None) -> dict:
    return {
        "brand": feats.get("brand"),
        "model_line": feats.get("model_line"),
        "model_number": feats.get("model_number"),
        "variant": feats.get("variant"),
        "capacity": feats.get("capacity"),
        "storage": feats.get("storage"),
        "ram": feats.get("ram"),
        "name_raw": name_raw or feats.get("original_title"),
    }


def row_from_mongo_doc(doc: dict, extractor: PhoneInfoExtractor) -> dict:
    title = doc.get("name") or doc.get("name_raw") or ""
    feats = extractor.extract_all_info(str(title))
    row = row_from_nlp_features(feats, name_raw=title)

    if not _present(row.get("brand")) and _present(doc.get("brand")):
        row["brand"] = doc.get("brand")
    if _present(doc.get("storage")):
        row["storage"] = doc.get("storage")
    if _present(doc.get("ram")):
        row["ram"] = doc.get("ram")
    return row


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
