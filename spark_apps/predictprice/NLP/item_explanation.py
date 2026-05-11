import pandas as pd
import re
import unicodedata
import json
import os
# from sklearn.feature_extraction.text import TfidfVectorizer
# import numpy as np
from flashtext import KeywordProcessor

class ItemExplanationExtractor:
    def __init__(self, config_path='nlp_config.json'):
        """Khởi tạo extractor với JSON config và FlashText"""
        self.tfidf_vectorizer = None
        self.feature_names = None
        self.config = self._load_config(config_path)
        
        # Khởi tạo FlashText cho các xử lý siêu tốc
        self.negation_processor = KeywordProcessor(case_sensitive=False)
        self._setup_processors()

    def _load_config(self, config_path):
        if not os.path.exists(config_path):
            nlp_dir = os.path.dirname(os.path.abspath(__file__))
            candidate1 = os.path.join(nlp_dir, 'config', 'nlp_config.json')
            candidate2 = os.path.join(os.path.dirname(nlp_dir), 'config', 'nlp_config.json')
            if os.path.exists(candidate2):
                config_path = candidate2
            elif os.path.exists(candidate1):
                config_path = candidate1
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Lỗi đọc file cấu hình NLP ({e}).")
            return {"negation_keywords": [], "condition_keywords": {}, "accessories": {}, "functional_issues": {}}

    def _setup_processors(self):
        """Đưa từ khóa phủ định vào FlashText để check Smart Window"""
        for neg in self.config.get('negation_keywords', []):
            self.negation_processor.add_keyword(neg, "NEGATION_FOUND")

    def preprocess_text(self, text):
        if not isinstance(text, str): return ""
        text = unicodedata.normalize('NFKC', text)
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = text.replace('　', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _smart_window_check(self, text, keywords, window_size=12, check_ahead=False):
        """
        Kỹ thuật Sliding Window thông minh để tìm từ khóa và kiểm tra phủ định.
        - check_ahead = False: Tìm phủ định SAU từ khóa (Ví dụ: 傷 -> なし)
        - check_ahead = True: Tìm phủ định TRƯỚC từ khóa (Ví dụ: 付属しません -> 箱)
        """
        if not keywords: return False
        
        # Tạo regex gộp tất cả keywords (Ví dụ: 箱|元箱|box)
        pattern = r'(' + '|'.join(map(re.escape, keywords)) + r')'
        matches = list(re.finditer(pattern, text, re.IGNORECASE))
        
        if not matches: return False
        
        # Nếu tìm thấy từ khóa, kiểm tra các cửa sổ xung quanh nó
        for match in matches:
            if not check_ahead:
                # Kiểm tra phía SAU (từ cuối từ khóa đến +window_size)
                window = text[match.end() : match.end() + window_size]
            else:
                # Kiểm tra phía TRƯỚC (từ -window_size đến đầu từ khóa)
                start_idx = max(0, match.start() - window_size)
                window = text[start_idx : match.start()]
                
            # Nếu trong cửa sổ có từ phủ định -> Từ khóa này bị phủ định (Bỏ qua)
            if self.negation_processor.extract_keywords(window):
                continue
                
            # Nếu có ít nhất 1 match KHÔNG bị phủ định -> Trả về True
            return True
            
        # Tất cả các match đều bị phủ định -> Trả về False
        return False

    def extract_battery_health(self, text):
        battery_info = {'battery_percentage': None, 'battery_status': None, 'battery_replaced': False}
        
        # % Pin
        patterns = [
            r'バッテリー[^\d]*(\d+)\s*[%％]', r'最大容量[^\d]*(\d+)\s*[%％]',
            r'battery[^\d]*(\d+)\s*[%％]', r'充電容量[^\d]*(\d+)\s*[%％]'
        ]
        for pattern in patterns:
            if match := re.search(pattern, text, re.IGNORECASE):
                battery_info['battery_percentage'] = int(match.group(1))
                break
                
        # Trạng thái
        if re.search(r'バッテリー.*良好|battery.*good', text, re.IGNORECASE):
            battery_info['battery_status'] = 'good'
        elif re.search(r'バッテリー.*劣化|battery.*degraded', text, re.IGNORECASE):
            battery_info['battery_status'] = 'degraded'
            
        # Đã thay pin
        if re.search(r'バッテリー.*交換|battery.*replaced|battery.*changed', text, re.IGNORECASE):
            battery_info['battery_replaced'] = True
            
        return battery_info

    def extract_storage_ram(self, text):
        storage_info = {'storage': None, 'ram': None}
        
        # Storage
        storage_patterns = [
            r'(\d+)\s*(GB|gb|TB|tb|ギガ|テラ)(?:\s*ストレージ|\s*ROM|\s*容量)?',
            r'(?:ストレージ|ROM|容量)[^\d]*(\d+)\s*(GB|gb|TB|tb)'
        ]
        capacities = []
        for pattern in storage_patterns:
            matches = re.findall(pattern, text)
            for num, unit in matches:
                num, unit = int(num), unit.upper()
                if unit == 'ギガ': unit = 'GB'
                elif unit == 'テラ': unit = 'TB'
                gb_val = num * 1024 if unit == 'TB' else num
                capacities.append((gb_val, f"{num}{unit}"))
                
        cands = [cap for cap in capacities if cap[0] >= 32]
        if cands: storage_info['storage'] = max(cands, key=lambda x: x[0])[1]

        # RAM
        ram_patterns = [r'(?:RAM|メモリ)[^\d]*(\d+)\s*(GB|gb)', r'(\d+)\s*(GB|gb)\s*(?:RAM|メモリ)']
        for pattern in ram_patterns:
            if match := re.search(pattern, text, re.IGNORECASE):
                if int(match.group(1)) <= 24:
                    storage_info['ram'] = f"{match.group(1)}GB"
                    break
        return storage_info

    def extract_accessories(self, text):
        acc_dict = self.config.get('accessories', {})
        
        # Dùng Smart Window: Tìm chữ '箱' (Hộp), kiểm tra 12 ký tự SAU nó có 'なし' (Không) không
        # Nếu có chữ 'なし' -> False. Nếu không có 'なし' -> True
        has_box = self._smart_window_check(text, acc_dict.get('box', []), window_size=12)
        has_charger = self._smart_window_check(text, acc_dict.get('charger', []), window_size=15)
        has_cable = self._smart_window_check(text, acc_dict.get('cable', []), window_size=15)
        has_earphones = self._smart_window_check(text, acc_dict.get('earphones', []), window_size=15)
        
        # Check set đầy đủ
        complete_set = False
        if any(w in text for w in acc_dict.get('complete_set', [])):
            complete_set = True
            has_box = has_charger = has_cable = True

        return {
            'has_box': has_box,
            'has_charger': has_charger,
            'has_cable': has_cable,
            'has_earphones': has_earphones,
            'accessories_complete': complete_set
        }

    def extract_sim_status(self, text):
        sim_info = {'is_sim_free': False, 'sim_lock_status': None, 'network_restriction': None}
        
        if re.search(r'SIM.*フリー|シムフリー|sim.*free|SIMロック.*解除', text, re.IGNORECASE):
            sim_info['is_sim_free'] = True
            sim_info['sim_lock_status'] = 'unlocked'
        elif re.search(r'SIMロック|sim.*lock', text, re.IGNORECASE):
            sim_info['sim_lock_status'] = 'locked'
            
        restrictions = [
            (r'利用制限.*[○〇◯]|判定.*[○〇◯]|ネットワーク.*制限.*なし|利用制限.*なし', 'none'),
            (r'利用制限.*[△▲]|判定.*[△▲]', 'possible'),
            (r'利用制限.*[×✕]|判定.*[×✕]', 'restricted')
        ]
        for pat, stat in restrictions:
            if re.search(pat, text):
                sim_info['network_restriction'] = stat
                break
        return sim_info

    def extract_physical_condition(self, text):
        cond_dict = self.config.get('condition_keywords', {})
        
        # Smart Window: Tìm chữ '傷' (Xước), nếu 10 ký tự sau nó có chữ 'なし' (Không) -> False (Máy đẹp)
        has_scratch = self._smart_window_check(text, cond_dict.get('scratches', []), window_size=10)
        has_crack = self._smart_window_check(text, cond_dict.get('cracks', []), window_size=10)
        
        screen_cond = 'clean'
        if has_crack: screen_cond = 'cracked'
        elif has_scratch: screen_cond = 'scratched'

        return {
            'has_scratches': has_scratch,
            'screen_condition': screen_cond,
            'body_condition': 'used' if has_scratch else 'good', # Fallback cơ bản
            'has_damage': has_crack
        }

    def extract_functional_status(self, text):
        func_dict = self.config.get('functional_issues', {})
        
        # FaceID / TouchID: Tìm chữ 'Face ID', nếu có chữ 'NG' hay '不可' ngay sau đó -> Lỗi
        face_id_broken = self._smart_window_check(text, func_dict.get('face_id', []), window_size=10)
        touch_id_broken = self._smart_window_check(text, func_dict.get('touch_id', []), window_size=10)
        
        # Chú ý logic bị đảo ngược so với Smart Window mặc định: 
        # Nếu hàm trả về True tức là TÌM THẤY chữ NG -> Tính năng BỊ HỎNG (False)
        # Để an toàn, chúng ta chỉ map nếu có nhắc đến FaceID
        face_id_status = None
        if any(w in text for w in func_dict.get('face_id', [])):
            face_id_status = False if face_id_broken else True
            
        touch_id_status = None
        if any(w in text for w in func_dict.get('touch_id', [])):
            touch_id_status = False if touch_id_broken else True
            
        # Lỗi chung (Junk)
        has_issues = False
        if any(w in text for w in func_dict.get('junk', [])):
            has_issues = True
            
        fully_functional = not has_issues and not face_id_broken and not touch_id_broken

        return {
            'face_id_working': face_id_status,
            'touch_id_working': touch_id_status,
            'fully_functional': fully_functional,
            'has_issues': has_issues
        }

    # def build_tfidf_model(self, texts):
    #     processed_texts = [self.preprocess_text(t) for t in texts]
    #     self.tfidf_vectorizer = TfidfVectorizer(max_features=500, ngram_range=(1, 2), min_df=2, max_df=0.8)
    #     tfidf_matrix = self.tfidf_vectorizer.fit_transform(processed_texts)
    #     self.feature_names = self.tfidf_vectorizer.get_feature_names_out()
    #     return tfidf_matrix, self.feature_names

    def extract_all_info(self, text):
        preprocessed = self.preprocess_text(text)
        result = {'original_explanation': text, 'preprocessed_explanation': preprocessed}
        
        result.update(self.extract_battery_health(preprocessed))
        result.update(self.extract_storage_ram(preprocessed))
        result.update(self.extract_accessories(preprocessed))
        result.update(self.extract_sim_status(preprocessed))
        result.update(self.extract_physical_condition(preprocessed))
        result.update(self.extract_functional_status(preprocessed))
        return result

    def process_dataframe(self, df, explanation_column='explanation'):
        results = []
        for idx, row in df.iterrows():
            extracted = self.extract_all_info(row[explanation_column])
            results.append({**row.to_dict(), **extracted})
            if (idx + 1) % 500 == 0: # Đã tăng lên 500 vì code chạy rất nhanh
                print(f"Đã xử lý {idx + 1}/{len(df)} records...")
        return pd.DataFrame(results)

# [Phần hàm print_nice_table và main() giữ nguyên như cũ, không cần thay đổi]
def print_nice_table(df, max_rows=10):
    print(f"\n Kết quả {max_rows} sản phẩm đầu tiên\n")
    
    display_data = []
    
    for idx, row in df.head(max_rows).iterrows():
        battery_pct = f"{row['battery_percentage']}%" if pd.notna(row['battery_percentage']) else "-"
        battery_status = row['battery_status'] if pd.notna(row['battery_status']) else "-"
        battery_replaced = "Có" if row['battery_replaced'] else "Không"
        
        storage = row['storage'] if pd.notna(row['storage']) else "-"
        ram = row['ram'] if pd.notna(row['ram']) else "-"
        
        acc_parts = []
        if row.get('has_box', False): acc_parts.append("Hộp")
        if row.get('has_charger', False): acc_parts.append("Sạc")
        if row.get('has_cable', False): acc_parts.append("Cáp")
        if row.get('has_earphones', False): acc_parts.append("Tai nghe")
        
        if row.get('accessories_complete', False):
            acc_text = "Đầy đủ"
        elif acc_parts:
            acc_text = ", ".join(acc_parts)
        else:
            acc_text = "Không có"
        
        sim_status = "Free" if row['is_sim_free'] else "Khóa"
        network_restriction = row['network_restriction'] if pd.notna(row['network_restriction']) else "-"
        
        screen = row['screen_condition'] if pd.notna(row['screen_condition']) else "-"
        body = row['body_condition'] if pd.notna(row['body_condition']) else "-"
        camera = row.get('camera_condition', '-') if pd.notna(row.get('camera_condition')) else "-"
        scratches = "Có" if row['has_scratches'] else "Không"
        damage = "Có" if row.get('has_damage', False) else "Không"
        
        face_id = "OK" if row.get('face_id_working') == True else ("Lỗi" if row.get('face_id_working') == False else "-")
        touch_id = "OK" if row.get('touch_id_working') == True else ("Lỗi" if row.get('touch_id_working') == False else "-")
        functional = "Tốt" if row['fully_functional'] else "-"
        issues = "Có" if row['has_issues'] else "Không"
        
        display_data.append({
            'STT': idx + 1,
            'Tên SP': row['name'][:35] + '...' if len(row['name']) > 35 else row['name'],
            'Giá': row['price'],
            'Pin%': battery_pct,
            'Pin TT': battery_status,
            'Pin Thay': battery_replaced,
            'Storage': storage,
            'RAM': ram,
            'Phụ kiện': acc_text[:25] + '...' if len(acc_text) > 25 else acc_text,
            'SIM': sim_status,
            'Mạng': network_restriction,
            'Màn hình': screen,
            'Thân': body,
            'Camera': camera,
            'Trầy': scratches,
            'Hư': damage,
            'FaceID': face_id,
            'TouchID': touch_id,
            'Hoạt động': functional,
            'Lỗi': issues
        })
    
    result_df = pd.DataFrame(display_data)
    
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    pd.set_option('display.max_colwidth', 40)
    pd.set_option('display.unicode.east_asian_width', True)
    pd.set_option('display.colheader_justify', 'center')
    
    print(result_df.to_string(index=False))
    print()
    
    print("\n Tóm tắt chi tiết:")
    print("-" * 80)
    
    for idx, row in df.head(max_rows).iterrows():
        print(f"\n[{idx + 1}] {row['name']}")
        print(f"    Giá: {row['price']} | Nguồn: {row['source']} | Tình trạng: {row['condition']}")
        
        # Pin
        pin_info = []
        if pd.notna(row['battery_percentage']):
            pin_info.append(f"Dung lượng {row['battery_percentage']}%")
        if pd.notna(row['battery_status']):
            pin_info.append(f"Trạng thái {row['battery_status']}")
        if row['battery_replaced']:
            pin_info.append("Đã thay pin")
        if pin_info:
            print(f"    Pin: {' | '.join(pin_info)}")
        
        # Bộ nhớ
        memory_info = []
        if pd.notna(row['storage']):
            memory_info.append(f"Storage {row['storage']}")
        if pd.notna(row['ram']):
            memory_info.append(f"RAM {row['ram']}")
        if memory_info:
            print(f"    Bộ nhớ: {' | '.join(memory_info)}")
        
        # Phụ kiện
        acc_list = []
        if row.get('has_box', False): acc_list.append("Hộp")
        if row.get('has_charger', False): acc_list.append("Sạc")
        if row.get('has_cable', False): acc_list.append("Cáp")
        if row.get('has_earphones', False): acc_list.append("Tai nghe")
        
        if row.get('accessories_complete', False):
            print(f"    Phụ kiện: Đầy đủ")
        elif acc_list:
            print(f"    Phụ kiện: {', '.join(acc_list)}")
        else:
            print(f"    Phụ kiện: Không có")
        
        # SIM
        sim_info = []
        if row['is_sim_free']:
            sim_info.append("SIM Free")
        if pd.notna(row['network_restriction']):
            sim_info.append(f"Hạn chế mạng: {row['network_restriction']}")
        if sim_info:
            print(f"    SIM: {' | '.join(sim_info)}")
        
        # Tình trạng vật lý
        condition_parts = []
        if pd.notna(row['screen_condition']):
            condition_parts.append(f"Màn hình {row['screen_condition']}")
        if pd.notna(row['body_condition']):
            condition_parts.append(f"Thân máy {row['body_condition']}")
        if pd.notna(row.get('camera_condition')):
            condition_parts.append(f"Camera {row['camera_condition']}")
        if row['has_scratches']:
            condition_parts.append("Có trầy xước")
        if row.get('has_damage', False):
            condition_parts.append("Có hư hỏng")
        if condition_parts:
            print(f"    Tình trạng: {' | '.join(condition_parts)}")
        
        # Chức năng
        func_parts = []
        if row.get('face_id_working') == True:
            func_parts.append("Face ID OK")
        elif row.get('face_id_working') == False:
            func_parts.append("Face ID lỗi")
        
        if row.get('touch_id_working') == True:
            func_parts.append("Touch ID OK")
        elif row.get('touch_id_working') == False:
            func_parts.append("Touch ID lỗi")
        
        if row['fully_functional']:
            func_parts.append("Hoạt động hoàn hảo")
        if row['has_issues']:
            func_parts.append("Có vấn đề")
        
        if func_parts:
            print(f"    Chức năng: {' | '.join(func_parts)}")
    
    print("\n" + "-" * 80)

def main():
    # 1. Đọc dữ liệu
    print("Bước 1: Đọc dữ liệu từ all_items.csv")
    df = pd.read_csv('book_data/all_items.csv')
    print(f"Tổng số sản phẩm: {len(df)}\n")
    
    # 2. Tạo Extractor
    print("Bước 2: Khởi tạo ItemExplanationExtractor")
    extractor = ItemExplanationExtractor()
    print("Extractor đã sẵn sàng\n")
    
    # # 3. Xây dựng TF-IDF model
    # print("Bước 3: Xây dựng TF-IDF model")
    # tfidf_matrix, feature_names = extractor.build_tfidf_model(df['explanation'].fillna(''))
    # print(f"TF-IDF model đã hoàn thành")
    # print(f"Tổng số features: {len(feature_names)}\n")
    
    # # 4. Hiển thị top keywords quan trọng nhất
    # print("Bước 4: Top 30 từ khóa quan trọng nhất (TF-IDF)")
    # avg_tfidf = tfidf_matrix.mean(axis=0).A1
    # top_indices = avg_tfidf.argsort()[-30:][::-1]
    
    # print(f"{'Từ khóa':<30} {'Điểm TF-IDF':>15}")
    # print("-" * 50)
    # for idx in top_indices:
    #     print(f"{feature_names[idx]:<30} {avg_tfidf[idx]:>15.4f}")
    # print()
    
    # 5. Trích xuất thông tin từ tất cả explanations
    print("Bước 5: Trích xuất thông tin từ explanations")
    results = extractor.process_dataframe(df)
    print("Hoàn thành trích xuất!\n")
    
    # 6. Lưu kết quả đầy đủ
    print("Bước 6: Lưu kết quả")
    results.to_csv('book_data/extracted_explanation_info.csv', index=False, encoding='utf-8-sig')
    print("Đã lưu: book_data/extracted_explanation_info.csv")
    
    # 7. Lưu chỉ các cột quan trọng
    essential_columns = [
        'name', 'price', 'condition', 'source',
        'battery_percentage', 'battery_status', 'battery_replaced',
        'storage', 'ram',
        'has_box', 'has_charger', 'has_cable', 'has_earphones', 'accessories_complete',
        'is_sim_free', 'network_restriction',
        'has_scratches', 'screen_condition', 'body_condition', 'camera_condition', 'has_damage',
        'face_id_working', 'touch_id_working', 'fully_functional', 'has_issues'
    ]
    
    results_essential = results[[col for col in essential_columns if col in results.columns]]
    results_essential.to_csv('book_data/explanation_features.csv', index=False, encoding='utf-8-sig')
    print("Đã lưu: book_data/explanation_features.csv\n")
    
    # 8. Thống kê kết quả
    print("Bước 7: Thống kê kết quả trích xuất")
    total = len(df)
    print(f"{'Loại thông tin':<30} {'Số lượng':<10} {'Tỷ lệ':<10}")
    print("-" * 50)
    print(f"{'Pin (Battery %)':<30} {results['battery_percentage'].notna().sum():<10} {results['battery_percentage'].notna().sum()/total*100:.1f}%")
    print(f"{'Bộ nhớ (Storage)':<30} {results['storage'].notna().sum():<10} {results['storage'].notna().sum()/total*100:.1f}%")
    print(f"{'RAM':<30} {results['ram'].notna().sum():<10} {results['ram'].notna().sum()/total*100:.1f}%")
    print(f"{'SIM Free':<30} {results['is_sim_free'].sum():<10} {results['is_sim_free'].sum()/total*100:.1f}%")
    print(f"{'Có hộp (Box)':<30} {results['has_box'].sum():<10} {results['has_box'].sum()/total*100:.1f}%")
    print(f"{'Trạng thái màn hình':<30} {results['screen_condition'].notna().sum():<10} {results['screen_condition'].notna().sum()/total*100:.1f}%")
    print(f"{'Hoạt động hoàn hảo':<30} {results['fully_functional'].sum():<10} {results['fully_functional'].sum()/total*100:.1f}%")
    print()
    
    # 9. Hiển thị bảng dạng tabular
    print_nice_table(results_essential, max_rows=5)

if __name__ == "__main__":
    main()
