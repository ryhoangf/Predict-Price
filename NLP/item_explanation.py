import pandas as pd
import re
import unicodedata
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np

class ItemExplanationExtractor:
    def __init__(self):
        """Khởi tạo extractor"""
        self.tfidf_vectorizer = None
        self.feature_names = None
        
    def preprocess_text(self, text):
        """
        Chuẩn hóa Unicode (１２３ → 123)
        Xóa ký tự thừa
        Chuẩn hóa khoảng trắng
        """
        if not isinstance(text, str):
            return ""
        text = unicodedata.normalize('NFKC', text)
        text = text.replace('\n', ' ').replace('\r', ' ')
        text = text.replace('　', ' ')
        text = re.sub(r'\s+', ' ', text).strip()
        return text
    
    def extract_battery_health(self, text):
        """
        Trích xuất thông tin pin
        """
        battery_info = {
            'battery_percentage': None,
            'battery_status': None,
            'battery_replaced': False
        }
        
        #tìm phần trăm pin
        battery_patterns = [
            r'バッテリー[^\d]*(\d+)\s*[%％]',
            r'最大容量[^\d]*(\d+)\s*[%％]',
            r'battery[^\d]*(\d+)\s*[%％]',
            r'充電容量[^\d]*(\d+)\s*[%％]'
        ]
        
        for pattern in battery_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                battery_info['battery_percentage'] = int(match.group(1))
                break
        
        # trạng thái pin
        if re.search(r'バッテリー.*良好|battery.*good', text, re.IGNORECASE):
            battery_info['battery_status'] = 'good'
        elif re.search(r'バッテリー.*劣化|battery.*degraded', text, re.IGNORECASE):
            battery_info['battery_status'] = 'degraded'
        
        # pin đã thay chưa
        if re.search(r'バッテリー.*交換|battery.*replaced|battery.*changed', text, re.IGNORECASE):
            battery_info['battery_replaced'] = True
        
        return battery_info
    
    def extract_storage_ram(self, text):
        """
        Trích xuất Storage và RAM
        """
        storage_info = {
            'storage': None,
            'ram': None
        }
        
        # tìm tất cả số có đơn vị GB/TB
        storage_patterns = [
            r'(\d+)\s*(GB|gb|TB|tb|ギガ|テラ)(?:\s*ストレージ|\s*ROM|\s*容量)?',
            r'(?:ストレージ|ROM|容量)[^\d]*(\d+)\s*(GB|gb|TB|tb)'
        ]
        
        capacities = []
        for pattern in storage_patterns:
            matches = re.findall(pattern, text)
            for number, unit in matches:
                number = int(number)
                unit = unit.upper()
                
                # chuẩn hóa đơn vị
                if unit in ['ギガ']:
                    unit = 'GB'
                elif unit in ['テラ']:
                    unit = 'TB'
                
                # chuyển về GB để so sánh
                gb_value = number * 1024 if unit == 'TB' else number
                capacities.append((gb_value, f"{number}{unit}"))
        
        # storage thường >= 32GB
        storage_candidates = [cap for cap in capacities if cap[0] >= 32]
        if storage_candidates:
            # lấy capacity lớn nhất làm storage
            storage_info['storage'] = max(storage_candidates, key=lambda x: x[0])[1]
        
        # tìm RAM (thường có keyword "RAM" hoặc "メモリ")
        ram_patterns = [
            r'(?:RAM|メモリ)[^\d]*(\d+)\s*(GB|gb)',
            r'(\d+)\s*(GB|gb)\s*(?:RAM|メモリ)'
        ]
        
        for pattern in ram_patterns:
            match = re.search(pattern, text)
            if match:
                ram_value = int(match.group(1))
                if ram_value <= 24:
                    storage_info['ram'] = f"{ram_value}GB"
                    break
        
        return storage_info
    
    def extract_accessories(self, text):
        """
        Trích xuất phụ kiện
        """
        accessories = {
            'has_box': False,
            'has_charger': False,
            'has_cable': False,
            'has_earphones': False,
            'accessories_complete': False
        }
        
        if re.search(r'箱.*付|箱.*あり|元箱|box.*included|with.*box|・\s*箱|•\s*箱', text, re.IGNORECASE):
            accessories['has_box'] = True
        elif re.search(r'箱.*なし|no.*box|本体のみ', text, re.IGNORECASE):
            accessories['has_box'] = False
        
        if re.search(r'充電器.*付|充電器.*あり|charger.*included|アダプタ.*付|アダプター.*付|AC.*adapter|・\s*充電器|・\s*AC.*アダプター|・\s*アダプター|•\s*charger', text, re.IGNORECASE):
            accessories['has_charger'] = True
        elif re.search(r'充電器.*なし|no.*charger|アダプタ.*なし', text, re.IGNORECASE):
            accessories['has_charger'] = False
        
        if re.search(r'ケーブル.*付|ケーブル.*あり|cable.*included|コード.*付|ライトニング.*ケーブル|USB.*ケーブル|・\s*ケーブル|・\s*充電.*ケーブル|・\s*ライトニング|•\s*cable', text, re.IGNORECASE):
            accessories['has_cable'] = True
        elif re.search(r'ケーブル.*なし|no.*cable|コード.*なし', text, re.IGNORECASE):
            accessories['has_cable'] = False
        
        if re.search(r'イヤホン.*付|イヤホン.*あり|earphone.*included|イヤフォン.*付|ヘッドホン.*付|AirPods|EarPods|・\s*イヤホン|・\s*イヤフォン|•\s*earphone', text, re.IGNORECASE):
            accessories['has_earphones'] = True
        elif re.search(r'イヤホン.*なし|no.*earphone|イヤフォン.*なし', text, re.IGNORECASE):
            accessories['has_earphones'] = False
        
        if re.search(r'付属品.*完備|付属品.*全て|全て.*揃|一式|フルセット|complete.*set|full.*accessories|新品.*付属品|純正.*付属品', text, re.IGNORECASE):
            accessories['accessories_complete'] = True
        elif re.search(r'付属品.*なし|本体のみ|body.*only|本体.*のみ', text, re.IGNORECASE):
            accessories['accessories_complete'] = False
        
        return accessories
    
    def extract_sim_status(self, text):
        """
        Trích xuất trạng thái SIM
        """
        sim_info = {
            'is_sim_free': False,
            'sim_lock_status': None,
            'network_restriction': None
        }
        
        # SIM Free
        if re.search(r'SIM.*フリー|シムフリー|sim.*free|SIMロック.*解除', text, re.IGNORECASE):
            sim_info['is_sim_free'] = True
            sim_info['sim_lock_status'] = 'unlocked'
        elif re.search(r'SIMロック|sim.*lock', text, re.IGNORECASE):
            sim_info['sim_lock_status'] = 'locked'
        
        restriction_patterns = [
            (r'利用制限.*[○〇◯]|判定.*[○〇◯]', 'none'),
            (r'利用制限.*[△▲]|判定.*[△▲]', 'possible'),
            (r'利用制限.*[×✕]|判定.*[×✕]', 'restricted'),
            (r'ネットワーク.*制限.*なし|利用制限.*なし', 'none')
        ]
        
        for pattern, status in restriction_patterns:
            if re.search(pattern, text):
                sim_info['network_restriction'] = status
                break
        
        return sim_info
    
    def extract_physical_condition(self, text):
        """
        Trích xuất tình trạng vật lý
        """
        condition_info = {
            'has_scratches': False,
            'screen_condition': None,
            'body_condition': None,
            'camera_condition': None,
            'has_damage': False
        }
        
        # có trầy xước không
        if re.search(r'傷|キズ|scratch|スレ|擦れ', text, re.IGNORECASE):
            condition_info['has_scratches'] = True
        
        # tình trạng màn hình
        if re.search(r'画面.*割れ.*なし|液晶.*割れ.*なし|ひび.*なし|割れ.*なし|画面.*ひび.*なし', text, re.IGNORECASE):
            condition_info['screen_condition'] = 'clean'
        elif re.search(r'画面.*綺麗|液晶.*綺麗|screen.*clean|画面.*問題.*なし', text, re.IGNORECASE):
            condition_info['screen_condition'] = 'clean'
        elif re.search(r'画面.*割れ|液晶.*割れ|screen.*crack|画面.*ひび|ひび.*あり|画面.*破損', text, re.IGNORECASE):
            condition_info['screen_condition'] = 'cracked'
        elif re.search(r'画面.*傷|液晶.*傷', text, re.IGNORECASE):
            condition_info['screen_condition'] = 'scratched'
        
        # tình trạng thân máy
        if re.search(r'美品|very.*good|excellent|綺麗', text, re.IGNORECASE):
            condition_info['body_condition'] = 'excellent'
        elif re.search(r'良好|good|きれい', text, re.IGNORECASE):
            condition_info['body_condition'] = 'good'
        elif re.search(r'使用感|used|中古', text, re.IGNORECASE):
            condition_info['body_condition'] = 'used'
        
        # camera
        if re.search(r'カメラ.*正常|カメラ.*問題.*なし|camera.*ok', text, re.IGNORECASE):
            condition_info['camera_condition'] = 'working'
        elif re.search(r'カメラ.*傷|カメラ.*レンズ.*傷', text, re.IGNORECASE):
            condition_info['camera_condition'] = 'scratched'
        
        # có hư hỏng không
        if re.search(r'機能不良.*なし|不具合.*なし|問題.*なし', text, re.IGNORECASE):
            condition_info['has_damage'] = False
        elif re.search(r'破損|割れ|欠け|damage|ジャンク|機能不良.*あり', text, re.IGNORECASE):
            condition_info['has_damage'] = True
        
        return condition_info
    
    def extract_functional_status(self, text):
        """
        Bước 2F: Trích xuất tình trạng chức năng
        Ví dụ: "Face ID正常、完動品" → face_id:True, fully_functional:True
        """
        functional_info = {
            'face_id_working': None,
            'touch_id_working': None,
            'fully_functional': False,
            'has_issues': False
        }
        
        # Face ID
        if re.search(r'Face.*ID.*[○〇◯OK正常]|顔認証.*[○〇◯OK正常]', text, re.IGNORECASE):
            functional_info['face_id_working'] = True
        elif re.search(r'Face.*ID.*[×✕NG不可使えない]|顔認証.*[×✕NG不可]', text, re.IGNORECASE):
            functional_info['face_id_working'] = False
        
        # Touch ID
        if re.search(r'Touch.*ID.*[○〇◯OK正常]|指紋.*[○〇◯OK正常]', text, re.IGNORECASE):
            functional_info['touch_id_working'] = True
        elif re.search(r'Touch.*ID.*[×✕NG不可使えない]|指紋.*[×✕NG不可]', text, re.IGNORECASE):
            functional_info['touch_id_working'] = False
        
        # Hoạt động hoàn hảo không
        if re.search(r'完動|動作.*確認.*済|fully.*functional|all.*working', text, re.IGNORECASE):
            functional_info['fully_functional'] = True
        
        # Có vấn đề không
        if re.search(r'問題.*なし|問題.*無|不具合.*なし|機能不良.*なし|動作.*問題.*なし', text, re.IGNORECASE):
            functional_info['has_issues'] = False
        elif re.search(r'不具合|問題|issue|defect|故障|ジャンク', text, re.IGNORECASE):
            functional_info['has_issues'] = True
        
        return functional_info
    
    def build_tfidf_model(self, texts):
        """
        Xây dựng mô hình TF-IDF
        Tìm 500 từ/cụm từ quan trọng nhất
        Sử dụng bi-gram (cụm 2 từ) để bắt context tốt hơn
        """
        # Tiền xử lý tất cả văn bản
        processed_texts = [self.preprocess_text(text) for text in texts]
        
        # xây dựng TF-IDF
        self.tfidf_vectorizer = TfidfVectorizer(
            max_features=500,
            ngram_range=(1, 2),
            min_df=2,
            max_df=0.8
        )
        
        tfidf_matrix = self.tfidf_vectorizer.fit_transform(processed_texts)
        self.feature_names = self.tfidf_vectorizer.get_feature_names_out()
        
        return tfidf_matrix, self.feature_names
    
    def get_top_keywords(self, text, top_n=10):
        """
        lấy top keywords từ 1 văn bản
        """
        if self.tfidf_vectorizer is None:
            return []
        
        processed_text = self.preprocess_text(text)
        tfidf_vector = self.tfidf_vectorizer.transform([processed_text])
        
        # lấy TF-IDF scores
        scores = tfidf_vector.toarray()[0]
        
        # sắp xếp và lấy top N
        top_indices = scores.argsort()[-top_n:][::-1]
        
        keywords = [(self.feature_names[i], scores[i]) for i in top_indices if scores[i] > 0]
        return keywords
    
    def extract_all_info(self, text):
        """
        Trích xuất TẤT CẢ thông tin từ 1 explanation
        """
        preprocessed = self.preprocess_text(text)
        
        result = {
            'original_explanation': text,
            'preprocessed_explanation': preprocessed
        }
        
        # Gọi tất cả các hàm trích xuất
        result.update(self.extract_battery_health(preprocessed))
        result.update(self.extract_storage_ram(preprocessed))
        result.update(self.extract_accessories(preprocessed))
        result.update(self.extract_sim_status(preprocessed))
        result.update(self.extract_physical_condition(preprocessed))
        result.update(self.extract_functional_status(preprocessed))
        
        return result
    
    def process_dataframe(self, df, explanation_column='explanation'):
        """
        Xử lý toàn bộ DataFrame
        """
        results = []
        
        for idx, row in df.iterrows():
            explanation = row[explanation_column]
            extracted = self.extract_all_info(explanation)
            
            # kết hợp dữ liệu gốc + dữ liệu trích xuất
            result = {**row.to_dict(), **extracted}
            results.append(result)
            
            # progress
            if (idx + 1) % 100 == 0:
                print(f"Đã xử lý {idx + 1}/{len(df)} records...")
        
        return pd.DataFrame(results)

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
    
    # 3. Xây dựng TF-IDF model
    print("Bước 3: Xây dựng TF-IDF model")
    tfidf_matrix, feature_names = extractor.build_tfidf_model(df['explanation'].fillna(''))
    print(f"TF-IDF model đã hoàn thành")
    print(f"Tổng số features: {len(feature_names)}\n")
    
    # 4. Hiển thị top keywords quan trọng nhất
    print("Bước 4: Top 30 từ khóa quan trọng nhất (TF-IDF)")
    avg_tfidf = tfidf_matrix.mean(axis=0).A1
    top_indices = avg_tfidf.argsort()[-30:][::-1]
    
    print(f"{'Từ khóa':<30} {'Điểm TF-IDF':>15}")
    print("-" * 50)
    for idx in top_indices:
        print(f"{feature_names[idx]:<30} {avg_tfidf[idx]:>15.4f}")
    print()
    
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