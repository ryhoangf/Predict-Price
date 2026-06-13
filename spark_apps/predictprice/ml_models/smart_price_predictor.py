import pandas as pd
import numpy as np
import joblib
import json
import os
import re
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GroupShuffleSplit, train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error

class SmartPricePredictor:
    
    def __init__(self, n_estimators=200, max_depth=25, random_state=42):
        self.model = RandomForestRegressor(
            n_estimators=n_estimators,
            max_depth=max_depth,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=random_state,
            n_jobs=-1
        )
        self.feature_columns = None
        self.feature_importance_ = None
        self.train_stats_ = None
        self.model_metadata_ = {}
        self.model_price_map = {}
        
        # Load từ điển năm ra mắt & Chuẩn bị Regex Vector hóa
        self.release_year_map = self._load_release_years()
        if self.release_year_map:
            self.release_year_map_lower = {k.lower(): v for k, v in self.release_year_map.items()}
            escaped_keys = [re.escape(k) for k in self.release_year_map.keys()]
            self.year_regex_pattern = r'(?i)(' + '|'.join(escaped_keys) + r')'
        else:
            self.release_year_map_lower = {}
            self.year_regex_pattern = None

    def _load_release_years(self):
        try:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            json_path = os.path.join(current_dir, '..', 'config', 'release_years.json')
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Warning: Could not load release_years.json ({e}). Using empty map.")
            return {}

    def engineer_features(self, df):
        df = df.copy()

        df['full_model_name'] = (
            df['model_line'].astype(str).fillna('') + ' ' + 
            df['model_number'].astype(str).fillna('') + ' ' + 
            df['variant'].astype(str).fillna('')
        ).str.replace('None', '').str.replace('nan', '').str.strip()
        
        # 0. TARGET ENCODING (Base Price)
        if self.model_price_map:
            df['model_base_price'] = df['full_model_name'].map(self.model_price_map)
            fallback_price = np.mean(list(self.model_price_map.values())) if self.model_price_map else 30000
            df['model_base_price'] = df['model_base_price'].fillna(fallback_price)
        else:
            df['model_base_price'] = 0.0

        # 0.1 RELEASE YEAR & DEVICE AGE (OPTIMIZED VECTORIZATION)
        if self.year_regex_pattern:
            extracted_model = df['full_model_name'].str.extract(self.year_regex_pattern, expand=False)
            df['release_year'] = extracted_model.str.lower().map(self.release_year_map_lower).fillna(2020)
        else:
            df['release_year'] = 2020
            
        current_year = 2026
        df['device_age_years'] = current_year - df['release_year']
        df['device_age_years'] = df['device_age_years'].clip(lower=0)

        # ===== 1. CONDITION SCORING =====
        condition_map = {
            'S': 100, 'A': 85, 'B': 70, 'C': 50, 'J': 30,
            'New': 100, 'Like new': 100, 'Unused': 100,
            'Excellent': 85, 'Very good': 85, 'Good': 70,
            'Fair': 50, 'Acceptable': 50, 'Poor': 30, 'Damaged': 30,
            'New, unused': 100, 'Like New': 100, 'EXCELLENT': 85, 'GOOD': 70,
            'No obvious damages/dirt': 85, 'Minor scratches/dirt': 70,
            'Obvious scratches/dirt': 50, 'Heavily damaged': 30
        }
        df['condition_score'] = df['condition'].map(condition_map).fillna(70)
        
        df['age_condition_interaction'] = df['device_age_years'] * df['condition_score']
        
        # ===== 2. BATTERY HEALTH SCORING =====
        df['battery_percentage'] = df['battery_percentage'].fillna(80.0)
        df['battery_penalty'] = np.where(
            df['battery_percentage'] < 80,
            (80 - df['battery_percentage']) * 0.5, 0
        )
        df['battery_score'] = (df['condition_score'] - df['battery_penalty']).clip(0, 100)
        
        # ===== 3. SCREEN CONDITION SCORING =====
        screen_penalty_map = {
            'clean': 0, 'scratched': -5, 'minor_scratch': -3,
            'cracked': -20, 'broken': -30
        }
        df['screen_condition'] = df['screen_condition'].fillna('clean')
        df['screen_penalty'] = df['screen_condition'].map(screen_penalty_map).fillna(0)
        df['screen_score'] = (df['condition_score'] + df['screen_penalty']).clip(0, 100)
        
        # ===== 4. BODY CONDITION SCORING =====
        body_penalty_map = {'perfect': 0, 'good': -2, 'fair': -5, 'poor': -10}
        df['body_condition'] = df.get('body_condition', 'good').fillna('good')
        df['body_penalty'] = df['body_condition'].map(body_penalty_map).fillna(-2)
        df['body_score'] = (df['condition_score'] + df['body_penalty']).clip(0, 100)
        
        # ===== 5. ACCESSORIES BONUS =====
        df['has_box'] = df.get('has_box', False).fillna(False)
        df['has_charger'] = df.get('has_charger', False).fillna(False)
        
        df['accessories_bonus'] = 0
        df.loc[df['has_box'] == True, 'accessories_bonus'] += 3
        df.loc[df['has_charger'] == True, 'accessories_bonus'] += 2
        df.loc[(df['has_box'] == True) & (df['has_charger'] == True), 'accessories_bonus'] += 5
        
        # ===== 6. COMPOSITE QUALITY SCORE =====
        df['quality_score'] = (
            df['battery_score'] * 0.40 + 
            df['screen_score'] * 0.30 + 
            df['body_score'] * 0.20 + 
            df['condition_score'] * 0.10 + 
            df['accessories_bonus']
        ).clip(0, 110)
        
        # ===== 7. STORAGE FEATURES =====
        if 'storage' in df.columns:
            df['storage_gb'] = df['storage'].str.extract(r'(\d+)').astype(float).fillna(64)
        else:
            df['storage_gb'] = 64.0
        
        df['storage_tier'] = pd.cut(
            df['storage_gb'], bins=[0, 64, 256, 1024], labels=[1, 2, 3]
        ).fillna(2).astype(int)
        
        # ===== 8. RAM FEATURES =====
        if 'ram' in df.columns:
            df['ram_gb'] = df['ram'].str.extract(r'(\d+)').astype(float).fillna(4)
        else:
            df['ram_gb'] = 4.0
        
        # ===== 9. INTERACTION FEATURES =====
        df['battery_storage'] = df['battery_score'] * np.log1p(df['storage_gb'])
        df['quality_storage'] = df['quality_score'] * np.log1p(df['storage_gb'])
        df['condition_accessories'] = df['condition_score'] * (1 + df['accessories_bonus'] / 10)
        
        # ===== 10. PLATFORM ENCODING =====
        platform_map = {'Mercari': 1, 'Rakuma': 2, 'YahooAuction': 3}
        df['platform_encoded'] = df.get('platform', 'Mercari').map(platform_map).fillna(1)
        
        # ===== 11. BOOLEAN FEATURES =====
        bool_features = {
            'has_box': False, 'has_charger': False, 'is_sim_free': True,
            'fully_functional': True, 'has_scratches': False,
            'has_damage': False, 'has_issues': False
        }
        for col, default in bool_features.items():
            if col not in df.columns: df[col] = default
            df[col] = df[col].fillna(default).astype(int)
        
        # ===== 12. DAMAGE FLAGS =====
        df['has_any_damage'] = (
            (df['has_scratches'] == 1) | (df['has_damage'] == 1) | (df['has_issues'] == 1)
        ).astype(int)
        
        df['damage_severity'] = 0
        df.loc[df['has_scratches'] == 1, 'damage_severity'] = 1
        df.loc[df['has_damage'] == 1, 'damage_severity'] = 2
        df.loc[df['screen_penalty'] < -15, 'damage_severity'] = 2
        
        # ===== 13. FUNCTIONAL FLAGS =====
        df['functionality_score'] = (
            df['fully_functional'] * 20 + df['is_sim_free'] * 10 - df['has_issues'] * 15
        )
        
        return df
    
    def _legacy_train(self, df, target_col='price', test_size=0.2, verbose=True):
        if verbose:
            print(f"\n{'='*80}")
            print(f"TRAINING SMART PRICE PREDICTOR")
            print(f"{'='*80}")
            print(f"Training with {len(df)} records...")
        
        # TÍNH TOÁN BASE PRICE TỪ TẬP TRAIN
        temp_df = df.copy()
        temp_df['full_model_name'] = (
            temp_df['model_line'].astype(str).fillna('') + ' ' + 
            temp_df['model_number'].astype(str).fillna('') + ' ' + 
            temp_df['variant'].astype(str).fillna('')
        ).str.replace('None', '').str.replace('nan', '').str.strip()
        
        self.model_price_map = temp_df.groupby('full_model_name')[target_col].median().to_dict()
        
        df_engineered = self.engineer_features(df)
        
        feature_cols = [
            'model_base_price', 'release_year', 'device_age_years', 'age_condition_interaction',
            'condition_score', 'battery_score', 'screen_score', 'body_score', 'quality_score',
            'battery_percentage', 'battery_penalty', 'screen_penalty', 'body_penalty', 'accessories_bonus',
            'storage_gb', 'storage_tier', 'ram_gb',
            'battery_storage', 'quality_storage', 'condition_accessories',
            'platform_encoded', 'has_box', 'has_charger', 'is_sim_free', 'fully_functional',
            'has_any_damage', 'damage_severity', 'functionality_score'
        ]
        
        feature_cols = [col for col in feature_cols if col in df_engineered.columns]
        self.feature_columns = feature_cols
        
        if verbose: print(f"Using {len(feature_cols)} features")
        
        X = df_engineered[feature_cols]
        y = df_engineered[target_col]
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=42)
        self.model.fit(X_train, y_train)
        
        train_pred = self.model.predict(X_train)
        test_pred = self.model.predict(X_test)
        
        train_mae = mean_absolute_error(y_train, train_pred)
        test_mae = mean_absolute_error(y_test, test_pred)
        train_r2 = r2_score(y_train, train_pred)
        test_r2 = r2_score(y_test, test_pred)
        
        self.train_stats_ = {
            'train_mae': train_mae, 'test_mae': test_mae,
            'train_r2': train_r2, 'test_r2': test_r2,
            'n_train': len(X_train), 'n_test': len(X_test), 'n_features': len(feature_cols)
        }
        
        self.feature_importance_ = pd.DataFrame({
            'feature': feature_cols,
            'importance': self.model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        if verbose:
            print(f"\n📊 Performance Metrics:")
            print(f"   Train MAE:  ¥{train_mae:>10,.0f}  |  R² = {train_r2:.3f}")
            print(f"   Test MAE:   ¥{test_mae:>10,.0f}  |  R² = {test_r2:.3f}")
            print(f"\n📈 Top 10 Feature Importance:")
            for idx, row in self.feature_importance_.head(10).iterrows():
                bar = '█' * int(row['importance'] * 50)
                print(f"   {row['feature']:<30} {row['importance']:>6.1%} {bar}")
                
        return self.train_stats_

    @staticmethod
    def _full_model_name(df):
        return (
            df['model_line'].astype(str).fillna('') + ' ' +
            df['model_number'].astype(str).fillna('') + ' ' +
            df['variant'].astype(str).fillna('')
        ).str.replace('None', '', regex=False).str.replace(
            'nan', '', regex=False
        ).str.strip()

    @classmethod
    def split_data(
        cls,
        df,
        *,
        test_size=0.2,
        random_state=42,
        strategy='auto',
        time_col=None,
    ):
        """Split raw rows before fitting target-derived features."""
        data = df.copy().reset_index(drop=True)
        requested_strategy = strategy
        time_candidates = [time_col] if time_col else []
        time_candidates += ['posted_at', 'ingested_at', 'etl_at', 'created_at']
        usable_time_col = next(
            (
                col for col in time_candidates
                if col and col in data.columns
                and pd.to_datetime(data[col], errors='coerce').notna().sum() >= 2
            ),
            None,
        )

        if strategy == 'auto':
            strategy = 'temporal' if usable_time_col else 'random'

        if strategy == 'temporal':
            if not usable_time_col:
                raise ValueError(
                    'Temporal split requested but no usable timestamp column was found.'
                )
            timestamps = pd.to_datetime(data[usable_time_col], errors='coerce')
            fallback = timestamps.dropna().min()
            ordered = data.assign(
                _split_time=timestamps.fillna(fallback)
            ).sort_values('_split_time')
            cut = max(1, min(len(ordered) - 1, int(len(ordered) * (1 - test_size))))
            train_df = ordered.iloc[:cut].drop(columns=['_split_time'])
            test_df = ordered.iloc[cut:].drop(columns=['_split_time'])
            split_info = {'strategy': 'temporal', 'time_col': usable_time_col}
        elif strategy == 'group':
            groups = cls._full_model_name(data).replace('', 'unknown')
            if groups.nunique() >= 2:
                splitter = GroupShuffleSplit(
                    n_splits=1,
                    test_size=test_size,
                    random_state=random_state,
                )
                train_idx, test_idx = next(splitter.split(data, groups=groups))
                train_df = data.iloc[train_idx]
                test_df = data.iloc[test_idx]
                split_info = {
                    'strategy': 'group',
                    'group_col': 'full_model_name',
                    'train_groups': int(groups.iloc[train_idx].nunique()),
                    'test_groups': int(groups.iloc[test_idx].nunique()),
                }
            else:
                strategy = 'random'

        if strategy == 'random':
            train_df, test_df = train_test_split(
                data,
                test_size=test_size,
                random_state=random_state,
            )
            split_info = {'strategy': 'random'}

        split_info.update({
            'requested_strategy': requested_strategy,
            'n_train': int(len(train_df)),
            'n_test': int(len(test_df)),
        })
        return (
            train_df.reset_index(drop=True),
            test_df.reset_index(drop=True),
            split_info,
        )

    def fit(self, df, target_col='price', verbose=True):
        """Fit the model and target encoding on training rows only."""
        temp_df = df.copy()
        temp_df['full_model_name'] = self._full_model_name(temp_df)
        self.model_price_map = (
            temp_df.groupby('full_model_name')[target_col].median().to_dict()
        )

        df_engineered = self.engineer_features(df)
        feature_cols = [
            'model_base_price', 'release_year', 'device_age_years',
            'age_condition_interaction', 'condition_score', 'battery_score',
            'screen_score', 'body_score', 'quality_score', 'battery_percentage',
            'battery_penalty', 'screen_penalty', 'body_penalty',
            'accessories_bonus', 'storage_gb', 'storage_tier', 'ram_gb',
            'battery_storage', 'quality_storage', 'condition_accessories',
            'platform_encoded', 'has_box', 'has_charger', 'is_sim_free',
            'fully_functional', 'has_any_damage', 'damage_severity',
            'functionality_score',
        ]
        self.feature_columns = [
            col for col in feature_cols if col in df_engineered.columns
        ]
        X = df_engineered[self.feature_columns]
        y = df_engineered[target_col]
        self.model.fit(X, y)

        train_pred = self.model.predict(X)
        self.train_stats_ = {
            'train_mae': float(mean_absolute_error(y, train_pred)),
            'train_r2': float(r2_score(y, train_pred)),
            'n_train': int(len(X)),
            'n_features': int(len(self.feature_columns)),
        }
        self.feature_importance_ = pd.DataFrame({
            'feature': self.feature_columns,
            'importance': self.model.feature_importances_,
        }).sort_values('importance', ascending=False)
        if verbose:
            print(
                f"Train MAE: ¥{self.train_stats_['train_mae']:,.0f} | "
                f"R² = {self.train_stats_['train_r2']:.3f}"
            )
        return self.train_stats_

    def evaluate(self, df, target_col='price'):
        if self.feature_columns is None:
            raise ValueError("Model not trained yet.")
        y_true = pd.to_numeric(df[target_col], errors='coerce')
        valid = y_true.notna()
        eval_df = df.loc[valid].copy()
        y_true = y_true.loc[valid].to_numpy()
        y_pred = self.predict(eval_df)
        abs_error = np.abs(y_true - y_pred)
        denominator = np.maximum(np.abs(y_true), 1.0)
        return {
            'mae': float(mean_absolute_error(y_true, y_pred)),
            'median_ae': float(np.median(abs_error)),
            'rmse': float(np.sqrt(mean_squared_error(y_true, y_pred))),
            'r2': float(r2_score(y_true, y_pred)),
            'smape': float(np.mean(
                2.0 * abs_error /
                np.maximum(np.abs(y_true) + np.abs(y_pred), 1.0)
            )),
            'within_10pct': float(np.mean(abs_error / denominator <= 0.10)),
            'within_20pct': float(np.mean(abs_error / denominator <= 0.20)),
            'n_samples': int(len(y_true)),
        }

    def train(
        self,
        df,
        target_col='price',
        test_size=0.2,
        verbose=True,
        split_strategy='auto',
        time_col=None,
    ):
        train_df, test_df, split_info = self.split_data(
            df,
            test_size=test_size,
            strategy=split_strategy,
            time_col=time_col,
        )
        self.fit(train_df, target_col=target_col, verbose=verbose)
        metrics = self.evaluate(test_df, target_col=target_col)
        self.train_stats_.update({
            'test_mae': metrics['mae'],
            'test_median_ae': metrics['median_ae'],
            'test_rmse': metrics['rmse'],
            'test_r2': metrics['r2'],
            'test_smape': metrics['smape'],
            'test_within_10pct': metrics['within_10pct'],
            'test_within_20pct': metrics['within_20pct'],
            'n_test': metrics['n_samples'],
            'split': split_info,
        })
        self.model_metadata_['split'] = split_info
        if verbose:
            print(
                f"Test MAE: ¥{metrics['mae']:,.0f} | "
                f"R² = {metrics['r2']:.3f} | "
                f"within 20% = {metrics['within_20pct']:.1%}"
            )
        return self.train_stats_

    def predict(self, df):
        if self.feature_columns is None: raise ValueError("Model not trained yet.")
        df_engineered = self.engineer_features(df)
        return self.model.predict(df_engineered[self.feature_columns])

    def prediction_quality_scores(self, df):
        """
        Return a case-level data quality score, not a statistical probability.

        The score is calibrated from held-out accuracy and reduced for inputs
        that the cold-start audit shows are unreliable.
        """
        rows = df.copy()
        full_names = self._full_model_name(rows)
        known_model = full_names.isin(self.model_price_map)
        base = float((self.train_stats_ or {}).get('test_within_20pct', 0.5))
        scores = pd.Series(base, index=rows.index, dtype=float)
        scores.loc[~known_model] *= 0.25

        if 'storage' in rows.columns:
            storage_known = rows['storage'].notna() & (
                rows['storage'].astype(str).str.strip() != ''
            )
            scores.loc[~storage_known] *= 0.75
        if 'listing_count' in rows.columns:
            listing_count = pd.to_numeric(
                rows['listing_count'], errors='coerce'
            ).fillna(0)
            scores.loc[listing_count < 3] *= 0.70
            scores.loc[(listing_count >= 3) & (listing_count < 10)] *= 0.85
        return scores.clip(lower=0.05, upper=0.90).to_numpy()
    
    def save(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump({
            'model': self.model, 'feature_columns': self.feature_columns,
            'feature_importance': self.feature_importance_, 'train_stats': self.train_stats_,
            'model_price_map': self.model_price_map,
            'model_metadata': self.model_metadata_,
        }, path)
        print(f"Model saved to {path}")
    
    def load(self, path):
        data = joblib.load(path)
        self.model = data['model']
        self.feature_columns = data['feature_columns']
        self.feature_importance_ = data.get('feature_importance')
        self.train_stats_ = data.get('train_stats')
        self.model_price_map = data.get('model_price_map', {})
        self.model_metadata_ = data.get('model_metadata', {})

class EnsemblePricePredictor:
    def __init__(self, **kwargs):
        self.apple_model = SmartPricePredictor(**kwargs)
        self.android_model = SmartPricePredictor(**kwargs)
        self.is_trained = False

    def _split_data(self, df):
        is_apple = df['ecosystem'] == 'Apple' if 'ecosystem' in df.columns else df.get('brand', pd.Series(['unknown']*len(df))).astype(str).str.lower() == 'apple'
        return df[is_apple].copy(), df[~is_apple].copy()

    def train(self, df, target_col='price', test_size=0.2, verbose=True):
        if verbose: print(f"\n{'='*80}\nTRAINING ENSEMBLE MODELS (APPLE vs ANDROID)\n{'='*80}")
        apple_df, android_df = self._split_data(df)
        
        if not apple_df.empty:
            if verbose: print("\n---> TRAINING APPLE MODEL <---")
            self.apple_model.train(apple_df, target_col, test_size, verbose)
            
        if not android_df.empty:
            if verbose: print("\n---> TRAINING ANDROID MODEL <---")
            self.android_model.train(android_df, target_col, test_size, verbose)

        self.is_trained = True

    def predict(self, df):
        if not self.is_trained: raise ValueError("Models not trained.")
        results = pd.Series(0.0, index=df.index)
        
        apple_idx = df[df['ecosystem'] == 'Apple'].index if 'ecosystem' in df.columns else df[df.get('brand', pd.Series()).astype(str).str.lower() == 'apple'].index
        android_idx = df.index.difference(apple_idx)
            
        if not apple_idx.empty: results.loc[apple_idx] = self.apple_model.predict(df.loc[apple_idx])
        if not android_idx.empty: results.loc[android_idx] = self.android_model.predict(df.loc[android_idx])
            
        return results.values

    def save(self, path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump({
            'apple_model': self.apple_model, 'android_model': self.android_model,
            'is_trained': self.is_trained
        }, path)
        print(f"Ensemble model saved to {path}")

    def load(self, path):
        data = joblib.load(path)
        self.apple_model, self.android_model, self.is_trained = data['apple_model'], data['android_model'], data['is_trained']

def create_and_train_model(df, target_col='price', **kwargs):
    predictor = SmartPricePredictor(**kwargs)
    predictor.train(df, target_col=target_col)
    return predictor
