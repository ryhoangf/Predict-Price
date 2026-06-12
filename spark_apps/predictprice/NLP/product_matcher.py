from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from NLP.title_nlp import product_identity_key_from_product_row


EXACT_MATCH_CONFIDENCE = 1.0
ACCEPT_EXISTING_THRESHOLD = 0.78
ACCEPT_NEW_THRESHOLD = 0.88
_MODEL_DIR = Path(__file__).resolve().parent / "models"
_PRODUCT_MATCH_MODEL = _MODEL_DIR / "lgbm_product_match_v1.txt"
_SPEC_IN_PRODUCT_NAME_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:gb|tb)\b|\b\d+\s*gb\s*ram\b",
    re.IGNORECASE,
)


def _present(value: Any) -> bool:
    if value is None:
        return False
    try:
        if pd.isna(value):
            return False
    except (TypeError, ValueError):
        pass
    return not (isinstance(value, str) and not value.strip())


def _norm_text(value: Any) -> str:
    if not _present(value):
        return ""
    return re.sub(r"\s+", " ", str(value)).strip().lower()


def _specs_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not _present(raw):
        return {}
    try:
        return json.loads(str(raw))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}


def _product_text(row: dict[str, Any]) -> str:
    specs = _specs_dict(row.get("base_specs"))
    parts = [
        row.get("brand"),
        row.get("name"),
        row.get("model_series"),
        specs.get("storage"),
        specs.get("ram"),
    ]
    return _norm_text(" ".join(str(p) for p in parts if _present(p)))


def _listing_text(row: dict[str, Any]) -> str:
    parts = [
        row.get("brand"),
        row.get("standard_name"),
        row.get("model_series"),
        row.get("base_specs"),
        row.get("name_raw"),
        row.get("description"),
    ]
    return _norm_text(" ".join(str(p) for p in parts if _present(p)))


def _storage_from_base_specs(raw: Any) -> str:
    specs = _specs_dict(raw)
    return _norm_text(specs.get("storage"))


def _ram_from_base_specs(raw: Any) -> str:
    specs = _specs_dict(raw)
    return _norm_text(specs.get("ram"))


def product_candidate_priority(row: dict[str, Any]) -> tuple[int, int]:
    name = str(row.get("name") or "")
    clean_model_name = int(not _SPEC_IN_PRODUCT_NAME_RE.search(name))
    try:
        listing_count = int(row.get("listing_count") or 0)
    except (TypeError, ValueError):
        listing_count = 0
    return clean_model_name, listing_count


@dataclass
class MatchDecision:
    accepted: bool
    product_id: str | None
    confidence: float
    reason: str
    candidate_identity_key: str | None
    candidate_name: str | None


class ProductMatcher:
    """
    Confidence gate between deterministic NLP identity and MySQL product creation.

    The deterministic identity key remains the source of truth. TF-IDF similarity is
    used only to catch suspicious titles before they create new catalog products.
    """

    def __init__(self, products_df: pd.DataFrame):
        self.products_df = products_df.copy() if products_df is not None else pd.DataFrame()
        self.key_to_product: dict[str, dict[str, Any]] = {}
        self._tfidf = None
        self._matrix = None
        self._lgbm_model = self._load_optional_lgbm()

        if not self.products_df.empty:
            selected_by_key: dict[str, dict[str, Any]] = {}
            for _, row in self.products_df.iterrows():
                rec = row.to_dict()
                rec["product_identity_key"] = product_identity_key_from_product_row(rec)
                rec["match_text"] = _product_text(rec)
                key = rec.get("product_identity_key")
                if not key:
                    continue
                current = selected_by_key.get(key)
                if current is None or product_candidate_priority(rec) > product_candidate_priority(
                    current
                ):
                    selected_by_key[key] = rec
            self.key_to_product = selected_by_key
            self.products_df = pd.DataFrame(selected_by_key.values())
            self._fit_tfidf()

    def _load_optional_lgbm(self):
        model_path = Path(os.getenv("PRODUCT_MATCH_MODEL_PATH", str(_PRODUCT_MATCH_MODEL)))
        if not model_path.is_file():
            return None
        try:
            import lightgbm as lgb

            print(f"Loaded product match LightGBM model: {model_path}")
            return lgb.Booster(model_file=str(model_path))
        except Exception as exc:
            print(f"Warning: product LightGBM matcher disabled: {exc}")
            return None

    def _fit_tfidf(self) -> None:
        texts = self.products_df.get("match_text")
        if texts is None or texts.empty:
            return
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer

            self._tfidf = TfidfVectorizer(
                analyzer="char_wb",
                ngram_range=(2, 5),
                max_features=20000,
                min_df=1,
            )
            self._matrix = self._tfidf.fit_transform(texts.fillna("").astype(str))
        except Exception as exc:
            print(f"Warning: product TF-IDF matcher disabled: {exc}")
            self._tfidf = None
            self._matrix = None

    def _top_tfidf_candidate(self, row: dict[str, Any]) -> tuple[dict[str, Any] | None, float]:
        if self._tfidf is None or self._matrix is None or self.products_df.empty:
            return None, 0.0
        try:
            from sklearn.metrics.pairwise import cosine_similarity

            q = self._tfidf.transform([_listing_text(row)])
            sims = cosine_similarity(q, self._matrix).ravel()
            if len(sims) == 0:
                return None, 0.0
            idx = int(sims.argmax())
            return self.products_df.iloc[idx].to_dict(), float(sims[idx])
        except Exception:
            return None, 0.0

    def _pair_features(
        self,
        row: dict[str, Any],
        candidate: dict[str, Any] | None,
        sim: float,
    ) -> list[float]:
        if not candidate:
            return [sim, 0.0, 0.0, 0.0, 0.0, 0.0]
        same_brand = float(_norm_text(row.get("brand")) == _norm_text(candidate.get("brand")))
        same_series = float(
            _norm_text(row.get("model_series")) == _norm_text(candidate.get("model_series"))
        )
        same_name = float(_norm_text(row.get("standard_name")) == _norm_text(candidate.get("name")))
        same_storage = float(
            _storage_from_base_specs(row.get("base_specs"))
            == _storage_from_base_specs(candidate.get("base_specs"))
        )
        same_ram = float(
            _ram_from_base_specs(row.get("base_specs"))
            == _ram_from_base_specs(candidate.get("base_specs"))
        )
        return [float(sim), same_brand, same_series, same_name, same_storage, same_ram]

    def _model_confidence(
        self,
        row: dict[str, Any],
        candidate: dict[str, Any] | None,
        sim: float,
        fallback: float,
    ) -> float:
        if self._lgbm_model is None or candidate is None:
            return fallback
        try:
            pred = self._lgbm_model.predict([self._pair_features(row, candidate, sim)])
            if len(pred):
                return max(0.0, min(0.99, float(pred[0])))
        except Exception:
            pass
        return fallback

    def decide(self, row: dict[str, Any]) -> MatchDecision:
        key = row.get("product_identity_key")
        if key in self.key_to_product:
            product = self.key_to_product[key]
            return MatchDecision(
                accepted=True,
                product_id=str(product.get("product_id")),
                confidence=EXACT_MATCH_CONFIDENCE,
                reason="exact_identity_key",
                candidate_identity_key=key,
                candidate_name=str(product.get("name") or ""),
            )

        candidate, sim = self._top_tfidf_candidate(row)
        candidate_key = candidate.get("product_identity_key") if candidate else None
        candidate_name = candidate.get("name") if candidate else None

        standard_name = _norm_text(row.get("standard_name"))
        brand = _norm_text(row.get("brand"))
        has_model = bool(standard_name)
        has_brand = bool(brand)
        has_storage_signal = '"storage": null' not in str(row.get("base_specs") or "")

        confidence = 0.0
        if has_model:
            confidence += 0.42
        if has_brand:
            confidence += 0.18
        if has_storage_signal:
            confidence += 0.12
        confidence += min(sim, 1.0) * 0.28
        confidence = min(confidence, 0.99)
        confidence = self._model_confidence(row, candidate, sim, confidence)

        if candidate_key == key and sim >= ACCEPT_EXISTING_THRESHOLD:
            return MatchDecision(
                accepted=True,
                product_id=str(candidate.get("product_id")),
                confidence=max(confidence, sim),
                reason="tfidf_existing_identity",
                candidate_identity_key=candidate_key,
                candidate_name=str(candidate_name or ""),
            )

        if confidence >= ACCEPT_NEW_THRESHOLD:
            return MatchDecision(
                accepted=True,
                product_id=None,
                confidence=confidence,
                reason="high_confidence_new_identity",
                candidate_identity_key=candidate_key,
                candidate_name=str(candidate_name or ""),
            )

        return MatchDecision(
            accepted=False,
            product_id=None,
            confidence=confidence,
            reason="low_confidence_product_identity",
            candidate_identity_key=candidate_key,
            candidate_name=str(candidate_name or ""),
        )


def apply_product_matching_gate(
    df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df is None or df.empty:
        return df, pd.DataFrame()

    matcher = ProductMatcher(products_df)
    accepted_rows: list[dict[str, Any]] = []
    review_rows: list[dict[str, Any]] = []

    for _, row in df.iterrows():
        rec = row.to_dict()
        decision = matcher.decide(rec)
        rec["product_match_confidence"] = decision.confidence
        rec["product_match_reason"] = decision.reason
        rec["matched_product_id"] = decision.product_id
        rec["candidate_identity_key"] = decision.candidate_identity_key
        rec["candidate_name"] = decision.candidate_name
        if decision.accepted:
            accepted_rows.append(rec)
        else:
            review_rows.append(rec)

    return pd.DataFrame(accepted_rows), pd.DataFrame(review_rows)
