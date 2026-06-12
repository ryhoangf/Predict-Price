import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from NLP.identity_quality import identity_quality_reason
from NLP.product_matcher import ProductMatcher
from NLP.title_nlp import (
    NLP_IDENTITY_VERSION,
    PhoneInfoExtractor,
    product_identity_key_from_product_row,
)
from repair_generic_product_buckets import classify_listing


class IdentityRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.extractor = PhoneInfoExtractor()

    def test_compact_iphone_pro_variants_are_parsed(self):
        # model_number giờ bao gồm suffix Pro/Max/mini để model_series chính xác
        cases = {
            "iPhone13Pro 256GB": ("13 Pro", "Pro"),
            "iPhone13ProMAX 256GB": ("13 Pro Max", "Pro Max"),
            "iPhone 13Promax 256GB": ("13 Pro Max", "Pro Max"),
            "iPhone 13 Pro Max 256GB": ("13 Pro Max", "Pro Max"),
            "iPhone 13 Pro 128GB": ("13 Pro", "Pro"),
            "iPhone 13 mini 256GB": ("13 mini", "mini"),
            "iPhone 13 128GB": ("13", None),
        }
        from NLP.title_nlp import build_model_series
        for title, (exp_number, exp_variant) in cases.items():
            with self.subTest(title=title):
                result = self.extractor.extract_all_info(title)
                self.assertEqual(result["model_number"], exp_number, msg=f"model_number mismatch for '{title}'")
                self.assertEqual(result["variant"], exp_variant, msg=f"variant mismatch for '{title}'")
                self.assertEqual(result["brand"], "Apple", msg=f"brand mismatch for '{title}'")
                self.assertEqual(result["nlp_identity_version"], NLP_IDENTITY_VERSION)

    def test_common_compact_and_japanese_model_forms_are_normalized(self):
        cases = {
            "iPhone SE\uFF08\u7B2C2\u4E16\u4EE3\uFF09 128GB": ("Apple", "iPhone", "SE2"),
            "iPhoneSE \u7B2C2\u4E16\u4EE3 64GB": ("Apple", "iPhone", "SE2"),
            "iPhone mini 13 128GB": ("Apple", "iPhone", "13 mini"),
            "Galaxy S25Ultra 512GB": ("Samsung", "Galaxy", "S25 Ultra"),
            "Xperia 1VII 256GB": ("Sony", "Xperia", "1 VII"),
            "realme P4 Power 8GB 256GB": ("Realme", "Realme", "P4 Power"),
        }
        for title, expected in cases.items():
            with self.subTest(title=title):
                result = self.extractor.extract_all_info(title)
                actual = (result["brand"], result["model_line"], result["model_number"])
                self.assertEqual(actual, expected)

    def test_description_keyword_spam_does_not_reject_clear_title(self):
        row = {
            "name_raw": "iPhone 13 128GB SIM free",
            "description": "Search tags: iPhone 12 iPhone 13 iPhone 14 Galaxy S23",
            "brand": "Apple",
            "model_line": "iPhone",
            "model_number": "13",
            "standard_name": "iPhone 13",
            "base_specs": '{"storage": "128", "ram": null}',
        }
        self.assertIsNone(identity_quality_reason(row))

    def test_only_explicit_description_model_can_refine_raw_title(self):
        listing = {
            "product_name": "iPhone 13",
            "product_brand": "Apple",
            "description": "SEO: iPhone 12 iPhone 13 iPhone 14 Galaxy S23",
        }
        raw_doc = {
            "name": "Apple iPhone 13 256GB",
            "explanation": "機種名：iPhone 13 Pro Max\n状態：中古",
        }
        action, canonical, confidence = classify_listing(listing, self.extractor, raw_doc)
        self.assertIn("explicit_description_model", action)
        self.assertEqual(canonical["name"], "iPhone 13 Pro Max")
        self.assertGreaterEqual(confidence, 0.9)

        raw_doc["explanation"] = "SEO: iPhone 12 iPhone 13 Pro Max iPhone 14 Galaxy S23"
        action, canonical, _ = classify_listing(listing, self.extractor, raw_doc)
        self.assertIn("raw_title", action)
        self.assertEqual(canonical["name"], "iPhone 13")

    def test_duplicate_identity_prefers_clean_populated_product(self):
        products = pd.DataFrame(
            [
                {
                    "product_id": "legacy",
                    "name": "iPhone 13 mini 256GB 4GB RAM",
                    "brand": "Apple",
                    "model_series": "iPhone 13",
                    "base_specs": '{"storage": "256", "ram": "4"}',
                    "listing_count": 60,
                },
                {
                    "product_id": "canonical",
                    "name": "iPhone 13 mini",
                    "brand": "Apple",
                    "model_series": "iPhone 13 mini",
                    "base_specs": '{"storage": "256", "ram": null}',
                    "listing_count": 500,
                },
            ]
        )
        matcher = ProductMatcher(products)
        decision = matcher.decide(
            {
                "product_identity_key": "apple|iphone 13 mini|256|",
                "brand": "Apple",
                "standard_name": "iPhone 13 mini",
                "model_series": "iPhone 13 mini",
                "base_specs": '{"storage": "256", "ram": null}',
                "name_raw": "iPhone13 mini 256GB",
            }
        )
        self.assertTrue(decision.accepted)
        self.assertEqual(decision.product_id, "canonical")

    def test_catalog_identity_uses_brand_for_short_model_name(self):
        key = product_identity_key_from_product_row(
            {
                "name": "A 3",
                "brand": "OPPO",
                "model_series": "A 3",
                "base_specs": '{"storage": "128", "ram": null}',
            }
        )
        self.assertEqual(key, "oppo|a 3|128|")


if __name__ == "__main__":
    unittest.main()
