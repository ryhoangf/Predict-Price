from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import pymongo

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config as cfg
from NLP.title_nlp import (
    PhoneInfoExtractor,
    build_product_identity_key,
    build_standard_name,
    row_from_mongo_doc,
)


LABEL_PATTERNS = (
    re.compile(
        r"(?:機種名|商品名|モデル名|型番|model\s*name|product\s*name)"
        r"\s*[:：]\s*([^\r\n|]{3,120})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:シリーズ|series)\s*[:：]\s*([^\r\n|]{3,80})",
        re.IGNORECASE,
    ),
)


def _present(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _explicit_label(doc: dict[str, Any]) -> str:
    description = str(doc.get("explanation") or doc.get("description") or "")
    for pattern in LABEL_PATTERNS:
        match = pattern.search(description)
        if match:
            return re.sub(r"\s+", " ", match.group(1)).strip()
    return ""


def _identity(text_value: str, extractor: PhoneInfoExtractor) -> dict[str, Any] | None:
    row = row_from_mongo_doc({"name": text_value}, extractor)
    name = build_standard_name(row)
    key = build_product_identity_key(row)
    if not name or not key:
        return None
    brand, model, storage, ram = key.split("|", 3)
    return {
        "brand": brand,
        "model": model,
        "storage": storage,
        "ram": ram,
        "name": name,
        "key": key,
    }


def _same_model(left: dict[str, Any], right: dict[str, Any]) -> bool:
    return left["brand"] == right["brand"] and left["model"] == right["model"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Measure title identity agreement against explicit model labels in raw descriptions. "
            "These are silver labels, not manually verified gold labels."
        )
    )
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--examples", type=int, default=40)
    parser.add_argument("--json-output")
    args = parser.parse_args()

    extractor = PhoneInfoExtractor()
    mongo = pymongo.MongoClient(cfg.MONGO_URI)
    collection = mongo[cfg.DB_NAME][cfg.COLLECTION_NAME]
    query = {
        "$or": [
            {"explanation": {"$regex": r"(機種名|商品名|モデル名|型番|model\s*name|product\s*name)\s*[:：]", "$options": "i"}},
            {"description": {"$regex": r"(機種名|商品名|モデル名|型番|model\s*name|product\s*name)\s*[:：]", "$options": "i"}},
        ]
    }
    projection = {
        "_id": 0,
        "name": 1,
        "explanation": 1,
        "description": 1,
        "source": 1,
        "link": 1,
    }
    cursor = collection.find(query, projection)
    if args.limit:
        cursor = cursor.limit(args.limit)

    counts = Counter()
    by_brand: dict[str, Counter] = defaultdict(Counter)
    by_source: dict[str, Counter] = defaultdict(Counter)
    mismatches: list[dict[str, Any]] = []

    for doc in cursor:
        counts["documents_scanned"] += 1
        title = str(doc.get("name") or "").strip()
        label = _explicit_label(doc)
        if not title or not label:
            counts["missing_text"] += 1
            continue

        predicted = _identity(title, extractor)
        expected = _identity(label, extractor)
        if expected is None:
            counts["label_unparseable"] += 1
            continue
        if predicted is None:
            counts["title_unparseable"] += 1
            outcome = "wrong"
        else:
            outcome = "correct" if _same_model(predicted, expected) else "wrong"

        counts["evaluated"] += 1
        counts[outcome] += 1
        brand = expected["brand"] or "unknown"
        source = str(doc.get("source") or "unknown")
        by_brand[brand]["evaluated"] += 1
        by_brand[brand][outcome] += 1
        by_source[source]["evaluated"] += 1
        by_source[source][outcome] += 1

        if outcome == "wrong" and len(mismatches) < args.examples:
            mismatches.append(
                {
                    "source": source,
                    "title": title,
                    "explicit_label": label,
                    "predicted": predicted,
                    "expected": expected,
                    "link": doc.get("link"),
                }
            )

    mongo.close()
    evaluated = counts["evaluated"]
    accuracy = counts["correct"] / evaluated if evaluated else 0.0
    report = {
        "method": "silver-label agreement from explicit description model fields",
        "counts": dict(counts),
        "model_accuracy": round(accuracy, 6),
        "by_brand": {
            key: {
                **dict(value),
                "accuracy": round(value["correct"] / value["evaluated"], 6)
                if value["evaluated"]
                else 0.0,
            }
            for key, value in sorted(
                by_brand.items(), key=lambda item: item[1]["evaluated"], reverse=True
            )
        },
        "by_source": {
            key: {
                **dict(value),
                "accuracy": round(value["correct"] / value["evaluated"], 6)
                if value["evaluated"]
                else 0.0,
            }
            for key, value in sorted(
                by_source.items(), key=lambda item: item[1]["evaluated"], reverse=True
            )
        },
        "mismatches": mismatches,
    }

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.json_output:
        Path(args.json_output).write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return 0 if evaluated else 2


if __name__ == "__main__":
    raise SystemExit(main())
