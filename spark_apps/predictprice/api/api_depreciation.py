"""
FastAPI — đường cong trượt giá + phân tích tác động linh kiện (counterfactual).

Chạy từ thư mục predictprice:
  uvicorn api.api_depreciation:app --reload --host 0.0.0.0 --port 8000

Thử:
  GET  http://localhost:8000/depreciation-curve?model_line=iPhone%208&storage=64&ram=3
  POST http://localhost:8000/feature-impact/counterfactual  (JSON body)
"""
from __future__ import annotations

import logging
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from ml_models.depreciation_curve import (
    DEFAULT_YEN_TO_VND,
    build_baseline_row,
    compute_depreciation_curve_response,
    get_model_version,
    load_curve_config,
    load_predictor,
)
from ml_models.feature_impact import (
    counterfactual_impact_report,
    raw_listing_from_flat_json,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Predict Price — Pricing ML API",
    description="Depreciation curve (theo tuổi mô hình) và feature impact (counterfactual).",
    version="1.0.0",
)

_origins = os.environ.get("CORS_ORIGINS", "*").strip()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _origins == "*" else [o.strip() for o in _origins.split(",") if o.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def _warmup_model() -> None:
    try:
        load_predictor()
        logger.info("SmartPricePredictor loaded.")
    except FileNotFoundError as e:
        logger.warning(
            "Model not loaded at startup: %s — endpoints needing the model return 503.",
            e,
        )


@app.get("/health")
def health() -> dict:
    return {"ok": True, "service": "pricing_ml_api"}


class FeatureImpactBody(BaseModel):
    model_line: str
    storage: str
    ram: str
    model_number: str = ""
    variant: str = ""
    condition: str = "Good"
    battery_percentage: float = 82.0
    screen_condition: str = "clean"
    body_condition: str = "good"
    platform: str = "Mercari"
    has_box: bool = False
    has_charger: bool = False
    is_sim_free: int = 1
    fully_functional: int = 1
    has_scratches: int = 0
    has_damage: int = 0
    has_issues: int = 0
    yen_to_vnd: float = Field(default=175, gt=0)
    include_all_scenarios: bool = False


def _listing_fields_from_body(body: FeatureImpactBody) -> dict:
    """Không đưa meta API (tỷ giá, flags) vào vector predict."""
    if hasattr(body, "model_dump"):
        d = body.model_dump()
    else:
        d = body.dict()
    d.pop("yen_to_vnd", None)
    d.pop("include_all_scenarios", None)
    return d


@app.post("/feature-impact/counterfactual")
def feature_impact_counterfactual(body: FeatureImpactBody) -> dict:
    try:
        pred = load_predictor()
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e

    listing = raw_listing_from_flat_json(_listing_fields_from_body(body))
    try:
        out = counterfactual_impact_report(
            pred,
            listing,
            yen_to_vnd=body.yen_to_vnd,
            include_all_scenarios=body.include_all_scenarios,
        )
        out["model_version"] = get_model_version(predictor=pred)
        return out
    except Exception as e:
        logger.exception("feature-impact failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/depreciation-curve")
def depreciation_curve(
    model_line: str = Query(..., description="Ví dụ: iPhone 8"),
    storage: str = Query(..., description="Dung lượng, ví dụ: 64"),
    ram: str = Query(..., description="RAM GB string, ví dụ: 3"),
    product_id: str = Query("", description="UUID sản phẩm (cache / logging)"),
    yen_to_vnd: float = Query(DEFAULT_YEN_TO_VND, gt=0, description="Quy đổi JPY→VND"),
) -> dict:
    try:
        cfg = load_curve_config()
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"Thiếu file config: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đọc config: {e}") from e

    raw = build_baseline_row(
        model_line=model_line.strip(),
        storage=str(storage).strip(),
        ram=str(ram).strip(),
        config=cfg,
    )

    try:
        return compute_depreciation_curve_response(
            raw,
            product_id=product_id.strip() or "anonymous",
            config=cfg,
            yen_to_vnd=yen_to_vnd,
        )
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Chưa có model: {e}. Đặt smart_price_predictor.pkl vào {_ROOT}/models/",
        ) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.exception("depreciation-curve failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
