"""
FastAPI — trượt giá, feature impact, dự báo giá 30 ngày (MySQL).

Chạy từ thư mục predictprice:
  uvicorn api.api_depreciation:app --reload --host 0.0.0.0 --port 8000

Thử:
  GET  http://localhost:8000/depreciation-curve?product_id=<uuid>
  GET  http://localhost:8000/depreciation-curve?model_line=iPhone%208&storage=64&ram=3
  POST http://localhost:8000/feature-impact/counterfactual  (product_id hoặc model_line+storage+ram)
  GET  http://localhost:8000/price-forecast/30d?product_id=<uuid>
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
from ml_models.price_forecast_30d import (
    build_product_baseline_row,
    compute_price_forecast_30d,
    load_forecast_config,
)
import pandas as pd
from api.mysql_db import (
    fetch_latest_stored_forecast,
    fetch_listings_for_product,
    fetch_price_history,
    fetch_product_row,
    get_mysql_engine,
    parse_base_specs,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Predict Price — Pricing ML API",
    description="Depreciation curve, feature impact, dự báo giá 30 ngày (price_history + ML).",
    version="1.1.0",
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
    product_id: str = Field(
        "",
        description="UUID MySQL — tự lấy model/specs + aggregate listings; có thể ghi đè field bên dưới",
    )
    model_line: str = ""
    storage: str = ""
    ram: str = ""
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
        d = body.model_dump(exclude_none=True)
    else:
        d = body.dict(exclude_none=True)
    d.pop("yen_to_vnd", None)
    d.pop("include_all_scenarios", None)
    return d


def _resolve_feature_impact_listing(body: FeatureImpactBody) -> tuple[dict, dict | None]:
    """Trả về (listing dict cho ML, product meta hoặc None)."""
    fields = _listing_fields_from_body(body)
    pid = (body.product_id or fields.pop("product_id", "") or "").strip()
    product_meta: dict | None = None

    if pid:
        try:
            engine = get_mysql_engine()
            product = fetch_product_row(engine, pid)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MySQL: {e}") from e
        if not product:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy product_id={pid}")
        product["base_specs"] = parse_base_specs(product.get("base_specs"))
        listings_df = fetch_listings_for_product(engine, pid)
        listing = build_product_baseline_row(product, listings_df)
        product_meta = product
        # Ghi đè từ body (tin đang xem / form người dùng)
        for k, v in fields.items():
            if v is None:
                continue
            if k in ("model_line", "storage", "ram", "model_number", "variant") and v == "":
                continue
            if k in ("has_box", "has_charger"):
                listing[k] = bool(v)
            elif k in ("is_sim_free", "fully_functional", "has_scratches", "has_damage", "has_issues"):
                listing[k] = int(v) if isinstance(v, bool) else v
            else:
                listing[k] = v
    else:
        ml = (fields.get("model_line") or "").strip()
        st = str(fields.get("storage") or "").strip()
        rm = str(fields.get("ram") or "").strip()
        if not (ml and st and rm):
            raise HTTPException(
                status_code=400,
                detail="Cần product_id, hoặc đủ model_line + storage + ram",
            )
        cfg = load_curve_config()
        listing = build_baseline_row(
            model_line=ml,
            storage=st,
            ram=rm,
            model_number=(fields.get("model_number") or ""),
            variant=(fields.get("variant") or ""),
            config=cfg,
        )
        for k, v in fields.items():
            if k in ("model_line", "storage", "ram", "model_number", "variant"):
                continue
            if v is not None:
                listing[k] = v

    return raw_listing_from_flat_json(listing), product_meta


@app.post("/feature-impact/counterfactual")
def feature_impact_counterfactual(body: FeatureImpactBody) -> dict:
    """
    Phân tích counterfactual: mỗi yếu tố (pin, hộp, màn…) so với mức chuẩn.

    - `product_id` + field cụ thể (pin 82%, không hộp…) từ listing đang xem
    - Hoặc gửi đủ `model_line`, `storage`, `ram` + điều kiện
    """
    try:
        pred = load_predictor()
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e)) from e

    listing, product_meta = _resolve_feature_impact_listing(body)
    try:
        out = counterfactual_impact_report(
            pred,
            listing,
            yen_to_vnd=body.yen_to_vnd,
            include_all_scenarios=body.include_all_scenarios,
        )
        out["model_version"] = get_model_version(predictor=pred)
        if product_meta:
            out["product_id"] = product_meta.get("product_id")
            out["product_name"] = product_meta.get("name")
            out["brand"] = product_meta.get("brand")
        out["input_summary"] = {
            "model_line": listing.get("model_line"),
            "storage": listing.get("storage"),
            "ram": listing.get("ram"),
            "battery_percentage": listing.get("battery_percentage"),
            "has_box": listing.get("has_box"),
            "has_charger": listing.get("has_charger"),
            "screen_condition": listing.get("screen_condition"),
            "body_condition": listing.get("body_condition"),
            "condition": listing.get("condition"),
        }
        return out
    except Exception as e:
        logger.exception("feature-impact failed")
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.get("/depreciation-curve")
def depreciation_curve(
    product_id: str = Query("", description="UUID trong MySQL — ưu tiên, tự lấy model + specs + listing"),
    model_line: str = Query("", description="Ví dụ: iPhone 13 (bắt buộc nếu không có product_id)"),
    storage: str = Query("", description="Dung lượng GB, ví dụ: 128"),
    ram: str = Query("", description="RAM GB string, ví dụ: 4"),
    yen_to_vnd: float = Query(DEFAULT_YEN_TO_VND, gt=0, description="Quy đổi JPY→VND"),
) -> dict:
    """
    Đường cong trượt giá theo tuổi máy (0–8 năm).

    Cách gọi (chọn một):
    - `?product_id=<uuid>` — đọc `products` + aggregate `active_listings`
    - `?model_line=iPhone%2013&storage=128&ram=4` — thủ công (không cần MySQL)
    - Kết hợp: `product_id` + ghi đè `model_line` / `storage` / `ram` nếu cần
    """
    try:
        cfg = load_curve_config()
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"Thiếu file config: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi đọc config: {e}") from e

    pid = product_id.strip()
    ml_q = model_line.strip()
    st_q = str(storage).strip()
    rm_q = str(ram).strip()
    product_meta: dict | None = None

    if pid:
        try:
            engine = get_mysql_engine()
            product = fetch_product_row(engine, pid)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MySQL: {e}") from e
        if not product:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy product_id={pid}")
        product["base_specs"] = parse_base_specs(product.get("base_specs"))
        try:
            listings_df = fetch_listings_for_product(engine, pid)
            raw = build_product_baseline_row(product, listings_df)
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MySQL listings: {e}") from e
        if ml_q:
            raw["model_line"] = ml_q
        if st_q:
            raw["storage"] = st_q
        if rm_q:
            raw["ram"] = rm_q
        product_meta = product
    else:
        if not (ml_q and st_q and rm_q):
            raise HTTPException(
                status_code=400,
                detail="Cần product_id, hoặc đủ tham số model_line + storage + ram",
            )
        raw = build_baseline_row(
            model_line=ml_q,
            storage=st_q,
            ram=rm_q,
            config=cfg,
        )

    try:
        out = compute_depreciation_curve_response(
            raw,
            product_id=pid or "anonymous",
            config=cfg,
            yen_to_vnd=yen_to_vnd,
        )
        pred = load_predictor()
        eng = pred.engineer_features(pd.DataFrame([raw]))
        out["baseline"] = {
            "model_line": raw.get("model_line"),
            "storage": raw.get("storage"),
            "ram": raw.get("ram"),
            "release_year": int(eng.iloc[0]["release_year"]),
            "device_age_years": float(eng.iloc[0]["device_age_years"]),
        }
        if product_meta:
            out["product_name"] = product_meta.get("name")
            out["brand"] = product_meta.get("brand")
            out["model_series"] = product_meta.get("model_series")
        return out
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


@app.get("/price-forecast/30d")
def price_forecast_30d(
    product_id: str = Query(..., min_length=8, description="UUID product trong MySQL"),
    horizon_days: int = Query(0, ge=0, le=90, description="0 = dùng config mặc định (30)"),
) -> dict:
    """
    Dự báo giá VND theo ngày (D+1 … D+horizon) cho một product.
    Kết hợp price_history (xu hướng tuyến tính) và độ dốc mô hình ML khi lịch sử mỏng.
    """
    pid = product_id.strip()
    try:
        engine = get_mysql_engine()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"MySQL: {e}") from e

    try:
        product = fetch_product_row(engine, pid)
    except Exception as e:
        logger.exception("mysql product fetch failed")
        raise HTTPException(status_code=500, detail=f"MySQL: {e}") from e

    if not product:
        raise HTTPException(status_code=404, detail=f"Không tìm thấy product_id={pid}")

    product["base_specs"] = parse_base_specs(product.get("base_specs"))

    try:
        history = fetch_price_history(engine, pid)
        listings_df = fetch_listings_for_product(engine, pid)
        stored = fetch_latest_stored_forecast(engine, pid)
        cfg = load_forecast_config()
        h = horizon_days if horizon_days > 0 else None
        return compute_price_forecast_30d(
            product=product,
            history=history,
            listings_df=listings_df,
            stored_forecast=stored,
            horizon_days=h,
            config=cfg,
        )
    except FileNotFoundError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Chưa có model ML: {e}. Cần smart_price_predictor.pkl trong models/",
        ) from e
    except Exception as e:
        logger.exception("price-forecast/30d failed")
        raise HTTPException(status_code=500, detail=str(e)) from e
