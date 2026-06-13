"""Walk-forward backtest for the 30-day price-history forecast."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import config as cfg
from ml_models.forecast_algorithms import (
    converging_rolling_median,
    damped_rolling_median_trend,
)


MYSQL_URI = (
    f"mysql+pymysql://{cfg.MYSQL_USER}:{cfg.MYSQL_PASSWORD}"
    f"@{cfg.MYSQL_HOST}:{cfg.MYSQL_PORT}/{cfg.MYSQL_DB}"
)


def load_forecast_config() -> dict:
    path = Path(__file__).resolve().parents[1] / "config" / "price_forecast_defaults.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _forecast(prices: list[float], horizon: int, config: dict) -> list[float]:
    anchor = float(prices[-1])
    changes = [
        (prices[index] - prices[index - 1]) / prices[index - 1]
        for index in range(1, len(prices))
        if prices[index - 1] > 0
    ]
    lookback = int(config.get("trend_lookback_days", 14))
    slope = float(np.median(changes[-lookback:])) if changes else 0.0
    slope = min(slope, float(config.get("max_upward_pct_per_day", 0.0)) / 100.0)
    daily_cap = float(config.get("max_daily_change_pct", 2.0)) / 100.0
    horizon_lo = anchor * (
        1.0 + float(config.get("min_horizon_change_pct", -8.0)) / 100.0
    )
    horizon_hi = anchor * (
        1.0 + float(config.get("max_horizon_change_pct", 0.0)) / 100.0
    )
    output = []
    previous = anchor
    for _ in range(horizon):
        value = previous * (1.0 + slope)
        value = float(np.clip(
            value,
            previous * (1.0 - daily_cap),
            previous * (1.0 + daily_cap),
        ))
        value = min(value, previous, anchor)
        value = float(np.clip(value, horizon_lo, horizon_hi))
        output.append(value)
        previous = value
    return output


def _metrics(errors: list[tuple[float, float]]) -> dict:
    if not errors:
        return {"n": 0}
    actual = np.array([row[0] for row in errors], dtype=float)
    predicted = np.array([row[1] for row in errors], dtype=float)
    absolute = np.abs(actual - predicted)
    denominator = np.maximum(np.abs(actual), 1.0)
    return {
        "n": int(len(actual)),
        "mae_vnd": float(absolute.mean()),
        "median_ae_vnd": float(np.median(absolute)),
        "mape_pct": float(np.mean(absolute / denominator) * 100.0),
        "within_10pct": float(np.mean(absolute / denominator <= 0.10)),
        "within_20pct": float(np.mean(absolute / denominator <= 0.20)),
    }


def backtest(
    history: pd.DataFrame,
    *,
    min_history: int = 14,
    horizons: tuple[int, ...] = (1, 7, 30),
    origin_step: int = 7,
) -> dict:
    cfg_forecast = load_forecast_config()
    results: dict[str, dict[int, list[tuple[float, float]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    eligible_products = 0

    for _, product_history in history.groupby("product_id"):
        series = product_history.sort_values("record_date").drop_duplicates(
            "record_date", keep="last"
        )
        by_date = {
            row.record_date.date(): float(row.avg_price)
            for row in series.itertuples()
        }
        dates = sorted(by_date)
        if len(dates) < min_history + min(horizons):
            continue
        eligible_products += 1

        for origin_index in range(min_history - 1, len(dates), origin_step):
            origin_date = dates[origin_index]
            train_dates = dates[: origin_index + 1]
            anchor = by_date[origin_date]
            forecasts = _forecast(
                [by_date[day] for day in train_dates],
                max(horizons),
                cfg_forecast,
            )
            moving_median = float(np.median(
                [by_date[day] for day in train_dates[-7:]]
            ))
            history_rows = [
                {"record_date": day, "avg_price": by_date[day]}
                for day in train_dates
            ]
            damped_forecasts, _ = damped_rolling_median_trend(
                history_rows,
                horizon_days=max(horizons),
                anchor_date=origin_date,
                window_days=int(cfg_forecast.get("rolling_median_window_days", 7)),
                slope_lookback_points=int(
                    cfg_forecast.get("trend_slope_lookback_points", 14)
                ),
                damping_days=float(cfg_forecast.get("trend_damping_days", 14)),
                max_daily_change_pct=float(
                    cfg_forecast.get("trend_max_daily_change_pct", 0.5)
                ),
                max_horizon_change_pct=float(
                    cfg_forecast.get("trend_max_horizon_change_pct", 8.0)
                ),
            )
            convergence_forecasts = {}
            for convergence_days in (1.0, 3.0, 7.0):
                convergence_forecasts[convergence_days], _ = (
                    converging_rolling_median(
                        history_rows,
                        horizon_days=max(horizons),
                        anchor_date=origin_date,
                        window_days=int(
                            cfg_forecast.get("rolling_median_window_days", 7)
                        ),
                        convergence_days=convergence_days,
                    )
                )

            for horizon in horizons:
                target_date = origin_date + pd.Timedelta(days=horizon)
                target_date = target_date.date() if hasattr(target_date, "date") else target_date
                if target_date not in by_date:
                    continue
                actual = by_date[target_date]
                results["history_trend"][horizon].append(
                    (actual, float(forecasts[horizon - 1]))
                )
                results["last_value"][horizon].append((actual, anchor))
                results["moving_median_7"][horizon].append((actual, moving_median))
                results["damped_median_trend"][horizon].append(
                    (
                        actual,
                        float(damped_forecasts[horizon - 1]["predicted_price_vnd"]),
                    )
                )
                for convergence_days, convergence in convergence_forecasts.items():
                    results[
                        f"converging_median_{int(convergence_days)}d"
                    ][horizon].append(
                        (
                            actual,
                            float(convergence[horizon - 1]["predicted_price_vnd"]),
                        )
                    )

    metrics = {
        method: {
            str(horizon): _metrics(rows)
            for horizon, rows in horizon_rows.items()
        }
        for method, horizon_rows in results.items()
    }
    winners = {}
    for horizon in horizons:
        candidates = [
            (method, values.get(str(horizon), {}).get("mae_vnd"))
            for method, values in metrics.items()
        ]
        candidates = [(method, mae) for method, mae in candidates if mae is not None]
        winners[str(horizon)] = min(candidates, key=lambda item: item[1])[0] if candidates else None
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "method": "walk_forward",
        "min_history": min_history,
        "origin_step_days": origin_step,
        "eligible_products": eligible_products,
        "metrics": metrics,
        "winner_by_horizon": winners,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="models/forecast_backtest_report.json")
    parser.add_argument("--min-history", type=int, default=14)
    parser.add_argument("--origin-step", type=int, default=7)
    args = parser.parse_args()
    engine = create_engine(MYSQL_URI, pool_pre_ping=True)
    history = pd.read_sql(
        text(
            """
            SELECT product_id, record_date, avg_price, listing_count
            FROM price_history
            WHERE avg_price IS NOT NULL AND avg_price > 0
            ORDER BY product_id, record_date
            """
        ),
        engine,
    )
    history["record_date"] = pd.to_datetime(history["record_date"])
    report = backtest(
        history,
        min_history=args.min_history,
        origin_step=args.origin_step,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
