"""Chuẩn hóa link, bỏ dòng link rỗng/trùng trước khi fetch detail — gọi từ các scraper listing."""

import pandas as pd


def _cell_link_str(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() == "nan":
        return None
    return s


def _is_listing_hub_url(url: str) -> bool:
    """Hub không phải trang item (hay trùng hàng loạt), ví dụ Amazon trên Buyee."""
    u = url.lower()
    if "buyee.jp/amazon" in u and "/item/" not in u:
        return True
    return False


def prepare_listing_dataframe(df: pd.DataFrame, source_label: str) -> pd.DataFrame:
    if df is None or df.empty or "link" not in df.columns:
        return df

    n0 = len(df)
    df = df.copy()
    df["link"] = df["link"].map(_cell_link_str)
    df = df.loc[df["link"].notna()]
    hub_mask = df["link"].map(_is_listing_hub_url)
    n_hub = int(hub_mask.sum())
    if n_hub:
        df = df.loc[~hub_mask]
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["link"], keep="first").reset_index(drop=True)
    n_dup = before_dedup - len(df)
    n_invalid = n0 - before_dedup - n_hub
    if n_invalid or n_hub or n_dup:
        print(
            f"   [{source_label}] listing cleanup: -{n_invalid} empty/null link, "
            f"-{n_hub} hub URL, -{n_dup} duplicate link → {len(df)} rows for detail fetch"
        )
    return df
