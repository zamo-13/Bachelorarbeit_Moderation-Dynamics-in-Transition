"""Shared data loader for all analysis scripts in 04_analysis/.

Usage
-----
    from data_loader import load_v1_dataset

    lf = load_v1_dataset()          # returns a Polars LazyFrame
    df = lf.collect(engine="streaming")

Two filter steps are applied before any analysis sees the data:

1. api_version == "v1"
   The DSA Transparency Database changed its reporting schema on 2025-07-01
   (v1 -> v2). The thesis compares TikTok and X within a single schema version
   to avoid confounding category distributions with schema-driven artefacts.
   Only v1 records (pre-transition) are included in the analytical dataset.

2. Boundary-bleed category exclusion
   A small number of v1-timestamped rows carry category codes that were
   introduced in v2 (OTHER_VIOLATION_TC, UNSAFE_AND_PROHIBITED_PRODUCTS) or
   that appear in v1 at negligible volume as a known DSA reporting anomaly
   (CYBER_VIOLENCE). Retaining these rows would contaminate category
   distribution comparisons with categories that do not represent genuine v1
   enforcement practice. They are excluded rather than re-mapped to preserve
   analytical transparency.
"""
from __future__ import annotations

from pathlib import Path

import polars as pl


DEFAULT_DATA_ROOT = Path(r"C:\BA_Data")

_PLATFORM_FILES: dict[str, str] = {
    "TikTok": "tiktok_de_2025.parquet",
    "X": "x_de_2025.parquet",
}

_BOUNDARY_BLEED_CATEGORIES: list[str] = [
    "OTHER_VIOLATION_TC",
    "CYBER_VIOLENCE",
    "UNSAFE_AND_PROHIBITED_PRODUCTS",
]


def load_v1_dataset(data_root: Path = DEFAULT_DATA_ROOT) -> pl.LazyFrame:
    """Return a combined lazy frame containing v1 records for TikTok and X.

    Parameters
    ----------
    data_root:
        Directory that contains tiktok_de_2025.parquet and x_de_2025.parquet.
        Defaults to C:\\BA_Data\\.

    Returns
    -------
    pl.LazyFrame
        Concatenated lazy frame with an added ``platform_name`` column
        (values: ``"TikTok"`` or ``"X"``), filtered to api_version == "v1"
        and with boundary-bleed categories removed.
        Call ``.collect(engine="streaming")`` to materialise.
    """
    frames: list[pl.LazyFrame] = []
    for platform_name, file_name in _PLATFORM_FILES.items():
        file_path = data_root / file_name
        if not file_path.exists():
            raise FileNotFoundError(
                f"Expected data file not found: {file_path}\n"
                f"Ensure the data root is set correctly (current: {data_root})."
            )
        lf = (
            pl.scan_parquet(str(file_path))
            .with_columns(pl.lit(platform_name).alias("platform_name"))
        )
        frames.append(lf)

    combined = (
        pl.concat(frames)
        .filter(pl.col("api_version") == "v1")
        .filter(~pl.col("category").is_in(_BOUNDARY_BLEED_CATEGORIES))
    )
    return combined
