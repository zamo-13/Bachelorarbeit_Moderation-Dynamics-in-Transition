"""
Count rows per month for TikTok and X harmonized parquet files.

Uses scan_parquet (lazy) + streaming collect — the full 21 GB TikTok file is
never loaded into RAM. Only the aggregated result (12 rows per platform) is
held in memory.

Output
------
- Printed table to terminal
- CSV saved to 02_data_cleaning/data_intermediate/monthly_row_counts.csv
"""
from __future__ import annotations

from pathlib import Path

import polars as pl

DATA = Path(r"C:\BA_Data")

INPUTS = {
    "tiktok": DATA / "tiktok_de_2025_harmonized.parquet",
    "x":      DATA / "x_de_2025_harmonized.parquet",
}

OUT_CSV = (
    Path(__file__).parent.parent          # 02_data_cleaning/
    / "data_intermediate"
    / "monthly_row_counts.csv"
)


def count_by_month(path: Path, platform: str) -> pl.DataFrame:
    """Stream through the parquet file and return a month → row_count table."""
    print(f"  Scanning {path.name} ...")
    return (
        pl.scan_parquet(str(path))
        # Keep only the one column we need — nothing else is read from disk
        .select("application_date")
        .with_columns(
            pl.lit(platform).alias("platform"),
            pl.col("application_date").dt.truncate("1mo").alias("month"),
        )
        .group_by(["platform", "month"])
        .agg(pl.len().alias("row_count"))
        .sort("month")
        # streaming=True tells Polars to process in chunks, never the full file
        .collect(engine="streaming")
    )


def main() -> None:
    results: list[pl.DataFrame] = []

    for platform, path in INPUTS.items():
        if not path.exists():
            print(f"WARNING: {path} not found — skipping")
            continue
        print(f"\n[{platform.upper()}]")
        df = count_by_month(path, platform)
        print(df)
        results.append(df)

    if not results:
        print("No input files found. Check C:\\BA_Data\\.")
        return

    combined = (
        pl.concat(results)
        .sort(["platform", "month"])
    )

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    combined.write_csv(str(OUT_CSV))

    print(f"\n=== COMBINED (saved to {OUT_CSV.name}) ===")
    print(combined)


if __name__ == "__main__":
    main()
