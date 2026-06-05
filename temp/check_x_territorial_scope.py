import polars as pl
from pathlib import Path

X_RAW_DIR = Path(r"E:\dsa-data\x___full\daily_dumps_chunked")
OUT = Path(__file__).parent / "x_territorial_scope_unique.csv"

print("Scanning X raw data...")

result = (
    pl.scan_parquet(str(X_RAW_DIR / "**" / "*.parquet"), glob=True)
    .select("territorial_scope")
    .group_by("territorial_scope")
    .agg(pl.len().alias("count"))
    .sort("count", descending=True)
    .collect(engine="streaming")
)

print(f"Found {len(result)} unique territorial_scope values.")

result.write_csv(str(OUT))
print(f"Saved to {OUT}")
