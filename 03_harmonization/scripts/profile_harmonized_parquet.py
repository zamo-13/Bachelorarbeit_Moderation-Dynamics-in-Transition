"""Profile harmonized parquet files using Polars lazy API.

Computes summary statistics on tiktok_de_2025_harmonized.parquet and
x_de_2025_harmonized.parquet without loading full datasets into memory.

Profiling includes:
- Total row counts
- Territorial-scope split (DE-only vs multi-territory)
- Monthly row counts (grouped by year-month)
- Automation-flag coverage (non-null counts/percentages, value distributions)
- API version split

Handles both list and string types for territorial_scope.
"""
from __future__ import annotations

from pathlib import Path
from datetime import datetime

import polars as pl


ROOT = Path(r"C:\BA_Data")
HARMONIZED_FILES = {
    "tiktok": ROOT / "tiktok_de_2025_harmonized.parquet",
    "x": ROOT / "x_de_2025_harmonized.parquet",
}
AUTOMATION_COLS = ["automated_detection", "automated_decision"]


def detect_territorial_scope_type(file_path: Path) -> str:
    """Detect whether territorial_scope is stored as list or string.
    
    Args:
        file_path: Path to the parquet file.
    
    Returns:
        'list' or 'string' indicating the dtype.
    """
    schema = pl.read_parquet_schema(str(file_path))
    ts_dtype = schema.get("territorial_scope")
    if ts_dtype is None:
        raise ValueError("territorial_scope column not found in schema")
    
    # Check if it's a List type or String type
    dtype_str = str(ts_dtype)
    if "List" in dtype_str:
        return "list"
    else:
        return "string"


def count_territorial_scope_split(lf: pl.LazyFrame, scope_type: str) -> tuple[int, int]:
    """Count rows where territorial_scope is exactly ["DE"] vs multi-territory.
    
    Args:
        lf: LazyFrame with territorial_scope column.
        scope_type: 'list' or 'string' indicating the dtype.
    
    Returns:
        Tuple of (de_only_count, multi_territory_count).
    """
    if scope_type == "list":
        # For list type: "DE" only means list has exactly one element "DE"
        de_only = lf.filter(
            (pl.col("territorial_scope").list.len() == 1)
            & (pl.col("territorial_scope").list.first() == "DE")
        ).select(pl.len().alias("count")).collect(engine="streaming")
        
        multi = lf.filter(
            ~((pl.col("territorial_scope").list.len() == 1)
              & (pl.col("territorial_scope").list.first() == "DE"))
        ).select(pl.len().alias("count")).collect(engine="streaming")
    else:
        # For string type: "DE" only means exact match "DE", multi means contains "DE" but not equal
        de_only = lf.filter(
            pl.col("territorial_scope") == "DE"
        ).select(pl.len().alias("count")).collect(engine="streaming")
        
        multi = lf.filter(
            (pl.col("territorial_scope").str.contains("DE"))
            & (pl.col("territorial_scope") != "DE")
        ).select(pl.len().alias("count")).collect(engine="streaming")
    
    de_only_count = int(de_only[0, "count"])
    multi_count = int(multi[0, "count"])
    
    return de_only_count, multi_count


def monthly_row_counts(lf: pl.LazyFrame) -> dict[str, int]:
    """Group by year-month of application_date and count rows.
    
    Args:
        lf: LazyFrame with application_date column.
    
    Returns:
        Dictionary mapping 'YYYY-MM' to row count.
    """
    monthly = (
        lf
        .select([
            pl.col("application_date")
            .dt.strftime("%Y-%m")
            .alias("year_month")
        ])
        .group_by("year_month")
        .agg(pl.len().alias("count"))
        .sort("year_month")
        .collect(engine="streaming")
    )
    
    result = {}
    for row in monthly.iter_rows(named=True):
        result[row["year_month"]] = row["count"]
    
    return result


def automation_coverage(lf: pl.LazyFrame, columns: list[str]) -> dict[str, dict]:
    """Compute non-null coverage and value distributions for automation columns.
    
    Args:
        lf: LazyFrame.
        columns: List of automation column names to profile.
    
    Returns:
        Dictionary mapping column name to {
            'non_null_count': int,
            'non_null_pct': float,
            'value_distribution': dict of value -> count
        }
    """
    result = {}
    
    # Get total row count
    total = int(
        lf.select(pl.len().alias("n")).collect(engine="streaming")[0, "n"]
    )
    
    for col in columns:
        # Non-null count
        non_null = int(
            lf.select(pl.col(col).is_not_null().sum().alias("n")).collect(engine="streaming")[0, "n"]
        )
        non_null_pct = (non_null / total * 100) if total > 0 else 0.0
        
        # Value distribution (only for non-null values)
        value_dist = (
            lf
            .filter(pl.col(col).is_not_null())
            .select(col)
            .group_by(col)
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .collect(engine="streaming")
        )
        
        value_dict = {}
        for row in value_dist.iter_rows(named=True):
            value_dict[row[col]] = row["count"]
        
        result[col] = {
            "non_null_count": non_null,
            "non_null_pct": round(non_null_pct, 2),
            "value_distribution": value_dict,
        }
    
    return result


def api_version_split(lf: pl.LazyFrame) -> dict[str, int]:
    """Count rows per api_version value.
    
    Args:
        lf: LazyFrame with api_version column.
    
    Returns:
        Dictionary mapping api_version to row count.
    """
    version_counts = (
        lf
        .group_by("api_version")
        .agg(pl.len().alias("count"))
        .sort("api_version")
        .collect(engine="streaming")
    )
    
    result = {}
    for row in version_counts.iter_rows(named=True):
        result[row["api_version"]] = row["count"]
    
    return result


def profile_platform(platform: str, file_path: Path) -> None:
    """Profile a single harmonized parquet file.
    
    Args:
        platform: Platform name (e.g., 'tiktok' or 'x').
        file_path: Path to the harmonized parquet file.
    """
    print(f"\n{'='*70}")
    print(f"PROFILING: {platform.upper()}")
    print(f"File: {file_path}")
    print(f"{'='*70}")
    
    if not file_path.exists():
        print(f"ERROR: File not found: {file_path}")
        return
    
    # Detect territorial_scope type
    scope_type = detect_territorial_scope_type(file_path)
    print(f"\n[Type Detection] territorial_scope dtype: {scope_type}")
    
    # Load LazyFrame with selected columns only
    lf = pl.scan_parquet(str(file_path)).select([
        "territorial_scope",
        "application_date",
        "automated_detection",
        "automated_decision",
        "api_version",
    ])
    
    # Total row count
    total_count = int(
        lf.select(pl.len().alias("n")).collect(engine="streaming")[0, "n"]
    )
    print(f"\n[Row Counts] Total rows: {total_count:,}")
    
    # Territorial scope split
    de_only, multi = count_territorial_scope_split(lf, scope_type)
    de_only_pct = (de_only / total_count * 100) if total_count > 0 else 0.0
    multi_pct = (multi / total_count * 100) if total_count > 0 else 0.0
    
    print(f"\n[Territorial Scope Split]")
    print(f"  DE-only (exactly ['DE'] or 'DE'): {de_only:,} ({de_only_pct:.2f}%)")
    print(f"  Multi-territory (contains 'DE' + others): {multi:,} ({multi_pct:.2f}%)")
    
    # Monthly row counts
    monthly_counts = monthly_row_counts(lf)
    print(f"\n[Monthly Row Counts] ({len(monthly_counts)} months)")
    for month, count in monthly_counts.items():
        print(f"  {month}: {count:,}")
    
    # Automation coverage
    automation = automation_coverage(lf, AUTOMATION_COLS)
    print(f"\n[Automation Column Coverage]")
    for col, stats in automation.items():
        print(f"  {col}:")
        print(f"    Non-null count: {stats['non_null_count']:,} ({stats['non_null_pct']:.2f}%)")
        print(f"    Value distribution:")
        for value, count in stats['value_distribution'].items():
            pct = (count / stats['non_null_count'] * 100) if stats['non_null_count'] > 0 else 0.0
            print(f"      {value}: {count:,} ({pct:.2f}%)")
    
    # API version split
    version_counts = api_version_split(lf)
    print(f"\n[API Version Split]")
    for version, count in version_counts.items():
        pct = (count / total_count * 100) if total_count > 0 else 0.0
        print(f"  {version}: {count:,} ({pct:.2f}%)")


def main() -> None:
    """Profile all harmonized files."""
    print("\n" + "="*70)
    print("HARMONIZED DATA PROFILING (Polars Lazy Execution)")
    print(f"Generated: {datetime.now().isoformat()}")
    print("="*70)
    
    for platform, file_path in HARMONIZED_FILES.items():
        try:
            profile_platform(platform, file_path)
        except Exception as e:
            print(f"\nERROR profiling {platform}: {e}")
    
    print(f"\n{'='*70}")
    print("Profiling complete.")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
