"""Screen the pre-harmonized working parquet files with lazy Polars.

This script profiles the current 2025 working files for TikTok and X and
writes a markdown report under 02_data_cleaning/logs/.

The screening report includes:
- total row and column counts,
- column names and dtypes,
- missingness and uniqueness-based quality indicators,
- variance for numeric columns only.

Uniqueness is reported as an approximate count so the script remains practical
on the very large TikTok parquet file.

The script uses Polars lazy scans so it does not materialize the full dataset
into memory.
"""
from __future__ import annotations

import argparse
from datetime import datetime
from pathlib import Path

import polars as pl


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_ROOT = Path(r"C:\BA_Data")
DEFAULT_OUTPUT_DIR = ROOT / "02_data_cleaning" / "logs"
DEFAULT_REPORT_NAME = "pre_harmonized_screening_report.md"

PLATFORM_FILES = {
    "tiktok": "tiktok_de_2025.parquet",
    "x": "x_de_2025.parquet",
}


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    """Return True when the dtype should receive a numeric variance calculation."""
    # String comparison instead of isinstance() keeps this compatible across Polars versions
    # where the internal type hierarchy changed between minor releases.
    dtype_name = str(dtype).lower()
    return dtype_name.startswith(("int", "uint", "float", "decimal"))


def format_value(value: object, precision: int = 4) -> str:
    """Format values for markdown output."""
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.{precision}f}"
    return str(value)


def date_time_stamp() -> str:
    """Return a stable date stamp for log entries."""
    return datetime.now().strftime("%Y-%m-%d")


def profile_platform(platform: str, file_path: Path) -> dict:
    """Collect screening metrics for one parquet file."""
    if not file_path.exists():
        raise FileNotFoundError(f"Input file not found: {file_path}")

    schema = pl.read_parquet_schema(str(file_path))
    columns = list(schema.keys())
    lazy_frame = pl.scan_parquet(str(file_path))

    total_cols = len(columns)

    # Build all metric expressions in one list so the file is scanned only once.
    # approx_n_unique uses HyperLogLog -- fast on 997 M rows but estimates, not exact counts.
    select_exprs: list[pl.Expr] = []
    for column in columns:
        select_exprs.append(pl.col(column).null_count().alias(f"{column}__null_count"))
        select_exprs.append(pl.col(column).approx_n_unique().alias(f"{column}__unique_count"))
        if is_numeric_dtype(schema[column]):
            # strict=False silently casts bad values to null rather than raising an error.
            select_exprs.append(
                pl.col(column)
                .cast(pl.Float64, strict=False)
                .var()
                .alias(f"{column}__variance")
            )

    # streaming engine avoids materializing the full dataset into RAM.
    summary_df = lazy_frame.select([pl.len().alias("total_rows"), *select_exprs]).collect(engine="streaming")
    summary_values = summary_df.to_dicts()[0]
    total_rows = int(summary_values["total_rows"])

    column_rows: list[dict[str, object]] = []
    for column in columns:
        dtype = schema[column]
        null_count = int(summary_values[f"{column}__null_count"])
        unique_count = int(summary_values[f"{column}__unique_count"])
        variance_key = f"{column}__variance"
        variance = summary_values.get(variance_key)
        non_null_count = total_rows - null_count
        null_pct = (null_count / total_rows * 100) if total_rows else 0.0
        unique_pct = (unique_count / total_rows * 100) if total_rows else 0.0

        column_rows.append(
            {
                "column": column,
                "dtype": str(dtype),
                "null_count": null_count,
                "null_pct": null_pct,
                "non_null_count": non_null_count,
                "unique_count": unique_count,
                "unique_pct": unique_pct,
                "variance": variance,
            }
        )

    return {
        "platform": platform,
        "file_path": str(file_path),
        "total_rows": total_rows,
        "total_cols": total_cols,
        "columns": columns,
        "column_rows": column_rows,
    }


def render_table(rows: list[dict[str, object]]) -> str:
    """Render a simple markdown table from row dictionaries."""
    headers = ["column", "dtype", "null_count", "null_pct", "non_null_count", "unique_count", "unique_pct", "variance"]
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]

    for row in rows:
        variance = row["variance"]
        variance_text = "n/a" if variance is None else format_value(variance, precision=6)
        lines.append(
            "| "
            + " | ".join(
                [
                    str(row["column"]),
                    str(row["dtype"]),
                    str(row["null_count"]),
                    f"{float(row['null_pct']):.2f}%",
                    str(row["non_null_count"]),
                    str(row["unique_count"]),
                    f"{float(row['unique_pct']):.2f}%",
                    variance_text,
                ]
            )
            + " |"
        )

    return "\n".join(lines)


def append_data_profile(report: dict, log_path: Path) -> None:
    """Append a concise quantitative note to data_profile.md."""
    # Skip silently if the log file was not created yet; do not create it here.
    if not log_path.exists():
        return

    lines = [
        "",
        f"- Date: {date_time_stamp()}",
        f"- Description: Pre-harmonized screening for {report['platform'].upper()} ({Path(report['file_path']).name})",
        "- Result:",
        f"  - Rows screened: {report['total_rows']:,}",
        f"  - Columns screened: {report['total_cols']}",
        f"  - Report written: {report['report_path']}",
    ]

    with log_path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def write_report(reports: list[dict], output_path: Path) -> None:
    """Write the combined markdown report."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sections = [
        "# Pre-Harmonized Data Screening Report",
        "",
        f"- Generated: {timestamp}",
        f"- Data root: {DEFAULT_DATA_ROOT}",
        "- Input files:",
    ]

    for report in reports:
        sections.append(f"  - {report['platform']}: {report['file_path']}")

    sections.extend(
        [
            "",
            "## Screening Scope",
            "- Row and column counts",
            "- Column names and data types",
            "- Missingness and uniqueness as quality indicators",
            "- Uniqueness counts are approximate to keep the scan practical on large files",
            "- Variance for numeric columns only",
        ]
    )

    for report in reports:
        sections.extend(
            [
                "",
                f"## {report['platform'].upper()}",
                f"- Rows: {report['total_rows']:,}",
                f"- Columns: {report['total_cols']}",
                f"- File: {report['file_path']}",
                "",
                render_table(report["column_rows"]),
            ]
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(sections) + "\n", encoding="utf-8")


def main() -> None:
    """Run the screening pipeline and write the markdown report."""
    parser = argparse.ArgumentParser(description="Screen pre-harmonized TikTok and X parquet files.")
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DEFAULT_DATA_ROOT,
        help="Root directory containing tiktok_de_2025.parquet and x_de_2025.parquet.",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_OUTPUT_DIR / DEFAULT_REPORT_NAME,
        help="Markdown report path under 02_data_cleaning/logs/.",
    )
    args = parser.parse_args()

    reports: list[dict] = []
    for platform, file_name in PLATFORM_FILES.items():
        file_path = args.data_root / file_name
        report = profile_platform(platform, file_path)
        reports.append(report)

    write_report(reports, args.output_path)

    data_profile_log = ROOT / "02_data_cleaning" / "logs" / "data_profile.md"
    for report in reports:
        report["report_path"] = str(args.output_path)
        append_data_profile(report, data_profile_log)

    print(f"Screening report written to: {args.output_path}")
    for report in reports:
        print(
            f"- {report['platform'].upper()}: {report['total_rows']:,} rows, {report['total_cols']} columns"
        )


if __name__ == "__main__":
    main()