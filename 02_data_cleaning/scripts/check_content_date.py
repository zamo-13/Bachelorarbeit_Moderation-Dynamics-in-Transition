"""
check_content_date.py  (modified)
===================================
Verifies the content_date == application_date claim across the raw DSA zip
archives for TikTok and X (Germany, 2025 scope).

Reads CSVs directly from the nested zip structure (outer zip -> inner csv.zip
-> csv).  No extraction to disk, no parquet files needed.

TikTok : randomly samples 50 % of all daily zips (reproducible via SEED).
X       : scans all zips (small dataset, completes quickly).

Memory  : one inner CSV chunk lives in RAM at a time (~30-50 MB peak).

Usage
-----
    python check_content_date.py

Requirements
------------
    pip install polars
"""
from __future__ import annotations

import io
import random
import zipfile
from pathlib import Path

import polars as pl

SEED = 42   # for reproducibility

# ── Paths ──────────────────────────────────────────────────────────────────
TIKTOK_ROOT = Path(r"C:\BA_Data\tiktok___full")
X_ROOT      = Path(r"C:\BA_Data\x___full")

DE_FILTER  = "DE"
DATE_START = pl.date(2025, 1, 1)
DATE_END   = pl.date(2025, 12, 31)

COLS_NEEDED = ["content_date", "application_date", "territorial_scope"]


# ── Zip selection ──────────────────────────────────────────────────────────

def select_zips(root: Path, random_half: bool) -> list[Path]:
    all_zips = sorted(root.glob("*.zip"))
    if not random_half:
        return all_zips
    rng = random.Random(SEED)
    n = max(1, len(all_zips) // 2)
    return sorted(rng.sample(all_zips, n))


# ── Per-CSV counting ───────────────────────────────────────────────────────

def _counts_from_csv(csv_bytes: bytes) -> tuple[int, int, int, int]:
    """
    Returns (total, null, equal, different) for one CSV chunk,
    filtered to DE territorial scope and 2025 application_date.
    """
    df = pl.read_csv(
        io.BytesIO(csv_bytes),
        columns=COLS_NEEDED,
        try_parse_dates=True,
        infer_schema_length=500,
    )

    df = df.filter(pl.col("territorial_scope").str.contains(DE_FILTER))
    if df.is_empty():
        return 0, 0, 0, 0

    # Cast to Date in case try_parse_dates left them as strings or Datetime
    df = df.with_columns([
        pl.col("application_date").cast(pl.Date, strict=False),
        pl.col("content_date").cast(pl.Date, strict=False),
    ])

    df = df.filter(pl.col("application_date").is_between(DATE_START, DATE_END))
    if df.is_empty():
        return 0, 0, 0, 0

    tagged = df.with_columns(
        pl.when(pl.col("content_date").is_null())
          .then(pl.lit("null"))
          .when(pl.col("content_date") == pl.col("application_date"))
          .then(pl.lit("equal"))
          .otherwise(pl.lit("different"))
          .alias("status")
    )
    counts = tagged.group_by("status").agg(pl.len().alias("n"))
    rows = {r["status"]: r["n"] for r in counts.iter_rows(named=True)}

    total = len(df)
    return total, rows.get("null", 0), rows.get("equal", 0), rows.get("different", 0)


# ── Per-outer-zip processing ───────────────────────────────────────────────

def process_outer_zip(zip_path: Path) -> tuple[int, int, int, int]:
    total = null_c = equal_c = diff_c = 0
    with zipfile.ZipFile(zip_path) as outer:
        for inner_name in outer.namelist():
            if not inner_name.endswith(".csv.zip"):
                continue
            inner_bytes = outer.read(inner_name)
            with zipfile.ZipFile(io.BytesIO(inner_bytes)) as inner:
                for csv_name in inner.namelist():
                    if not csv_name.endswith(".csv"):
                        continue
                    t, n, e, d = _counts_from_csv(inner.read(csv_name))
                    total += t
                    null_c += n
                    equal_c += e
                    diff_c += d
    return total, null_c, equal_c, diff_c


# ── Platform-level check ───────────────────────────────────────────────────

def check_platform(root: Path, label: str, random_half: bool) -> dict:
    zips = select_zips(root, random_half=random_half)
    note = "(random 50% sample)" if random_half else "(all zips)"
    print(f"\n[{label}] {len(zips)} zip(s) to process {note}")

    total = null_c = equal_c = diff_c = 0
    for i, z in enumerate(zips, 1):
        print(f"  [{i:>3}/{len(zips)}] {z.name} ...", end=" ", flush=True)
        t, n, e, d = process_outer_zip(z)
        total += t
        null_c += n
        equal_c += e
        diff_c += d
        print(f"{t:>10,} rows (DE+2025)")

    return {
        "label":     label,
        "note":      note,
        "total":     total,
        "null":      null_c,
        "null_pct":  null_c  / total * 100 if total else 0,
        "equal":     equal_c,
        "equal_pct": equal_c / total * 100 if total else 0,
        "different": diff_c,
        "diff_pct":  diff_c  / total * 100 if total else 0,
    }


# ── Formatting ─────────────────────────────────────────────────────────────

def fmt(r: dict) -> str:
    lines = [
        f"Platform : {r['label']}  {r['note']}",
        f"Total rows (DE, 2025): {r['total']:>15,}",
        "",
        f"  content_date is null            : {r['null']:>15,}  ({r['null_pct']:>6.2f}%)",
        f"  content_date == application_date: {r['equal']:>15,}  ({r['equal_pct']:>6.2f}%)",
        f"  content_date != application_date: {r['different']:>15,}  ({r['diff_pct']:>6.2f}%)",
        "",
        f"  Rows where content_date is usable (not null, not = application_date):",
        f"  {r['different']:,} rows = {r['diff_pct']:.2f}% of total",
    ]
    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    results = [
        check_platform(TIKTOK_ROOT, "TikTok", random_half=True),
        check_platform(X_ROOT,      "X",      random_half=False),
    ]

    output_lines = [
        "=" * 65,
        "content_date vs application_date — raw zip check",
        "Scope: territorial_scope contains DE, application_date 2025",
        "=" * 65,
    ]
    for r in results:
        output_lines.append("")
        output_lines.append(fmt(r))
        output_lines.append("-" * 65)

    output = "\n".join(output_lines)
    print("\n" + output)

    out_path = Path(__file__).parent / "check_content_date_results.txt"
    out_path.write_text(output, encoding="utf-8")
    print(f"\nResults written to: {out_path}")
