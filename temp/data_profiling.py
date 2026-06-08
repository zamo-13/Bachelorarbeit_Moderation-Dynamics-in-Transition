"""
Data profiling + column quality check.

TikTok (51 GB): sample 20 files spread across date range — all checks on sample.
X      (~40 MB): full scan — all checks on full data.
"""

import sys
sys.stdout.reconfigure(encoding="utf-8")

import polars as pl
from pathlib import Path
import time

# ── Config ─────────────────────────────────────────────────────────────────────
OUT_DIR = Path(r"c:\Users\MoZa\OneDrive - Universität Paderborn\0_UPB\BA\Repo\Bachelor-Arbeit\temp\profiling_output")
OUT_DIR.mkdir(parents=True, exist_ok=True)

TIKTOK_PATH = Path(r"E:\dsa-data\tiktok___full\daily_dumps_chunked")
X_PATH      = Path(r"E:\dsa-data\x___full\daily_dumps_chunked")

TOP_N          = 10
TIKTOK_SAMPLE  = 20   # files sampled for TikTok


# ── File helpers ───────────────────────────────────────────────────────────────

def all_parquet_files(path: Path) -> list[Path]:
    return sorted(path.rglob("*.parquet"))


def evenly_spaced_sample(files: list[Path], n: int) -> list[Path]:
    if len(files) <= n:
        return files
    step = len(files) / n
    return [files[int(i * step)] for i in range(n)]


# ── Core profiling (runs on a loaded DataFrame) ────────────────────────────────

def profile_df(df: pl.DataFrame, name: str, source_note: str) -> dict[str, pl.DataFrame]:
    print(f"  [{name}] Profiling {len(df):,} rows × {len(df.columns)} cols  ({source_note})")
    results = {}

    # — Null + empty string rates —
    null_rows = []
    for col in df.columns:
        nulls = df[col].is_null().sum()
        empty = 0
        if df[col].dtype in (pl.Utf8, pl.String):
            empty = df[col].drop_nulls().str.strip_chars().eq("").sum()
        n = len(df)
        null_rows.append({
            "platform": name, "column": col, "source": source_note,
            "total_rows": n,
            "null_count": nulls,
            "null_pct": round(100 * nulls / n, 2) if n else 0.0,
            "empty_str_count": empty,
            "empty_str_pct": round(100 * empty / n, 2) if n else 0.0,
            "total_missing_pct": round(100 * (nulls + empty) / n, 2) if n else 0.0,
        })
    results["null_empty"] = pl.DataFrame(null_rows)

    # — Cardinality —
    card_rows = []
    for col in df.columns:
        card_rows.append({
            "platform": name, "column": col, "source": source_note,
            "n_unique": df[col].n_unique(),
        })
    results["cardinality"] = pl.DataFrame(card_rows)

    # — Top-N values —
    topn_rows = []
    for col in df.columns:
        top = (
            df.select(pl.col(col).cast(pl.String))
              .group_by(col)
              .agg(pl.len().alias("count"))
              .sort("count", descending=True)
              .head(TOP_N)
        )
        total = top["count"].sum()
        for rank, row in enumerate(top.iter_rows(named=True), start=1):
            topn_rows.append({
                "platform": name, "column": col, "source": source_note,
                "rank": rank, "value": str(row[col]),
                "count": row["count"],
                "pct": round(100 * row["count"] / total, 2) if total else 0.0,
            })
    results["topn"] = pl.DataFrame(topn_rows)

    # — String stats —
    str_rows = []
    for col in df.columns:
        if df[col].dtype not in (pl.Utf8, pl.String):
            continue
        s = df[col].drop_nulls()
        lengths = s.str.len_chars()
        ws = s.filter(s.str.starts_with(" ") | s.str.ends_with(" ")).len()
        str_rows.append({
            "platform": name, "column": col, "source": source_note,
            "len_min": int(lengths.min() or 0),
            "len_max": int(lengths.max() or 0),
            "len_mean": round(float(lengths.mean() or 0), 2),
            "whitespace_anomaly_count": ws,
        })
    if str_rows:
        results["string_stats"] = pl.DataFrame(str_rows)

    # — Numeric stats —
    num_rows = []
    for col in df.columns:
        if df[col].dtype not in (pl.Int8, pl.Int16, pl.Int32, pl.Int64,
                                  pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64,
                                  pl.Float32, pl.Float64):
            continue
        s = df[col].drop_nulls()
        num_rows.append({
            "platform": name, "column": col, "source": source_note,
            "min": s.min(), "max": s.max(),
            "mean": round(float(s.mean() or 0), 4),
            "std": round(float(s.std() or 0), 4),
            "median": float(s.median() or 0),
            "p05": float(s.quantile(0.05) or 0),
            "p95": float(s.quantile(0.95) or 0),
            "negative_count": int((s < 0).sum()),
            "zero_count": int((s == 0).sum()),
        })
    if num_rows:
        results["numeric_stats"] = pl.DataFrame(num_rows)

    return results


# ── File inventory ─────────────────────────────────────────────────────────────

def file_inventory(files: list[Path], name: str) -> pl.DataFrame:
    sizes = [f.stat().st_size / (1024 ** 2) for f in files]
    return pl.DataFrame([{
        "platform":      name,
        "total_files":   len(files),
        "total_size_gb": round(sum(sizes) / 1024, 2),
        "min_file_mb":   round(min(sizes), 2),
        "max_file_mb":   round(max(sizes), 2),
        "avg_file_mb":   round(sum(sizes) / len(sizes), 2),
    }])


# ── Schema ─────────────────────────────────────────────────────────────────────

def schema_report(files: list[Path], name: str) -> pl.DataFrame:
    schema = pl.scan_parquet(str(files[0])).collect_schema()
    return pl.DataFrame([
        {"platform": name, "column": col, "dtype": str(dtype)}
        for col, dtype in schema.items()
    ])


# ── Platform runners ───────────────────────────────────────────────────────────

def run_tiktok():
    name = "tiktok"
    print(f"\n{'='*60}\n  Profiling: TIKTOK (sample {TIKTOK_SAMPLE} files)\n{'='*60}")

    all_files = all_parquet_files(TIKTOK_PATH)
    sample = evenly_spaced_sample(all_files, TIKTOK_SAMPLE)
    sample_mb = sum(f.stat().st_size for f in sample) / (1024 ** 2)

    print(f"  Total files: {len(all_files)} ({sum(f.stat().st_size for f in all_files)/1024**3:.1f} GB)")
    print(f"  Sample files: {len(sample)} ({sample_mb:.0f} MB) — evenly spread across date range")
    print(f"  Sample files used:")
    for f in sample:
        print(f"    {f.parent.name}/{f.name}")

    # File inventory uses all files (just stat, no data read)
    file_inventory(all_files, name).write_csv(OUT_DIR / f"{name}_01_file_inventory.csv")
    schema_report(all_files, name).write_csv(OUT_DIR / f"{name}_02_schema.csv")
    pl.DataFrame([{"platform": name, "sample_files": len(sample),
                   "sample_mb": round(sample_mb, 1),
                   "total_files": len(all_files),
                   "note": f"profiling stats based on {TIKTOK_SAMPLE} evenly-spaced files"}]
    ).write_csv(OUT_DIR / f"{name}_03_sample_info.csv")

    t0 = time.time()
    print(f"\n  [{name}] Loading sample into memory...")
    df = pl.read_parquet([str(f) for f in sample])
    print(f"  [{name}] Loaded: {len(df):,} rows, {df.estimated_size('mb'):.0f} MB  in {time.time()-t0:.1f}s")

    results = profile_df(df, name, f"sample {len(sample)} files")
    _save_results(results, name)


def run_x():
    name = "x"
    print(f"\n{'='*60}\n  Profiling: X (full data)\n{'='*60}")

    all_files = all_parquet_files(X_PATH)
    total_mb = sum(f.stat().st_size for f in all_files) / (1024 ** 2)
    print(f"  Total files: {len(all_files)} ({total_mb:.1f} MB)")

    file_inventory(all_files, name).write_csv(OUT_DIR / f"{name}_01_file_inventory.csv")
    schema_report(all_files, name).write_csv(OUT_DIR / f"{name}_02_schema.csv")

    t0 = time.time()
    print(f"  [{name}] Loading full dataset...")
    df = pl.read_parquet([str(f) for f in all_files])
    print(f"  [{name}] Loaded: {len(df):,} rows, {df.estimated_size('mb'):.0f} MB  in {time.time()-t0:.1f}s")

    results = profile_df(df, name, "full dataset")
    _save_results(results, name)


def _save_results(results: dict, name: str):
    tag_map = {
        "null_empty":    "04_null_empty",
        "cardinality":   "05_cardinality",
        "topn":          "06_topn_values",
        "string_stats":  "07_string_stats",
        "numeric_stats": "08_numeric_stats",
    }
    for key, tag in tag_map.items():
        if key in results:
            results[key].write_csv(OUT_DIR / f"{name}_{tag}.csv")
    print(f"  [{name}] Reports saved to: {OUT_DIR}")


# ── Main ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    t0 = time.time()
    run_tiktok()
    run_x()
    print(f"\nTotal runtime: {(time.time()-t0)/60:.1f} min")
    print(f"All outputs in: {OUT_DIR}")
