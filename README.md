# Bachelor-Arbeit — Data Pipeline Reproduction Guide

This document explains how to reproduce the full data pipeline from raw DSA dump files to the analysis-ready parquet files used in the thesis.

---

## Prerequisites

### Python environment

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

`requirements.txt` installs `polars` and `pandas`.

### Data location

All data lives at `C:\BA_Data\` (outside the repository — OneDrive caused VS Code crashes under heavy I/O on the ~120 GB dataset). The repository contains only scripts, notebooks, logs, and metadata.

---

## Raw Data

The raw data was downloaded from the [DSA Transparency Database](https://transparency.dsa.ec.europa.eu/) using the `dsa-tdb` extraction tool (tool files removed from this repo after extraction; the tool is available at its original source).

The downloads are stored as nested zips:

```text
C:\BA_Data\
  tiktok___full\
    sor-tiktok-2025-01-01-full.zip   <- outer zip
      sor-tiktok-2025-01-01-full-00000.csv.zip  <- inner zip
        sor-tiktok-2025-01-01-full-00000.csv    <- raw CSV (37 columns)
    ...
  x___full\
    sor-x-2025-01-01-full.zip
    ...
```

These zip files are the **only copy of the original unmodified data**. Do not delete them.

The `dsa-tdb` tool also extracted each zip into `daily_dumps_chunked/` as parquet files (one sub-folder per day). Those intermediate parquets were modified and then deleted during the pipeline — only the zips survive.

---

## Pipeline Steps

Run steps in order. Each step reads from the previous step's output.

### Step 1 — Column reduction (in-place)

Reduces the extracted daily parquet files from 37 columns to the 9 thesis-relevant columns, overwriting each file in place to save disk space.

```bash
python 02_data_cleaning\scripts\batch_select_columns.py \
  --input-root C:\BA_Data \
  --output-root 02_data_cleaning\data_intermediate \
  --platform tiktok___full \
  --in-place-replace

python 02_data_cleaning\scripts\batch_select_columns.py \
  --input-root C:\BA_Data \
  --output-root 02_data_cleaning\data_intermediate \
  --platform x___full \
  --in-place-replace
```

Columns retained: `uuid`, `category`, `content_type`, `automated_detection`, `automated_decision`, `territorial_scope`, `application_date`, `platform_name`, `decision_visibility`.

Processing summaries are written to `02_data_cleaning/data_intermediate/`.

> **Warning:** this step modifies the parquet files in `daily_dumps_chunked/` permanently. The original full-schema data remains only in the zip archives.

---

### Step 2 — DE filter, platform split, api_version tagging

Reads all chunked parquets, filters to records where `territorial_scope` contains `"DE"`, assigns `api_version` (`"v1"` before 2025-07-01, `"v2"` from 2025-07-01 onward), writes one combined parquet per platform, then deletes the chunked parquets.

```bash
python 02_data_cleaning\scripts\filter_split_save_de_polars.py
```

Outputs written to `C:\BA_Data\`:

| File | Rows |
| --- | --- |
| `tiktok_de_2025.parquet` | 999,079,277 |
| `x_de_2025.parquet` | 183,324 |

A run log is appended to `02_data_cleaning/logs/data_profile.md`.

> **Warning:** this step deletes all `daily_dumps_chunked/*.parquet` files after writing the outputs.

---

### Step 3 — Date scope filter (in-place)

Filters both parquets to `application_date` between 2025-01-01 and 2025-12-31, overwriting the files in place.

```bash
python 02_data_cleaning\scripts\apply_date_filter.py
```

The script checks that both raw dump folders still exist as a safety guard before touching anything.

---

### Step 4 — Platform name normalisation (in-place)

Renames `"X (formerly Twitter)"` to `"X"` in the `platform_name` column of the X parquets.

```bash
python C:\BA_Data\fix_platform_name.py
```

Modifies in place: `x_de_2025.parquet` and `x_de_2025_harmonized.parquet` (if it already exists).

---

### Step 5 — Category harmonisation

Maps raw DSA category codes to thesis Super-Clusters using `Archive/taxonomy_mapping_v1_v2.csv`. Handles the v1→v2 schema break (2025-07-01). Adds columns `category_raw`, `category_harmonized`, `application_date_raw`, `application_date_harmonized`.

Run the harmonisation notebook:

```text
Archive/03_harmonization.ipynb
```

Outputs written to `C:\BA_Data\`:

| File | Description |
| --- | --- |
| `tiktok_de_2025_harmonized.parquet` | TikTok with Super-Cluster labels |
| `x_de_2025_harmonized.parquet` | X with Super-Cluster labels |

---

## Final Data State

After all steps, `C:\BA_Data\` contains:

```text
C:\BA_Data\
  tiktok___full\                       <- raw zip archives (untouched)
  x___full\                            <- raw zip archives (untouched)
  tiktok_de_2025.parquet               <- post steps 1-4
  x_de_2025.parquet                    <- post steps 1-4
  tiktok_de_2025_harmonized.parquet    <- post step 5
  x_de_2025_harmonized.parquet         <- post step 5
```

---

## Analysis Entry Point

All analysis scripts in `04_analysis/` import from `04_analysis/data_loader.py`:

```python
from data_loader import load_v1_dataset

lf = load_v1_dataset()   # Polars LazyFrame, no RAM materialised yet
df = lf.collect(engine="streaming")
```

`load_v1_dataset()` returns a combined TikTok + X frame with two filters pre-applied:

- `api_version == "v1"` only (pre-July-2025 schema)
- Boundary-bleed categories removed (`OTHER_VIOLATION_TC`, `CYBER_VIOLENCE`, `UNSAFE_AND_PROHIBITED_PRODUCTS`)

---

## Utility / Diagnostic Scripts

These scripts are not part of the main pipeline but were used for validation:

| Script | Purpose |
| --- | --- |
| `02_data_cleaning/scripts/screen_pre_harmonized_data.py` | Profiles the DE parquets (row counts, nulls, dtypes) |
| `02_data_cleaning/scripts/count_monthly_rows.py` | Monthly row counts per platform |
| `02_data_cleaning/scripts/extract_unique_categories_by_api.py` | Lists all distinct category codes by api_version |
| `02_data_cleaning/scripts/check_content_date.py` | Checks whether `content_date == application_date` in the raw zips |
| `02_data_cleaning/scripts/validate_thesis_session.py` | Validates pipeline state against expected counts |

---

## Key Counts (verified)

| Metric | TikTok | X |
| --- | --- | --- |
| Pre-filter rows (all EEA) | 1,002,729,268 | 670,093 |
| Post-filter rows (DE only) | 999,079,277 | 183,324 |
| DE retention rate | 99.6% | 27.4% |
| Scale ratio TikTok : X | ~4,500 : 1 | |
