# Pre-Harmonized Data Screening Report

- Generated: 2026-06-03
- Data root: C:\BA_Data
- Input files:
  - tiktok: C:\BA_Data\tiktok_de_2025.parquet
  - x: C:\BA_Data\x_de_2025.parquet

## Screening Scope
- Row and column counts
- Column names and data types
- Missingness and uniqueness as quality indicators
- Uniqueness counts are approximate to keep the scan practical on large files

## TIKTOK
- Rows: 997,688,267
- Columns: 10
- File: C:\BA_Data\tiktok_de_2025.parquet

| column | dtype | null_count | null_pct | non_null_count | unique_count (approx.) | unique_pct |
|---|---|---|---|---|---|---|
| uuid | String | 0 | 0.00% | 997688267 | 942862968 | ~94.50% |
| category | String | 0 | 0.00% | 997688267 | 17 | 0.00% |
| content_type | String | 0 | 0.00% | 997688267 | 8 | 0.00% |
| automated_detection | String | 0 | 0.00% | 997688267 | 2 | 0.00% |
| automated_decision | String | 0 | 0.00% | 997688267 | 3 | 0.00% |
| territorial_scope | String | 0 | 0.00% | 997688267 | 158976 | 0.02% |
| application_date | Datetime(time_unit='ms', time_zone=None) | 0 | 0.00% | 997688267 | 394 | 0.00% |
| platform_name | String | 0 | 0.00% | 997688267 | 1 | 0.00% |
| decision_visibility | String | 12379074 | 1.24% | 985309193 | 10 | 0.00% |
| api_version | String | 0 | 0.00% | 997688267 | 2 | 0.00% |

## X
- Rows: 183,321
- Columns: 10
- File: C:\BA_Data\x_de_2025.parquet

| column | dtype | null_count | null_pct | non_null_count | unique_count (approx.) | unique_pct |
|---|---|---|---|---|---|---|
| uuid | String | 0 | 0.00% | 183321 | 189501 | ~100% (est. exceeds row count -- HyperLogLog artifact on small dataset) |
| category | String | 0 | 0.00% | 183321 | 15 | 0.01% |
| content_type | String | 0 | 0.00% | 183321 | 2 | 0.00% |
| automated_detection | String | 0 | 0.00% | 183321 | 1 | 0.00% |
| automated_decision | String | 0 | 0.00% | 183321 | 2 | 0.00% |
| territorial_scope | String | 0 | 0.00% | 183321 | 1 | 0.00% |
| application_date | Datetime(time_unit='ms', time_zone=None) | 0 | 0.00% | 183321 | 408 | 0.22% |
| platform_name | String | 0 | 0.00% | 183321 | 2 | 0.00% |
| decision_visibility | String | 0 | 0.00% | 183321 | 1 | 0.00% |
| api_version | String | 0 | 0.00% | 183321 | 2 | 0.00% |
