# Pre-Harmonized Data Screening Report

- Generated: 2026-06-03 15:23:32
- Data root: C:\BA_Data
- Input files:
  - tiktok: C:\BA_Data\tiktok_de_2025.parquet
  - x: C:\BA_Data\x_de_2025.parquet

## Screening Scope
- Row and column counts
- Column names and data types
- Missingness and uniqueness as quality indicators
- Uniqueness counts are approximate to keep the scan practical on large files
- Variance for numeric columns only

## TIKTOK
- Rows: 997,688,267
- Columns: 10
- File: C:\BA_Data\tiktok_de_2025.parquet

| column | dtype | null_count | null_pct | non_null_count | unique_count | unique_pct | variance |
|---|---|---|---|---|---|---|---|
| uuid | String | 0 | 0.00% | 997688267 | 942862968 | 94.50% | n/a |
| category | String | 0 | 0.00% | 997688267 | 17 | 0.00% | n/a |
| content_type | String | 0 | 0.00% | 997688267 | 8 | 0.00% | n/a |
| automated_detection | String | 0 | 0.00% | 997688267 | 2 | 0.00% | n/a |
| automated_decision | String | 0 | 0.00% | 997688267 | 3 | 0.00% | n/a |
| territorial_scope | String | 0 | 0.00% | 997688267 | 158976 | 0.02% | n/a |
| application_date | Datetime(time_unit='ms', time_zone=None) | 0 | 0.00% | 997688267 | 394 | 0.00% | n/a |
| platform_name | String | 0 | 0.00% | 997688267 | 1 | 0.00% | n/a |
| decision_visibility | String | 12379074 | 1.24% | 985309193 | 10 | 0.00% | n/a |
| api_version | String | 0 | 0.00% | 997688267 | 2 | 0.00% | n/a |

## X
- Rows: 183,321
- Columns: 10
- File: C:\BA_Data\x_de_2025.parquet

| column | dtype | null_count | null_pct | non_null_count | unique_count | unique_pct | variance |
|---|---|---|---|---|---|---|---|
| uuid | String | 0 | 0.00% | 183321 | 189501 | 103.37% | n/a |
| category | String | 0 | 0.00% | 183321 | 15 | 0.01% | n/a |
| content_type | String | 0 | 0.00% | 183321 | 2 | 0.00% | n/a |
| automated_detection | String | 0 | 0.00% | 183321 | 1 | 0.00% | n/a |
| automated_decision | String | 0 | 0.00% | 183321 | 2 | 0.00% | n/a |
| territorial_scope | String | 0 | 0.00% | 183321 | 1 | 0.00% | n/a |
| application_date | Datetime(time_unit='ms', time_zone=None) | 0 | 0.00% | 183321 | 408 | 0.22% | n/a |
| platform_name | String | 0 | 0.00% | 183321 | 2 | 0.00% | n/a |
| decision_visibility | String | 0 | 0.00% | 183321 | 1 | 0.00% | n/a |
| api_version | String | 0 | 0.00% | 183321 | 2 | 0.00% | n/a |
