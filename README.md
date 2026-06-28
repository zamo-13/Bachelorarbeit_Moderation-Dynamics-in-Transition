# Moderation Dynamics in Transition: A Comparative Analysis of TikTok and X during the 2025 German Snap Election using Methodological Taxonomy Alignment

This thesis examines content moderation decisions on TikTok and X in Germany throughout 2025, with particular focus on the February 2025 German Federal Snap Election. It uses the DSA Transparency Database public API as its sole data source to compare moderation intensity, automation reliance, and reporting granularity across platforms for Germany-targeted decisions covering the period 2025-01-01 to 2025-12-31.

## Repository Structure

| Folder | Contents |
| --- | --- |
| `01_data_extraction/` | Documentation of the raw data download process and data provenance |
| `02_data_cleaning/` | Pipeline scripts, cleaning decisions, logs, and the full pipeline runbook |
| `03_analysis/` | Analysis scripts and the shared data loader |
| `Archive/` | Category harmonisation notebook and taxonomy mapping CSV |
| `notebooks/` | Exploratory and profiling notebooks |

## Reproducing the Analysis

See `02_data_cleaning/README_RUN.md` for the full pipeline runbook.

## Data

Raw data is not included in this repository. The data originates from the [DSA Transparency Database public API](https://transparency.dsa.ec.europa.eu/) and was downloaded using the `dsa-tdb` extraction tool. Processed parquet files are stored at `C:\BA_Data\` by default; this path can be overridden via script arguments as documented in the pipeline runbook.
