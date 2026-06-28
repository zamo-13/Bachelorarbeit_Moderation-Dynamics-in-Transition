# 01_data_extraction

This folder documents the data extraction phase of the thesis pipeline. The raw data was downloaded from the DSA Transparency Database using the official `dsa-tdb` extraction tool. The tool files themselves were removed from this repository after extraction was complete; only documentation is retained here.

---

## Data Provenance

**Extraction tool repository:**
https://code.europa.eu/dsa/transparency-database/dsa-tdb

**Download completed:**
February/March 2026 (the full 2025 reference year was downloaded after the year ended)

**DSA-TDB API endpoints used:**
Both the v1 and v2 endpoints of the DSA Transparency Database public API were used. The schema break between v1 and v2 occurs on 2025-07-01; records with `application_date` before that date were retrieved via the v1 schema, records from 2025-07-01 onward via the v2 schema. See `02_data_cleaning/docs/cleaning_decisions.md` for how this break is handled in the pipeline.

**Platforms downloaded:**
- TikTok
- X (formerly Twitter)

**Date range covered by the raw dumps:**
2025-01-01 to 2025-12-31 (full reference year)

**Storage location:**
Raw zip archives are stored at `C:\BA_Data\tiktok___full\` and `C:\BA_Data\x___full\` (outside the repository). These are the only copies of the original unmodified data and must not be deleted.
