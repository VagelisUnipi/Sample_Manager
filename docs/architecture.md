# Sample Manager — Pipeline Architecture

## Triggers

| Trigger | Schedule | Scope |
|---|---|---|
| **SAP DS Job** | Daily `hh:mm` | Extracts source data from Sample Manager and drops CSV files to S3 landed zone |
| **Step Function** `prsm-sfn-dev-samplemanager-run` | Daily `hh:mm` CET | Orchestrates the full ingestion, transformation, and load pipeline |

---

## Source System

**SAMPLE MANAGER** — 2 sources: `FOS` and `DKQ`

Tables extracted from both sources:

| Table |
|---|
| C_SAMP_TEST_RESULT_LAFA |
| C_SAMPLE |
| CODE_CONTROLE |
| CUSTOMER |
| FOURNISSEUR |
| JOB_HEADER |
| LOCATION |
| MENSUEL |
| MLP_HEADER |
| MLP_VIEW |
| PHRASE |
| SAMP_TEST_RESULT_LAFA |
| SAMPLE |
| SAMPLE_POINT |
| VERSIONED_ANALYSIS |
| VERSIONED_COMPONENT |

---

## S3 Zones

### 1. Landed Zone (new files)

**Bucket:** `imerys-glb-core-prsm-qa-nr-raw`
**Prefix:** `industry/sample-manager/landed/`

CSV files containing **full source data** — bulk drop and load pattern.
One file per table per source system:

| FOS files | DKQ files |
|---|---|
| `FOS_C_SAMP_TEST_RESULT_LAFA` | `DKQ_C_SAMP_TEST_RESULT_LAFA` |
| `FOS_C_SAMPLE` | `DKQ_C_SAMPLE` |
| `FOS_CODE_CONTROLE` | `DKQ_CODE_CONTROLE` |
| `FOS_CUSTOMER` | `DKQ_CUSTOMER` |
| `FOS_FOURNISSEUR` | `DKQ_FOURNISSEUR` |
| `FOS_JOB_HEADER` | `DKQ_JOB_HEADER` |
| `FOS_LOCATION` | `DKQ_LOCATION` |
| `FOS_MENSUEL` | `DKQ_MENSUEL` |
| `FOS_MLP_HEADER` | `DKQ_MLP_HEADER` |
| `FOS_MLP_VIEW` | `DKQ_MLP_VIEW` |
| `FOS_PHRASE` | `DKQ_PHRASE` |
| `FOS_SAMP_TEST_RESULT_LAFA` | `DKQ_SAMP_TEST_RESULT_LAFA` |
| `FOS_SAMPLE` | `DKQ_SAMPLE` |
| `FOS_SAMPLE_POINT` | `DKQ_SAMPLE_POINT` |
| `FOS_VERSIONED_ANALYSIS` | `DKQ_VERSIONED_ANALYSIS` |
| `FOS_VERSIONED_COMPONENT` | `DKQ_VERSIONED_COMPONENT` |

### 2. Replicated Zone

**Bucket / Prefix:** from the `REPLICATED_S3_BASE` Lambda env var
(e.g. `s3://imerys-glb-core-prsm-qa-nr-raw/industry/sample-manager/replicated/`)

The Step Function copies each landed file here **as-is**, mirroring the landed
layout — one subfolder per factory:

```
replicated/DKQ/{filename}
replicated/FOS/{filename}
```

No date partitions or renamed copies: the key is identical every run, so each
daily copy overwrites the previous one.

### 3. Processed Zone

**Bucket:** `imerys-glb-core-prsm-qa-nr-raw`
**Prefix:** `industry/sample-manager/processed/`

All landed CSV files are converted here to **Snappy Parquet**, one folder per
table, Hive-partitioned by `source` (FOS / DKQ). Each daily run **overwrites**
the affected source partition in place — no dated copies accumulate here
(history lives in the archive zone).

---

## Glue

**Job name:** `lims_ingestion_job`
**Database:** `prism_edhid_samplemanager_ext`

The Glue job reads each CSV from the landed zone, adds the `source` partition column (FOS / DKQ) and a `last_updated_at` timestamp, converts to Parquet, and registers the table in the Glue Data Catalog via `.saveAsTable()`.

Pattern: **bulk drop and reload** — each daily run overwrites the table's `source` partition (dynamic partition overwrite, so a DKQ run never touches FOS data). The Parquet in `/processed` always holds exactly the latest full extract per source.

---

## Redshift External Schema (Spectrum)

The external schema is created **once** and maps directly onto the Glue database:

```sql
CREATE EXTERNAL SCHEMA samplemanager_ext
FROM DATA CATALOG
DATABASE 'prsm_glb_{env}_samplemanager_raw'
IAM_ROLE '<redshift-spectrum-role-arn>';
```

The 16 ext tables are **not managed by any loader** — they are the Glue Catalog
entries over the Parquet files in `/processed`, exposed automatically via
Redshift Spectrum. When the Glue job overwrites a table's Parquet on the daily
run, the ext table reflects the new data instantly. **Nothing is ever dropped,
recreated, or copied on the ext side** — ext *is* the Parquet in S3.

> Data from the 2 sources (FOS, DKQ) already carries the **`source`** partition
> column (the Plant identifier), added by the Glue job at transform time —
> no extra column is needed at load time.

## Lambda Function (Redshift Loader)

**Function:** `lims-redshift-loader-lambda` — final Step Function state, runs
after all Glue jobs complete. Uses the Redshift Data API (no VPC connection
needed). Loads the 16 DW tables **from the ext schema**, per table, as bulk
drop and reload:

1. `TRUNCATE` the DW table
2. `INSERT INTO dw_table SELECT ... FROM samplemanager_ext.{table}` — casting
   string columns to the DW types and loading the ext `source` column into
   the `SOURCE` Plant-identifier column

---

## Redshift

**Schema / Database:** `prism_edhid_samplemanager_dw`

See slide 2 for table-level detail.

---

## End-to-End Flow

```
SAP DS Job (hh:mm)
    └── Drops {FOS,DKQ}_{TABLE}.csv → S3 landed/

Step Function prsm-sfn-dev-samplemanager-run (daily hh:mm CET)
    │
    ├── DiscoverAndConfigLambda
    │       Scans landed/, reads glue-table-configs/{file}.json
    │       Builds Glue args + S3 copy params per file
    │
    └── ProcessFilesMap  [up to 10 concurrent]
            │
            ├── Branch 1: S3 CopyObject
            │       landed/{DKQ|FOS}/{file}  →  replicated/{DKQ|FOS}/{file}  (overwrite)
            │
            └── Branch 2: Glue Job  lims_ingestion_job
                    Reads CSV from landed/
                    Adds source (Plant identifier) + last_updated_at columns
                    Overwrites Snappy Parquet → processed/{table}/source=X/
                    Registers table in Glue catalog (first run only)

Redshift Spectrum (automatic — no pipeline step)
    samplemanager_ext.{table} = Glue Catalog entry over processed/ Parquet
    Overwritten Parquet is instantly visible in ext — no drop/reload needed

LoadRedshiftDw  (final Step Function state → lims-redshift-loader-lambda)
    For each of the 16 DW tables:
        TRUNCATE dw_table
        INSERT INTO dw_table
            SELECT <casts> FROM samplemanager_ext.{table}
    FOS + DKQ rows already merged; `source` column = Plant identifier
    Target: prism_edhid_samplemanager_dw
```

---

## Discrepancies — Code vs. Architecture Diagram

The following gaps were identified between the diagram and the current Lambda/Glue code. These need to be resolved before a production run.

| # | Item | Diagram says | Code currently has | Action needed |
|---|---|---|---|---|
| 1 | **Landed bucket** | `prsm-qa-nr-raw` | `prsm-dev-nr-raw` (via `RAW_S3_BASE` env var) | Set `RAW_S3_BASE` Lambda env var to the `qa` bucket |
| 2 | **File prefix separator** | `FOS_` / `DKQ_` (underscore) | `DKQ-` (hyphen) in mock files and configs | Rename mocks and configs to `DKQ_` / `FOS_` |
| 3 | **Glue database name** | `prism_edhid_samplemanager_ext` | `prsm_glb_{ENV}_samplemanager_raw` | Align `DATABASE_NAME` arg in Lambda |
| 4 | **Archive step** | ~~S3 copy to `archive/` with date suffix~~ | Resolved 2026-07: `replicated/` mirrors the landed layout (`replicated/{DKQ\|FOS}/{file}`), overwritten each run | Done |
| 5 | **FOS source** | Full FOS table set (13 tables) | Only DKQ configs created | Create FOS configs — actual files provided by API team |
