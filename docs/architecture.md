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

### 2. Archive Zone

**Bucket:** `imerys-glb-core-prsm-qa-nr-raw`
**Prefix:** `industry/sample-manager/archive/`

The Step Function copies pre-processed files here and appends the date.
File naming: `{file_name}_archived_dd-mm-yyyy`

### 3. Processed Zone

**Bucket:** `imerys-glb-core-prsm-qa-nr-raw`
**Prefix:** `industry/sample-manager/processed/`

All landed CSV files are moved here as **Snappy Parquet**, Hive-partitioned by `source/year/month/day`.
File naming: `{file_name}_dd-mm-yyyy`

---

## Glue

**Job name:** `lims_ingestion_job`
**Database:** `prism_edhid_samplemanager_ext`

The Glue job reads each CSV from the landed zone, injects partition columns (`source`, `year`, `month`, `day`), converts to Parquet, and registers the table in the Glue Data Catalog via `.saveAsTable()`.

Pattern: **bulk drop and reload** — full dataset replaced on each daily run.

---

## Lambda Function (Redshift Loader)

Copies processed Parquet files from S3 into the Redshift external schema. Runs as **bulk drop and reload** per table.

For each external table:
1. The existing ext table is **dropped**
2. `ext_{tablename}_raw` is **recreated**
3. `processedfile_MM-DD-YYYY` data is **added** to each ext table

> Tables contain the data extracted daily.
> Data from the 2 sources (FOS, DKQ) is merged. A **Plant identifier** column (`FOS` / `DKQ`) is added to each row at load time to distinguish the source.

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
            │       landed/{file}.csv  →  archive/{file}_archived_dd-mm-yyyy
            │
            └── Branch 2: Glue Job  lims_ingestion_job
                    Reads CSV from landed/
                    Adds source, year, month, day columns
                    Writes Snappy Parquet → processed/{table}/source=X/year=Y/month=M/day=D/
                    Registers table in Glue catalog: prism_edhid_samplemanager_ext.{table}

Lambda function (Redshift loader)
    For each ext table:
        DROP ext table
        RECREATE ext_{table}_raw
        LOAD processedfile_MM-DD-YYYY data
    Merge FOS + DKQ rows, tag each with Plant identifier
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
| 4 | **Archive step** | S3 copy to `archive/` with date suffix | Step Function copies to `replicated/` with date prefix | Rename replicated → archive and align key format |
| 5 | **FOS source** | Full FOS table set (13 tables) | Only DKQ configs created | Create FOS configs — actual files provided by API team |
