# active_pipeline

Curated snapshot of the **currently-used** SampleManager LIMS pipeline components
only — nothing unused, deprecated, mock, or example. Files are copied here from
their working locations and grouped by role.

## Layout

```
active_pipeline/
├── lambdas/                     AWS Lambda functions
│   ├── lims-config-router-lambda.py    # scans landed S3, emits per-file Step Function params
│   └── lims-redshift-loader-lambda.py  # loads ext (Spectrum) -> typed DW (DELETE + INSERT..SELECT)
├── glue-job/
│   └── lims_ingestion_job.py            # raw CSV -> partitioned Parquet + Glue Catalog
├── stepfunction/
│   └── prsm-sfn-dev-samplemanager-run.asl  # orchestrates router -> Glue map -> loader
├── ddl/
│   ├── vw_lafa_report_base.sql          # base reporting view (union of both facts + 5 joins)
│   └── tables/                          # 15 Redshift DW typed tables, numbered in deploy order
│       ├── 01_versioned_analysis.sql
│       ├── ...
│       └── 15_c_sample.sql
└── glue-configs/                        # ingestion config, one JSON per ingested table (15)
```

## Scope decisions

- **Tables:** all **15 tables the loader currently ingests** (each has a config +
  a DW table). The non-ingested `mlp_view`, `destination`, `phrase_format` are
  excluded (no config; not loaded).
- **Views:** the **base view only** (`vw_lafa_report_base`). The two report
  wrappers (`vw_bilan_kk_dkq`, `vw_recherche_simplifiee`) are not included here.
- **Table DDL:** the **Redshift typed DW DDL** (source of truth
  `DDL/latest_dll_truth/ALL_TABLES_REDSHIFT.sql`), split one file per table in
  forward-FK deploy order (run `01_…` → `15_…`).

## Notes

- This is a **copy**, not the live source. Edits here do not affect the deployed
  pipeline; keep changes in sync with the working files.
- The DW tables carry two pipeline-added columns (`SOURCE`, `LAST_UPDATED_AT`).
- Reporting note: of the 15 tables, only 7 are actually read by the base view
  (`samp_test_result_lafa`, `c_samp_test_result_lafa`, `code_controle`,
  `mlp_header`, `job_header`, `versioned_analysis`, `phrase`).
