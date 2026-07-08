Subject: Sample Manager Pipeline — Development Task List


PROJECT: Sample Manager Data Pipeline (FOS + DKQ → S3 → Glue → Redshift)
Owner: Vaggelis Karaferis
Reference: prsm-sfn-dev-samplemanager-run

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 1 — Source Analysis & Schema Design

  ✅  Analyse source system schema from Business Objects universe export (DKQ DFX file)
  ✅  Identify and document all 16 source tables and their column structures
  ✅  Produce DDL definitions (CREATE TABLE) for all 16 tables to serve as the data contract
  ✅  Document pipeline architecture (triggers, S3 zones, Glue, Redshift, data flow)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 2 — Pipeline Configuration Layer

  ✅  Design metadata-driven config pattern (one JSON config file per source table)
  ✅  Create 16 Glue table config files for DKQ source (DKQ_{TABLE}.json)
  ⬜  Create 16 Glue table config files for FOS source (FOS_{TABLE}.json)
  ⬜  Upload all config files to S3 config zone (glue-table-configs/)
  ⬜  Create Glue database prism_edhid_samplemanager_ext in AWS console

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 3 — Lambda: Discovery & Routing

  ✅  Build lims-config-router-lambda — scans landed zone, reads config, builds Glue job arguments per file
  ✅  Add support for HasHeader and Delimiter per-table config fields
  ✅  Fix compound extension handling for .csv.gz files
  ✅  Implement environment-variable-only configuration (no hardcoded paths — promotion-ready)
  ⬜  Deploy Lambda to AWS (dev environment)
  ⬜  Set Lambda environment variables (S3 paths, environment name)
  ⬜  Validate Lambda output against Step Function input contract

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 4 — Glue Job: Transformation

  ✅  Build lims_ingestion_job — reads CSV, injects partition columns, writes Snappy Parquet, registers Glue catalog table
  ✅  Wire HAS_HEADER and DELIMITER arguments into Spark reader
  ⬜  Upload Glue job script to S3 scripts bucket
  ⬜  Create and configure Glue job in AWS console (IAM role, worker type, connections)
  ⬜  Validate job runs successfully against real source files

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 5 — Testing

  🚫  Create mock CSV.GZ test files for DKQ tables — ABORTED (actual files provided by API team: 13 tables per source)
  🚫  Create mock CSV.GZ test files for FOS tables — ABORTED (actual files provided by API team: 13 tables per source)
  ⬜  Upload FOS and DKQ source files to S3 landed zone
  ⬜  Trigger Step Function manually and validate end-to-end (landed → archive → processed → Glue catalog)
  ⬜  Confirm Parquet output is correctly partitioned by source / year / month / day
  ⬜  Confirm Glue catalog tables are created with correct schema

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 6 — Redshift Integration

  ⬜  Create Redshift external schema prism_edhid_samplemanager_ext pointing to Glue catalog database
  ⬜  Validate all 13 FOS and 13 DKQ tables are queryable in Redshift via Spectrum after a Glue job run
  ⬜  Define and create target schema prism_edhid_samplemanager_dw with merged FOS + DKQ views
  ⬜  Validate Plant identifier column (FOS / DKQ) is correctly populated per row

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 7 — Alignment & Cleanup

  ⬜  Align file naming convention (FOS_ / DKQ_ underscore separator confirmed with source team)
  ⬜  Align Glue database name between Lambda code and architecture (prism_edhid_samplemanager_ext)
  ⬜  Align S3 landed bucket (prsm-qa-nr-raw vs prsm-dev-nr-raw)
  ⬜  Rename archive S3 path to match architecture diagram (archive/ with _dd-mm-yyyy suffix)
  ⬜  Code review (internal or external — Lampros / Dev Factory)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

PHASE 8 — Production Readiness

  ⬜  Document all environment variable values per environment (dev / qa / prod)
  ⬜  Define Step Function schedule (exact daily CET trigger time)
  ⬜  Set up CloudWatch alarms for Lambda and Glue job failures
  ⬜  Validate pipeline with full historical load
  ⬜  Sign-off and handover documentation

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Legend:  ✅ Done     ⬜ Pending     🚫 Aborted
