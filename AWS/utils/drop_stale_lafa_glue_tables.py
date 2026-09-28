"""
Drop the stale LAFA tables from the Glue Data Catalog so the Glue ingestion
job recreates them with the current source schema on the next run.

Deleting a Glue catalog table does NOT delete the S3 Parquet data — only the
metadata definition, which lims_ingestion_job re-creates via saveAsTable.

Adjust GLUE_DB / REGION if your environment differs.
"""
import boto3

REGION   = 'eu-central-1'
GLUE_DB  = 'prsm_glb_dev_samplemanager_raw'   # dev Glue catalog database
TABLES   = ['samp_test_result_lafa', 'c_samp_test_result_lafa']

glue = boto3.client('glue', region_name=REGION)

for tbl in TABLES:
    print(f"\n=== {GLUE_DB}.{tbl} ===")
    try:
        meta = glue.get_table(DatabaseName=GLUE_DB, Name=tbl)['Table']
        cols = [c['Name'] for c in meta['StorageDescriptor']['Columns']]
        parts = [p['Name'] for p in meta.get('PartitionKeys', [])]
        print(f"  current columns ({len(cols)}): {cols}")
        print(f"  partition keys: {parts}")
        glue.delete_table(DatabaseName=GLUE_DB, Name=tbl)
        print(f"  -> DELETED (Glue job will recreate on next run)")
    except glue.exceptions.EntityNotFoundException:
        print("  not found — nothing to delete (Glue job will create it fresh)")
    except Exception as e:
        print(f"  ERROR: {e}")

print("\nDone. Next: re-run the Glue ingestion job (or Step Function) on the new "
      "files, then run the loader lambda.")
