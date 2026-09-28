import sys
import logging
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import lit, current_timestamp

# Required args — always provided by the Lambda config router.
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'SOURCE_S3_PATH', 'PROCESSED_S3_PATH', 'TARGET_TABLE', 'SOURCE_SYSTEM', 'DATABASE_NAME']
)

# Optional args — safe fallback for manual test runs that omit them.
_optional = {}
try:
    _optional = getResolvedOptions(sys.argv, ['HAS_HEADER', 'DELIMITER', 'ENCODING'])
except Exception:
    pass

has_header = _optional.get('HAS_HEADER', 'true').lower() == 'true'
delimiter  = _optional.get('DELIMITER', ',')
encoding   = _optional.get('ENCODING', 'utf-8')

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def cleanup_table_versions(database: str, table: str) -> None:
    """Delete all but the latest Glue catalog version for a table.

    MSCK REPAIR TABLE creates a new version on every call. Without cleanup
    the account-wide TABLE_VERSION limit (1,000,000) is hit quickly.
    """
    glue_client = boto3.client('glue')
    try:
        versions = glue_client.get_table_versions(
            DatabaseName=database, TableName=table
        )['TableVersions']
        to_delete = [v['VersionId'] for v in versions[1:]]  # keep latest (index 0)
        if to_delete:
            for i in range(0, len(to_delete), 100):
                glue_client.batch_delete_table_version(
                    DatabaseName=database, TableName=table,
                    VersionIds=to_delete[i:i + 100]
                )
            logger.info(f"Deleted {len(to_delete)} old version(s) for {database}.{table}")
    except Exception as e:
        logger.warning(f"Could not clean table versions for {database}.{table}: {e}")

logger.info(f"Processing: {args['SOURCE_S3_PATH']} → {args['PROCESSED_S3_PATH']}")
logger.info(f"Table: {args['DATABASE_NAME']}.{args['TARGET_TABLE']} | header={has_header} | delimiter='{delimiter}' | encoding={encoding}")

# 1. Ingest Raw CSV
df = spark.read \
    .option("header", str(has_header).lower()) \
    .option("inferSchema", "false") \
    .option("sep", delimiter) \
    .option("encoding", encoding) \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(args['SOURCE_S3_PATH'])

# 2. Add source identifier and load timestamp — no date partitioning since
# files are full drop-reload daily; source is the only partition key.
df_partitioned = df \
    .withColumn("source",          lit(args['SOURCE_SYSTEM'])) \
    .withColumn("last_updated_at", current_timestamp())

# S3 client used for both partition refresh (step 3) and EMRFS cleanup (step 4).
s3_client = boto3.client('s3')

# 3. Write Snappy Parquet & Register in Glue Catalog
# First run: saveAsTable creates the catalog entry + partitioned S3 layout.
# Re-runs: delete the source partition from S3 directly with boto3 (no ALTER
# TABLE DDL, no catalog permissions needed), then write parquet straight to the
# partition path — the catalog already knows the partition from the first run.
# Safe for concurrent DKQ / FOS runs: each only deletes its own partition prefix.
table_full_name = f"{args['DATABASE_NAME']}.{args['TARGET_TABLE']}"
source_system = args['SOURCE_SYSTEM']

try:
    spark.sql(f"DESCRIBE TABLE {table_full_name}")
    table_exists = True
except Exception:
    table_exists = False

# rstrip trailing slash to avoid double-slash in partition path
processed_s3_path_clean = args['PROCESSED_S3_PATH'].rstrip('/')
partition_s3_path = f"{processed_s3_path_clean}/source={source_system}"
part_bucket, _, part_prefix = partition_s3_path.replace('s3://', '').partition('/')

# Schema-drift guard: the catalog schema is frozen at first saveAsTable, so
# without this a new/renamed source column stays invisible to Spectrum (the
# Parquet has it but the catalog does not). If the catalog columns no longer
# match the incoming source columns, drop the catalog definition and wipe the
# table's S3 prefix so saveAsTable below recreates it with the current schema.
if table_exists:
    catalog_cols = set(c.lower() for c in spark.table(table_full_name).columns)
    incoming_cols = set(c.lower() for c in df_partitioned.columns)
    if catalog_cols != incoming_cols:
        logger.warning(
            f"Schema drift for {table_full_name}: recreating table. "
            f"added={sorted(incoming_cols - catalog_cols)} "
            f"removed={sorted(catalog_cols - incoming_cols)}"
        )
        spark.sql(f"DROP TABLE IF EXISTS {table_full_name}")
        tbl_bucket, _, tbl_prefix = processed_s3_path_clean.replace('s3://', '').partition('/')
        drift_objs = []
        for page in s3_client.get_paginator('list_objects_v2').paginate(
                Bucket=tbl_bucket, Prefix=tbl_prefix.rstrip('/') + '/'):
            drift_objs.extend({'Key': o['Key']} for o in page.get('Contents', []))
        for i in range(0, len(drift_objs), 1000):
            s3_client.delete_objects(
                Bucket=tbl_bucket,
                Delete={'Objects': drift_objs[i:i + 1000], 'Quiet': True}
            )
        table_exists = False  # fall through to the saveAsTable recreate path

if table_exists:
    # Delete this source's existing S3 partition objects before overwriting.
    part_objects = []
    for page in s3_client.get_paginator('list_objects_v2').paginate(
            Bucket=part_bucket, Prefix=part_prefix.rstrip('/') + '/'):
        part_objects.extend({'Key': obj['Key']} for obj in page.get('Contents', []))
    if part_objects:
        for i in range(0, len(part_objects), 1000):
            s3_client.delete_objects(
                Bucket=part_bucket,
                Delete={'Objects': part_objects[i:i + 1000], 'Quiet': True}
            )
    logger.info(f"Refreshed {len(part_objects)} object(s) at {partition_s3_path}")
    # Drop the partition column — it is encoded in the directory name, not stored
    # inside the parquet files (consistent with partitionBy on first run).
    df_partitioned.drop("source").write \
        .mode("overwrite") \
        .format("parquet") \
        .save(partition_s3_path)
    # Overwriting parquet in the existing partition path is a pure S3 operation and
    # creates NO Glue table version — Spectrum reads the new files on the next query
    # because the partition location already points here. Only register the partition
    # the first time a source appears (the single case needing a catalog write). This
    # keeps steady-state runs at zero TABLE_VERSION churn, so the account-wide 1M
    # version limit is never approached. (MSCK REPAIR here used to add a version every
    # run for a partition that was already registered — the sole cause of the limit.)
    glue_client = boto3.client('glue')
    try:
        glue_client.get_partition(
            DatabaseName=args['DATABASE_NAME'],
            TableName=args['TARGET_TABLE'],
            PartitionValues=[source_system],
        )
    except glue_client.exceptions.EntityNotFoundException:
        spark.sql(
            f"ALTER TABLE {table_full_name} ADD IF NOT EXISTS "
            f"PARTITION (source='{source_system}') LOCATION '{partition_s3_path}'"
        )
else:
    try:
        df_partitioned.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("source") \
            .option("path", processed_s3_path_clean) \
            .saveAsTable(table_full_name)
    except Exception:
        # Race condition: concurrent factory job created the table first.
        # Switch to re-run path: delete S3 partition and write directly.
        logger.warning(f"saveAsTable race condition for {table_full_name}, switching to re-run path.")
        part_objects = []
        for page in s3_client.get_paginator('list_objects_v2').paginate(
                Bucket=part_bucket, Prefix=part_prefix.rstrip('/') + '/'):
            part_objects.extend({'Key': obj['Key']} for obj in page.get('Contents', []))
        if part_objects:
            for i in range(0, len(part_objects), 1000):
                s3_client.delete_objects(
                    Bucket=part_bucket,
                    Delete={'Objects': part_objects[i:i + 1000], 'Quiet': True}
                )
        df_partitioned.drop("source").write \
            .mode("overwrite") \
            .format("parquet") \
            .save(partition_s3_path)
        spark.sql(f"MSCK REPAIR TABLE {table_full_name}")
    cleanup_table_versions(args['DATABASE_NAME'], args['TARGET_TABLE'])

# 4. Remove EMRFS zero-byte directory markers (*_$folder$) left by saveAsTable.
# Recreated on every run because the overwrite deletes the partition folders.
bucket, _, table_prefix = args['PROCESSED_S3_PATH'].replace('s3://', '').partition('/')
table_prefix = table_prefix.rstrip('/')

markers = [{'Key': f"{table_prefix}_$folder$"}]  # marker for the table folder itself sits beside it
paginator = s3_client.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket=bucket, Prefix=f"{table_prefix}/"):
    markers.extend(
        {'Key': obj['Key']}
        for obj in page.get('Contents', [])
        if obj['Key'].endswith('_$folder$')
    )

# Deleting a key that doesn't exist is a no-op, so the table-level marker is safe to always include.
for i in range(0, len(markers), 1000):
    s3_client.delete_objects(Bucket=bucket, Delete={'Objects': markers[i:i + 1000], 'Quiet': True})
logger.info(f"Removed {len(markers)} _$folder$ marker(s) under s3://{bucket}/{table_prefix}")

job.commit()
