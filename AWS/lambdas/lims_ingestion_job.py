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

# 3. Write Snappy Parquet & Register in Glue Catalog
# Dynamic partition overwrite replaces only the affected source partition,
# so a DKQ run never touches FOS data and re-runs are idempotent.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_partitioned.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("source") \
    .option("path", args['PROCESSED_S3_PATH']) \
    .saveAsTable(f"{args['DATABASE_NAME']}.{args['TARGET_TABLE']}")

# 4. Remove EMRFS zero-byte directory markers (*_$folder$) left by saveAsTable.
# Recreated on every run because the overwrite deletes the partition folders.
s3_client = boto3.client('s3')
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
