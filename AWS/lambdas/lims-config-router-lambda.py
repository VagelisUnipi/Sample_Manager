import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

# All paths are driven exclusively by environment variables — no defaults embed
# bucket names. Each deployed environment (dev/qa/prod) sets these explicitly
# via Lambda configuration (Terraform / CDK / console). This ensures promotion
# never requires code changes.
ENV                = os.environ["ENVIRONMENT"]
RAW_S3_BASE        = os.environ["RAW_S3_BASE"]
REPLICATED_S3_BASE = os.environ["REPLICATED_S3_BASE"]
CONFIG_S3_BASE     = os.environ["CONFIG_S3_BASE"]
PROCESSED_S3_BASE  = os.environ["PROCESSED_S3_BASE"]


def parse_s3_uri(uri: str) -> tuple:
    """Helper to extract bucket and prefix from an S3 URI."""
    parts = uri.replace("s3://", "").split("/", 1)
    return parts[0], parts[1] if len(parts) > 1 else ""


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, List[Dict[str, Any]]]:
    """
    Scans the landed zone, reads the config, and outputs Step Functions parameters.
    """
    raw_bucket, raw_prefix = parse_s3_uri(RAW_S3_BASE)
    config_bucket, config_prefix = parse_s3_uri(CONFIG_S3_BASE)
    files_to_process = []

    # Scan the landed folder for new files — paginate to handle > 1000 objects.
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=raw_bucket, Prefix=raw_prefix)

    all_objects = []
    for page in pages:
        all_objects.extend(page.get('Contents', []))

    if not all_objects:
        return {"Files": []}

    now = datetime.now(timezone.utc)
    time_partition = f"year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}"

    for obj in all_objects:
        source_key = obj['Key']
        filename = os.path.basename(source_key)
        if not filename:
            continue

        try:
            # 1. Fetch Configuration for this specific file
            # Strip compound extensions (.csv.gz, .csv) to get the bare stem.
            stem = filename
            for ext in ('.csv.gz', '.tsv.gz', '.csv', '.tsv'):
                if stem.lower().endswith(ext):
                    stem = stem[:-len(ext)]
                    break
            config_key = f"{config_prefix}{stem}.json"
            config_obj = s3_client.get_object(Bucket=config_bucket, Key=config_key)
            config_data = json.loads(config_obj['Body'].read().decode('utf-8'))

            source_s3_path = f"s3://{raw_bucket}/{source_key}"
            target_table = config_data['TargetTable']
            # Source system is the subfolder name under landed/ (DKQ or FOS),
            # not a config field — the same config file is shared by all factories.
            source_system = source_key.split('/')[-2]
            has_header = config_data.get('HasHeader', 'true')
            delimiter = config_data.get('Delimiter', ',')

            # 2. Build S3 Copy Parameters for Step Functions Replication State
            replicated_copy_params = {
                "Bucket": raw_bucket,
                "Key": f"industry/sample-manager/replicated/{time_partition}/{filename}",
                "CopySource": f"{raw_bucket}/{source_key}"
            }

            # Glue must point to the root table folder, not the date-partitioned leaf.
            processed_s3_path = f"{PROCESSED_S3_BASE.rstrip('/')}/{target_table}/"

            # 3. Build AWS Glue Job Arguments
            glue_arguments = {
                "--SOURCE_S3_PATH": source_s3_path,
                "--PROCESSED_S3_PATH": processed_s3_path,
                "--TARGET_TABLE": target_table,
                "--SOURCE_SYSTEM": source_system,
                "--DATABASE_NAME": f"prsm_glb_{ENV}_samplemanager_raw",
                "--HAS_HEADER": has_header,
                "--DELIMITER": delimiter
            }

            files_to_process.append({
                "ReplicatedCopyParams": replicated_copy_params,
                "GlueArguments": glue_arguments
            })

        except s3_client.exceptions.NoSuchKey:
            logger.error(f"No configuration file found for {filename}. Skipping.")
        except Exception as e:
            logger.error(f"Error processing {filename}: {str(e)}")

    return {"Files": files_to_process}
