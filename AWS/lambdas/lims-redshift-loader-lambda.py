import os
import json
import logging
import boto3
import redshift_connector


# Configure Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Schema names are overridable via env vars; defaults match the architecture.
EXT_SCHEMA = os.environ.get('EXT_SCHEMA', 'samplemanager_ext')
DW_SCHEMA  = os.environ.get('DW_SCHEMA', 'prism_edhid_samplemanager_dw')

# DW column -> Redshift type, mirroring DDL/ALL_TABLES_REDSHIFT.sql.
# The ext (Spectrum) tables expose every CSV column as string, so each column
# is cast explicitly. The ext partition column `source` fills SOURCE and the
# Glue run timestamp fills LAST_UPDATED_AT.
# A 3rd tuple element gives the ext column name when the source extract names
# it differently than the DW model (e.g. MLP_HEADER key is ENTRY_CODE).
TABLES = {
    "versioned_analysis": [
        ("IDENTITY", "VARCHAR(50)"),
        ("ANALYSIS_VERSION", "BIGINT"),
    ],
    "versioned_component": [
        ("NAME", "VARCHAR(100)"),
        ("ANALYSIS", "VARCHAR(50)"),
        ("ANALYSIS_VERSION", "BIGINT"),
    ],
    "mlp_header": [
        ("IDENTITY", "VARCHAR(50)", "ENTRY_CODE"),  # extract has no IDENTITY; key is ENTRY_CODE
        ("PRODUCT_VERSION", "BIGINT"),
    ],
    "sample": [
        ("ID_NUMERIC", "BIGINT"),
    ],
    "sample_point": [
        ("IDENTITY", "VARCHAR(50)"),
    ],
    "customer": [
        ("IDENTITY", "VARCHAR(50)"),
    ],
    "location": [
        ("IDENTITY", "VARCHAR(50)"),
    ],
    "code_controle": [
        ("IDENTITY", "VARCHAR(50)"),
    ],
    "fournisseur": [
        ("IDENTITY", "VARCHAR(50)"),
    ],
    "job_header": [
        ("JOB_NAME", "VARCHAR(100)"),
    ],
    "phrase": [
        ("PHRASE_ID", "VARCHAR(50)"),
        ("PHRASE_TYPE", "VARCHAR(20)"),
    ],
    "destination": [
        ("IDENTITY", "VARCHAR(50)"),
    ],
    "phrase_format": [
        ("PHRASE_ID", "VARCHAR(50)"),
        ("PHRASE_TYPE", "VARCHAR(20)"),
    ],
    "mlp_view": [
        ("ANALYSIS_ID", "VARCHAR(50)"),
        ("COMPONENT_NAME", "VARCHAR(100)"),
        ("PRODUCT_ID", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
    ],
    "mensuel": [
        ("ANALYSE", "VARCHAR(50)"),
        ("ANNUEL", "CHAR(1)"),
        ("MESURE", "VARCHAR(100)"),
        ("PRODUIT", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
        ("LOCALISATION", "VARCHAR(50)"),
        ("CODE_CONTROLE", "VARCHAR(50)"),
    ],
    "samp_test_result_lafa": [
        ("ANALYSIS", "VARCHAR(50)"),
        ("ANALYSIS_VERSION", "BIGINT"),
        ("COMPONENT_NAME", "VARCHAR(100)"),
        ("PRODUCT", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
        ("SAMPLING_POINT", "VARCHAR(50)"),
        ("LOCATION_ID", "VARCHAR(50)"),
        ("DESTINATION_MATIERE", "VARCHAR(50)"),
        ("CODE_CONTROLE", "VARCHAR(50)"),
        ("CUSTOMER_ID", "VARCHAR(50)"),
        ("FOURNISSEUR", "VARCHAR(50)"),
        ("STATUS", "CHAR(1)"),
        ("RESULT_STATUS", "CHAR(1)"),
        ("TEST_STATUS", "CHAR(1)"),
        ("ORIGINAL_SAMPLE", "BIGINT"),
        ("JOB_NAME", "VARCHAR(100)"),
        ("GRANULOMETRIE", "VARCHAR(50)"),
        ("FORMAT", "VARCHAR(50)"),
        ("TEST_SCHEDULE", "VARCHAR(50)"),
    ],
}


def _cast_expr(column, sql_type, ext_column=None):
    # All identifiers are quoted: IDENTITY / ANALYSE / FORMAT are reserved words.
    # Redshift folds quoted identifiers to lowercase by default, matching both
    # the DW tables and the Glue catalog column names.
    quoted = f'"{column}"'
    src = f'"{ext_column}"' if ext_column else quoted
    if sql_type == "BIGINT":
        # CSV empties arrive as '' — NULLIF prevents cast failures on optional columns.
        return f"CAST(NULLIF(TRIM({src}), '') AS BIGINT) AS {quoted}"
    return f"CAST({src} AS {sql_type}) AS {quoted}"


def _build_queries(table, columns):
    """
    Two statements per table: DELETE + INSERT-SELECT with casts.
    DELETE (not TRUNCATE) on purpose — TRUNCATE auto-commits in Redshift, so a
    failed INSERT would leave the table empty. DELETE + single final commit
    means a failure saves nothing and yesterday's data survives.
    """
    dw_table  = f'{DW_SCHEMA}."{table}"'
    ext_table = f'{EXT_SCHEMA}."{table}"'
    col_list    = ", ".join(f'"{c[0]}"' for c in columns) + ', "SOURCE", "LAST_UPDATED_AT"'
    select_list = ",\n    ".join(_cast_expr(*c) for c in columns)
    insert_sql = (
        f"INSERT INTO {dw_table} ({col_list})\n"
        f"SELECT\n"
        f"    {select_list},\n"
        f'    CAST("source" AS VARCHAR(10)) AS "SOURCE",\n'
        f'    CAST("last_updated_at" AS TIMESTAMP) AS "LAST_UPDATED_AT"\n'
        f"FROM {ext_table}"
    )
    return [f"DELETE FROM {dw_table}", insert_sql]


QUERIES = {table: _build_queries(table, columns) for table, columns in TABLES.items()}


def lambda_handler(event, context):
    """
    Reloads DW tables from the Spectrum ext schema (DELETE + INSERT with casts).

    Event structure:
      { "table_name": "fournisseur" }  -> reload that one table
      {} or { "table_name": "all" }    -> reload all 16 tables in one transaction
    """
    table_name = (event or {}).get('table_name')

    if table_name and table_name != 'all' and table_name not in QUERIES:
        logger.error(f"Unknown table_name: {table_name}")
        return {'statusCode': 400, 'body': f'Error: Query for {table_name} not defined'}

    tables_to_load = [table_name] if table_name and table_name != 'all' else list(QUERIES)

    # Credentials: env vars win (dev override, no Secrets Manager involved),
    # otherwise fall back to Secrets Manager via 'SecretId'.
    if os.environ.get('REDSHIFT_HOST'):
        logger.info("Using credentials from environment variables (dev override).")
        try:
            redshift_params = {
                'host': os.environ['REDSHIFT_HOST'],
                'database': os.environ['REDSHIFT_DATABASE'],
                'user': os.environ['REDSHIFT_USER'],
                'password': os.environ['REDSHIFT_PASSWORD'],
                'port': int(os.environ.get('REDSHIFT_PORT', '5439'))
            }
        except KeyError as e:
            logger.error(f"REDSHIFT_HOST is set but {e} is missing.")
            return {'statusCode': 500, 'body': f'Configuration Error: missing env var {e}'}
    else:
        secret_id = os.environ.get('SecretId')

        if not secret_id:
            logger.error("Neither REDSHIFT_HOST nor 'SecretId' is set.")
            return {'statusCode': 500, 'body': 'Configuration Error: no credentials configured'}

        logger.info(f"Retrieving credentials from Secrets Manager: {secret_id}")

        try:
            sm_client = boto3.client('secretsmanager')
            secret_response = sm_client.get_secret_value(SecretId=secret_id)
            secret_dict = json.loads(secret_response['SecretString'])

            # The keys 'host', 'database', 'user', 'password', 'port' must exist in the secret
            redshift_params = {
                'host': secret_dict['host'],
                'database': secret_dict['database'],
                'user': secret_dict['user'],
                'password': secret_dict['password'],
                'port': int(secret_dict['port'])
            }
        except Exception as e:
            logger.error(f"Failed to retrieve or parse secret: {str(e)}")
            return {'statusCode': 500, 'body': f"Secret Retrieval Error: {str(e)}"}

    logger.info(
        f"Connecting to Redshift host: {redshift_params.get('host')} | "
        f"database: {redshift_params.get('database')} | "
        f"user (from secret): {redshift_params.get('user')}"
    )

    loaded = {}
    skipped = []
    try:
        with redshift_connector.connect(**redshift_params) as conn:

            with conn.cursor() as cursor:
                # Ask Redshift who we actually are — authoritative, unlike the secret value.
                cursor.execute("SELECT current_user, current_database()")
                who, db = cursor.fetchone()
                logger.info(f"Connected as user '{who}' on database '{db}' "
                            f"(target schemas: ext={EXT_SCHEMA}, dw={DW_SCHEMA})")

                # Ext tables only exist once their source file has been processed by
                # Glue. Skip (and report) the ones not delivered yet instead of
                # failing the whole run; their DW tables keep their previous data.
                cursor.execute(
                    "SELECT tablename FROM svv_external_tables WHERE schemaname = %s",
                    (EXT_SCHEMA,)
                )
                existing_ext = {row[0] for row in cursor.fetchall()}
                skipped = [t for t in tables_to_load if t not in existing_ext]
                if skipped:
                    logger.warning(f"No ext table yet (source not delivered/processed): {skipped}")

                for table in tables_to_load:
                    if table in skipped:
                        continue
                    logger.info(f"Reloading {DW_SCHEMA}.{table} from {EXT_SCHEMA}.{table}...")
                    for query in QUERIES[table]:
                        cursor.execute(query)
                    loaded[table] = cursor.rowcount
                    logger.info(f"Inserted {cursor.rowcount} rows into {DW_SCHEMA}.{table}.")

            # Single commit: the whole reload is atomic — if any table failed
            # above, nothing was saved and the previous load is still in place.
            conn.commit()
            logger.info("Transaction committed.")

        return {
            'statusCode': 200,
            'body': json.dumps({'loaded': loaded, 'skipped_missing_ext': skipped})
        }

    except Exception as e:
        logger.error(f"DW load failed (nothing committed). Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing DW load: {str(e)}")
        }
