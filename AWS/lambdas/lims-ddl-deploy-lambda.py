import os
import re
import json
import logging
import boto3
import redshift_connector

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DW_SCHEMA  = os.environ.get('DW_SCHEMA',  'prsm_edhid_samplemanager_dw')
DDL_S3_URI = os.environ.get('DDL_S3_URI') # e.g. s3://bucket/ddl/ALL_TABLES_REDSHIFT.sql

def _get_ddl(uri: str) -> str:
    if not uri:
        raise ValueError("DDL_S3_URI env var is not set.")
    bucket, _, key = uri.replace('s3://', '').partition('/')
    s3  = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj['Body'].read().decode('utf-8')


def _parse_create_statements(sql: str) -> list:
    """Strip comments and extract individual CREATE TABLE blocks."""
    # Remove single-line comments
    sql = re.sub(r'--[^\n]*', '', sql)
    # Extract each CREATE TABLE ... ); block
    pattern = re.compile(
        r'(CREATE\s+TABLE\s+.+?)\s*;',
        re.IGNORECASE | re.DOTALL
    )
    stmts = pattern.findall(sql)
    return [s.strip() for s in stmts]


def _prefix_schema(create_stmt: str, schema: str) -> str:
    """
    Inject schema into CREATE TABLE and all REFERENCES clauses.
    Handles both quoted ("LOCATION") and unquoted table names.
    """
    # CREATE TABLE
    create_stmt = re.sub(
        r'CREATE\s+TABLE\s+("?\w+"?)',
        lambda m: f'CREATE TABLE {schema}.{m.group(1)}',
        create_stmt, count=1, flags=re.IGNORECASE
    )
    # REFERENCES
    create_stmt = re.sub(
        r'REFERENCES\s+("?\w+"?)',
        lambda m: f'REFERENCES {schema}.{m.group(1)}',
        create_stmt, flags=re.IGNORECASE
    )
    return create_stmt


def _get_redshift_params() -> dict:
    if os.environ.get('REDSHIFT_HOST'):
        logger.info("Using credentials from environment variables.")
        try:
            return {
                'host':     os.environ['REDSHIFT_HOST'],
                'database': os.environ['REDSHIFT_DATABASE'],
                'user':     os.environ['REDSHIFT_USER'],
                'password': os.environ['REDSHIFT_PASSWORD'],
                'port':     int(os.environ.get('REDSHIFT_PORT', '5439')),
            }
        except KeyError as e:
            raise ValueError(f"Missing env var: {e}")

    secret_id = os.environ.get('SecretId')
    if not secret_id:
        raise ValueError("Neither REDSHIFT_HOST nor SecretId is set.")

    sm  = boto3.client('secretsmanager')
    sec = json.loads(sm.get_secret_value(SecretId=secret_id)['SecretString'])
    return {
        'host':     sec['host'],
        'database': sec['database'],
        'user':     sec['user'],
        'password': sec['password'],
        'port':     int(sec['port']),
    }


def lambda_handler(event, context):
    """
    Event (all optional — env vars are the defaults):
      { "dw_schema": "prsm_edhid_samplemanager_dw",
        "ddl_s3_uri": "s3://bucket/path/ALL_TABLES_REDSHIFT.sql" }
    """
    schema     = event.get('dw_schema',  DW_SCHEMA)
    ddl_s3_uri = event.get('ddl_s3_uri', DDL_S3_URI)

    logger.info(f"Schema: {schema}  |  DDL: {ddl_s3_uri}")

    # 1. Read and parse DDL
    raw_ddl    = _get_ddl(ddl_s3_uri)
    create_stmts = _parse_create_statements(raw_ddl)
    logger.info(f"Parsed {len(create_stmts)} CREATE TABLE statements.")

    # 2. Connect
    params = _get_redshift_params()
    logger.info(f"Connecting to {params['host']} / {params['database']}")

    created = []
    errors  = []

    try:
        conn = redshift_connector.connect(**params)
        # Autocommit: each DDL statement is its own transaction.
        # Without this, one failed CREATE aborts the whole transaction
        # and all subsequent statements fail with 'transaction is aborted'.
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SELECT current_user, current_database()")
            who, db = cur.fetchone()
            logger.info(f"Connected as '{who}' on '{db}'")

            for stmt in create_stmts:
                m = re.search(r'CREATE\s+TABLE\s+"?(\w+)"?', stmt, re.IGNORECASE)
                tname = m.group(1) if m else '?'
                prefixed = _prefix_schema(stmt, schema)
                logger.info(f"Creating {schema}.{tname} ...")
                try:
                    cur.execute(prefixed)
                    created.append(tname)
                    logger.info(f"  -> OK")
                except Exception as e:
                    msg = f"{tname}: {str(e)}"
                    logger.error(msg)
                    errors.append(msg)

        conn.close()
        logger.info("Done.")

    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e), 'created': created, 'errors': errors})
        }

    status = 200 if not errors else 207
    return {
        'statusCode': status,
        'body': json.dumps({
            'created': created,
            'errors':  errors,
        }, indent=2)
    }
