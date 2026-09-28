import os
import json
import logging
import boto3
import redshift_connector


# Configure Logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Schema names are overridable via env vars; defaults match the architecture.
EXT_SCHEMA = os.environ.get('EXT_SCHEMA', 'prsm_edhid_samplemanager_ext')
DW_SCHEMA  = os.environ.get('DW_SCHEMA', 'prsm_edhid_samplemanager_dw')

# DW column -> Redshift type, mirroring DDL/ALL_TABLES_REDSHIFT.sql.
# The ext (Spectrum) tables expose every CSV column as string, so each column
# is cast explicitly. The ext partition column `source` fills SOURCE and the
# Glue run timestamp fills LAST_UPDATED_AT.
# A 3rd tuple element gives the ext column name when the source extract names
# it differently than the DW model (e.g. MLP_HEADER key is ENTRY_CODE).

# SAMP_TEST_RESULT_LAFA and C_SAMP_TEST_RESULT_LAFA are identical flat fact
# tables (81 source columns). Column names match the extract header exactly,
# including the source typo TES_ON_SPEC.
SAMP_TEST_RESULT_LAFA_COLUMNS = [
    ("ID_NUMERIC", "BIGINT"),
    ("ID_TEXT", "VARCHAR(100)"),
    ("STATUS", "CHAR(1)"),
    ("CUSTOMER_ID", "VARCHAR(50)"),
    ("FOURNISSEUR", "VARCHAR(50)"),
    ("PRODUCT", "VARCHAR(50)"),
    ("PRODUCT_VERSION", "BIGINT"),
    ("COMMENTAIRES", "VARCHAR(500)"),
    ("LOGIN_BY", "VARCHAR(50)"),
    ("LOGIN_DATE", "VARCHAR(20)"),
    ("SAMPLED_DATE", "VARCHAR(20)"),
    ("DUREE_STOCKAGE", "VARCHAR(50)"),
    ("CONDITIONNEMENT", "VARCHAR(50)"),
    ("NUM_LOT", "VARCHAR(50)"),
    ("ON_SPEC", "CHAR(1)"),
    ("DATE_PREL_VP", "VARCHAR(20)"),
    ("LOCATION_ID", "VARCHAR(50)"),
    ("DESTINATION_MATIERE", "VARCHAR(50)"),
    ("CODE_CONTROLE", "VARCHAR(50)"),
    ("SAMPLING_POINT", "VARCHAR(50)"),
    ("LOT_AN", "VARCHAR(50)"),
    ("ORIGINAL_SAMPLE", "BIGINT"),
    ("TEST_SCHEDULE", "VARCHAR(50)"),
    ("JOB_NAME", "VARCHAR(100)"),
    ("OLD_STATUS", "CHAR(1)"),
    ("RECD_DATE", "VARCHAR(20)"),
    ("DATE_STARTED", "VARCHAR(20)"),
    ("DATERESREQ", "VARCHAR(20)"),
    ("DATE_COMPLETED", "VARCHAR(20)"),
    ("COMPLETER", "VARCHAR(50)"),
    ("DATERESAVAIL", "VARCHAR(20)"),
    ("TESTS_TO_DO", "BIGINT"),
    ("BATCH_NAME", "VARCHAR(100)"),
    ("SAMPLE_TYPE", "VARCHAR(50)"),
    ("SAMPLE_NAME", "VARCHAR(100)"),
    ("DESCRIPTION", "VARCHAR(500)"),
    ("PREPARATION", "VARCHAR(100)"),
    ("STANDARD", "VARCHAR(50)"),
    ("BATCH_ID", "VARCHAR(50)"),
    ("HAS_INCIDENTS", "CHAR(1)"),
    ("PRELEVEUR", "VARCHAR(50)"),
    ("SAMPLE_AUTO", "CHAR(1)"),
    ("ANALYSE_AUTO", "CHAR(1)"),
    ("TONNAGE", "VARCHAR(50)"),
    ("UNIT_TYPE", "VARCHAR(50)"),
    ("COQ", "CHAR(1)"),
    ("PRODUIT_QUALIFIE", "VARCHAR(50)"),
    ("GRANULOMETRIE", "VARCHAR(50)"),
    ("REF_LIVRAISON", "VARCHAR(50)"),
    ("DATE_LIVRAISON", "VARCHAR(20)"),
    ("FORMAT", "VARCHAR(50)"),
    ("TEST_NUMBER", "BIGINT"),
    ("TEST_COUNT", "BIGINT"),
    ("REPLICATE_TEST", "CHAR(1)"),
    ("ANALYSIS", "VARCHAR(50)"),
    ("TEST_STATUS", "CHAR(1)"),
    ("TECHNICIEN", "VARCHAR(50)"),
    ("TEST_DATE_STARTED", "VARCHAR(20)"),
    ("TEST_DATE_COMPLETED", "VARCHAR(20)"),
    ("INSTRUMENT", "VARCHAR(50)"),
    ("TES_ON_SPEC", "CHAR(1)"),
    ("ORDER_NUM", "BIGINT"),
    ("VALIDATION_STATUS", "VARCHAR(50)"),
    ("AUTHORISATION_COMMENT", "VARCHAR(500)"),
    ("TEST_HAS_INCIDENTS", "CHAR(1)"),
    ("ANALYSIS_VERSION", "BIGINT"),
    ("COMPONENT_NAME", "VARCHAR(100)"),
    ("RESULT_TYPE", "VARCHAR(50)"),
    ("RESULT_STATUS", "CHAR(1)"),
    ("RESULT_TEXT", "VARCHAR(200)"),
    ("RESULT_VALUE", "VARCHAR(50)"),
    ("UNITS", "VARCHAR(50)"),
    ("MINIMUM", "VARCHAR(50)"),
    ("MAXIMUM", "VARCHAR(50)"),
    ("OUT_OF_RANGE", "CHAR(1)"),
    ("DATE_RESULT_ENTERED", "VARCHAR(20)"),
    ("RESULT_ENTERED_BY", "VARCHAR(50)"),
    ("ORDER_NUMBER_BY", "BIGINT"),
    ("TYPICAL", "VARCHAR(50)"),
    ("CALCULATION", "VARCHAR(500)"),
    ("PLACES", "BIGINT"),
]

TABLES = {
    "versioned_analysis": [
        ("IDENTITY", "VARCHAR(50)"),
        ("ANALYSIS_VERSION", "BIGINT"),
        ("ANALYSIS_ORDER", "BIGINT"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("ANALYSIS_TYPE", "VARCHAR(50)"),
        ("DESCRIPTION", "VARCHAR(200)"),
        ("INSTTYPE_ID", "VARCHAR(50)"),
        ("PREPARATION_ID", "VARCHAR(50)"),
        ("EXPECTED_TIME", "VARCHAR(50)"),
        ("CHARGECODE", "VARCHAR(50)"),
        ("RESULT_SCREEN", "VARCHAR(50)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
        ("CALCULATION", "VARCHAR(500)"),
        ("REPLICATES", "BIGINT"),
        ("ACCESS_FLAGS", "BIGINT"),
        ("COMP_REPLICATES", "BIGINT"),
        ("GENERIC_ANALYSIS", "CHAR(1)"),
        ("APPROVAL_REQD", "CHAR(1)"),
        ("ALLOW_CATEGORY", "CHAR(1)"),
        ("APPROVAL_STATUS", "CHAR(1)"),
        ("INSPECTION_PLAN", "VARCHAR(50)"),
        ("SAMPLING_PLAN_TYPE", "VARCHAR(50)"),
        ("AUTO_VALIDATE", "CHAR(1)"),
        ("SHIPPING_FLAG", "CHAR(1)"),
    ],
    "versioned_component": [
        ("NAME", "VARCHAR(100)"),
        ("ANALYSIS", "VARCHAR(50)"),
        ("ANALYSIS_VERSION", "BIGINT"),
        ("ORDER_NUMBER", "BIGINT"),
        ("RESULT_TYPE", "VARCHAR(50)"),
        ("UNITS", "VARCHAR(50)"),
        ("MINIMUM", "VARCHAR(50)"),
        ("MAXIMUM", "VARCHAR(50)"),
        ("TRUE_WORD", "VARCHAR(50)"),
        ("FALSE_WORD", "VARCHAR(50)"),
        ("ALLOWED_CHARACTERS", "VARCHAR(200)"),
        ("CALCULATION", "VARCHAR(500)"),
        ("PLACES", "BIGINT"),
        ("REP_CONTROL", "VARCHAR(50)"),
        ("REPLICATES", "VARCHAR(10)"),
        ("SIG_FIGS_NUMBER", "BIGINT"),
        ("SIG_FIGS_ROUNDING", "VARCHAR(50)"),
        ("SIG_FIGS_FILTER", "VARCHAR(50)"),
        ("MINIMUM_PQL", "VARCHAR(50)"),
        ("MAXIMUM_PQL", "VARCHAR(50)"),
        ("PQL_CALCULATION", "VARCHAR(500)"),
        ("FORMULA", "VARCHAR(500)"),
        ("MATRIX_NO", "BIGINT"),
        ("MATRIX_NAME", "VARCHAR(50)"),
        ("COLUMN_NO", "BIGINT"),
        ("COLUMN_NAME", "VARCHAR(50)"),
        ("ROW_NO", "BIGINT"),
        ("ROW_NAME", "VARCHAR(50)"),
        ("DESCRIPTION", "VARCHAR(200)"),
        ("DESCRIPTION_CERQFRA", "VARCHAR(200)"),
    ],
    "mlp_header": [
        ("IDENTITY", "VARCHAR(50)", "ENTRY_CODE"),
        ("PRODUCT_GROUP", "VARCHAR(50)"),
        ("DESCRIPTION", "VARCHAR(200)"),
        ("TEST_SCHEDULE", "VARCHAR(50)"),
        ("PRODUCT_STATUS", "VARCHAR(50)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
        ("VERSION_COMMENT", "VARCHAR(200)"),
        ("VERSION_STATUS", "VARCHAR(50)"),
        ("CREATED_DATE", "VARCHAR(20)"),
        ("APPROVAL_REQD", "CHAR(1)"),
        ("APPROVAL_STATUS", "CHAR(1)"),
        ("INSPECTION_PLAN", "VARCHAR(50)"),
        ("DUREE_ARCHI", "VARCHAR(50)"),
    ],
    "sample": [
        ("ID_NUMERIC", "BIGINT"),
        ("ID_TEXT", "VARCHAR(50)"),
        ("JOB_NAME", "VARCHAR(100)"),
        ("STATUS", "CHAR(1)"),
        ("OLD_STATUS", "CHAR(1)"),
        ("COMPARED", "CHAR(1)"),
        ("ON_SPEC", "CHAR(1)"),
        ("RE_SAMPLED", "CHAR(1)"),
        ("ORIGINAL_SAMPLE", "BIGINT"),
        ("LINK_NUMBER", "BIGINT"),
        ("LOGIN_DATE", "VARCHAR(20)"),
        ("LOGIN_BY", "VARCHAR(50)"),
        ("SAMPLED_DATE", "VARCHAR(20)"),
        ("RECD_DATE", "VARCHAR(20)"),
        ("DATE_STARTED", "VARCHAR(20)"),
        ("STARTER", "VARCHAR(50)"),
        ("DATERESREQ", "VARCHAR(20)"),
        ("DATE_COMPLETED", "VARCHAR(20)"),
        ("COMPLETER", "VARCHAR(50)"),
        ("DATERESAVAIL", "VARCHAR(20)"),
        ("DATE_AUTHORISED", "VARCHAR(20)"),
        ("AUTHORISER", "VARCHAR(50)"),
        ("AUTHORISATION_NOTES", "VARCHAR(500)"),
        ("PRODUCT", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
        ("GRADE_CODE", "VARCHAR(50)"),
        ("TESTS_TO_DO", "BIGINT"),
        ("ON_WKS", "CHAR(1)"),
        ("PROJECT_ID", "VARCHAR(50)"),
        ("BATCH_NAME", "VARCHAR(100)"),
        ("SAMPLING_POINT", "VARCHAR(50)"),
        ("SAMPLE_TYPE", "VARCHAR(50)"),
        ("SAMPLE_NAME", "VARCHAR(100)"),
        ("DESCRIPTION", "VARCHAR(500)"),
        ("PREPARATION", "VARCHAR(100)"),
        ("HAZARD", "VARCHAR(100)"),
        ("PRIORITY", "BIGINT"),
        ("LOCATION_ID", "VARCHAR(50)"),
        ("CUSTOMER_ID", "VARCHAR(50)"),
        ("INVOICE_NUMBER", "VARCHAR(50)"),
        ("TEST_SCHEDULE", "VARCHAR(50)"),
        ("TEMPLATE_ID", "VARCHAR(50)"),
        ("COMP_PROD_NAME", "VARCHAR(100)"),
        ("COMP_PROD_VER", "BIGINT"),
        ("COMP_PROD_GRADE", "VARCHAR(50)"),
        ("STANDARD", "VARCHAR(50)"),
        ("STANDARD_ID", "VARCHAR(50)"),
        ("STANDARD_VERSION", "BIGINT"),
        ("INSPECTION_HEADER", "VARCHAR(50)"),
        ("STANDARD_TYPE", "VARCHAR(50)"),
        ("BATCH_ID", "VARCHAR(50)"),
        ("DYNAMIC_PRODUCT", "VARCHAR(50)"),
        ("TOTAL_ELEMENTS", "BIGINT"),
        ("SAMPLING_PROCEDURE", "VARCHAR(50)"),
        ("REPLICATE_LINK_NO", "BIGINT"),
        ("AUTO_VALIDATE", "CHAR(1)"),
        ("HAS_INCIDENTS", "CHAR(1)"),
        ("CODE_CONTROLE", "VARCHAR(50)"),
        ("CONDITIONNEMENT", "VARCHAR(50)"),
        ("DESTINATION_MATIERE", "VARCHAR(50)"),
        ("FOURNISSEUR", "VARCHAR(50)"),
        ("NUM_LOT", "VARCHAR(50)"),
        ("PRELEVEUR", "VARCHAR(50)"),
        ("DUREE_STOCKAGE", "VARCHAR(50)"),
        ("SAMPLE_AUTO", "CHAR(1)"),
        ("INDEX_POLAB", "VARCHAR(50)"),
        ("NUM_POLAB", "VARCHAR(50)"),
        ("COMMENTAIRES", "VARCHAR(500)"),
        ("LOT_AN", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("ANALYSE_AUTO", "CHAR(1)"),
        ("DATE_PREL_VP", "VARCHAR(20)"),
        ("TONNAGE", "VARCHAR(50)"),
        ("PLT_NUMBER_FROM", "VARCHAR(20)"),
        ("PLT_NUMBER_TO", "VARCHAR(20)"),
        ("UNIT_TYPE", "VARCHAR(50)"),
        ("PLT_NUMBER", "VARCHAR(20)"),
        ("LAF_BATCH", "VARCHAR(50)"),
        ("DATE_SHIPPED", "VARCHAR(20)"),
        ("PACKAGE", "VARCHAR(50)"),
        ("SHIPPING", "VARCHAR(50)"),
        ("SHIP_USR", "VARCHAR(50)"),
        ("COQ", "CHAR(1)"),
        ("SUBMIT_SHIP", "CHAR(1)"),
        ("PRODUIT_QUALIFIE", "VARCHAR(50)"),
        ("GRANULOMETRIE", "VARCHAR(50)"),
        ("REF_LIVRAISON", "VARCHAR(50)"),
        ("DATE_LIVRAISON", "VARCHAR(20)"),
        ("LOT_FOURNISSEUR", "VARCHAR(50)"),
        ("TAS_STOCKAGE", "VARCHAR(50)"),
        ("FORMAT", "VARCHAR(50)"),
    ],
    "sample_point": [
        ("IDENTITY", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("POINT_LOCATION", "VARCHAR(50)"),
        ("SAMPLE_TYPE", "VARCHAR(50)"),
        ("DESCRIPTION", "VARCHAR(200)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
    ],
    "customer": [
        ("IDENTITY", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("COMPANY_NAME", "VARCHAR(200)"),
        ("ADDRESS1", "VARCHAR(100)"),
        ("ADDRESS2", "VARCHAR(100)"),
        ("ADDRESS3", "VARCHAR(100)"),
        ("ADDRESS4", "VARCHAR(100)"),
        ("ADDRESS5", "VARCHAR(100)"),
        ("ADDRESS6", "VARCHAR(100)"),
        ("PHONE_NUM", "VARCHAR(50)"),
        ("CONTACT", "VARCHAR(100)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
        ("SUSPENDFLAG", "CHAR(1)"),
    ],
    "location": [
        ("IDENTITY", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("DESCRIPTION", "VARCHAR(200)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
        ("ORDER_NUMBER", "BIGINT"),
    ],
    "code_controle": [
        ("IDENTITY", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("DESCRIPTION", "VARCHAR(200)"),
        ("PRODUIT", "VARCHAR(50)"),
        ("ORIGINE_MATIERE", "VARCHAR(50)"),
        ("POINT_PRELEVEMENT", "VARCHAR(50)"),
        ("CODE_MAT_POLAB", "VARCHAR(50)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
    ],
    "fournisseur": [
        ("IDENTITY", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("COMPANY_NAME", "VARCHAR(200)"),
        ("CONTACT", "VARCHAR(100)"),
        ("TELEPHONE", "VARCHAR(50)"),
        ("ADDRESS1", "VARCHAR(100)"),
        ("ADDRESS2", "VARCHAR(100)"),
        ("ADDRESS3", "VARCHAR(100)"),
        ("ADDRESS4", "VARCHAR(100)"),
        ("MODIFIED_ON", "VARCHAR(20)"),
        ("MODIFIED_BY", "VARCHAR(50)"),
        ("MODIFIABLE", "CHAR(1)"),
        ("REMOVEFLAG", "CHAR(1)"),
    ],
    "job_header": [
        ("JOB_NAME", "VARCHAR(100)"),
        ("JOB_STATUS", "CHAR(1)"),
        ("BROWSE_DESCRIPTION", "VARCHAR(200)"),
        ("SAMPLE_SYNTAX_ID", "VARCHAR(50)"),
        ("DATE_CREATED", "VARCHAR(20)"),
        ("DATE_COMPLETED", "VARCHAR(20)"),
        ("DATE_RECEIVED", "VARCHAR(20)"),
        ("DATE_TO_START", "VARCHAR(20)"),
        ("SUBMITTER_OPER", "VARCHAR(50)"),
        ("SAMPLES_TO_DO", "BIGINT"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("TEMPLATE_ID", "VARCHAR(50)"),
        ("SAMPLE_TEMPLATE", "VARCHAR(50)"),
        ("DATE_AUTHORISED", "VARCHAR(20)"),
        ("AUTHORISER", "VARCHAR(50)"),
        ("AUTHORISATION_NOTES", "VARCHAR(500)"),
        ("OLD_STATUS", "CHAR(1)"),
        ("INSPECTION_HEADER", "VARCHAR(50)"),
        ("NODE_TRIGGER_PATH", "VARCHAR(200)"),
        ("ORIGINAL_JOB", "VARCHAR(100)"),
        ("BEEN_COPIED", "CHAR(1)"),
        ("AUTO_VALIDATE", "CHAR(1)"),
        ("TOTAL_ELEMENTS", "BIGINT"),
        ("LOT_ID", "VARCHAR(50)"),
        ("LOT_NUMBER", "VARCHAR(50)"),
        ("BAD_PALLETS", "BIGINT"),
        ("QTY_AVAILBLE", "BIGINT"),
        ("FOURNISSEUR", "VARCHAR(50)"),
        ("GRANULOMETRIE", "VARCHAR(50)"),
        ("REF_LIVRAISON", "VARCHAR(50)"),
        ("DATE_LIVRAISON", "VARCHAR(20)"),
        ("TONNAGE", "VARCHAR(50)"),
        ("LOT_FOURNISSEUR", "VARCHAR(50)"),
        ("TAS_STOCKAGE", "VARCHAR(50)"),
        ("FORMAT", "VARCHAR(50)"),
    ],
    "phrase": [
        ("PHRASE_TYPE", "VARCHAR(20)"),
        ("PHRASE_ID", "VARCHAR(50)"),
        ("ORDER_NUM", "BIGINT"),
        ("PHRASE_TEXT", "VARCHAR(500)"),
        ("ICON", "VARCHAR(100)"),
    ],
    "c_sample": [
        ("ID_NUMERIC", "BIGINT"),
        ("ID_TEXT", "VARCHAR(50)"),
        ("JOB_NAME", "VARCHAR(100)"),
        ("STATUS", "CHAR(1)"),
        ("OLD_STATUS", "CHAR(1)"),
        ("COMPARED", "CHAR(1)"),
        ("ON_SPEC", "CHAR(1)"),
        ("RE_SAMPLED", "CHAR(1)"),
        ("ORIGINAL_SAMPLE", "BIGINT"),
        ("LINK_NUMBER", "BIGINT"),
        ("LOGIN_DATE", "VARCHAR(20)"),
        ("LOGIN_BY", "VARCHAR(50)"),
        ("SAMPLED_DATE", "VARCHAR(20)"),
        ("RECD_DATE", "VARCHAR(20)"),
        ("DATE_STARTED", "VARCHAR(20)"),
        ("STARTER", "VARCHAR(50)"),
        ("DATERESREQ", "VARCHAR(20)"),
        ("DATE_COMPLETED", "VARCHAR(20)"),
        ("COMPLETER", "VARCHAR(50)"),
        ("DATERESAVAIL", "VARCHAR(20)"),
        ("DATE_AUTHORISED", "VARCHAR(20)"),
        ("AUTHORISER", "VARCHAR(50)"),
        ("AUTHORISATION_NOTES", "VARCHAR(500)"),
        ("PRODUCT", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
        ("GRADE_CODE", "VARCHAR(50)"),
        ("TESTS_TO_DO", "BIGINT"),
        ("ON_WKS", "CHAR(1)"),
        ("PROJECT_ID", "VARCHAR(50)"),
        ("BATCH_NAME", "VARCHAR(100)"),
        ("SAMPLING_POINT", "VARCHAR(50)"),
        ("SAMPLE_TYPE", "VARCHAR(50)"),
        ("SAMPLE_NAME", "VARCHAR(100)"),
        ("DESCRIPTION", "VARCHAR(500)"),
        ("PREPARATION", "VARCHAR(100)"),
        ("HAZARD", "VARCHAR(100)"),
        ("PRIORITY", "BIGINT"),
        ("LOCATION_ID", "VARCHAR(50)"),
        ("CUSTOMER_ID", "VARCHAR(50)"),
        ("INVOICE_NUMBER", "VARCHAR(50)"),
        ("TEST_SCHEDULE", "VARCHAR(50)"),
        ("TEMPLATE_ID", "VARCHAR(50)"),
        ("COMP_PROD_NAME", "VARCHAR(100)"),
        ("COMP_PROD_VER", "BIGINT"),
        ("COMP_PROD_GRADE", "VARCHAR(50)"),
        ("STANDARD", "VARCHAR(50)"),
        ("STANDARD_ID", "VARCHAR(50)"),
        ("STANDARD_VERSION", "BIGINT"),
        ("INSPECTION_HEADER", "VARCHAR(50)"),
        ("STANDARD_TYPE", "VARCHAR(50)"),
        ("BATCH_ID", "VARCHAR(50)"),
        ("DYNAMIC_PRODUCT", "VARCHAR(50)"),
        ("TOTAL_ELEMENTS", "BIGINT"),
        ("SAMPLING_PROCEDURE", "VARCHAR(50)"),
        ("REPLICATE_LINK_NO", "BIGINT"),
        ("AUTO_VALIDATE", "CHAR(1)"),
        ("HAS_INCIDENTS", "CHAR(1)"),
        ("CODE_CONTROLE", "VARCHAR(50)"),
        ("CONDITIONNEMENT", "VARCHAR(50)"),
        ("DESTINATION_MATIERE", "VARCHAR(50)"),
        ("FOURNISSEUR", "VARCHAR(50)"),
        ("NUM_LOT", "VARCHAR(50)"),
        ("PRELEVEUR", "VARCHAR(50)"),
        ("DUREE_STOCKAGE", "VARCHAR(50)"),
        ("SAMPLE_AUTO", "CHAR(1)"),
        ("INDEX_POLAB", "VARCHAR(50)"),
        ("NUM_POLAB", "VARCHAR(50)"),
        ("COMMENTAIRES", "VARCHAR(500)"),
        ("LOT_AN", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
        ("ANALYSE_AUTO", "CHAR(1)"),
        ("DATE_PREL_VP", "VARCHAR(20)"),
        ("TONNAGE", "VARCHAR(50)"),
        ("PLT_NUMBER_FROM", "VARCHAR(20)"),
        ("PLT_NUMBER_TO", "VARCHAR(20)"),
        ("UNIT_TYPE", "VARCHAR(50)"),
        ("PLT_NUMBER", "VARCHAR(20)"),
        ("LAF_BATCH", "VARCHAR(50)"),
        ("DATE_SHIPPED", "VARCHAR(20)"),
        ("PACKAGE", "VARCHAR(50)"),
        ("SHIPPING", "VARCHAR(50)"),
        ("SHIP_USR", "VARCHAR(50)"),
        ("COQ", "CHAR(1)"),
        ("SUBMIT_SHIP", "CHAR(1)"),
        ("PRODUIT_QUALIFIE", "VARCHAR(50)"),
        ("GRANULOMETRIE", "VARCHAR(50)"),
        ("REF_LIVRAISON", "VARCHAR(50)"),
        ("DATE_LIVRAISON", "VARCHAR(20)"),
        ("LOT_FOURNISSEUR", "VARCHAR(50)"),
        ("TAS_STOCKAGE", "VARCHAR(50)"),
        ("FORMAT", "VARCHAR(50)"),
    ],
    # Both share the identical 81-column structure (see above).
    "samp_test_result_lafa": SAMP_TEST_RESULT_LAFA_COLUMNS,
    "c_samp_test_result_lafa": SAMP_TEST_RESULT_LAFA_COLUMNS,
    "mensuel": [
        ("IDENT_NUMBER", "BIGINT"),
        ("ID_DATE", "VARCHAR(20)"),
        ("CODE_CONTROLE", "VARCHAR(50)"),
        ("PRODUIT", "VARCHAR(50)"),
        ("PRODUCT_VERSION", "BIGINT"),
        ("LOCALISATION", "VARCHAR(50)"),
        ("POINT_PRELEVEMENT", "VARCHAR(50)"),
        ("ANALYSE", "VARCHAR(50)"),
        ("MESURE", "VARCHAR(100)"),
        ("ANNUEL", "CHAR(1)"),
        ("NB_MESURE", "BIGINT"),
        ("MOYENNE", "VARCHAR(50)"),
        ("ECART_TYPE", "VARCHAR(50)"),
        ("ECART_TYPE_BDQ", "VARCHAR(50)"),
        ("SPEC_MIN", "VARCHAR(50)"),
        ("SPEC_MAX", "VARCHAR(50)"),
        ("CIBLE", "VARCHAR(50)"),
        ("GROUP_ID", "VARCHAR(50)"),
    ],
}


def _cast_expr(column, sql_type, ext_column=None):
    # DW alias is uppercase-quoted (matches the DDL column names).
    # Ext column reference is lowercased — Glue catalog normalises all column
    # names to lowercase, and Redshift Spectrum quoted identifiers are
    # case-sensitive, so uppercase refs return NULL against the ext table.
    dw_col = f'"{column}"'
    src = f'"{(ext_column or column).lower()}"'
    if sql_type == "BIGINT":
        return f"CAST(NULLIF(TRIM({src}), '') AS BIGINT) AS {dw_col}"
    return f"CAST({src} AS {sql_type}) AS {dw_col}"


# TEMPORARY WORKAROUND (remove once the upstream export is fixed):
# The SAMP_TEST_RESULT_LAFA extracts can arrive corrupt — a failed Oracle
# SQL*Plus spool writes error text (e.g. "ORA-00942...", "FROM SMFP...") into
# the file as junk rows, which lands in the numeric id_numeric column and aborts
# the BIGINT cast. Drop rows whose id_numeric is not a clean integer so the load
# still runs and only valid fact rows reach the DW. Delete this dict when the
# source delivers clean data again.
ROW_FILTERS = {
    "samp_test_result_lafa":   'TRIM("id_numeric") ~ \'^[0-9]+$\'',
    "c_samp_test_result_lafa": 'TRIM("id_numeric") ~ \'^[0-9]+$\'',
}


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
    where_clause = ROW_FILTERS.get(table)
    insert_sql = (
        f"INSERT INTO {dw_table} ({col_list})\n"
        f"SELECT\n"
        f"    {select_list},\n"
        f'    CAST("source" AS VARCHAR(10)) AS "SOURCE",\n'
        f'    CAST("last_updated_at" AS TIMESTAMP) AS "LAST_UPDATED_AT"\n'
        f"FROM {ext_table}"
        + (f"\nWHERE {where_clause}" if where_clause else "")
    )
    return [f"DELETE FROM {dw_table}", insert_sql]


QUERIES = {table: _build_queries(table, columns) for table, columns in TABLES.items()}


def lambda_handler(event, context):
    """
    Reloads DW tables from the Spectrum ext schema (DELETE + INSERT with casts).

    Event structure:
      { "table_name": "fournisseur" }  -> reload that one table
      {} or { "table_name": "all" }    -> reload all 15 tables in one transaction
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
