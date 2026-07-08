"""
Generates mock CSV.GZ files for all 16 standard tables discovered from
SAMPLEMANAGER_DKQ_DFX.md. Output goes to mocks/ with naming DKQ-{TABLE}.csv.gz
matching the config file convention DKQ-{TABLE}.json.
"""
import csv
import gzip
import io
import os

MOCKS_DIR = os.path.join(os.path.dirname(__file__), "mocks")
os.makedirs(MOCKS_DIR, exist_ok=True)

TABLES = {
    "MENSUEL": {
        "columns": ["ANALYSE", "ANNUEL", "MESURE", "PRODUIT", "PRODUCT_VERSION", "LOCALISATION", "CODE_CONTROLE"],
        "rows": [
            ["ANA001", "F", "PH", "PROD001", "1", "LOC01", "CC001"],
            ["ANA002", "F", "TURBIDITY", "PROD002", "2", "LOC02", "CC002"],
            ["ANA003", "F", "CONDUCTIVITY", "PROD001", "1", "LOC03", ""],
        ],
    },
    "SAMPLE": {
        "columns": ["ID_NUMERIC"],
        "rows": [
            ["100001"],
            ["100002"],
            ["100003"],
        ],
    },
    "SAMPLE_POINT": {
        "columns": ["IDENTITY"],
        "rows": [
            ["SP-INLET-01"],
            ["SP-OUTLET-01"],
            ["SP-PROCESS-01"],
        ],
    },
    "VERSIONED_COMPONENT": {
        "columns": ["NAME", "ANALYSIS", "ANALYSIS_VERSION"],
        "rows": [
            ["PH", "ANA001", "1"],
            ["TURBIDITY", "ANA002", "1"],
            ["CONDUCTIVITY", "ANA001", "2"],
        ],
    },
    "VERSIONED_ANALYSIS": {
        "columns": ["IDENTITY", "ANALYSIS_VERSION"],
        "rows": [
            ["ANA001", "1"],
            ["ANA001", "2"],
            ["ANA002", "1"],
        ],
    },
    "MLP_HEADER": {
        "columns": ["IDENTITY", "PRODUCT_VERSION"],
        "rows": [
            ["PROD001", "1"],
            ["PROD002", "1"],
            ["PROD001", "2"],
        ],
    },
    "CUSTOMER": {
        "columns": ["IDENTITY"],
        "rows": [
            ["CUST-001"],
            ["CUST-002"],
            ["CUST-003"],
        ],
    },
    "LOCATION": {
        "columns": ["IDENTITY"],
        "rows": [
            ["LOC01"],
            ["LOC02"],
            ["LOC03"],
        ],
    },
    "CODE_CONTROLE": {
        "columns": ["IDENTITY"],
        "rows": [
            ["CC001"],
            ["CC002"],
            ["CC003"],
        ],
    },
    "FOURNISSEUR": {
        "columns": ["IDENTITY"],
        "rows": [
            ["FURN-001"],
            ["FURN-002"],
            ["FURN-003"],
        ],
    },
    "MLP_VIEW": {
        "columns": ["ANALYSIS_ID", "COMPONENT_NAME", "PRODUCT_ID", "PRODUCT_VERSION"],
        "rows": [
            ["ANA001", "PH", "PROD001", "1"],
            ["ANA002", "TURBIDITY", "PROD002", "1"],
            ["ANA001", "CONDUCTIVITY", "PROD001", "2"],
        ],
    },
    "SAMP_TEST_RESULT_LAFA": {
        "columns": [
            "ANALYSIS", "ANALYSIS_VERSION", "COMPONENT_NAME", "PRODUCT", "PRODUCT_VERSION",
            "SAMPLING_POINT", "LOCATION_ID", "DESTINATION_MATIERE",
            "CODE_CONTROLE", "CUSTOMER_ID", "FOURNISSEUR",
            "STATUS", "RESULT_STATUS", "TEST_STATUS",
            "ORIGINAL_SAMPLE", "JOB_NAME", "GRANULOMETRIE", "FORMAT", "TEST_SCHEDULE",
        ],
        "rows": [
            ["ANA001", "1", "PH", "PROD001", "1", "SP-INLET-01", "LOC01", "DEST01",
             "CC001", "CUST-001", "FURN-001", "V", "V", "V", "100001", "JOB-2026-001", "GRAN01", "FMT01", "SCH01"],
            ["ANA002", "1", "TURBIDITY", "PROD002", "1", "SP-OUTLET-01", "LOC02", "DEST02",
             "CC002", "CUST-002", "FURN-002", "V", "V", "V", "100002", "JOB-2026-002", "GRAN02", "FMT01", "SCH02"],
            ["ANA001", "2", "CONDUCTIVITY", "PROD001", "2", "SP-PROCESS-01", "LOC03", "DEST01",
             "CC001", "CUST-001", "FURN-003", "V", "V", "V", "100003", "JOB-2026-003", "", "FMT02", "SCH01"],
        ],
    },
    "JOB_HEADER": {
        "columns": ["JOB_NAME"],
        "rows": [
            ["JOB-2026-001"],
            ["JOB-2026-002"],
            ["JOB-2026-003"],
        ],
    },
    "PHRASE": {
        "columns": ["PHRASE_ID", "PHRASE_TYPE"],
        "rows": [
            ["GRAN01", "GRANULO"],
            ["GRAN02", "GRANULO"],
            ["GRAN03", "GRANULO"],
        ],
    },
    "DESTINATION": {
        "columns": ["IDENTITY"],
        "rows": [
            ["DEST01"],
            ["DEST02"],
            ["DEST03"],
        ],
    },
    "PHRASE_FORMAT": {
        "columns": ["PHRASE_ID", "PHRASE_TYPE"],
        "rows": [
            ["FMT01", "FORMAT"],
            ["FMT02", "FORMAT"],
            ["FMT03", "FORMAT"],
        ],
    },
}

for table_name, definition in TABLES.items():
    out_path = os.path.join(MOCKS_DIR, f"DKQ-{table_name}.csv.gz")
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(definition["columns"])
    writer.writerows(definition["rows"])

    with gzip.open(out_path, "wt", encoding="utf-8") as gz:
        gz.write(buf.getvalue())

    print(f"  created: DKQ-{table_name}.csv.gz  ({os.path.getsize(out_path)} bytes)")

print(f"\nDone — {len(TABLES)} files written to mocks/")
