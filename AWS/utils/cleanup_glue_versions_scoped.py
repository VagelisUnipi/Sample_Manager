"""
Scoped Glue TABLE_VERSION cleanup.

Deletes all but the latest catalog version for every table in a fixed list of
databases. Paginates get_table_versions (the earlier scripts did not, so they
could only ever remove the first ~100 versions per table and never drained the
backlog that hit the account-wide 1,000,000 TABLE_VERSION limit).

Usage (AWS CloudShell):
    python3 cleanup_glue_versions_scoped.py            # dry run — prints what it would delete
    python3 cleanup_glue_versions_scoped.py --execute  # actually deletes
"""
import sys
import boto3

REGION = 'eu-central-1'

# Target selection. A database is cleaned if its name starts with any of
# DB_PREFIXES OR is listed exactly in DATABASES. Leave one empty to use the other.
DB_PREFIXES = []
DATABASES = [
    'prsm-gdb-prd-curated_erp002',
    'prsm-gdb-prd-curated_erp003',
    'prsm-gdb-prd-curated_erp004',
    'prsm-gdb-prd-curated_erp009',
    'prsm-gdb-prd-curated_erp016',
    'prsm-gdb-prd-curated_erp022',
    'prsm-gdb-prd-curated_erp023',
    'prsm-gdb-prd-curated_erp024',
    'prsm-gdb-prd-curated_erp025',
    'prsm-gdb-prd-curated_erp026',
    'prsm-gdb-prd-curated_erp027',
    'prsm-gdb-prd-curated_erp028',
    'prsm-gdb-prd-curated_erp031',
    'prsm-gdb-prd-curated_erp035',
    'prsm-gdb-prd-curated_erp036',
]

DRY_RUN = '--execute' not in sys.argv

glue  = boto3.client('glue', region_name=REGION)
db_p  = glue.get_paginator('get_databases')
tbl_p = glue.get_paginator('get_tables')
ver_p = glue.get_paginator('get_table_versions')


def resolve_databases() -> list:
    """Return the concrete database names to clean, from prefixes + exact names."""
    selected = set(DATABASES)
    if DB_PREFIXES:
        for page in db_p.paginate():
            for db in page['DatabaseList']:
                if any(db['Name'].startswith(p) for p in DB_PREFIXES):
                    selected.add(db['Name'])
    return sorted(selected)


def main() -> None:
    databases = resolve_databases()
    mode = 'DRY RUN' if DRY_RUN else 'EXECUTE'
    print(f"Mode: {mode} | region: {REGION} | matched {len(databases)} database(s):")
    for db in databases:
        print(f"  - {db}")
    print()

    total = 0
    for db in databases:
        print(f"DB: {db}")
        tables_seen = 0
        try:
            for tbl_page in tbl_p.paginate(DatabaseName=db):
                for table in tbl_page['TableList']:
                    tables_seen += 1
                    name = table['Name']
                    version_ids = []
                    for ver_page in ver_p.paginate(DatabaseName=db, TableName=name):
                        version_ids += [v['VersionId'] for v in ver_page['TableVersions']]
                    # Keep the latest version, drop the rest.
                    to_delete = sorted(version_ids, key=int, reverse=True)[1:]
                    # Always report what was inspected so an empty result is explainable.
                    print(f"  {name}: {len(version_ids)} version(s), "
                          f"{'would delete' if DRY_RUN else 'delete'} {len(to_delete)}")
                    if not to_delete:
                        continue
                    if not DRY_RUN:
                        for i in range(0, len(to_delete), 100):
                            glue.batch_delete_table_version(
                                DatabaseName=db,
                                TableName=name,
                                VersionIds=to_delete[i:i + 100],
                            )
                    total += len(to_delete)
            if tables_seen == 0:
                print("  (no tables in this database)")
        except glue.exceptions.EntityNotFoundException:
            print(f"  !! database not found: {db}")

    verb = 'would delete' if DRY_RUN else 'deleted'
    print(f"\n{verb} {total} version(s) across {len(databases)} database(s)")


if __name__ == '__main__':
    main()
