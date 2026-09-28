import boto3

glue = boto3.client('glue', region_name='eu-central-1')

# Only clean databases that belong to the QA environment.
QA_DB_PREFIX = '_qa_'

db_paginator = glue.get_paginator('get_databases')
table_paginator = glue.get_paginator('get_tables')

total_deleted = 0

for db_page in db_paginator.paginate():
    for db in db_page['DatabaseList']:
        db_name = db['Name']

        if QA_DB_PREFIX not in db_name:
            print(f"Skipping non-QA database: {db_name}")
            continue

        print(f"Cleaning database: {db_name}")
        try:
            for tbl_page in table_paginator.paginate(DatabaseName=db_name):
                for table in tbl_page['TableList']:
                    table_name = table['Name']
                    try:
                        versions = glue.get_table_versions(
                            DatabaseName=db_name, TableName=table_name
                        )['TableVersions']
                        # Sort descending so index 0 is always the latest
                        # version regardless of API return order.
                        versions = sorted(versions, key=lambda v: int(v['VersionId']), reverse=True)
                        to_delete = [v['VersionId'] for v in versions[1:]]
                        if to_delete:
                            for i in range(0, len(to_delete), 100):
                                glue.batch_delete_table_version(
                                    DatabaseName=db_name,
                                    TableName=table_name,
                                    VersionIds=to_delete[i:i + 100]
                                )
                            total_deleted += len(to_delete)
                            print(f"  Cleaned {len(to_delete)} version(s) from {table_name}")
                    except Exception as e:
                        print(f"  Skipped {table_name}: {e}")
        except Exception as e:
            print(f"Skipped database {db_name}: {e}")

print(f"\nDone. Total versions deleted: {total_deleted}")
