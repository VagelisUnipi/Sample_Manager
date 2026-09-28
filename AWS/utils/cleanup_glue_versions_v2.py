import boto3

glue = boto3.client('glue', region_name='eu-central-1')

db_paginator = glue.get_paginator('get_databases')
table_paginator = glue.get_paginator('get_tables')

total_deleted = 0

for db_page in db_paginator.paginate():
    for db in db_page['DatabaseList']:
        db_name = db['Name']
        try:
            for tbl_page in table_paginator.paginate(DatabaseName=db_name):
                for table in tbl_page['TableList']:
                    table_name = table['Name']
                    try:
                        versions = glue.get_table_versions(
                            DatabaseName=db_name, TableName=table_name
                        )['TableVersions']
                        to_delete = [v['VersionId'] for v in versions[1:]]
                        if to_delete:
                            for i in range(0, len(to_delete), 100):
                                glue.batch_delete_table_version(
                                    DatabaseName=db_name,
                                    TableName=table_name,
                                    VersionIds=to_delete[i:i + 100]
                                )
                            total_deleted += len(to_delete)
                            print(f"Cleaned {len(to_delete)} version(s) from {db_name}.{table_name}")
                    except Exception as e:
                        print(f"Skipped {db_name}.{table_name}: {e}")
        except Exception as e:
            print(f"Skipped database {db_name}: {e}")

print(f"\nDone. Total versions deleted: {total_deleted}")
