import boto3

glue = boto3.client('glue', region_name='eu-central-1')

db_paginator = glue.get_paginator('get_databases')
table_paginator = glue.get_paginator('get_tables')

total_versions = 0
db_summary = {}

for db_page in db_paginator.paginate():
    for db in db_page['DatabaseList']:
        db_name = db['Name']
        db_versions = 0
        try:
            for tbl_page in table_paginator.paginate(DatabaseName=db_name):
                for table in tbl_page['TableList']:
                    table_name = table['Name']
                    try:
                        versions = glue.get_table_versions(
                            DatabaseName=db_name, TableName=table_name
                        )['TableVersions']
                        count = len(versions)
                        db_versions += count
                        total_versions += count
                    except Exception:
                        pass
        except Exception as e:
            print(f"Could not access {db_name}: {e}")
        db_summary[db_name] = db_versions

print(f"\n{'Database':<60} {'Versions':>10}")
print('-' * 72)
for db_name, count in sorted(db_summary.items(), key=lambda x: x[1], reverse=True):
    print(f"{db_name:<60} {count:>10}")

print('-' * 72)
print(f"{'TOTAL':<60} {total_versions:>10}")
