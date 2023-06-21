import psycopg2
import json
import os
from sql_queries import create_table_queries, drop_table_queries

# Get the home path string
home = os.path.expanduser('~')

# Import variables from terraform.tvars.json
with open(home + '/github-repos/de-with-aws/terraform/terraform.tvars.json') as f:
    data = json.load(f)
    DWH_DB = data['redshift_database_name']
    DWH_DB_USER = data['redshift_admin_username']
    DWH_DB_PASSWORD = data['redshift_admin_password']

# Import outputs from terraform_output.json
with open(home + '/github-repos/de-with-aws/terraform/terraform_output.json') as f:
    data = json.load(f)
    DWH_ENDPOINT = data['cluster_endpoint']['value']

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():

    conn = psycopg2.connect(f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()