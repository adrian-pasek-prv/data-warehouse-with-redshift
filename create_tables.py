import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    '''
    Iterate over list of DROP statements and drop existing tables
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Iterate over list of CREATE statements and create tables
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Read config file, establish connection to Redshift and execute SQL statements, finally close the connection
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()