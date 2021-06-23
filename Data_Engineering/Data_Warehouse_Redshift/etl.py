import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

"""
 This method copies tables from s3 into redshift i.e (extract and transform).
 
 Input:
 * cur the cursor variable for the connection to redshift
 * conn the connection
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

"""

This method insert information into redshift tables created from the creates_tales.py (load)
 
 Input:
 * cur the cursor variable for the connection to redshift
 * conn the connection

"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
 
    load_staging_tables(cur, conn)

    insert_tables(cur, conn)
    

#     conn.close()


if __name__ == "__main__":
    main()