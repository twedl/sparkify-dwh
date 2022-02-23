import configparser
import psycopg2
from sql_queries import copy_staging_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Creates each table using the queries in `copy_staging_queries` list. 
    INPUTS:
        - cur: psycopg2 database cursor 
        - conn: psycopg2 database connection
    OUTPUTS:
        - None
    """
    for query in copy_staging_queries:
        print(query)
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    Creates each table using the queries in `insert_table_queries` list. 
    INPUTS:
        - cur: psycopg2 database cursor 
        - conn: psycopg2 database connection
    OUTPUTS:
        - None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Loads all the staging tables from s3
    
    - Insert the data from the staging tables to the final analytical tables
    
    - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()