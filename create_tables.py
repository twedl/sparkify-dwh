import configparser
import psycopg2
from sql_queries import create_staging_queries, drop_staging_queries, create_table_queries, drop_table_queries

def drop_staging_tables(cur, conn):
    """
    Drops each table using the queries in `drop_staging_queries` list.
    INPUTS:
        - cur: psycopg2 database cursor 
        - conn: psycopg2 database connection
    """
    for query in drop_staging_queries:
        cur.execute(query)
        conn.commit()

def create_staging_tables(cur, conn):
    """
    Creates each table using the queries in `create_staging_queries` list. 
    INPUTS:
        - cur: psycopg2 database cursor 
        - conn: psycopg2 database connection
    """
    for query in create_staging_queries:
        cur.execute(query)
        conn.commit()
        
def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    INPUTS:
        - cur: psycopg2 database cursor 
        - conn: psycopg2 database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    INPUTS:
        - cur: psycopg2 database cursor 
        - conn: psycopg2 database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and creates the sparkify redshift database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the staging tables
    - Create the staging tables
    
    - Drops all the final tables
    - Creates all the final tables needed. 
    
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_staging_tables(cur, conn)
    create_staging_tables(cur, conn)
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()