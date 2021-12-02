import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    Drops all tables for multiple runs of create_tables.py using the queries in the `drop_table_queries` list.
    
    Input:
    cur - Database cursor
    conn - Database connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    Creates all tables using the queries in the `create_table_queries` list. 
    
    Input:
    cur - Database cursor
    conn - Database connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    Loads the configuration in dwh.cfg;
    
    connects to the database and gets a cursor to it;
    
    drops all tables if exists;
    
    creates all tables needed;
    
    finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    ROLE_ARN = config.get('IAM_ROLE', 'arn')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()