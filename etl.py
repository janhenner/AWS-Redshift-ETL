import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the listening activity (logs) and song metadata from JSON directories in an S3 bucket 
    to the staging area (= two staging tables) of the Redshift database.
    
    Input:
    cur - Database cursor
    conn - Database connection
    """
    for query in copy_table_queries:
        print('======= LOADING STAGING TABLE: ** {} ** ======='.format(str(query).split(' ')[1]))
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Creates all tables using the queries in the `create_table_queries` list.
    
    Input:
    cur - Database cursor
    conn - Database connection
    """
    for query in insert_table_queries:
        print('======= INSERT INTO TABLE: ** {} ** ======='.format(query.strip().split(' ')[2]))
        cur.execute(query)
        conn.commit()


def main():
    """
    Loads the configuration in dwh.cfg;
    
    connects to the database and gets a cursor to it;
    
    Loads data to the staging tables;
    
    Inserts the data from the staging tables into the analytical tables
    
    finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()