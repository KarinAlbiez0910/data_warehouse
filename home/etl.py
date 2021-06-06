import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loops through the list of copy_table_queries hereby creating
    the two staging tables staging_events and staging_songs

    Arguments:
        cur: the cursor object. 
        conn: the connection to postgres DB. 

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function loops through the list of insert_table_queries hereby creating
    the definitive tables on the basis of the two staging tables staging_events and staging_songs

    Arguments:
        cur: the cursor object. 
        conn: the connection to postgres DB. 

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function creates a connection to the redshift cluster.
    Subsequently, the functions load_staging_tables and insert_tables are run.

    Arguments:
       None

    Returns:
       None
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