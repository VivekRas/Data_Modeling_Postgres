import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    This function 
    a) creates a studentdb (default) database, 
    b) uses that cursor to first drop if exists sparkifydb database and then create it
    c) closes connection to studentdb database
    d) creates a connection and cursor for sparkify database
    
    args : none
    
    output : cur (cursor) and conn (connection)
    """
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    This function reads all the "drop table if exists" queries from SQL_queries.py and executes them
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function reads all the "create table if exists" queries from SQL_queries.py and executes them
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function is the main fns, we use it to call all the other fns one after the other
    and finally close the connection
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()