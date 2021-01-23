import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
"""
drop_tables function to drop tables

cur is the cursor object
conn is the connection object
"""
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()



def create_tables(cur, conn):
"""
create_tables function to drop tables
"""
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print('1')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('2')
    cur = conn.cursor()
    print('3')
    drop_tables(cur, conn)
    print('4')
    create_tables(cur, conn)
    print('5')
    conn.close()
    if __name__ == "__main__":
        main()
