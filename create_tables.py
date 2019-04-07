import psycopg2
from sql_queries import drop_table_queries, create_table_queries


def create_database():
    try:
        # connect to default database
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

        # create a fresh instance of sparkify database with UTF-8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0 ")

        # close connection to default database and reconnect to sparkify
        conn.close()
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
        print(conn)

        cur = conn.cursor()
    except psycopg2.Error as e:
        print('Error: Issue creating the database')
        print(e)

    return cur, conn


def drop_tables(cur, conn):
    try:
        for query in drop_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print('Error: Issue dropping a table')
        print(e)


def create_tables(cur, conn):
    try:
        for query in create_table_queries:
            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print('Error: Issue creating a table')
        print(e)


def main():
    try:
        cur, conn = create_database()
        drop_tables(cur, conn)
        create_tables(cur, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()

main()