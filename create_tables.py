import psycopg2
from sql_queries import \
    drop_table_queries, create_table_queries, dim_songs_table_insert, dim_artists_table_insert, \
    dim_time_table_insert, dim_users_table_insert, select_dim_songs_song_id, select_dim_artist_artist_id, \
    fact_songplays_table_insert


def create_database():
    """
    connects to postgres default database, and drops (if exists) the sparkifydb and recreates it
    """
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
        conn.set_session(autocommit=True)
        cur = conn.cursor()

    # create a fresh instance of sparkify database with UTF-8 encoding
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0 ")
    except psycopg2.Error as e:
        print('Error: Could not drop or create the database')
        print(e)
    finally:
        conn.close()


def connect_to_database():
    try:
        conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
        cur = conn.cursor()
        conn.set_session(autocommit=True)

        return cur, conn
    except psycopg2.Error as e:
        print('Error: Issue creating the database')
        print(e)


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


def insert_into_dim_songs_table(cur, conn, datalist):
    try:
        cur.execute(dim_songs_table_insert, datalist)
        conn.commit()
    except psycopg2.Error as e:
        print('Error while inserting')
        print(e)


def insert_into_dim_artists_table(cur, conn, datalist):
    try:
        cur.execute(dim_artists_table_insert, datalist)
        conn.commit()
    except psycopg2.Error as e:
        print('Error while inserting')
        print(e)


def insert_into_dim_time_table(cur, conn, datalist):
    try:
        cur.execute(dim_time_table_insert, datalist)
        conn.commit()
    except psycopg2.Error as e:
        print('Error while inserting')
        print(e)


def insert_into_dim_user_table(cur, conn, datalist):
    try:
        cur.execute(dim_users_table_insert, datalist)
        conn.commit()
    except psycopg2.Error as e:
        print('Error while inserting')
        print(e)


def insert_into_fact_songplays_table(cur, conn, datalist):
    try:
        cur.execute(fact_songplays_table_insert, datalist)
        conn.commit()
    except psycopg2.Error as e:
        print('Error while inserting')
        print(e)


def get_song_id(cur, conn, datalist):
    try:
        cur.execute(select_dim_songs_song_id, datalist)
        conn.commit()

        return cur.fetchone()

    except psycopg2.Error as e:
        print('Error while selecting')
        print(e)


def get_artist_id(cur, conn, datalist):
    try:
        cur.execute(select_dim_artist_artist_id, datalist)
        conn.commit()

        return cur.fetchone()

    except psycopg2.Error as e:
        print('Error while selecting')
        print(e)


def main():
    """
    Creates a clean instance of the sparkifydb with empty tables
    """
    try:
        create_database()
        cur, conn = connect_to_database()

        drop_tables(cur, conn)
        create_tables(cur, conn)
    finally:
        conn.close()
        cur.close()


# create database and tables
main()


