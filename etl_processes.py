import os
import glob
import psycopg2
import pandas as pd
from create_tables import main, insert_into_dim_songs_table

# create database and tables
main()

# create connection to db
try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=postgres")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the Database")
    print(e)

conn.set_session(autocommit=True)


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))
    return all_files


def insert_into_dim_songs():
    for song in song_files:
        df = pd.DataFrame(pd.read_json(song, typ='series'))
        song_table_data = df[0][['song_id', 'title', 'artist_id', 'year', 'duration']].values.tolist()
        insert_into_dim_songs_table(cur, conn, song_table_data)


filepath = 'data/song_data'
song_files = get_files(filepath)
insert_into_dim_songs()



cur.close()
conn.close()