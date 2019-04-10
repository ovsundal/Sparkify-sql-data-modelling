import os
import glob
import psycopg2
import pandas as pd
from create_tables import main, insert_into_dim_songs_table, insert_into_dim_artists_table, \
    insert_into_dim_time_table, insert_into_dim_user_table, get_song_id, get_artist_id, insert_into_fact_songplays_table


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


def insert_into_dim_artists():
    for artist in song_files:
        df = pd.DataFrame(pd.read_json(artist, typ='series'))
        artist_table_data = df[0][['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values.tolist()
        insert_into_dim_artists_table(cur, conn, artist_table_data)


def insert_into_dim_time():
    for log in log_data:
        df = pd.DataFrame(pd.read_json(log, lines=True, typ='series'))

        for row in df[0]:
            time_in_ms = row['ts']
            timestamp = pd.to_datetime(time_in_ms, unit='ms')

            # convert timestamp to all required time units
            time_in_hours = timestamp.hour
            time_in_days = timestamp.day
            time_in_week_of_year = timestamp.week
            time_in_month_of_year = timestamp.month
            time_in_year = timestamp.year
            time_in_weekday = timestamp.weekday()
            time_data = [timestamp, time_in_hours, time_in_days, time_in_week_of_year,
                         time_in_month_of_year, time_in_year, time_in_weekday]

            insert_into_dim_time_table(cur, conn, time_data)


def insert_into_dim_users():
    for log in log_data:
        df = pd.DataFrame(pd.read_json(log, lines=True, typ='series'))
        
        for row in df[0]:
            user_data = [row['userId'], row['firstName'], row['lastName'], row['gender'], row['level']]

            insert_into_dim_user_table(cur, conn, user_data)


def insert_into_fact_songplays():
    """
    Inserts data into the songplays table. Skips insert if song_id or artist_id is None
    """
    for log in log_data:
        df = pd.DataFrame(pd.read_json(log, lines=True, typ='series'))

        for row in df[0]:
            # skip the row insert if None value is present
            if row['song'] is None or row['length'] is None or row['artist'] is None:
                continue

            song_data = [row['song'], row['length']]
            artist_data = [row['artist']]

            # if no song id exists, skip row insert
            song_id = get_song_id(cur, conn, song_data)

            if song_id is None:
                continue

            artist_id = get_artist_id(cur, conn, artist_data)

            # if no artist id exists, skip row insert
            if artist_id is None:
                continue

            time_in_ms = row['ts']
            timestamp = pd.to_datetime(time_in_ms, unit='ms')

            fact_songplays_data = [timestamp, row['userId'], row['level'], song_id, artist_id,
                                   row['sessionId'], row['location'], row['userAgent']]

            insert_into_fact_songplays_table(cur, conn, fact_songplays_data)


# add song files
filepath_song_files = 'data/song_data'
song_files = get_files(filepath_song_files)
insert_into_dim_songs()
insert_into_dim_artists()

# add log files
filepath_log_files = 'data/log_data'
log_data = get_files(filepath_log_files)
insert_into_dim_time()
insert_into_dim_users()
insert_into_fact_songplays()


cur.close()
conn.close()
