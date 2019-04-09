# DROP TABLES
fact_songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
dim_user_table_drop = "DROP TABLE IF EXISTS dim_users"
dim_songs_table_drop = "DROP TABLE IF EXISTS dim_songs"
dim_artists_table_drop = "DROP TABLE IF EXISTS dim_artists"
dim_time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
fact_songplay_table_create = """
CREATE TABLE IF NOT EXISTS fact_songplays
(
songplay_id int PRIMARY KEY,
start_time time NOT NULL,
user_id int NOT NULL,
level int NOT NULL,
song_id int NOT NULL,
artist_id int NOT NULL,
session_id int NOT NULL,
location varchar(255),
user_agent varchar(255)
);
"""

dim_user_table_create = """
CREATE TABLE IF NOT EXISTS dim_users 
(
user_id int PRIMARY KEY,
first_name varchar(255) NOT NULL,
last_name varchar(255) NOT NULL,
gender char,
level int NOT NULL
);"""

dim_songs_table_create = """
CREATE TABLE IF NOT EXISTS dim_songs
(
song_id varchar(255) PRIMARY KEY,
title varchar(255) NOT NULL,
artist_id varchar(255) NOT NULL,
year smallint NOT NULL,
duration numeric NOT NULL
);
"""

dim_artists_table_create = """
CREATE TABLE IF NOT EXISTS dim_artists
(
artist_id varchar(255) NOT NULL,
name varchar(255) NOT NULL,
location varchar(255) NOT NULL,
latitude real NULL,
longitude real NULL
);
"""

dim_time_table_create = """
CREATE TABLE IF NOT EXISTS dim_time
(
start_time timestamp,
hour smallint NOT NULL,
day smallint NOT NULL,
week smallint NOT NULL,
month smallint NOT NULL,
year smallint NOT NULL,
weekday varchar(255) NOT NULL
);
"""

# INSERTS
dim_songs_table_insert = "INSERT INTO dim_songs(song_id, title, artist_id, year, duration) " \
                         "VALUES(%s,%s,%s,%s,%s);"
dim_artists_table_insert = "INSERT INTO dim_artists(artist_id, name, location, latitude, longitude) " \
                           "VALUES(%s,%s,%s,%s,%s);"
dim_time_table_insert = "INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday) " \
                        "VALUES(%s,%s,%s,%s,%s,%s,%s);"


# QUERY LISTS
drop_table_queries = [
    fact_songplay_table_drop,
    dim_user_table_drop,
    dim_songs_table_drop,
    dim_artists_table_drop,
    dim_time_table_drop
]

create_table_queries = [
    fact_songplay_table_create,
    dim_user_table_create,
    dim_songs_table_create,
    dim_artists_table_create,
    dim_time_table_create
]