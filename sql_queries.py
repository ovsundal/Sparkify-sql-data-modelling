# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
songplay_id serial PRIMARY KEY,
timestamp bigint NOT NULL,
user_id int NULL,
level varchar(255) NOT NULL,
song_id varchar(255) NULL,
artist_id varchar(255) NULL,
session_id int NOT NULL,
location varchar(255),
user_agent varchar(255)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
user_id varchar(255) PRIMARY KEY,
first_name varchar(255),
last_name varchar(255),
gender char,
level varchar(255) NOT NULL
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
song_id varchar(255) PRIMARY KEY,
title varchar(255) NOT NULL,
artist_id varchar(255) NOT NULL,
year int NOT NULL,
duration float NOT NULL
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
artist_id varchar(255) NOT NULL,
name varchar(255) NOT NULL,
location varchar(255) NOT NULL,
latitude real NULL,
longitude real NULL
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
timestamp timestamp,
hour int NOT NULL,
day int NOT NULL,
week int NOT NULL,
month int NOT NULL,
year int NOT NULL,
weekday int NOT NULL
);
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays(timestamp, user_id, level, song_id, artist_id,
session_id, location, user_agent) 
VALUES(%s,%s,%s,%s,%s,%s,%s,%s);
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
VALUES(%s,%s,%s,%s,%s)
ON CONFLICT(user_id) DO UPDATE
    SET first_name = excluded.first_name,
        last_name = excluded.last_name,
        gender = excluded.gender,
        level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
VALUES(%s,%s,%s,%s,%s);
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, name, location, latitude, longitude)
VALUES(%s,%s,%s,%s,%s);
""")


time_table_insert = ("""
INSERT INTO time(timestamp, hour, day, week, month, year, weekday)
VALUES(%s,%s,%s,%s,%s,%s,%s);
""")

# FIND SONGS

song_select = ("""
SELECT s.song_id, a.artist_id FROM songs AS s
LEFT OUTER JOIN artists AS a ON s.artist_id = a.artist_id
WHERE s.duration = %s AND s.title = %s AND a.name = %s;
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]