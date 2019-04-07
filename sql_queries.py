# DROP TABLES
fact_songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
dim_user_table_drop = "DROP TABLE IF EXISTS dim_users"
dim_songs_table_drop = "DROP TABLE IF EXISTS dim_songs"
dim_artists_table_drop = "DROP TABLE IF EXISTS dim_artists"
dim_time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES
fact_songplay_table_create = "CREATE TABLE IF NOT EXISTS fact_songplays "
"("
"songplay_id int PRIMARY KEY,"
"start_time time NOT NULL,"
"user_id int NOT NULL,"
"level int NOT NULL,"
"song_id int NOT NULL,"
"artist_id int NOT NULL,"
"session_id int NOT NULL,"
"location varchar(255),"
"user_agent varchar(255)"
");"

dim_user_table_create = "CREATE TABLE IF NOT EXISTS dim_users "
"("
"user_id int PRIMARY KEY,"
"first_name varchar(255) NOT NULL,"
"last_name varchar(255) NOT NULL,"
"gender char,"
"level int NOT NULL"
");"

dim_songs_table_create = "CREATE TABLE IF NOT EXISTS dim_songs "
"("
"song_id int PRIMARY KEY,"
"title varchar(255) NOT NULL,"
"artist_id int NOT NULL,"
"year smallint NOT NULL,"
"duration time NOT NULL"
");"

dim_artists_table_create = "CREATE TABLE IF NOT EXISTS dim_artists "
"("
"artist_id int PRIMARY KEY,"
"name varchar(255) NOT NULL,"
"location varchar(255) NOT NULL,"
"latitude point NOT NULL,"
"longitude point NOT NULL"
");"

dim_time_table_create = "CREATE TABLE IF NOT EXISTS dim_time "
"("
"start_time time PRIMARY KEY,"
"hour smallint NOT NULL,"
"day smallint NOT NULL,"
"month smallint NOT NULL,"
"year smallint NOT NULL,"
"weekday varchar(255) NOT NULL"
");"



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