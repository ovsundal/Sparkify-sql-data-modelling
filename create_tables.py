import psycopg2

try:
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkify user=postgres password=postgres")
except psycopg2.Error as e:
    print("Error: Could not make connection to the Postgres database")
    print(e)

try:
    cur = conn.cursor()
except psycopg2.Error as e:
    print("Error: Could not get cursor to the Database")
    print(e)

conn.set_session(autocommit=True)

# drop tables if exists
try:
    cur.execute("DROP TABLE IF EXISTS fact_songplays")
except psycopg2.Error as e:
    print("Error: Issue dropping table 'fact_songplays'")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS dim_users")
except psycopg2.Error as e:
    print("Error: Issue dropping table 'dim_users'")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS dim_songs")
except psycopg2.Error as e:
    print("Error: Issue dropping table 'dim_songs'")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS dim_artists")
except psycopg2.Error as e:
    print("Error: Issue dropping table 'dim_songs'")
    print(e)

try:
    cur.execute("DROP TABLE IF EXISTS dim_time")
except psycopg2.Error as e:
    print("Error: Issue dropping table 'dim_songs'")
    print(e)


# create tables
try:
    cur.execute("CREATE TABLE IF NOT EXISTS fact_songplays "
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
                "")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS dim_users "
                "("
                "user_id int PRIMARY KEY,"
                "first_name varchar(255) NOT NULL,"
                "last_name varchar(255) NOT NULL,"
                "gender char,"
                "level int NOT NULL"
                ");"
                "")
except psycopg2.Error as e:
    print("Error: Issue creating table")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS dim_songs "
                "("
                "song_id int PRIMARY KEY,"
                "title varchar(255) NOT NULL,"
                "artist_id int NOT NULL,"
                "year smallint NOT NULL,"
                "duration time NOT NULL"
                ");"
                "")
except psycopg2.Error as e:
    print("Error: Issue creating table 'dim_songs'")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS dim_artists "
                "("
                "artist_id int PRIMARY KEY,"
                "name varchar(255) NOT NULL,"
                "location varchar(255) NOT NULL,"
                "latitude point NOT NULL,"
                "longitude point NOT NULL"
                ");"
                "")
except psycopg2.Error as e:
    print("Error: Issue creating table 'dim_artists'")
    print(e)

try:
    cur.execute("CREATE TABLE IF NOT EXISTS dim_time "
                "("
                "start_time time PRIMARY KEY,"
                "hour smallint NOT NULL,"
                "day smallint NOT NULL,"
                "month smallint NOT NULL,"
                "year smallint NOT NULL,"
                "weekday varchar(255) NOT NULL"
                ");"
                "")
except psycopg2.Error as e:
    print("Error: Issue creating table 'dim_time'")
    print(e)


cur.close()
conn.close()