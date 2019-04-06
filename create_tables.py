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




cur.close()
conn.close()