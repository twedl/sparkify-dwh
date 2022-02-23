import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
    song_id TEXT PRIMARY KEY,
    num_songs INT,
    title TEXT,
    artist_name TEXT,
    artist_latitude REAL,
    year INT,
    duration FLOAT,
    artist_id TEXT,
    artist_longitude REAL,
    artist_location TEXT);
""")

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
    event_id BIGINT IDENTITY(0,1) PRIMARY KEY,
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration REAL,
    sessionId INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId TEXT);
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(
    songplay_id TEXT PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES time,
    user_id TEXT NOT NULL REFERENCES users,
    level TEXT,
    song_id TEXT,
    artist_id TEXT,
    session_id INT,
    location TEXT,
    user_agent TEXT);
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users(
    user_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT);
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs(
    song_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_id TEXT NOT NULL,
    year INT,
    duration FLOAT);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists(
    artist_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT,
    latitude REAL,
    longitude REAL);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time(
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT);
""")
# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region 'us-west-2'
    json {};
""").format(config["S3"]["LOG_DATA"], config["IAM_ROLE"]["ARN"], config["S3"]["LOG_JSONPATH"])

staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role {}
    region 'us-west-2'
    json 'auto';
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = (""" 
    INSERT INTO songplays (
        SELECT
            event_id as songplay_id,
            date_add('ms', ts, '1970-01-01') as start_time,
            userid as user_id,
            level,
            b.song_id,
            b.artist_id,
            sessionid as session_id,
            location,
            useragent as user_agent
        FROM staging_events AS a
        LEFT JOIN staging_songs AS b ON a.song = b.title AND a.artist = b.artist_name AND a.length = b.duration
        WHERE a.page = 'NextSong'
    );
""")

user_table_insert = ("""
    INSERT INTO users(
        SELECT DISTINCT userid as user_id, firstname as first_name, lastname as last_name, gender,
        LAST_VALUE(level) OVER(
            PARTITION BY userid
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) as level
        FROM staging_events
    );
""")

song_table_insert = ("""
    INSERT INTO songs(
        SELECT song_id, title, artist_id, year, duration
        FROM staging_songs
    );
""")

artist_table_insert = ("""
    INSERT INTO artists(
        SELECT DISTINCT artist_id, artist_name as name, artist_location as location,
        artist_latitude as latitude, artist_longitude as longitude
        FROM staging_songs
    );
""")


time_table_insert = ("""
    INSERT INTO time(
        SELECT DISTINCT 
        date_add('ms', ts, '1970-01-01') as start_time, 
        EXTRACT(HOUR from start_time) as hour,
        EXTRACT(DAY from start_time) as day,
        EXTRACT(WEEK from start_time) as week,
        EXTRACT(MONTH from start_time) as month,
        EXTRACT(YEAR from start_time) as YEAR,
        EXTRACT(WEEKDAY from start_time) as weekday
        FROM staging_events
    );
""")


# QUERY LISTS
# Split staging and analytical table queries to be able to run the analytical table
# queries by themselves without repeating the staging queries

# Staging queries
create_staging_queries = [staging_events_table_create, staging_songs_table_create]
drop_staging_queries = [staging_events_table_drop, staging_songs_table_drop]
copy_staging_queries = [staging_events_copy, staging_songs_copy]

# Final analytical table queries
create_table_queries = [time_table_create, user_table_create, songplay_table_create, song_table_create, artist_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]

