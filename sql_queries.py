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

staging_events_table_create = ("""
    CREATE TABLE staging_events(
        event_id int IDENTITY(0,1) PRIMARY KEY,
        artist_name varchar,
        auth varchar,
        user_first_name varchar,
        user_gender varchar,
        item_in_session	int,
        user_last_name varchar,
        song_length	float, 
        level varchar,
        location varchar,	
        method varchar,
        page varchar,	
        registration varchar,	
        session_id int,
        song_title varchar,
        status INTEGER, 
        ts varchar,
        user_agent TEXT,	
        user_id int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        song_id varchar PRIMARY KEY,
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location varchar,
        artist_name varchar,
        title varchar,
        song_length float,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE songplays(
        songplay_id int IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id int NOT NULL,
        level varchar NOT NULL,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location text NOT NULL,
        user_agent text NOT NULL
    )
""")

user_table_create = ("""
    CREATE TABLE users(
        user_id int PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE songs(
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        artist_id varchar NOT NULL,
        year int NOT NULL,
        duration float NOT NULL
    )
""")

artist_table_create = ("""
    CREATE TABLE artists(
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location text NOT NULL,
        latitude float,
        longitude float
    )
""")

time_table_create = ("""
    CREATE TABLE time(
        start_time TIMESTAMP PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekDay int NOT NULL
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    iam_role {}
    region 'us-west-2'
    json {};
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs 
    from {} 
    iam_role {}
    region 'us-west-2'
    json 'auto';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    SELECT  
        TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time, 
        staging_events.user_id, 
        staging_events.level, 
        staging_songs.song_id,
        staging_songs.artist_id, 
        staging_events.session_id,
        staging_events.location, 
        staging_events.user_agent
    FROM staging_events INNER JOIN staging_songs ON (
        AND staging_events.song_title = staging_songs.title 
        AND staging_events.artist_name = staging_songs.artist_name
        AND staging_events.song_length = staging_songs.song_length
    )
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT  
        user_id, 
        user_first_name, 
        user_last_name, 
        user_gender, 
        level
    FROM staging_events
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration) 
    SELECT DISTINCT 
        song_id, 
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekDay)
    SELECT start_time, 
        extract(hour from start_time),
        extract(day from start_time),
        extract(week from start_time), 
        extract(month from start_time),
        extract(year from start_time), 
        extract(dayofweek from start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
