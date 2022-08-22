import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events(
        staging_events_id INT IDENTITY(1, 1),
        artist VARCHAR(255),
        auth VARCHAR(50),
        firstName VARCHAR(255),
        gender CHAR(1),
        itemInSession INT,
        lastName VARCHAR(255),
        length DOUBLE PRECISION,
        level CHAR(10),
        location VARCHAR(255),
        method CHAR(3),
        page VARCHAR(50),
        registration DOUBLE PRECISION,
        sessionId BIGINT,
        song VARCHAR(255),
        status INT,
        ts BIGINT,
        userAgent TEXT,
        userId BIGINT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs(
        staging_songs_id INT IDENTITY(1, 1),
        artist_id VARCHAR(255),
        artist_latitude DOUBLE PRECISION,
        artist_location VARCHAR(255),
        artist_longitude DOUBLE PRECISION,
        artist_name VARCHAR(255),
        duration DOUBLE PRECISION,
        num_songs INT,
        song_id VARCHAR(255),
        title VARCHAR(255),
        year INT
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
        songplay_id INT IDENTITY(1, 1),
        start_time TIMESTAMP,
        user_id BIGINT,
        level CHAR(10),
        song_id VARCHAR(255),
        artist_id VARCHAR(255),
        session_id BIGINT,
        location VARCHAR(255),
        user_agent TEXT,
        PRIMARY KEY (songplay_id)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
        user_id INT,
        first_name VARCHAR(255) not null,
        last_name VARCHAR(255),
        gender CHAR(1),
        level CHAR(10),
        PRIMARY KEY (user_id)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
        song_id VARCHAR(255),
        title VARCHAR(255) not null,
        artist_id VARCHAR(255) not null,
        year INT,
        duration DOUBLE PRECISION,
        PRIMARY KEY (song_id)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
        artist_id VARCHAR(255),
        name VARCHAR(255) not null,
        location VARCHAR(255),
        lattitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        PRIMARY KEY (artist_id)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time TIMESTAMP,
        hour SMALLINT,
        day SMALLINT,
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday SMALLINT,
        PRIMARY KEY (start_time)
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF
    json {} region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    COMPUPDATE OFF
    json 'auto' region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
        e.userId,
        e.level,
        s.song_id,
        s.artist_id,
        e.sessionId,
        e.location,
        e.userAgent
    FROM staging_events AS e
    LEFT JOIN staging_songs AS s ON
        e.song = s.title AND
        e.artist = s.artist_name
    WHERE
        e.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId,
        firstName,
        lastName,
        gender,
        level
    FROM staging_events
    WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT start_time,
        EXTRACT(hour FROM start_time),
        EXTRACT(day FROM start_time),
        EXTRACT(week FROM start_time),
        EXTRACT(month FROM start_time),
        EXTRACT(year FROM start_time),
        EXTRACT(weekday FROM start_time)
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
