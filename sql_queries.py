import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DWH_ROLE_ARN = config.get("IAM_ROLE", "ARN")
DWH_LOG_DATA = config.get("S3", "LOG_DATA")
DWH_LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
DWH_SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= (""" 
    CREATE TABLE IF NOT EXISTS staging_events (
    artist          varchar ,
    auth            varchar, 
    firstName       varchar, 
    gender          varchar, 
    itemInSession   int, 
    lastName        varchar, 
    length          float, 
    level           varchar, 
    location        varchar, 
    method          varchar, 
    page            varchar, 
    registration    float, 
    sessionId       int, 
    song            varchar, 
    status          int, 
    ts              bigint, 
    user_agent      varchar, 
    userId          int                          
);
""")

staging_songs_table_create = (""" 
    CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs       int , 
    artist_id       varchar , 
    artist_latitude float , 
    artist_longitude float , 
    artist_location varchar(max) , 
    artist_name     varchar(max),         
    song_id         varchar , 
    title           varchar(max) , 
    duration        float, 
    year            int                  
);
""")

songplay_table_create = (""" 
    CREATE TABLE IF NOT EXISTS songplay (
    songplay_id     bigint identity(0, 1), 
    start_time      timestamp NOT NULL, 
    user_id         int NOT NULL, 
    level           varchar, 
    song_id         varchar, 
    artist_id       varchar, 
    session_id      varchar, 
    location        varchar, 
    user_agent      varchar                    
);
""")

user_table_create = (""" 
    CREATE TABLE IF NOT EXISTS users ( 
    user_id         int PRIMARY KEY , 
    first_name      varchar, 
    last_name       varchar, 
    gender          varchar, 
    level           varchar                
);
""")

song_table_create = (""" 
    CREATE TABLE IF NOT EXISTS song (
    song_id         varchar PRIMARY KEY, 
    title           varchar(max), 
    artist_id       varchar, 
    year            int, 
    duration        float                
);
""")

artist_table_create = (""" 
    CREATE TABLE IF NOT EXISTS artist (
    artist_id      varchar PRIMARY KEY, 
    name            varchar(max), 
    location        varchar(max), 
    latitude        float, 
    longitude       float                
);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time      timestamp PRIMARY KEY, 
    hour            int, 
    day             int, 
    week            int, 
    month           int, 
    year            int, 
    weekday         int                        
);
""")

# STAGING TABLES

staging_events_copy = (""" copy staging_events from {}
    credentials 'aws_iam_role={}'
    json {} compupdate off region 'us-west-2' timeformat as 'epochmillisecs';
""").format(DWH_LOG_DATA, DWH_ROLE_ARN, DWH_LOG_JSONPATH)

staging_songs_copy = (""" copy staging_songs from {}
    credentials 'aws_iam_role={}'
    json 'auto' compupdate off region 'us-west-2';
""").format(DWH_SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplay (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent
    )
    SELECT 
        (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') as start_time,
        se.userId as user_id,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId as session_id,
        se.location,
        se.user_agent
   FROM staging_events se
   LEFT JOIN staging_songs ss
        ON se.song = ss.title
        AND se.artist = ss.artist_name
        AND se.length = ss.duration
   WHERE se.page = 'NextSong'
        AND se.userId IS NOT NULL
        AND ss.artist_id IS NOT NULL; 
""")

user_table_insert = (""" INSERT INTO users (
    user_id, 
    first_name,
    last_name,
    gender,
    level
    )
    SELECT
        userId as user_id,
        firstName as first_name,
        lastName as last_name,
        gender,
        level
    FROM staging_events
    WHERE userId IS NOT NULL
    ;
""")

song_table_insert = (""" INSERT INTO song (
    song_id,
    title,
    artist_id,
    year,
    duration
    )
    SELECT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE artist_id IS NOT NULL
    ;
""")

artist_table_insert = (""" INSERT INTO artist (
    artist_id,
    name,
    location,
    latitude,
    longitude
    )
    SELECT
        ss.artist_id,
        ss.artist_name as name,
        se.location,
        ss.artist_latitude as latitude,
        ss.artist_longitude as longitude
    FROM staging_events se
    LEFT JOIN staging_songs ss
        ON se.song = ss.title
        AND se.artist = ss.artist_name
        AND se.length = ss.duration
    WHERE ss.artist_id IS NOT NULL    
        ;
""")

time_table_insert = (""" INSERT INTO time (
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday
    )
    SELECT 
        (TIMESTAMP 'epoch' + se.ts/1000*INTERVAL '1 second') as start_time,
        extract(hour from start_time) as hour,
        extract(day from start_time) as day,
        extract(week from start_time) as week,
        extract(month from start_time) as month,
        extract(year from start_time) as year,
        extract(weekday from start_time) as dow
    FROM staging_events se
        ;   
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
