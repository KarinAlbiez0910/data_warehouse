import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table if exists staging_events"
staging_songs_table_drop = "DROP table if exists staging_songs"

songplay_table_drop = "DROP table if exists songplays"
user_table_drop = "DROP table if exists users"
song_table_drop = "DROP table if exists songs"
artist_table_drop = "DROP table if exists artists"
time_table_drop = "DROP table if exists time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE if not exists staging_events
                                 (  
                                       event_id BIGINT IDENTITY(0,1),
                                       artist text,
                                       auth text,
                                       firstName text,
                                       gender text,
                                       itemInSession int,
                                       lastName text,
                                       length float,
                                       level text,
                                       location text,
                                       method text,
                                       page text,
                                       registration float,
                                       sessionId int,
                                       song text, 
                                       status int, 
                                       ts bigint,
                                       userAgent text,
                                       userId text
                                 )
                             ;""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs
                                 (
                                     num_songs int,
                                     artist_id text,
                                     artist_latitude float,
                                     artist_longitude float,
                                     artist_location text,
                                     artist_name text,
                                     song_id text,
                                     title text,
                                     duration float,
                                     year int
                                 )
                             ;""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays
                             (
                                 songplay_id INT IDENTITY(0,1) PRIMARY KEY sortkey, 
                                 start_time timestamp NOT NULL,
                                 user_id VARCHAR NOT NULL, 
                                 level text,
                                 song_id text,
                                 artist_id text NOT NULL,
                                 session_id int,
                                 location text,
                                 user_agent text
                             )
                        diststyle even;""")


user_table_create = ("""CREATE TABLE IF NOT EXISTS users
                         (
                             user_id VARCHAR PRIMARY KEY sortkey,
                             first_name text,
                             last_name text,
                             gender text,
                             level text
                         )
                     diststyle all;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs
                        (
                            song_id text PRIMARY KEY sortkey, 
                            title text, 
                            artist_id text, 
                            year int, 
                            duration float
                        )
                    diststyle all;""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists
                          (
                              artist_id text PRIMARY KEY sortkey, 
                              artist_name text, 
                              artist_location text, 
                              artist_latitude float, 
                              artist_longitude float
                          )
                      diststyle all;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time
                        (
                            start_time timestamp PRIMARY KEY sortkey, 
                            hour int, 
                            day int, 
                            week int, 
                            month int, 
                            year int, 
                            weekday int
                        )
                    diststyle all;""")

# STAGING TABLES

staging_events_copy = (f"""
                            copy staging_events from {config.get('S3','LOG_DATA')}
                            credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
                            JSON {config.get('S3','LOG_JSONPATH')}
                            compupdate off region 'us-west-2';
                        """)

staging_songs_copy = (f"""
                            COPY staging_songs FROM {config.get('S3','SONG_DATA')}
                            credentials 'aws_iam_role={config.get('IAM_ROLE', 'ARN')}'
                            JSON 'auto' 
                            compupdate off region 'us-west-2';
                      """)


# FINAL TABLES

songplay_table_insert = ("""
                            INSERT INTO songplays (
                                                     start_time,
                                                     user_id, 
                                                     level,
                                                     song_id,
                                                     artist_id,
                                                     session_id,
                                                     location,
                                                     user_agent
                                                  )
                             SELECT DISTINCT timestamp 'epoch' + events.ts/1000 * interval '1 second' as start_time,
                                             events.userId,
                                             events.level,
                                             songs.song_id,
                                             songs.artist_id,
                                             events.sessionId,
                                             events.location,
                                             events.userAgent
                            FROM staging_events AS events
                            JOIN staging_songs AS songs
                            ON (events.artist = songs.artist_name)
                            AND (events.song = songs.title)
                            AND (events.length = songs.duration)
                            WHERE events.page = 'NextSong'
                            ;""")

user_table_insert = ("""
                    INSERT INTO users (
                                         user_id,
                                         first_name,
                                         last_name,
                                         gender,
                                         level
                                      )
                    SELECT DISTINCT userId AS user_id,
                                    firstName AS first_name,
                                    lastName AS last_name,
                                    gender AS gender,
                                    level AS level
                    FROM staging_events
                    ;""")

song_table_insert = ("""
                    INSERT INTO songs (
                                        song_id,
                                        title,
                                        artist_id,
                                        year,
                                        duration
                                      )
                    SELECT DISTINCT song_id,
                                    title,
                                    artist_id,
                                    year,
                                    duration
                    FROM staging_songs
                    ;""")


artist_table_insert = ("""
                        INSERT INTO artists (
                                              artist_id, 
                                              artist_name,
                                              artist_location,
                                              artist_latitude,
                                              artist_longitude
                                            )
                        SELECT DISTINCT artist_id AS artist_id,
                                        artist_name AS artist_name,
                                        artist_location AS artist_location,
                                        artist_latitude AS artist_latitude,
                                        artist_longitude AS artist_longitude
                        FROM staging_songs
                        ;""")

time_table_insert = ("""INSERT INTO time (
                                          start_time, 
                                          hour, 
                                          day, 
                                          week, 
                                          month, 
                                          year, 
                                          weekday)

                        SELECT DISTINCT a.start_time,
                        EXTRACT (HOUR FROM a.start_time), 
                        EXTRACT (DAY FROM a.start_time),
                        EXTRACT (WEEK FROM a.start_time), 
                        EXTRACT (MONTH FROM a.start_time),
                        EXTRACT (YEAR FROM a.start_time), 
                        EXTRACT (WEEKDAY FROM a.start_time) FROM
                        (SELECT TIMESTAMP 'epoch' + staging_events.ts/1000 *INTERVAL '1 second' as start_time FROM staging_events) a
                       ;""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
