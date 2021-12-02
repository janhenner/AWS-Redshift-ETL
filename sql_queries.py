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

staging_events_table_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
                                    event_id INT IDENTITY(0, 1),
                                    artist varchar,
                                    auth varchar,
                                    firstName varchar,
                                    gender varchar,
                                    itemInSession int,
                                    lastName varchar,
                                    length decimal,
                                    level varchar,
                                    location varchar,
                                    method varchar,
                                    page varchar,
                                    registration varchar,
                                    sessionId int,
                                    song varchar,
                                    status int,
                                    ts timestamp,
                                    userAgent varchar,
                                    userId int,
                                    PRIMARY KEY (event_id)
                                    )
""")    # ts as bigint would allow to import the time format in units since 1970; 
        # however, better appears this approach:
        # ts as timestamp and TIMEFORMAT 'epochmillisecs' in the query staging_events_copy  

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                                    song_id varchar,
                                    num_songs int,
                                    artist_id varchar,
                                    artist_latitude double precision,
                                    artist_longitude double precision,
                                    artist_location varchar,
                                    artist_name varchar,
                                    title varchar,
                                    duration numeric,
                                    year int,
                                    PRIMARY KEY (song_id)
                                    )                                             
""")    # choosing changed from decimal to double precision for artist_latitude, artist_longitude


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                            songplay_id int IDENTITY(0,1), start_time timestamp NOT NULL, 
                            userId int NOT NULL, level varchar, song_id varchar, artist_id varchar, 
                            session_id int, location varchar, user_agent text,
                            PRIMARY KEY(songplay_id))
""")    # Redshift: IDENTITY <- see https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                            userId int, firstName varchar, lastName varchar, 
                            gender char(1), level varchar, 
                            PRIMARY KEY(userId))
""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                            song_id varchar, title varchar, artist_id varchar, year int, duration numeric, 
                            PRIMARY KEY(song_id))
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                            artist_id varchar, artist_name varchar, artist_location varchar, 
                            artist_latitude double precision, artist_longitude double precision, 
                            PRIMARY KEY(artist_id))
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                            start_time timestamp, hour int, day int, week int, month int, 
                            year int, weekday int, 
                            PRIMARY KEY(start_time))
""") 

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            json {}
                            region 'us-west-2'
                            TIMEFORMAT 'epochmillisecs';
""").\
                            format( config.get('S3', 'log_data'),
                                    config.get('IAM_ROLE', 'arn'),
                                    config.get('S3', 'log_jsonpath')
                           ) 
    # for timeformat: 
    # https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat

staging_songs_copy = ("""COPY staging_songs FROM {}
                            CREDENTIALS 'aws_iam_role={}'
                            json 'auto'
                            region 'us-west-2' COMPUPDATE OFF STATUPDATE OFF;
""").\
                            format( config.get('S3', 'song_data'),
                                    config.get('IAM_ROLE', 'arn')
                            ) 
    # for songs JSON data no jsonpaths_file is provided ->
    # without JSON in copy query? -> error: "Delimiter not found"
    # -> use json 'auto' as in:
    # https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
    # query did run for >15min, Redshift Queries show in GUI at "COPY ANALYZE staging_songs" step;
    # therefore added: COMPUPDATE OFF STATUPDATE OFF 
    # <- inspired by https://www.intermix.io/blog/improve-redshift-copy-performance/
    

# FINAL TABLES

songplay_table_insert = ("""
                        INSERT INTO songplays 
                        (
                            start_time, userId, level, song_id, artist_id, session_id, location, user_agent
                        ) 
                        SELECT DISTINCT 
                            e.ts start_time, 
                            e.userId, 
                            e.level,
                            s.song_id,
                            s.artist_id,
                            e.sessionId,
                            e.location,
                            e.userAgent
                        FROM staging_events e
                        LEFT JOIN staging_songs s ON 
                            (s.artist_name = e.artist
                            AND s.title = e.song
                            AND s.duration = e.length)
                        WHERE e.page = 'NextSong';            
""")

user_table_insert = ("""
                        INSERT INTO users 
                        (
                            userId, firstName, lastName, gender, level
                        ) 
                        SELECT DISTINCT 
                            userId, 
                            firstName, 
                            lastName, 
                            gender, 
                            level
                        FROM staging_events e1
                        WHERE   e1.page = 'NextSong' and 
                                e1.userId IS NOT NULL and
                                e1.ts = (   select max(ts) from staging_events e2
                                            where e1.userId = e2.userId);            
""")    # filtering "WHERE page = " without the table prefix yields:
        # psycopg2.NotSupportedError: Function: "page" is using an OID value reserved for GIS
    
        # for the Postgres database we could use: ON CONFLICT (user_id) DO UPDATE SET level = EXCLUDED.level;
        # In Redshift it has to be handled differently: WHERE condition such that we do not update if there is already a newer timestamp for this user

song_table_insert = ("""
                        INSERT INTO songs 
                        (
                            song_id, title, artist_id, year, duration
                        ) 
                        SELECT DISTINCT 
                            song_id,
                            title, 
                            artist_id,
                            year, 
                            duration
                        FROM staging_songs
                        WHERE   song_id IS NOT NULL and 
                                song_id NOT IN (select distinct song_id from songs);
""")    # for a Postgres database we could use: ON CONFLICT (song_id) DO NOTHING;
        # In Redshift we use: WHERE song_id NOT IN (select distinct song_id from songs);

artist_table_insert = ("""
                        INSERT INTO artists 
                        (
                            artist_id, artist_name, artist_location, artist_latitude, artist_longitude
                        ) 
                        SELECT DISTINCT 
                            artist_id, 
                            artist_name, 
                            artist_location, 
                            artist_latitude, 
                            artist_longitude
                        FROM staging_songs
                        WHERE   artist_id IS NOT NULL and
                                artist_id NOT IN (select distinct artist_id from artists);
""")
    
time_table_insert = ("""
                        INSERT INTO time 
                        (
                            start_time, hour, day, week, month, year, weekday
                        ) 
                        SELECT DISTINCT 
                            ts as start_time, 
                            EXTRACT(hour from ts),
                            EXTRACT(day from ts),
                            EXTRACT(week from ts),
                            EXTRACT(month from ts),
                            EXTRACT(year from ts),
                            EXTRACT(weekday from ts)
                        FROM staging_events
                        WHERE   staging_events.page = 'NextSong' and
                                start_time IS NOT NULL and
                                start_time NOT IN (select distinct start_time from time);
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]