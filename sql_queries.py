import configparser
import json
import os

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    create table "staging_events" (
        "artist_name" text,
        "auth" text,
        "user_first_name" text,
        "user_gender" varchar(1),
        "item_in_session" smallint,
        "user_last_name" text,
        "song_length" double precision,
        "user_level" text,
        "location" text,
        "method" text,
        "page" text,
        "registration" double precision,
        "session_id" int,
        "song_title" text,
        "status" smallint,
        "ts" bigint,
        "user_agent" text,
        "user_id" int
    );
""")

staging_songs_table_create = ("""
    create table "staging_songs" (
        "num_songs" smallint,
        "artist_id" text,
        "artist_latitude" double precision,
        "artist_longitude" double precision,
        "artist_location" text,
        "artist_name" text,
        "song_id" text,
        "title" text,
        "duration" double precision,
        "year" smallint
    );
""")

songplay_table_create = ("""
    create table "songplays" (
        "songplay_id" int identity(0,1),
        "start_time" timestamp not null,
        "user_id" text not null,
        "level" text not null,
        "song_id" text,
        "artist_id" text,
        "session_id" int not null,
        "location" text,
        "user_agent" text not null,
        primary key("songplay_id")
    );
""")

user_table_create = ("""
    create table "users" (
        "user_id" text,
        "first_name" text not null,
        "last_name" text not null,
        "gender" varchar(1) not null,
        "level" text not null,
        primary key ("user_id")
    );
""")

song_table_create = ("""
    create table "songs" (
        "song_id" text,
        "title" text not null,
        "artist_id" text not null,
        "year" smallint not null,
        "duration" double precision not null,
        primary key ("song_id")
    );
""")

artist_table_create = ("""
    create table "artists" (
        "artist_id" text,
        "name" text not null,
        "location" text,
        "latitude" double precision,
        "longitude" double precision,
        primary key ("artist_id")
    );
""")

time_table_create = ("""
    create table "time" (
        "start_time" timestamp,
        "hour" smallint not null,
        "day" smallint not null,
        "week" smallint not null,
        "month" smallint not null,
        "year" smallint not null,
        "weekday" smallint not null,
        primary key ("start_time")
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events
    from {}
    iam_role {}
    region 'us-west-2'
    json {};
""").format(config.get('S3','LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3','LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
    from {}
    iam_role {}
    region 'us-west-2'
    json 'auto';
""").format(config.get('S3','SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select
        timestamp 'epoch' + e.ts / 1000 * interval '1 second' as start_time,
        e.user_id, 
        e.user_level, 
        s.song_id,
        s.artist_id, 
        e.session_id,
        e.location, 
        e.user_agent
    from staging_events e
    join staging_songs s on e.song_title = s.title
        and e.artist_name = s.artist_name
        and e.song_length = s.duration
    and e.page = 'NextSong'
""")

user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select distinct
        user_id,
        user_first_name, 
        user_last_name, 
        user_gender, 
        user_level
    from staging_events
    where page = 'NextSong'
""")

song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select distinct
        song_id, 
        title,
        artist_id,
        year,
        duration
    from staging_songs
    where song_id is not null
""")

artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude) 
    select distinct
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs
    where artist_id is not null
""")

time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    select
        start_time,
        extract(hour from start_time) as hour,
        extract(day from start_time) as day,
        extract(week from start_time) as week, 
        extract(month from start_time) as month,
        extract(year from start_time) as year, 
        extract(dayofweek from start_time) as weekday
    from songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
