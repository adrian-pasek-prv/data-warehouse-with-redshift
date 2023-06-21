import configparser
import json
import os

# Get the home path string
home = os.path.expanduser('~')

# Import variables from terraform.tvars.json
with open(home + '/github-repos/de-with-aws/terraform/terraform.tvars.json') as f:
    data = json.load(f)
    DWH_DB = data['redshift_database_name']
    DWH_DB_USER = data['redshift_admin_username']
    DWH_DB_PASSWORD = data['redshift_admin_password']

# Import outputs from terraform_output.json
with open(home + '/github-repos/de-with-aws/terraform/terraform_output.json') as f:
    data = json.load(f)
    DWH_ENDPOINT = data['cluster_endpoint']['value']
    DWH_ROLE_ARN = data['redshift_iam_arn']['value']

# Import AWS access key and secret key with configparser
config = configparser.ConfigParser()
config.read_file(open(home + '/.aws/credentials'))

# Import keys from default profile
AWS_ACCESS_KEY = config.get('default','aws_access_key_id')
AWS_SECRET_KEY = config.get('default','aws_secret_access_key')

# Import S3 paths specified in s3_paths.cfg
config.read_file(open('s3_paths.cfg'))

# Import values from S3 profile
LOG_DATA_PATH = config.get('S3','LOG_DATA')
# Mapping of column names if json item names don't match column names in table
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#copy-from-json-examples-using-jsonpaths
LOG_JSONPATH_COL_MAPPING = config.get('S3','LOG_JSONPATH')
SONG_DATA_PATH = config.get('S3','SONG_DATA')

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
        "artist" text,
        "auth" text,
        "firstName" text,
        "gender" varchar(1),
        "itemInSession" smallint,
        "lastName" text,
        "length" double precision,
        "level" text,
        "location" text,
        "method" text,
        "page" text,
        "registration" double precision,
        "sessionId" int,
        "song" text,
        "status" smallint,
        "ts" bigint,
        "userAgent" text,
        "userId" int
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
        "start_time" timestamp,
        "user_id" text,
        "level" text,
        "song_id" text,
        "artist_id" text,
        "session_id" int,
        "location" text,
        "user_agent" text,
        primary key("songplay_id")
    );
""")

user_table_create = ("""
    create table "users" (
        "user_id" text,
        "first_name" text,
        "last_name" text,
        "gender" varchar(1),
        "level" text,
        primary key ("user_id")
    );
""")

song_table_create = ("""
    create table "songs" (
        "song_id" text,
        "title" text,
        "artist_id" text,
        "year" smallint,
        "duration" double precision,
        primary key ("song_id")
    );
""")

artist_table_create = ("""
    create table "artists" (
        "artist_id" text,
        "name" text,
        "location" text,
        "latitude" double precision,
        "longitude" double precision,
        primary key ("artist_id")
    );
""")

time_table_create = ("""
    create table "time" (
        "start_time" timestamp,
        "hour" smallint,
        "day" smallint,
        "week" smallint,
        "month" smallint,
        "year" smallint,
        "weekday" smallint,
        primary key ("start_time")
    );
""")

# STAGING TABLES

staging_events_copy = (f"""
    copy staging_events
    from {LOG_DATA_PATH}
    iam_role {DWH_ROLE_ARN}
    json {LOG_JSONPATH_COL_MAPPING};
""")

staging_songs_copy = (f"""
    copy staging_songs
    from {SONG_DATA_PATH}
    iam_role {DWH_ROLE_ARN}
    json 'auto';
""")

# FINAL TABLES

songplay_table_insert = ("""
    insert into songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select
        to_timestamp(e.ts) as start_time,
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
