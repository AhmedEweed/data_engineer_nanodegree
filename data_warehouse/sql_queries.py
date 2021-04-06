import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop =  "drop table if exists staging_songs;"
songplay_table_drop =       "drop table if exists songplays;"
user_table_drop =           "drop table if exists users;"
song_table_drop =           "drop table if exists songs;"
artist_table_drop =         "drop table if exists artists;"
time_table_drop =           "drop table if exists time;"

# CREATE TABLES

staging_events_table_create= ("""
create table if not exists staging_events
( 
 artist        varchar(100),
 auth          varchar(10),
 firstName     varchar(15),
 gender        varchar(1),
 itemInSession integer,
 lastName      varchar(15),
 length        float,
 level         varchar(10),
 location      varchar(50),
 method        varchar(3),
 page          varchar(10),
 registration  float,
 sessionId     integer,
 song          varchar(30),
 status        integer,
 ts            timestamp,
 userAgent     varchar(50),
 userId        integer 
)
""")

staging_songs_table_create = ("""
create table if not exists staging_songs
(
num_songs        integer,
artist_id        varchar(20),
artist_latitude  float,
artist_longitude float, 
artist_location  varchar(20),
artist_name      varchar(20), 
song_id          varchar(20), 
title            varchar(30), 
duration         float,
year             integer
)
""")

songplay_table_create = ("""
create table if not exists songplays
(
songplay_id integer IDENTITY(0, 1) PRIMARY KEY, 
start_time  timestamp not null     SORTKEY DISTKEY, 
user_id     integer   not null, 
level       varchar(10), 
song_id     varchar(20) not null, 
artist_id   varchar(20) not null, 
session_id  integer, 
location    varchar(30), 
user_agent  varchar(50)
)
""")

user_table_create = ("""
create table if not exists users
(
user_id    integer SORTKEY PRIMARY KEY, 
first_name varchar(10) not null, 
last_name  varchar(10) not null, 
gender     varchar(1) not null, 
level      varchar(10) not null
)
""")

song_table_create = ("""
create table if not exists songs 
(
song_id   varchar(20) not null SORTKEY PRIMARY KEY, 
title     varchar(30) not null, 
artist_id varchar(20) not null, 
year      integer     not null, 
duration  float
)
""")

artist_table_create = ("""
create table if not exists artists 
(
artist_id varchar(20) not null SORTKEY PRIMARY KEY, 
name      varchar(20) not null,
location  varchar(30),
latitude float,
longitude float
)
""")

time_table_create = ("""
create table if not exists time 
(
start_time timestamp   not null DISTKEY SORTKEY PRIMARY KEY, 
hour       integer     not null, 
day        integer     not null, 
week       integer     not null, 
month      integer     not null, 
year       integer     not null, 
weekday    varchar(10) not null
)
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {data_bucket}
credentials 'aws_iam_role = {role_arn}'
region 'us-east-2' format as JSON {log_json_path}
timeformat as 'epochmillisecs' ;
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
copy staging_songs from {data_bucket}
credentials 'aws_iam_role = {role_arn}'
region 'us-east-2' format as JSON auto;
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT  DISTINCT(e.ts)  AS start_time,
        e.userId        AS user_id,
        e.level         AS level,
        s.song_id       AS song_id,
        s.artist_id     AS artist_id,
        e.sessionId     AS session_id,
        e.location      AS location,
        e.userAgent     AS user_agent
    FROM staging_events e
    JOIN staging_songs  s   ON (e.song = s.title AND e.artist = s.artist_name)
    AND e.page  ==  'NextSong'
""")

user_table_insert = ("""
insert into users (user_id, first_name, last_name, gender, level)
SELECT  DISTINCT(userId)    AS user_id,
        firstName           AS first_name,
        lastName            AS last_name,
        gender,
        level
    FROM staging_events
    WHERE user_id IS NOT NULL
    AND page  ==  'NextSong';
""")

song_table_insert = ("""
insert into songs (song_id, title, artist_id, year, duration)
 SELECT  DISTINCT(song_id) AS song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
insert into artists (artist_id, name, location, latitude, longitude)
SELECT  DISTINCT(artist_id) AS artist_id,
        artist_name         AS name,
        artist_location     AS location,
        artist_latitude     AS latitude,
        artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
insert into time (start_time, hour, day, week, month, year, weekday)
SELECT  DISTINCT(start_time)                AS start_time,
        EXTRACT(hour FROM start_time)       AS hour,
        EXTRACT(day FROM start_time)        AS day,
        EXTRACT(week FROM start_time)       AS week,
        EXTRACT(month FROM start_time)      AS month,
        EXTRACT(year FROM start_time)       AS year,
        EXTRACT(dayofweek FROM start_time)  as weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]    