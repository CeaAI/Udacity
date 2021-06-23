import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN =config.get('IAM_ROLE','ARN')
REGION=config.get('IAM_ROLE','REGION')
SONG_DATA= config.get('S3','SONG_DATA')
LOG_DATA = config.get('S3','LOG_DATA')
LOG_PATH = config.get('S3','LOG_JSONPATH')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist varchar,
    auth varchar,
    firstname varchar,
    lastname varchar,
    gender varchar,
    iteminsession varchar,
    length numeric(30, 4),
    level varchar,
    location varchar,
    method char(6) ,
    page varchar,
    registration BIGINT,
    sessionid integer,
    song varchar,
    status integer,
    ts BIGINT,
    useragent varchar,
    userid integer
)

""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs int not null,
    artist_id varchar not null,
    artist_latitude float,
    artist_longitude float,
    artist_location varchar,
    artist_name varchar not null,
    song_id varchar not null,
    title varchar not null,
    duration float not null,
    year int not null
)

""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplayid int IDENTITY(0,1) primary key, 
    starttime Timestamp not null,
    userid int not null,
    level varchar , 
    songid varchar not null , 
    artistid varchar  not null, 
    sessionid int , 
    location varchar , 
    useragent varchar 
)
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(
    userid int primary key, 
    firstname varchar, 
    lastname varchar ,
    gender varchar , 
    level varchar
)
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    songid varchar primary key,
    title varchar, 
    artistid varchar , 
    year int,
    duration numeric)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artistid varchar primary key, 
    name varchar , 
    location varchar,
    longitude float, 
    latitude float
)
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    starttime timestamp primary key, 
    hour int not null, 
    day int not null, 
    week int not null,
    month int not null, 
    year int not null, 
    weekday int not null
)

""")


# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region {}
    json {}
""").format(LOG_DATA, DWH_ROLE_ARN, REGION, LOG_PATH)


staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region {}
    json 'auto ignorecase'
""").format(SONG_DATA, DWH_ROLE_ARN, REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(
    starttime,userid,
    level,songid, artistid, sessionid, 
    location, useragent

) 

SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS starttime, se.userid, se.level,
    ss.song_id, ss.artist_id, se.sessionid, se.location,
    se.useragent 
FROM staging_events se
JOIN staging_songs ss on se.artist = ss.artist_name
WHERE se.song = ss.title and se.page = 'NextSong'

""")

user_table_insert = ("""
INSERT INTO users
(
    userid,firstname, lastname, gender, level
)
SELECT distinct se.userid, se.firstName, se.lastName, se.gender, se.level
FROM staging_events se
WHERE se.userid is not null 
AND se.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO songs
(
    songid,title, artistid, year, duration
)

SELECT distinct ss.song_id, ss.title, ss.artist_id, ss.year, ss.duration
FROM staging_songs ss
where ss.song_id is not null
""")

artist_table_insert = ("""
INSERT INTO artists
(
    artistid,name,location, longitude ,latitude
)
SELECT distinct ss.artist_id, ss.artist_name, ss.artist_location, ss.artist_longitude, ss.artist_latitude
FROM staging_songs ss
where ss.artist_id is not null

""")

time_table_insert = ("""
INSERT INTO time
(
    starttime, hour,day, week ,month, year, weekday
)
SELECT TIMESTAMP 'epoch' + se.ts/1000 * interval '1 second' AS starttime, extract(hr from starttime), extract(doy from starttime) , extract(w from starttime), extract(mon from starttime), extract(yr from starttime), extract(dow from starttime)
FROM staging_events se
WHERE starttime is not null
AND se.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]