
import configparser
import psycopg2
import json
import boto3

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

s3 = boto3.resource('s3',
                       region_name="us-east-2",
                       aws_access_key_id=config.get('AWS','KEY'),
                       aws_secret_access_key=config.get('AWS','SECRET')

                   )

iam = boto3.client('iam',aws_access_key_id=config.get('AWS','KEY'),
                     aws_secret_access_key=config.get('AWS','SECRET')
,
                     region_name='us-east-2'
                  )

#1.1 Create the role,
try:
    print("1.1 Creating a new IAM Role")
    myRedshiftRole = iam.create_role(
        Path='/',
        RoleName = config.get("CLUSTER","DWH_IAM_ROLE_NAME"),
        Description = "Allows Redshift clusters to call AWS services on your behalf.",
        AssumeRolePolicyDocument=json.dumps(
            {
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        #"AWS": "arn:aws:iam::186673354858:user/RedshiftUser",
        "Service": "redshift.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
})
    )
except Exception as e:
    print(e)

print("1.2 Attaching Policy")

iam.attach_role_policy(RoleName=config.get("CLUSTER","DWH_IAM_ROLE_NAME"),
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

print("1.3 Get the IAM role ARN")
roleArn = iam.get_role(RoleName=config.get("CLUSTER","DWH_IAM_ROLE_NAME"))['Role']['Arn']

print(roleArn)


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
staging_events_table_create = (f"""
CREATE TABLE IF NOT EXISTS staging_events(
    artist TEXT,
    auth TEXT,
    first_name TEXT,
    gender CHAR(1),
    item_session INTEGER,
    last_name TEXT,
    length NUMERIC,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration NUMERIC,
    session_id INTEGER,
    song TEXT,
    status INTEGER,
    ts BIGINT,
    user_agent TEXT,
    user_id INTEGER)
""")

staging_songs_table_create = (f"""
CREATE  TABLE IF NOT EXISTS staging_songs(
 num_songs int4,
    artist_id varchar(256),
    artist_name varchar(256),
    artist_latitude numeric(18,0),
    artist_longitude numeric(18,0),
    artist_location varchar(256),
    song_id varchar(256),
    title varchar(256),
    duration numeric(18,0),
    "year" int4)
""")



songplay_table_create = ("""
 CREATE TABLE IF NOT EXISTS songplay
    (songplay_id IDENTITY(0,1) PRIMARY KEY,
    start_time timestamp INTEGER NULL,
    user_id varchar NOT NULL,
    level varchar ,
    song_id varchar NOT NULL,
    artist_id varchar NOT NULL,
    session_id varchar NOT NULL,
    location varchar,
    user_agent varchar
    );
""")

user_table_create = ("""
 CREATE TABLE IF NOT EXISTS users
    (user_id varchar PRIMARY KEY NOT NULL,
    first_name varchar,
    last_name varchar,
    gender varchar,
    level varchar
    );
""")

song_table_create = ("""
 CREATE TABLE IF NOT EXISTS song
    (song_id varchar PRIMARY KEY NOT NULL,
    title varchar,
    artist_id varchar,
    year int,
    duration numeric
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist
    (artist_id varchar PRIMARY KEY,
    artist_name varchar,
    artist_location varchar,
    artist_latitude numeric,
    artist_longitude numeric
    );
""")

time_table_create = ("""
 CREATE TABLE IF NOT EXISTS time
    (start_time timestamp PRIMARY KEY NOT NULL,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data'
    credentials 'aws_iam_role=arn:aws:iam::186673354858:role/myRedshiftRole'
    JSON 's3://udacity-dend/log_json_path.json'
    compupdate off region 'us-east-2';
""")
staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data'
    credentials 'aws_iam_role=arn:aws:iam::186673354858:role/myRedshiftRole'
    JSON 'auto'
    compupdate off region 'us-east-2';
""")

# FINAL TABLES

songplay_table_insert = (f"""
SELECT distinct   e.ts,
         e.userId,
         e.level,
         s.song_id,
         s.artist_id,
         e.sessionId,
         e.location,
         e.userAgent
INTO songplay
FROM staging_events AS e
JOIN staging_songs AS s
     ON (e.artist = s.artist_name)
     AND (e.song = s.title)
     AND (e.length = s.duration)
     WHERE e.page = 'NextSong';
""")

user_table_insert = ("""
SELECT distinct user_id,
    first_name,
    last_name,
    gender,
    level
INTO users
FROM staging_events
WHERE page = 'NextSong';
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
select distinct artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
INTO artist
FROM staging_songs
""")

time_table_insert = ("""
SELECT distinct start_time,
                extract(hour from start_time),
                extract(day from start_time),
                extract(week from start_time),
                extract(month from start_time),
                extract(year from start_time),
                extract(dayofweek from start_time)
INTO time
FROM songplay
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
