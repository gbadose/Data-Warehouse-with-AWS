# Data warehouse with Redshift AWS.
> We created a cluster: redshift-cluster.chyepmqtfema.us-east-2.redshift.amazonaws.com and assigned parameters in file dwh.cfg
>> HOST=redshift-cluster.chyepmqtfema.us-east-2.redshift.amazonaws.com
>> DB_NAME=dev
>> DB_USER=awsuser
>> DB_PASSWORD=****
>> DB_PORT=5439
>> DWH_IAM_ROLE_NAME=myRedshiftRole

> Also assigned KEY and SECRET KEY paramenters to allow user and role access S3 and load data into Redshift in the cluster.

>We created the IAM Role "MyRedshiftRole" to facilitate the ETL process. 

# We are creating the following tables to get started:

## 1 Fact Table
> songplays - records in event data associated with song plays i.e. records with page NextSong
>> songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

## 2 Dimension Tables
> users - users in the app
>> user_id, first_name, last_name, gender, level
> songs - songs in music database
>> song_id, title, artist_id, year, duration
> artists - artists in music database
>> artist_id, name, location, lattitude, longitude
> time - timestamps of records in songplays broken down into specific units
>> start_time, hour, day, week, month, year, weekday

## 3 Staging Tables
    staging_events table to capture all event data from log_data.json resided in s3://udacity-dend/log_data
    staging_songs table to capture all songs data from song_data.json resided in s3://udacity-dend/song_data

# ETL Process
## COPY Staging tables from S3

### Inserting data into staging_events and staging_songs tables by using the following statements 
>staging_events_copy = ("""
>> copy staging_events from #'s3://udacity-dend/log_data'
>> credentials 'aws_iam_role=arn:aws:iam::186673354858:role/myRedshiftRole'
>> JSON 's3://udacity-dend/log_json_path.json'
>> compupdate off region 'us-east-2';
>> """)
> staging_songs_copy = ("""
>> copy staging_songs from 's3://udacity-dend/song_data'
>> credentials 'aws_iam_role=arn:aws:iam::186673354858:role/myRedshiftRole'
>> JSON 'auto'
>> compupdate off region 'us-east-2';
>> """)

## Inserting data into Dimensional and Fact tables using the staging tables using regular SQL INSERT statement.
>> def insert_tables(cur, conn):
>> for query in insert_table_queries:
>> cur.execute(query)
>> conn.commit()

# Running Process

## 1. Run create_tables.py
## 2. Run etl.py 