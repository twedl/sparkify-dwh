# Schema for Song Play Analysis 

This repo has python code and sql queries for a data pipeline in AWS (S3 and Redshift) that creates analytical database for song play analysis at Sparkify. Analysis could include summary statistics for song plays by user type, time, artist, and location, among many other possibilities.

## Setup

### Install

First, start a Redshift server and copy your AWS credentials into a file named `dwh.cfg`.

To set up the scripts, first clone the git repo:
```
git clone git@github.com:twedl/sparkify-dwh.git
cd sparkify-dwh
```
Then create and activate a python virtual environment (for bash/zsh):
```
python3 -m venv env 
source env/bin/activate
```
Then install all python packages listed in `requirements.txt`:
```
pip install -r requirements.txt
```

### Files

* `sql_queries.py`: SQL queries stored as Python strings for execution with `psycopg2` in the scripts `create_tables.py` and `etl.py`
* `create_tables.py`: script to delete (if exists) and re-create the staging and analytical tables
* `etl.py`: script to ingest all the data in the S3 buckets and insert into the staging and analytical tables

### Run

To create the database and insert the data into tables, run:
```
python create_tables.py && python etl.py
```

After running this, the Redshift database should exist with tables `songplays`, `users`, `songs`, `artists`, and `time`. 

## Schema

The database uses a star schema optimized for song play analytical queries. It includes the following tables:

The fact table is `songplays` (populated from files located in the S3 buckets `s3://udacity-dend/log_data`, with columns `song_id` from `songs` table and `artist_id` from `artist` table)
- `songplays` (fact table); columns: 
  - `songplay_id`,`start_time`,`user_id`,`level`,`song_id`,`artist_id`,`session_id`,`location`,`user_agent`

The dimensions tables are populated with data from the song information in the S3 buckets `s3://udacity-dend/song_data`. They include the following tables:

- `users`: users in the app, with columns: 
  - `user_id`,`first_name`,`last_name`,`gender`,`level`
- `songs`: songs in the music database, with columns:
  - `song_id`,`title`,`artist_id`,`year`,`duration`
- `artists`: artists in the music database, with columns:
  - `artist_id`,`name`,`loction`,`latitude`,`longitude`
- `time`: timestamps of records in `songplays` broken down into specific units
  - `start_time`,`hour`,`day`,`week`,`month`,`year`,`weekday`

This star schema was chosen to reduce redundency in the database; this reduces storage required as well as reducing the chance that errors are introduced because song and artist information can be updated in one table only. The presence of `level` in both the `songplays` table and `users` table represents the fact that level can change over time; `level` in `users` is the current subscription level (paid or free) of the user, but that may not reflect the subscription level the user had when they played the song. The schema trades-off the confusing nature of a database that is not completely normalized for the accuracy of historical subscription levels. This is important for analytical queries of streaming behaviour and will inform the direction of the business. In the future, the schema should be changed to reflect highlight the difference in `levels`, by, e.g., renaming `level` to `level_historical` in the `songplays` table, or `level_current` in the `users` table.

Using the star schema may increase query time if every query requires joining the dimension tables to the fact tables to produce results. In this case, future schemas may include commonly required dimensions into the fact table itself. However, this increases the likelihood of errors introduced by updating dimensions in the dimension table but not the fact table, or vice versa.

## ETL Pipeline

The pipeline: 

1. Create Redshift cluster (via AWS console), copy endpoint and user credentials to `dwh.cfg`
2. Drop existing staging tables: `staging_events` and `staging_songs`
3. Drop existing analytical tables: `songplays`, `users`, `songs`, `artists`, and `time`. 
4. Create new staging and analytical tables
5. Copy events and songs from S3 buckets into staging tables
6. Insert songplay data into from the staging tables into the `songplays` fact table and dimensions from the staging tables into the dimension tables

## Example queries

Top 5 songs by number of plays:
```
SELECT COUNT(*) AS total_plays, songs.title, artists.name
FROM songplays
LEFT JOIN songs ON songplays.song_id = songs.song_id
LEFT JOIN artists ON songplays.artist_id = artists.artist_id
GROUP BY songs.title, artists.name
ORDER BY total_plays DESC
LIMIT 5;
```

Number of plays per hour of the day:
```
SELECT count(*) as count, time.hour
FROM songplays
INNER JOIN time ON songplays.start_time = time.start_time
GROUP BY time.hour
ORDER BY time.hour;
```