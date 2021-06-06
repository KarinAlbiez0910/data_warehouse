## Introduction

Sparkify has collected song and user data in json format, which reside in s3 under the following paths:

Song data: s3://udacity-dend/song_data

User data: s3://udacity-dend/log_data

In order to be able to create a star schema, we have first created a staging table both for the song and user data.
This proceeding allows us to transform the data contained in the temporary staging tables into the below star schema.

## Database schema design and ETL process

The star schema consists of a fact table called songplays and four dimension tables: users, songs, artists and time.
The fact table songplays includes all of the sortkeys of the dimension tables, that is user_id, song_id, artist_id and start_time,
so the schema allows for a very flexible usage of JOIN SELECT statements.


![](./tables.jpg)


## How to run the Python project
In order to run the project, please kindly run the following files:


create_tables.py

etl.py
