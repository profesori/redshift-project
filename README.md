# Context

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

In this project we are building a ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

# Files

- `create_tables.py`: Script that creates the fact and dimension tables directly to Redshift.
- `dwh.cfg`: Config file containing DB credentials and S3 bucket path
- `etl.py`: ETL script that extracts data from S3 json files and loads them to Redshift
- `sql_queries.py`: File containing all the necessary queries to execute

# How to run scripts

- Create the necessary tables by running the python script: `python3 create_tables.py`
- Running the ETL pipelin with `python3 etl.py`
