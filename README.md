# Sparkify Data Warehouse

## Overview
Goals of the project are to use music streaming startup, Sparkify, raw data stored in S3 buckets and process it to a final shape of a data warehouse hosted on AWS Redshift. ETL pipeline extracts JSON files from Sparkify's S3 buckets, stages them as Redshift tables, executes SQL statements that create tables in the star schema form and inserts transformed data.


## Files
- `create_tables.py` - drops and creates staging tables, as well as fact and dimension tables
- `etl.py` - extracts data from JSON files stored on S3 bucket and inserts them into Redshift
- `generate_config.py` - generates `dwh.cfg` files from Terraform output and variables files
- `sql_queries.py` - contains SQL CREATE/DROP/INSERT statements in form of variables to be used by `create_tables.py` and `etl.py`
- `dwh.cfg` - output file of `generate_config.py` that contains info about infrastucture of the project

## How to Run

- Run your infrastructure in Terraform and locate the paths of Terraform's output and .tvars files
- In your console run `python generate_config.py` with your Terraform config's path properly defined
- In your console run `python create_tables.py`
- In your console run `python etl.py`