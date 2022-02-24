# Overview
A music streaming startup, Sparkify, wants to improve business intelligence by 
analysing the songs their users are listening to.

The objective of this project is to build a pipeline thats extracts user activity and song data on the app, which is stored in S3, transform it and load it into Redshift for the analytics team to use to find insights in what songs their users are listening to. 

![alt text](https://github.com/adelolaadebo/music-streaming_datawarehouse/blob/main/er_diagram.jpg?raw=true)

The steps followed to achieve the above stated objectives are:
1. Extract user activity and song data from S3 in a JSON logs directory.
2. Stage the data in Redshift.
3. Transform the data and load it into star schema with fact and dimension tables.

## Files
The project contains five files.
- `README.md`
- `sql_queries.py`
- `create_table.py`
- `etl.py`
- `dwh.cfg`

### README.md
Briefly explains the goal of the project, what each file contains and how to run tthe python scripts.

### sql_queries.py
This is where the schemas for the fact and dimension tables are defined and will be imported into create_tables.py and etl.py. 

### create_tables.py
Contains logic to connect to the cluster and create/drop the fact and dimension tables.

### etl.py
contains logic to load the data from S3 into staging tables and from staging tables into the fact and dimension tables.

### dwh.cfg
contains AWS Redshift cluster configuration settings and link to data in S3

## How to execute the python scripts
1. Create a Redshift cluster and fill in configuration setting in `dwh.cfg`
2. Run the `create_table.py` 
3. Execute `etl.py` 

**N.B: You will need access to a AWS Redshift cluster**.