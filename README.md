# Data-Pipeline-with-Apache-Airflow
<img src="images/redshift.png" width="200"/><img src="images/S3.png" width="200"/><img src="images/airflow.png" width="200"/>

## Motivation
This project is to demonstrate how to apply Apache Airflow, a well-known workflow management platform on data warehousing. In this project, we will transfer Sparkify application data from Amazon S3 storage to Amazon Redshift data warehouse.

## Introduction
There are two main source of data in this project. The first one is song_data that containing all the information of available songs within Sparkify system. The other is log data that preserves all user activities. 

We will be using Apache Airflow dags to contro and monitor all tasks required for this project. We will build task creating tables with desired schema and then stage the dataset from S3 to Redshift. Finally, we will run insert command to build fact and dimension tables from staging tables. In the last part of the dag, we will be checking data quality by calling some results from Redshift connection.

## Airflow Settings
1. Configuration settings
<img src="images/airflow_conn.png" width="200"/>

2. Configure settings for AWS credentials: 
- The login is your AWS access key id
- The passwork is your AWS secret access key
<img src="images/aws_credentials.png" width="200"/>

3. Configue connection settings for Redshift
- You can get the host in AWS Redshift cluster property
- The login is your database user configured for the Redshift database
- The password is the password for the user mentioned above
<img src="images/redshift_conn.png" width="200"/>

## Data Workflow

Below is how each task within dag works


## Schema of Tables

Below is the diagram of schemas and structure of this project <br>
<img src = "image/DWH_Redshift.png" width = 750px>

### Staging Tables

- **Staging_Events: Records of all activities** <br>
*event_id,artist,auth,firstName text, gender text, itemInSession int, lastName text, length float, level text, location text, method, page, registration, sessionId, song, status, ts, userAgent, userId*

- **Staging_Songs: Records of all songs**<br>
*num_songs, artist_id, artist_latitude, artist_longitude, artist_location, artist_name, song_id, title, duration, year*


### Redshift Tables 

- **songplays (fact table): Records in event data associated with song plays** <br>
*songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*

- **users (dimension table): Users in the app** <br>
*user_id, first_name, last_name, gender, level*

- **songs (dimension table): Songs in music database** <br>
*song_id, title, artist_id, year, duration*

- **artists (dimension table): Artists in music database** <br>
*artist_id, name, location, lattitude, longitude*

- **time (dimension table): Timestamps of records in songplays broken down into specific units** <br>
*start_time, hour, day, week, month, year, weekday*

## Libraries Used 
- datetime: Handling datetime
- airflow

## Files and Folders
- dags
  |- create_tables.sql: Include sql commands required for creating tables in Redshift
  |- s3toRedshift_dwh_dag: Create dag object for the project

- plugins
  |- __init__.py: Configure operators and helpers by creating AirflowPlugin object
  |- helpers
    |- sql_queries.py: Include obejct containing sql queries required for table insertion in Redshift
  |- operators
    |- data_quality.py: Create DataQualityOperator for data quality check
    |- load_dimension.py: Create LoadDimensionOperator class for loading dimension tabel in Redshift
    |- load_fact.py: Create LoadFactOperator class for loading fact table in Redshift
    |- stage_redshift.py :Create StageToRedshiftOperator class for staging tables from S3 to Redshift
  
