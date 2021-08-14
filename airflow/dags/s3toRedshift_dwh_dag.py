#import libraries
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (LoadFactOperator, StageToRedshiftOperator, LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

#Create default argument for dag
default_args = {
    'owner': 'burger_wu',
    'start_date': datetime.utcnow(),
    'depends_on_past': False,
    'catchup': False,
    'retries': 2,
    'retry_delay': timedelta(minutes = 5)
}

#Create DAG with daily schedule_interval and start data as now
dag = DAG('s3toRedshift_dwh_dag',
          default_args = default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval = '@hourly')

#Create start_operator task
start_operator = DummyOperator(task_id = 'Begin_execution',  dag = dag)

#Create create_tables task
create_tables = PostgresOperator(
    task_id = 'Create_tables',
    dag = dag,
    postgres_conn_id = "redshift",
    sql = open(os.path.dirname(os.path.realpath(__file__)) + '/create_tables.sql').read())

#Create stage_events_to_redshift task
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events', 
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table = "staging_events",
    s3_bucket = "udacity-dend",  
    s3_key = "log_data",
    json_path = "s3://udacity-dend/log_json_path.json",
    ignore_headers = 1)

#Create stage_songs_to_redshift task
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    dag = dag,
    redshift_conn_id = "redshift",
    aws_credentials_id = "aws_credentials",
    table ="staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    json_path = 'auto',
    ignore_headers = 1)

#Create load_songplays_table task
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "songplays",
    sql_queries = SqlQueries.songplay_table_insert,
    append_only = False)

#Create load_user_dimension_table task
load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "users",
    sql_queries = SqlQueries.user_table_insert,
    append_only = False)
    
#Create load_song_dimension_table task
load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "songs",
    sql_queries = SqlQueries.song_table_insert,
    append_only = False)

#Create load_artist_dimension_table task
load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = dag,
    redshift_conn_id ="redshift",
    table = "artists",
    sql_queries = SqlQueries.artist_table_insert,
    append_only = False)

#Create load_time_dimension_table task
load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = dag,
    redshift_conn_id = "redshift",
    table = "time",
    sql_queries = SqlQueries.time_table_insert,
    append_only = False)

#Create run_quality_checks task
run_quality_checks = DataQualityOperator(
    task_id = 'Run_data_quality_checks',
    dag = dag,
    redshift_conn_id = "redshift",
    tables = [ "songplays", "songs", "artists",  "time", "users"])

#Create end_operator task
end_operator = DummyOperator(task_id = 'Stop_execution',  dag=dag)

#Schedule sequential relationship between tasks
start_operator \
>> create_tables \
>> [stage_events_to_redshift, stage_songs_to_redshift] \
>> load_songplays_table \
>> [load_user_dimension_table, load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table] \
>> run_quality_checks \
>> end_operator