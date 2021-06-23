from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
'''\
## Sparkify Pipelines

This dag is used to build a pipeline for sparkify, a digital music streaming service, automating ETL processes from S3 into Redshift (data warehouse)

'''
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
    
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly"
          )
dag.doc_md = __doc__

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
start_operator.doc_md = '''\
    ### Start Operator
    Starts up the execution of the pipeline
    
    '''
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,    
    table="staging_songs",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag= dag,
    sql= SqlQueries.songplay_table_insert,
    operation = "truncate"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table="users",
    sql=SqlQueries.user_table_insert,
    columns="""userid, first_name, last_name, gender, level"""
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table="songs",
    sql=SqlQueries.song_table_insert,
    columns="""songid, title, artistid, year, duration"""
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table="artists",
    sql=SqlQueries.artist_table_insert,
    columns="""artistid, name, location, latitude, longitude"""
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table="time",
    sql=SqlQueries.time_table_insert,
    columns="""start_time, hour,day, week ,month, year, weekday"""
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',    
    dag=dag,
    checks = [
        {"check_sql": """SELECT COUNT(*) FROM {}""" , "expected_result":1}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Task dependencies 
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift >> load_songplays_table 
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator