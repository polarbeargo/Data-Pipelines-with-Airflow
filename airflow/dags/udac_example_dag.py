from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')
AWS_KEY = AwsHook('aws_credentials').get_credentials().access_key
AWS_SECRET = AwsHook('aws_credentials').get_credentials().secret_key

default_args = {
    'owner': 'udacity',
    "start_date": datetime.now(),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
    "catchup": False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql',
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket=Variable.get("s3_bucket"),
    s3_path='s3://udacity-dend/log_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region='us-west-2',
    json_option="s3://udacity-dend/log_json_path.json",
    provide_context=True,
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    s3_path='s3://udacity-dend/song_data',
    aws_key=AWS_KEY,
    aws_secret=AWS_SECRET,
    region='us-west-2',
    provide_context=True,
    json_option='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    sql=SqlQueries.songplay_table_insert,
    table='songplays',
    truncate=False,
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    sql=SqlQueries.user_table_insert,
    truncate=False,
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    sql=SqlQueries.song_table_insert,
    truncate=False,
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    sql=SqlQueries.artist_table_insert,
    truncate=False,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    sql=SqlQueries.time_table_insert,
    truncate=False,
)

    
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[
        {
            "table": "SELECT COUNT(*) FROM users WHERE userid IS NULL",
            "returnt": 0,
        },
        {
            "table": "SELECT COUNT(*) FROM songs WHERE song_id IS NULL",
            "return": 0,
        },
        {
            "table": "SELECT COUNT(*) FROM artists WHERE artist_id IS NULL",
            "return": 0,
        },
        {
            "table": "SELECT COUNT(*) FROM time WHERE start_time IS NULL",
            "return": 0,
        },
        {
            "table": "SELECT COUNT(*) FROM songplays WHERE songplay_id IS NULL",
            "return": 0,
        },
    ],
    ignore_fails=False, 
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift,
] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_operator