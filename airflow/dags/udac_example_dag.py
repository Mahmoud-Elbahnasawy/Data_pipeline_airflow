import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator

from operators.my_data_quality import MyDataQualityOperator

from helpers import SqlQueries



default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':3,
    'retry_delay': timedelta(minutes=5)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

mydag = DAG(dag_id = 'my_udac_example_dag',
          default_args=default_args,
          description = 'Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs  = 1,
          catchup = False
           )



start_operator = DummyOperator(task_id='Begin_execution',  dag=mydag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=mydag,
    table = "staging_events",
    source_bucket = "s3://udacity-dend/log_data",
    prefix = "log_data",
    postgres_conn_id="redshift",
    create_query = SqlQueries.create_staging_events,
    json_path = "s3://udacity-dend/log_json_path.json"
)
    

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=mydag,
    table = "staging_songs",
    source_bucket = "s3://udacity-dend/song_data",
    prefix = "song_data",
    postgres_conn_id="redshift",
    create_query = SqlQueries.create_staging_songs,
    json_path = "auto"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=mydag,
    table = "songplays",
    redshift_conn_id="redshift",
    insert_query = SqlQueries.songplay_table_insert
)


load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=mydag,
    table = "user",
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.user_table_insert,
    loading_type = "truncate"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=mydag,
    table = "song",
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.song_table_insert,
    loading_type = "truncate"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=mydag,
    table = "artist",
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.artist_table_insert,
    loading_type = "truncate"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=mydag,
    table = "time",
    redshift_conn_id = "redshift",
    insert_query = SqlQueries.time_table_insert,
    loading_type = "truncate"
)





# here we have 7 quality check tasks for each certain table we cab uncomment them to check how many rows exist in a certain table
"""
# first check staging_events
staging_events_quality_checks = DataQualityOperator(
    task_id='staging_events_quality_checks',
    dag=mydag,
    table = "staging_events"
)
# second check staging_songs
staging_songs_quality_checks = DataQualityOperator(
    task_id='staging_song_quality_checks',
    dag=mydag,
    table = "staging_songs"
)
# third check songplay fact table
songplay_quality_checks = DataQualityOperator(
    task_id='songplay_quality_checks',
    dag=mydag,
    table = "songplays"
)

# fourth check user dimension
user_quality_checks = DataQualityOperator(
    task_id='user_quality_checks',
    dag=mydag,
    table = "user"
)
# fifth check song dimensions
song_quality_checks = DataQualityOperator(
    task_id='song_quality_checks',
    dag=mydag,
    table = "song"
)
# sixth check artist dimensions
artist_quality_checks = DataQualityOperator(
    task_id='artist_quality_checks',
    dag=mydag,
    table = "artist"
)
# seventh check time dimensions
time_quality_checks = DataQualityOperator(
    task_id='time_quality_checks',
    dag=mydag,
    table = "time"
)
"""




All_tables_quality_check = MyDataQualityOperator(
    task_id='run_data_quality_check',
    dag=mydag,
    test_cases_list = [{"""SELECT COUNT(*) FROM "user" WHERE userid is null""":0} ,
                      {"SELECT COUNT(*) FROM song WHERE songid is null":0},
                      {"SELECT COUNT(*) FROM artist WHERE artistid is null":0},
                      {"SELECT COUNT(*) FROM time WHERE start_time is null" : 0}]
                                       
)

# ending operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=mydag)










#setting up dependencies


start_operator >> [stage_songs_to_redshift , stage_events_to_redshift]
# start_operator >> stage_events_to_redshift


[stage_songs_to_redshift ,stage_events_to_redshift]  >> load_songplays_table
#stage_events_to_redshift >> load_songplays_table


load_songplays_table >> [ load_user_dimension_table , load_song_dimension_table , load_artist_dimension_table , load_time_dimension_table ]
#load_songplays_table >> load_song_dimension_table
#load_songplays_table >> load_artist_dimension_table
#load_songplays_table >> load_time_dimension_table

[load_song_dimension_table , load_user_dimension_table , load_artist_dimension_table , load_time_dimension_table] >> All_tables_quality_check
#load_user_dimension_table >>  All_tables_quality_check
#load_artist_dimension_table >> All_tables_quality_check
#load_time_dimension_table >>  All_tables_quality_check 


All_tables_quality_check >> end_operator

