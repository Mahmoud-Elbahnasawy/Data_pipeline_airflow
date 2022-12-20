import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.stage_redshift import StageToRedshiftOperator#, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from operators.view_bucket_contents import viewBucketContents

from my_subdag import create_load_dimensions

from helpers import SqlQueries as SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

mydag = DAG(dag_id = 'my_udac_example_dag',
          default_args=default_args,
          description = 'Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *',
          schedule_interval='@hourly',
          catchup = False
        )



start_operator = DummyOperator(task_id='Begin_execution',  dag=mydag)



"""
def list_attr ():
    #print(dir(SqlQueries))
    logging.info(dir(SqlQueries))
    logging.info(SqlQueries.create_staging_events)
    logging.info(SqlQueries.create_staging_songs)
    
listing_SqlQueries_attr = PythonOperator(
    task_id = "listing_SqlQueries_attr",
    dag = mydag,
    python_callable = list_attr
)

"""






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




"""
create_table = PostgresOperator(
    task_id="create_table",
    dag=mydag,
    postgres_conn_id="redshift",
    sql=my_create_tables.CREATE_TRIPS_TABLE_SQL
)

def load_data_to_redshift(*args, **kwargs):
    aws_hook = AwsHook("aws_credentials")
    credentials = aws_hook.get_credentials()
    redshift_hook = PostgresHook("redshift")
    redshift_hook.run(sql_statements.COPY_ALL_TRIPS_SQL.format(credentials.access_key, credentials.secret_key))
"""






stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=mydag,
    table = "staging_songs",
    source_bucket = "s3://udacity-dend/song_data/A/A/A",
    prfix = "song_data",
    postgres_conn_id="redshift",
    create_query = SqlQueries.create_staging_songs,
    json_path = "auto"
)





load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=mydag,
    table = "songplays",
    redshift_conn_id="redshift",
    create_query = SqlQueries.songplay_table_insert
    
)



load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=mydag,
    table = "user",
    redshift_conn_id = "redshift",
    create_query = SqlQueries.user_table_insert    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=mydag,
    table = "song",
    redshift_conn_id = "redshift",
    create_query = SqlQueries.song_table_insert  
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=mydag,
    table = "artist",
    redshift_conn_id = "redshift",
    create_query = SqlQueries.artist_table_insert  
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=mydag,
    table = "time",
    redshift_conn_id = "redshift",
    create_query = SqlQueries.time_table_insert  
)




"""
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=mydag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=mydag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=mydag
)
"""

"""
loading_dimension_user = SubDagOperator(
    subdag = create_load_dimensions(
        start_date = mydag.default_args['start_date'],
        parent_dag_name = mydag.dag_id,
        task_id = "creating_and_inserting_data_into_user_table",
        redshift_conn_id = "redshift",
        aws_credentials_id = "aws_credentials",
        table ="user",
        create_sql_statement = SqlQueries.user_table_insert.format("user")
    ),
    task_id = "creating_and_inserting_data_into_user_table",
dag = mydag,)       
"""

staging_events_quality_checks = DataQualityOperator(
    task_id='events_quality_checks',
    dag=mydag,
    table = "staging_events"
)

staging_songs_quality_checks = DataQualityOperator(
    task_id='song_quality_checks',
    dag=mydag,
    table = "staging_songs"
)

songplay_quality_checks = DataQualityOperator(
    task_id='songplay_quality_checks',
    dag=mydag,
    table = "songplays"
)

user_quality_checks = DataQualityOperator(
    task_id='user_quality_checks',
    dag=mydag,
    table = "user"
)

song_quality_checks = DataQualityOperator(
    task_id='song_quality_checks',
    dag=mydag,
    table = "song"
)

artist_quality_checks = DataQualityOperator(
    task_id='artist_quality_checks',
    dag=mydag,
    table = "artist"
)

time_quality_checks = DataQualityOperator(
    task_id='time_quality_checks',
    dag=mydag,
    table = "time"
)





end_operator = DummyOperator(task_id='Stop_execution',  dag=mydag)










#setting up dependencies

start_operator >> stage_songs_to_redshift
start_operator >> stage_events_to_redshift
stage_songs_to_redshift >> staging_songs_quality_checks >> load_songplays_table 
stage_events_to_redshift >> staging_events_quality_checks >> load_songplays_table
load_songplays_table >> songplay_quality_checks 

songplay_quality_checks >> load_user_dimension_table 
songplay_quality_checks >> load_song_dimension_table
songplay_quality_checks >> load_artist_dimension_table
songplay_quality_checks >> load_time_dimension_table

#load_song_dimension_table >> song_quality_checks >> end_operator
load_user_dimension_table >> user_quality_checks >> end_operator
load_artist_dimension_table >> artist_quality_checks >> end_operator
load_time_dimension_table >> time_quality_checks >> end_operator 


