import configparser
import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
#from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator)
from operators.view_bucket_contents import viewBucketContents
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries






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

my_listing_dag = DAG('only_listing_the_content_of_s3_buckets',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *',
          schedule_interval='@once',
          catchup = False
        )


# defing the needed variables to load songs datra
#config = configparser.ConfigParser()
#config.read('dwh.cfg')
#SONG_DATA = config.get("S3","SONG_DATA")
#ARN = config.get('IAM_ROLE',"ARN")



list_song_data = viewBucketContents(
    task_id = "trying_to_list__song_form_a_created_operator",
    dag = my_listing_dag,
    bucket = "udacity-dend",
    prefix="song_data/A/A/A/",
    redshift_conn_id = "aws_credentials")

"""

list_event_data = viewBucketContents(
    task_id = "trying_to_list_bucket_log_data_form_a_created_operator",
    dag = my_listing_dag,
    bucket = "udacity-dend",
    prefix="log_data",
    redshift_conn_id = "aws_credentials")
"""
