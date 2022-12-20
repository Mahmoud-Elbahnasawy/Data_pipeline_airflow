import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
"""
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,viewBucketContents)"""

from operators.view_bucket_contents import viewBucketContents
from operators.stage_redshift import StageToRedshiftOperator

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

mydag = DAG('trying_to_list_bucket_content_form_a_created_operator',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@once',
          catchup = False)

list_bucket_contents_task = viewBucketContents(
    task_id = "trying_to_list_bucket_content_form_a_created_operator",
    dag = mydag,
    bucket = "udacity-dend",
    prfix="data-pipelines",
    redshift_conn_id = "aws_credentials")


def print_my_name():
    logging.info("\n\n\n\n\n\n\n\n My name is ","\n\n\n\n\n\n\n\n")

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=print_my_name,
    dag=mydag
)

#list_task >> list_bucket_contents_task