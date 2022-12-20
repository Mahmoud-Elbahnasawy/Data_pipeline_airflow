import logging
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries as SqlQueries

def create_load_dimensions(
    parent_dag_name,
    task_id,
    start_date,
    redshift_conn_id,
    aws_credentials_id,
    table,
    create_sql_statement
    ,*args,**kwargs):
    subdag = DAG(
        f"{parent_dag_name}.{task_id}",
        start_date =start_date,
        **kwargs
    )
    
    table_create_insert = LoadDimensionOperator(
        task_id=f"creating_and_inserting_data_into_{table}_table",
        dag=subdag,
        redshift_conn_id = redshift_conn_id,
        table = table,
        create_query = create_sql_statement)
    
    dummy_operator = DummyOperator(
    task_id='dummy_task',
    dag=subdag,
  )
    
    
    return subdag