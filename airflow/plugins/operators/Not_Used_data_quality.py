from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 table= "",
                 
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.table = table
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('\nDataQualityOperator not implemented yet')
        """
        # you can uncomment this lines from 29 to 46 so that to check every table individually
        self.log.info(f"\nWe are going to check for records in {self.table}")
        self.log.info('\n\nFirst establishing connection to s3 using connection established in the airflow UI')
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        self.log.info('Second establishing connection to redshift using connection established in the airflow UI')
        redshift_hook = PostgresHook("redshift")
        
        self.log.info('counting rows as a quality checks...')
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        self.log.info(f"Data quality on table {self.table} check passed with {records[0][0]} records")
        """
        
        
        