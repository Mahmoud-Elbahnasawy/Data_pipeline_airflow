import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator


class viewBucketContents(BaseOperator):
    #ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 bucket="",
                 prefix = "",
                 redshift_conn_id = "",
                 
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(viewBucketContents, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.redshift_conn_id = redshift_conn_id
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('viewBucketContents not implemented yet')
        self.log.info('Trying to establish connection to s3')
        hook = S3Hook(aws_conn_id=self.redshift_conn_id)
        #bucket = Variable.get('s3_bucket')
        #bucket = self.bucket
        #prefix = Variable.get('s3_prefix')
        prefix = self.prefix
        logging.info(f"\n\n\n\n\n your prefix is {self.prefix} \n\n\n\n\n\n")
        logging.info(f"\n\n\n\n\n your bucket is {self.bucket} \n\n\n\n\n\n")
        logging.info(f"\n\n\n\n\n Listing Keys from {self.bucket}/{self.prefix} \n\n\n\n\n\n")
        keys = hook.list_keys(self.bucket, prefix=self.prefix)
        counter = 0
        for key in keys:
            if counter < 25:
                counter += 1
                logging.info(f"\n\n\n\nKey number {counter} is in the next line")
                logging.info(f"- s3://{self.bucket}/{key}\n\n\n\n")
                
        for key in keys:
                counter += 1
        logging.info(f"\n\n\nthis bucket containts {counter} items \n\n\n")