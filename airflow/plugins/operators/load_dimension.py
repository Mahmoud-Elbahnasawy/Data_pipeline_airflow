from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id="",
                 create_query = "",
                 
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.create_query = create_query
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        self.log.info('First establishing connection to s3 using connection established in the airflow UI')
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        self.log.info('Second establishing connection to redshift using connection established in the airflow UI')
        redshift_hook = PostgresHook("redshift")
        
        self.log.info(f"Drop the {self.table} if existed")
        redshift_hook.run(SqlQueries.drop_table_if_existed.format(self.table))
        
        self.log.info(f'trying to create {self.table} table in redshift out of staging tables')
        redshift_hook.run(self.create_query.format(self.table))