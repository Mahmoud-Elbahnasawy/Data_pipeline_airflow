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
                 insert_query = "",
                 loading_type = None,
                 
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_query = insert_query
        self.loading_type = loading_type
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
        
        #self.log.info(f'trying to create {self.table} table in redshift out of staging tables')
        #redshift_hook.run(self.create_query.format(self.table))
        
        
        if self.loading_type == "truncate":
            self.log.info(f"truncate the table {self.table}")
            redshift_hook.run(SqlQueries.truncate_table_if_existed.format(self.table))
            self.log.info(f"loading the table {self.table}")
            redshift_hook.run(self.insert_query.format(self.table))
            
        elif self.loading_type == "append":
            self.log.info(f"appending the table {self.table}")
            redshift_hook.run(self.insert_query.format(self.table))
        elif self.loading_type == None:
            raise Exception("Sorry, you should provide a loading_type eg. ('append' , 'truncate')")
        else :
            raise Exception("ERROR, load_type can't be anything except 'append' , 'trucate' please write them carefully ")
