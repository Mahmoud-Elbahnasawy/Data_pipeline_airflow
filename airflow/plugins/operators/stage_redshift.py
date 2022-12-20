from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook
from operators.view_bucket_contents import *
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    @apply_defaults
    def __init__(self,
                 table = "",
                 source_bucket = "",
                 prefix = "",
                 redshift_conn_id = "",
                 create_query="",
                 copy_query="",
                 json_path="",
                 
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.source_bucket = source_bucket
        self.prefix = prefix
        self.redshift_conn_id = redshift_conn_id
        self.create_query = create_query
        self.copy_query = copy_query
        self.json_path = json_path
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info('First establishing connection to s3 using connection established in the airflow UI')
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        self.log.info('Second establishing connection to redshift using connection established in the airflow UI')
        redshift_hook = PostgresHook("redshift")
        
        self.log.info(f"Drop the {self.table} if existed")
        redshift_hook.run(SqlQueries.drop_table_if_existed.format(self.table))
        
        self.log.info(f'trying to create {self.table} table in redshift')
        redshift_hook.run(self.create_query)
        
        self.log.info(f'trying to populate {self.table} in from the s3 bucket')
        redshift_hook.run(SqlQueries.copy_sql.format(self.table , self.source_bucket, credentials.access_key, credentials.secret_key, self.json_path))
        self.log.info(f"{self.table} has been successfully populated from s3 bucket")
        
        
        
#        if self.table == "staging_songs":
#            redshift_hook.run(SqlQueries.COPY_songs.format(self.table , self.source_bucket, credentials.access_key , #credentials.secret_key))
#            self.log.info(f"{self.table} has been successfully populated from s3 bucket")
        
        
#        elif self.table == "staging_events" :
#            redshift_hook.run(SqlQueries.copy_events.format(self.table , self.source_bucket, credentials.access_key , #credentials.secret_key))
#            self.log.info(f"{self.table} has been successfully populated from s3 bucket")
#            
#        else :
#            self.log.info(f" {self.table} is not a correct table name ")
            
            