# SIMPLE NOTE
# lines that start with a single # is an ordinary comment
# lines that start with a double ##was helping me debugging my code
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

class MyDataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 test_cases_list = None,
                 *args, **kwargs):

        super(MyDataQualityOperator, self).__init__(*args, **kwargs)
        self.test_cases_list = test_cases_list

    def execute(self, context):
        self.log.info('\nDataQualityOperator not implemented yet')
        
        self.log.info('\n\nFirst establishing connection to s3 using connection established in the airflow UI')
        aws_hook = AwsHook("aws_credentials")
        credentials = aws_hook.get_credentials()
        
        self.log.info('Second establishing connection to redshift using connection established in the airflow UI')
        redshift_hook = PostgresHook("redshift")
        
        
        if (self.test_cases_list == None ) :
            raise Exception(" test_cases_list was not provided , you have to provide it")
        else :    
            self.log.info("Third, run a query to check a test_case")
            
            # counting the number of provided test_cases 
            test_cases_counts = len(self.test_cases_list)
            
            for i in range(test_cases_counts):
                for query , expected_result in (self.test_cases_list[i].items()) :
                    actual_results = redshift_hook.get_records(f"""{query}""")[0][0]
                    if actual_results == expected_result :
                        self.log.info(f"The actual_result of query({query} ;) is {actual_results} and the the expected result is {expected_result}.")
                        self.log.info(f"Test number {i+1} passed successfully, actual_result = expected_result ")
                    else :
                        self.log.info(f"The actual_result of query({query} ;) is {actual_results} while the the expected result was {expected_result}.")
                        raise Exception(f"Test was not successful, actual_result (does not equal) correct_result")
                
            # first getting the column name of this table (from iterations) 
                
                ##self.log.info(f"\n\nNow we have the column {col_name}")
                ##self.log.info(f"""\n\nNow we start with the query (SELECT COUNT({records[j][0]}) FROM "{self.list_of_tables[i]}" WHERE {records[j][0]} ISNULL)""")
                ##self.log.info(f"\n\nthe record[j]  is like this  {records[j]}")
                ##self.log.info(f"\n\nthe type of this records[j] is {type(records[j])}")
                ##self.log.info(f"\n\n\n\n\nthe records[j][0] is like this  {records[j][0]}")
                ##self.log.info(f"\n\n\n\n\nthe type of this records[j][0] is {type(records[j][0])}")
                # applying a sql query to get the count of records having nulls in certain column in a certain table 
                
                
                # checking for the null count so that to get the status of this column whether it has a null or not
                ##self.log.info(f"\n\n\n\n\nThis nulls_count looks like this {nulls_count}")
                ##self.log.info(f"\n\n\n\n\nThis nulls_count[0][0] looks like this {nulls_count[0][0]}")
            
                    
                
                    
                   
                    
            

                
         
            
    
                
    