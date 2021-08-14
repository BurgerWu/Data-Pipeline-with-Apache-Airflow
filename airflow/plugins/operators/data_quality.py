#import libraries and modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

#Create DataQualityOperator class
class DataQualityOperator(BaseOperator):
    #Define UI_color
    ui_color = '#89DA59'

    #Apply appy_defaults decorators
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        
    def execute(self, context):
        #Create redshift hook
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #Iterate through all valid tables to perform data quality check
        self.log.info("Start data quality")       
        for table in self.tables:
            
            #Call result counts from redshift
            records =  redshift_hook.get_records("SELECT COUNT(*) FROM {}".format(table))
            
            #Check if there are results returned
            if len(records) < 1 or len(records[0]) < 1:
                
                #Raise value error if there are no results returned
                raise ValueError("Data quality check failed. {} returned no results".format(table))
            
            #Check if there are rows returned
            num_records = records[0][0]
            if num_records < 1:
                
                #Raise value error if there are no rows returned 
                raise ValueError("Data quality check failed. {} contained 0 rows".format(table))
               
            #Write logs if table passes data quality check
            self.log.info("Table {} passed data quality check".format(table))
