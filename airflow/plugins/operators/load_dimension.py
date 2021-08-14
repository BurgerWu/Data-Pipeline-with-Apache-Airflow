#import libraries and modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

#Create LoadDimensionOperator class
class LoadDimensionOperator(BaseOperator):
    """
    LoadFDimensionOperator runs insert command in redshift to load dimension table from staging tables
    """
    #Define UI_color
    ui_color = '#80BD9E'

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table='',
                 sql_queries = '',
                 append_only = False, #Determine whether to wipe out table first or only append data
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_queries = sql_queries
        self.append_only = append_only

    def execute(self, context):
        """
        Execution function of LoadFactOperator
        """
        #Create PostgresHook to connect to redshift
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        #Wipe table first if append_only is set to False
        if self.append_only == False:
            self.log.info("Because append_only is set to False, deleting original data from table {}".format(self.table))
            redshift.run("""
            DELETE FROM {}
            """.format(self.table))
        
        #Run sql insert command in redshift
        redshift.run("""
        INSERT INTO {}
        {}
        """.format(self.table, self.sql_queries))
        
        self.log.info("Finish loading dimension table {} in Redshift".format(self.table))
