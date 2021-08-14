#import libraries and modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

#Create LoadFactOperator
class LoadFactOperator(BaseOperator):
    """
    LoadFactOperator runs insert command in redshift to load fact table from staging tables
    """
    #Define UI_color
    ui_color = '#F98866'

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 table="",
                 sql_queries = '',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_queries = sql_queries

    def execute(self, context):
        """
        Execution function of LoadFactOperator
        """
        #Create PostgresHook to connect to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Run sql command in redshift
        redshift.run("""
        INSERT INTO {}
        {}
        """.format(self.table,self.sql_queries))

        self.log.info("Finish loading fact table {} in Redshift".format(self.table))
