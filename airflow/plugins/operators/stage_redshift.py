#import libraries and modules
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

#Create StageToRedshiftOperator class
class StageToRedshiftOperator(BaseOperator):
    """
    StageToRedshiftOperator stages tables from S3 to redshift by running copy command
    """
    #Define UI_color
    ui_color = '#358140'

    #Apply apply_defaults decorator
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path = "auto",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        
    def execute(self, context):
        """
        The execution function executes sql copy command with valid credentials and connection
        """
        
        #Writing logs at the beginning of execution
        self.log.info('StageToRedshiftOperator executing')
        
        #Create AWS hook and get credentials 
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        #Create PostgesHook to connet to redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Writing logs and create s3 path
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        #Formatting staging_sql
        staging_sql = """
        COPY {}
        FROM '{}'
        JSON '{}'
        REGION 'us-west-2'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        """.format(self.table,
                   s3_path,
                   self.json_path,
                   credentials.access_key,
                   credentials.secret_key)

        #Add compupdate and statupdate for staging_songs table insertion
        if self.table == 'staging_events':
            redshift.run(staging_sql)
        elif self.table == 'staging_songs':
            redshift.run("""
            {}
            COMPUPDATE OFF 
            STATUPDATE OFF;
            """.format(staging_sql))
        
        self.log.info("Finish copying data from S3 to Redshift")
  
