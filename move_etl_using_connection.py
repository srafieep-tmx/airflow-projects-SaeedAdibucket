from airflow.providers.amazon.aws.operators.s3 import (S3CopyObjectOperator, S3ListOperator)
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import Dataset
import pandas as pd

##########################################################################
##################  CONFIGURATION ########################################
# Landing bucket
landing_bucket = "aditya-saeed-landing-bucket"
landing_folder = "landing-file/"
landing_bucket_delimiter = "/"
file_name = "tripdata.csv"

# Raw bucket
raw_bucket = "saeed-rawdata-bucket"


# Transformed bucket
transformed_bucket = 'saeed-transformed-bucket'



aws_conn_id = "aws_conn"


my_dataset = Dataset(
    f's3://{landing_bucket}{landing_bucket_delimiter}{landing_folder}{file_name}'
)

@dag(
    dag_id ="move_etl_with_conn",
    start_date = datetime(2022, 12, 1),
    schedule = None,
    catchup = False,
    tags = ['move', 'etl', 'write'],

)
def move_etl_with_conn():

    start_task= BashOperator(
        task_id = "start_task",
        bash_command = 'echo "This workflow started for {{ds}} "'
    )

    # list all teh files in the first folder in S3 bucket
    list_files = S3ListOperator(
        task_id = f'list_files',
        aws_conn_id = aws_conn_id,
        bucket = landing_bucket,
        prefix = landing_folder,
        delimiter = landing_bucket_delimiter,
    )

    # copy files
    #@task
    copy_files = S3CopyObjectOperator.partial(
        task_id = "copy_files",
        aws_conn_id = aws_conn_id,
        source_bucket_name = landing_bucket,
        dest_bucket_name = raw_bucket,
        email_on_failure = False,
    ).expand_kwargs(
        list_files.output.map(
            lambda x:{
                "source_bucket_key": x,
                "dest_bucket_key": x               
            }
        )
    )

    @task
    def etl(raw_file_key):
        """
        This function transform the data and returns a csv file than can be then uploaded to the s3 bucket
        """
        raw_key = raw_file_key['CopySource']['Key']
        source_s3_uri = f's3://{raw_bucket}/{raw_key}'
        
        print(f"Reading data directly from S3 using Pandas: {source_s3_uri}")
        try:
            df = pd.read_csv(source_s3_uri)
        except Exception as e:
            raise ValueError (f'Failed to read CSV file from s3 URI ({source_s3_uri}): {e}')
        
        # Transform the dataframe and add two new columns
        df['processing_ts'] = datetime.now().isoformat()
        df['total_amount_excep_tip'] = df['fare_amount'] + df['extra'] + df["mta_tax"]

        return {
            "df": df,
            "filename": raw_key.split('/')[-1] 
        }
    
    etl_results = etl.expand(raw_file_key = copy_files.output)

        
    
    @task
    def write_to_s3(result):

        
        df = result["df"]
        source_file = result["filename"]
            
            # Construct final S3 URI
        FINAL_S3_URI = f"s3://{transformed_bucket}/{source_file}"

        print(f"Writing transformed data to S3: {FINAL_S3_URI}")
            
            # Pandas writes the DataFrame directly to the S3 URI via s3fs
        df.to_csv(FINAL_S3_URI, index=False)

    write_task = write_to_s3(result=etl_results)
    
    start_task >> list_files >> copy_files >> etl_results >> write_task

move_etl_with_conn()