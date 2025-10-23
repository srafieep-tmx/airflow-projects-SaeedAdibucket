from airflow.providers.amazon.aws.operators.s3 import (S3CopyObjectOperator, S3ListOperator)
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow import Dataset
import pandas as pd

##################  CONFIGURATION ########################################
raw_bucket_delimiter = "/"
file_name = "tripdata.csv"
raw_bucket = "saeed-rawdata-bucket"
transformed_bucket = 'saeed-transformed-bucket'

aws_conn_id = "aws_conn"

@dag(
    dag_id ="transformation_load",
    start_date = datetime(2025, 10, 21),
    schedule = None,
    catchup = False,
    tags = ['transformation', 'loading'],
    params={
    "input_s3_key": {
        "type": "string",
        "default": "tripdata.csv", 
        "title": "S3 File Key",
        "description": "The full S3 key (path/filename) to the raw data file. (e.g., 'folder/file.csv' or just 'file.csv')",
    }
}

)
def transformation_load():

    @task
    def transformation(params):
        """
        This function transform the data and returns a csv file than can be then uploaded to the s3 bucket
        """
        raw_key = params["input_s3_key"]
        source_s3_uri = f's3://{raw_bucket}{raw_bucket_delimiter}{raw_key}'
        
        print(f"Reading data directly from S3 using Pandas: {source_s3_uri}")
        try:
            df = pd.read_csv(source_s3_uri)
        except Exception as e:
            raise ValueError (f'Failed to read CSV file from s3 URI ({source_s3_uri}): {e}')
        
        # Transform the dataframe and add two new columns
        df['processing_ts'] = datetime.now().isoformat()
        df['total_amount_excep_tip'] = df['fare_amount'] + df['extra'] + df["mta_tax"]

        return df
    
       
    @task
    def load(result):

        DATE_STR = datetime.now().strftime("%Y%m%d%H%M%S")
        # Construct final S3 URI
        FINAL_S3_URI = f"s3://{transformed_bucket}/{DATE_STR}_{file_name}"

        print(f"Writing transformed data to S3: {FINAL_S3_URI}")
            
            # Pandas writes the DataFrame directly to the S3 URI via s3fs
        result.to_csv(FINAL_S3_URI, index=False)
        
    transformation_results = transformation()
    write_task = load(result=transformation_results)
    

transformation_load()