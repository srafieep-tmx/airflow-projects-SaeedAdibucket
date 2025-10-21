import pandas as pd
import boto3
from datetime import datetime, timedelta
from io import StringIO
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.s3 import (S3CopyObjectOperator, S3ListOperator)


# Define the variables
landing_bucket = "aditya-saeed-landing-bucket"
raw_bucket = "saeed-rawdata-bucket"
transformed_bucket = "saeed-transformed-bucket"

landing_folder = "landing-file/"
delimeter_s3_bucket = "/"
file_name = "tripdata.csv"
aws_conn_id = "aws_conn"


# Default argument

default_args = {
    'owner' : "Saeed",
    "retries" : 2,
    "retry_delay" : timedelta(minutes=2)
}


# Define the DAG 

@dag(
    dag_id ="move_data_etl_writebacktobucket",
    start_date = datetime(2025, 10, 21),
    schedule = "@daily",
    catchup = False,
    description="This dag move data, conduct data transformation and write back the data on the s3 bucket",
    tags = ['datasets', 'taskflow', 'usecase'],

)
def move_etl():
    
    start_task = BashOperator(
    task_id="start_task",
    bash_command="echo 'Workflow started for {{ ds }}'",
    )



# Move data into the raw bucket from the landing bucket and then delete the file in the landing bucket

    @task
    def move_landing_raw():
      
        """
        This function moves the file from the landing bucket to the raw bucket
        and then deletes the original file in the landing bucket, using Boto3.
        Returns the new S3 key in the raw bucket.
        """

        #######################################################################################################################
        # creating a boto3 object so that we can do API call on S3 and perform operations on them
        s3 = boto3.client("s3")
        SOURCE_KEY = f"{landing_folder}{file_name}"  # this provides the path to the source s3 bucket

        DEST_KEY = f"raw/{datetime.now().strftime('%Y%m%d')}/{file_name}"  # to make it unique, we use the datetime.now and name it dynamically

        COPY_SOURCE = {'Bucket': landing_bucket, 'Key': SOURCE_KEY}  # create a dictionary that has the bucket name and the source key in one place
        '''
        Identifies the source file. This is the original object being copied. It must be a dictionary specifying both the Bucket 
        (the origin bucket) and the Key (the full path to the source file).
        '''
        print(f"Attempting to copy {SOURCE_KEY} from {landing_bucket} to {raw_bucket}/{DEST_KEY}")

        # 1. Copy the file
        s3.copy_object(
            CopySource=COPY_SOURCE,  #the copysource neends to be a dictionary
            Bucket=raw_bucket,
            Key=DEST_KEY
        )

        # 2. Delete the original file ONLY after a successful copy
        #s3.delete_object(Bucket=landing_bucket, Key=SOURCE_KEY)
        
        print(f"Successfully moved. New raw key: {DEST_KEY}")
        
        return DEST_KEY # Pass the new key to the next task via XCom
    
        #########################################################################################################################
        # Transform the data using pandas dataframe

    @task
    def etl(raw_file_key: str):
            '''
            This function reads the file from the S3 bucket and conduct some transformation on it
            '''

            # we need to access the raw s3 bucket to download the data and work on them
            s3 = boto3.client("s3")

            print(f"Starting ETL on s3://{raw_bucket}/{raw_file_key}")

            # Extract: read data directly into the memory

            try:
                s3_object = s3.get_object(Bucket=raw_bucket, Key=raw_file_key)
                file_content = s3_object['Body'].read().decode('utf-8')
                df = pd.read_csv(StringIO(file_content))
            except Exception as e:
                raise ValueError(f"Failed to read or parse CSV from S3: {e}")

            # Transform the dataframe and add two new columns
            df['processing_ts'] = datetime.now().isoformat()
            df['total_amount_excep_tip'] = df['fare_amount'] + df['extra'] + df["mta_tax"]

            # Assume 'trip_duration' exists for a meaningful transformation
            if 'trip_duration' in df.columns:
             # Calculate a simple metric (e.g., duration in minutes)
                df['duration_minutes'] = df['trip_duration'] / 60 
                print("Transformation applied: duration_minutes added.")
            else:
                print("Warning: 'trip_duration' column not found; added only processing_ts and total_amount except_tip.")

            # --- Return Transformed Data ---
            # Convert the DataFrame back into an in-memory CSV string for XCom
            output_buffer = StringIO()
            df.to_csv(output_buffer, index=False)
        
            # Airflow will handle pushing this string to XCom
            return output_buffer.getvalue()

        
        ##########################################################################################################################
        # Load the data into another S3 bucket
    @task
    def write_transformed_s3(transformed_data_csv: str):
            '''
            This function will take the string from the previous step and upload it to the final s3 bucket     
            '''

            s3 = boto3.client("s3")

            # create a final and unique destination key for the path to the transformed bucket
            DATE_STR = datetime.now().strftime("%Y%m%d%H%M%S")
            FINAL_KEY = f'transformed_data/{DATE_STR}_{file_name}' 

            print(f"Uploading transformed data to s3://{transformed_bucket}/{FINAL_KEY}")

             # Upload the CSV string content
            s3.put_object(
            Bucket=transformed_bucket,
            Key=FINAL_KEY,
            Body=transformed_data_csv
            )
            print("Final data upload complete.")
        
            return FINAL_KEY
        
        # --- Define the Dependencies ---
    task_move = move_landing_raw()
    task_etl = etl(raw_file_key=task_move)
    task_write = write_transformed_s3(transformed_data_csv=task_etl)

    start_task >> task_move >> task_etl >> task_write

move_etl()