from airflow.decorators import dag, task
from airflow import Dataset
#from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.s3 import (S3CopyObjectOperator, S3ListOperator)
from pendulum import datetime

MY_S3_BUCKET = "aditya-saeed-landing-bucket"
MY_FIRST_FOLDER = "landing-file/"
MY_S3_BUCKET_DELIMITER = "/"
MY_FILE_NAME = "tripdata.csv"
AWS_CONN_ID = "aws_conn"



MY_S3_BUCKET_TO_COPY_TO = "saeed-rawdata-bucket"

my_dataset = Dataset(
    f's3://{MY_S3_BUCKET}{MY_S3_BUCKET_DELIMITER}{MY_FIRST_FOLDER}{MY_FILE_NAME}'
)

@dag(
    dag_id ="file_transfer_btw_s3s",
    start_date = datetime(2022, 12, 1),
    schedule = None,
    catchup = False,
    tags = ['datasets', 'taskflow', 'usecase'],
)
def downstream_datasets_taskflow_usecase():

    # list all teh files in the first folder in S3 bucket
    #@task
    list_files = S3ListOperator(
        task_id = f'list_files',
        aws_conn_id = AWS_CONN_ID,
        bucket = MY_S3_BUCKET,
        prefix = MY_FIRST_FOLDER,
        delimiter = MY_S3_BUCKET_DELIMITER,
    )

    # copy files
    #@task
    copy_files = S3CopyObjectOperator.partial(
        task_id = "copy_files",
        aws_conn_id = AWS_CONN_ID,
    ).expand_kwargs(
        list_files.output.map(
            lambda x:{
                "source_bucket_key": f"s3://{MY_S3_BUCKET}{MY_S3_BUCKET_DELIMITER}{x}",
                "dest_bucket_key" : f"s3://{MY_S3_BUCKET_TO_COPY_TO}{MY_S3_BUCKET_DELIMITER}"
            }
        )
    )
    list_files >> copy_files

downstream_datasets_taskflow_usecase()