import awswrangler as wr
import json
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from smart_open import smart_open
from datetime import datetime
import pytz
import pyarrow.parquet as pq
import s3fs
import boto3
s3 = s3fs.S3FileSystem()

sqs_client = boto3.client('sqs', region_name='us-east-1')
lambda_client = boto3.client('lambda', region_name='us-east-1')

def write_to_iceberg(source_s3_file_path):
    print("write_to_iceberg")
    s3 = boto3.client("s3") 
    # Read the downloaded Parquet file into a Pandas DataFrame
    df = wr.s3.read_csv(source_s3_file_path)
    print(df)
    # Rename the columns
    df.columns = ["facility_id_zip", "operation_code", "scanned_at", "routing_code", "imdb_tracking_code"]
    # Add "filename" column with the S3 object key value
    df['filename'] = source_s3_file_path.split('/')[-1]
    # Add "load_timestamp" column with the current timestamp
    df['load_timestamp'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(df)
    print(df.count())
    # Specify the Iceberg-related configurations
    database_name = "pilot_landing_layer"
    table_name = "usps_scans"
    table_location = "s3://aaalife-data-dse-preprd-dbt-pilot-lakehouse-us-east-1/table_path/usps/"
    temp_table_path = "s3://aaalife-data-dse-preprd-dbt-pilot-lakehouse-us-east-1/temp_paths/usps_temp_path/"
    # Cleanup the table before creating it
    # wr.catalog.delete_table_if_exists(database=database_name, table=table_name)

    try:
        wr.athena.to_iceberg(
            df=df,
            database=database_name,
            table=table_name,
            table_location=table_location,
            temp_path=temp_table_path,
        )

        return True

    except Exception as e:
        print('-----Exception-------')
        print(str(e))
        return False

def delete_sqs_message(event):
    sqs_receipt_handle = event['sqs_receipt_handle']
    sqs_url = event['sqs_url']
    s3_filepath = event['s3_filepath']


    dlt_response = sqs_client.delete_message(
        QueueUrl=sqs_url,
        ReceiptHandle=sqs_receipt_handle
    )

    print(f"Deleted sqs message for file: {s3_filepath}")
    print(dlt_response['ResponseMetadata']['HTTPStatusCode'])

def lambda_handler(event, context):
    print('-----------event-----------')
    print("payload", event)
    s3_url_file = event['s3_filepath']
    sqs_receipt_handle=event['sqs_receipt_handle'] 
    sqs_url=event['sqs_url']
    # sqs_msg_details = get_s3_filepath_n_queue_info(event)
    write_status = write_to_iceberg(source_s3_file_path=event['s3_filepath'])

    if write_status:
        delete_sqs_message(event)
    else:
        print(f"------Error processing {event['s3_filepath']}---------")