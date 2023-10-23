import json
import pandas as pd
import pyarrow as pa
import boto3
from pyiceberg.catalog import load_catalog
import awswrangler as wr

def lambda_handler(event, context):
    # Initialize the Iceberg catalog
    ice_catalog = load_catalog("glue", **{"type": "glue"})

    # Initialize the S3 client
    s3 = boto3.client("s3")

    # Define the S3 bucket name
    s3_bucket_name = "aaa-lakehouse-pilot"
    s3_prefix = "1-raw"

    # List of S3 object keys and corresponding Iceberg table names
    lookup_tables = [
        {
            "s3_object_key": "club_city_zip/club_city_zip.parquet",
            "iceberg_table_name": "club_city_zip_iceberg_table",
        },
        {
            "s3_object_key": "club_city_zip_suppression/club_city_zip_suppression.parquet",
            "iceberg_table_name": "club_city_zip_suppression_iceberg_table",
        },
        {
            "s3_object_key": "club_state_rollup_scd/club_state_rollup_scd.parquet",
            "iceberg_table_name": "club_state_rollup_scd_iceberg_table",
        },
        {
            "s3_object_key": "metadata/metadata.parquet",
            "iceberg_table_name": "metadata_iceberg_table",
        },
        {
            "s3_object_key": "operation_codes/operationcodes.parquet",
            "iceberg_table_name": "operationcodes_iceberg_table",
        },
        # Add more S3 object keys and table names as needed
    ]

    for table_info in lookup_tables:
        s3_object_key = table_info["s3_object_key"]
        iceberg_table_name = table_info["iceberg_table_name"]

        s3_url_file = f"s3://{s3_bucket_name}/{s3_prefix}/{s3_object_key}"

        # Read the downloaded CSV file into a Pandas DataFrame
        df = wr.s3.read_parquet(s3_url_file)
                
        print(f"Reading data from {s3_object_key}")
        print(df)
        print(df.count())
        print("Parquet file read into Pandas DataFrame.")
        print("test .")

        # Convert all columns to strings
        df = df.astype(str)
        print("Parquet file columns coverted into string.")

        database_name = "lakehouse_pilot_db"

        # Cleanup table before create
        wr.catalog.delete_table_if_exists(database=database_name, table=iceberg_table_name)

        table_location = f"s3://{s3_bucket_name}/iceberg-data/{iceberg_table_name}"
        temp_table_path = f"s3://{s3_bucket_name}/iceberg_data_temp/{iceberg_table_name}"

        wr.athena.to_iceberg(
            df=df,
            database=database_name,
            table=iceberg_table_name,
            table_location=table_location,
            temp_path=temp_table_path,
        )
        print(f"Iceberg table {iceberg_table_name} created successfully.")

    return {
        "statusCode": 200,
        "body": json.dumps("Data converted and written to Iceberg tables successfully!"),
    }

