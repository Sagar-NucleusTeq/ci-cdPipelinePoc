import json
import pandas as pd
import awswrangler as wr

def lambda_handler(event, context):
    # Create a simple DataFrame
    data = {'Name': ['John', 'Alice', 'Bob'],
            'Age': [28, 24, 32]}

    df = pd.DataFrame(data)

    # Define the Iceberg table name 
    iceberg_table_name = "iceberg_table"

    # Create the Iceberg table
    database_name = "lakehouse_pilot_db"

    # Cleanup table before create (optional)
    wr.catalog.delete_table_if_exists(database=database_name, table=iceberg_table_name)

    # Create the Iceberg table
    table_location = f"s3://{s3_bucket_name}/iceberg-data/{iceberg_table_name}"
    temp_table_path = f"s3://{s3_bucket_name}/iceberg_data_temp/{iceberg_table_name}"

    wr.athena.to_iceberg(
        df=df,
        database=database_name,
        table=iceberg_table_name,
        table_location=table_location,
        temp_path=temp_table_path
    )

    return {
        "statusCode": 200,
        "body": json.dumps("Dataframe created and written to Iceberg table successfully!")
    }
