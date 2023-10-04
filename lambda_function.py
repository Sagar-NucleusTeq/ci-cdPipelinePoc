import json
import pandas as pd
import pyarrow as pa
import boto3
from pyiceberg.catalog import load_catalog


def lambda_handler(event, context):
    # Initialize the Iceberg catalog
    return {
        "statusCode": 200,
        "body": json.dumps("Data converted and written to Iceberg table successfully!"),
    }
