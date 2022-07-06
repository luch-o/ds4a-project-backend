import io
import os
import json
import boto3
import pandas as pd
import psycopg2
from typing import Dict, List
from functions.tables import (
    BaseTable,
    Departments,
    DepartmentsPopulationHistory,
    Municipalities,
    MunicipalityPopulationHistory,
    InterfamilyViolence,
    Suicides,
    SuicideAttempts
)

# contants

SECRET_NAME = os.environ["SECRET_NAME"]
TABLES_DICT: Dict[str, List[BaseTable]] = {
    "department": [Departments, DepartmentsPopulationHistory],
    "municipalities": [Municipalities],
    "municipality": [MunicipalityPopulationHistory],
    "suicides": [Suicides],
    "attempts": [SuicideAttempts],
    "interfamily": [InterfamilyViolence]
}

# functions

def get_secret(aws: boto3.Session, secret_name: str) -> Dict[str, str]:
    """
    Retrive secret from secrets manager
    """
    sm = aws.client("secretsmanager")
    response = sm.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)

# intialization

aws = boto3.Session()
s3 = aws.resource("s3")
db_secret = get_secret(aws, SECRET_NAME)
conn = psycopg2.connect(
    host=db_secret["host"],
    database=db_secret["dbname"],
    user=db_secret["username"],
    password=db_secret["password"]
)
cur = conn.cursor()

# handler
def handler(event, context):
    """
    Entry point for the lambda function.

    Identify table corresponding to file, create it and ingest data from the file to it
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        
        filename_prefix = key.split("/")[-1].split("_")[0]
        tables = TABLES_DICT.get(filename_prefix)
        print(f"Processing {filename_prefix} ({key})")

        if tables:
            buffer = io.BytesIO()        
            s3_obj = s3.Object(bucket, key)
            s3_obj.download_fileobj(buffer)
            df = pd.read_parquet(buffer)

            for table in tables:
                print(f"Ingesing data into {table.name}")
                table.create_table(cur, conn)
                table.insert_data(df, cur, conn)
    
            print("Data ingestion succesfull")
        else:
            print("No tables associated to file found by prefix")


    return {"status": 200}
