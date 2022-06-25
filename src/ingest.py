import os
import json
import boto3
import pandas as pd
import psycopg2
from typing import Dict, List
from src.tables import (
    BaseTable,
    Departments,
    DepartmentsPopulationHistory,
    Municipalies,
    MunicipalityPopulationHistory,
)

# contants

SECRET_NAME = os.environ["SECRET_NAME"]
TABLES_DICT: Dict[str, List[BaseTable]] = {
    "departments": [Departments, DepartmentsPopulationHistory],
    "municipalities": [Municipalies, MunicipalityPopulationHistory]
}

# functions

def get_secret(aws: boto3.Session, secret_name: str) -> Dict[str, str]:
    sm = aws.client("secretsmanager")
    response = sm.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)

# intialization

aws = boto3.Session()
s3 = aws.client("s3")
db_secret = get_secret(aws, SECRET_NAME)
conn = psycopg2.connect(
    host=db_secret["host"],
    database=db_secret["dbname"],
    user=db_secret["username"],
    password=db_secret["password"]
)
cur = conn.cursor()

# handler
def lambda_handler(event, context):
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        
        filename_prefix = key.split("/")[-1].split("_")[0]
        tables = TABLES_DICT.get(filename_prefix)

        if tables:        
            obj_bytes = s3.get_object(Bucket=bucket, Key=key).get("Body")
            df = pd.read_parquet(obj_bytes)

            for table in tables:
                table.create_table(cur, conn)
                table.insert_data(df, cur, conn)

    return {"status": 200}
