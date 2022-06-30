import boto3
import json
from typing import Dict

def get_secret(aws: boto3.Session, secret_name: str) -> Dict[str, str]:
    sm = aws.client("secretsmanager")
    response = sm.get_secret_value(SecretId=secret_name)
    secret = response["SecretString"]
    return json.loads(secret)
