import io
import json
import boto3
import pandas as pd

SEMICOLON_FILES = {"department", "municipality"}


def sizeof_fmt(num, suffix="B"):
    """
    Human readable file size. Taken from https://stackoverflow.com/a/1094933
    """
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


s3 = boto3.client("s3")


def handler(event, context):
    """
    Entry point for the lambda function.

    Load csv file, store ir as parquet in a different location and delete the
    original csv file
    """
    for record in event["Records"]:
        bucket = record["s3"]["bucket"]["name"]
        key = record["s3"]["object"]["key"]
        size = record["s3"]["object"]["size"]
        filename_prefix = key.split("/")[-1].split("_")[0]

        print(f"Loading s3://{bucket}/{key} of size {sizeof_fmt(size)} bytes")
        csv_bytes = s3.get_object(Bucket=bucket, Key=key).get("Body")
        df = pd.read_csv(
            csv_bytes, sep=";" if filename_prefix in SEMICOLON_FILES else ","
        )

        pq_key = key.replace("preprocessed", "parquetized").replace("csv", "parquet")
        with io.BytesIO() as pq_buffer:
            df.to_parquet(pq_buffer, index=False)
            s3.put_object(Bucket=bucket, Key=pq_key, Body=pq_buffer.getvalue())

        pq_size = s3.head_object(Bucket=bucket, Key=pq_key).get("ContentLength")
        print(
            f"Stored s3://{bucket}/{pq_key} of size {sizeof_fmt(pq_size)} bytes.",
            f"Size reduction of {(size-pq_size)/size:.2%}",
        )

        s3.delete_object(Bucket=bucket, Key=key)

    return {"statusCode": 200, "body": {}}


if __name__ == "__main__":
    with open("test-event.json") as jsonfile:
        event = json.load(jsonfile)
    handler(event, None)
