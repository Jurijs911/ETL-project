import logging
import boto3
from botocore.exceptions import ClientError


def write_updated_time(timestamp, table):
    s3_resource = boto3.resource("s3")

    bucket_name = "kp-northcoder-ingestion-bucket"
    key = f"{table}/created_at.txt"

    updated_time = timestamp

    with open("created_at.txt", "w") as f:
        f.write(updated_time)

    try:
        response = s3_resource.Object(bucket_name, key).put(
            Body=open("created_at.txt", "rb")
        )

    except ClientError as e:
        logging.error(e)
        return False
    return response
