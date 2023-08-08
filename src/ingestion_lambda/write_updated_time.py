import logging
import boto3
from botocore.exceptions import ClientError


def write_updated_time(timestamp, table):
    """
    Write the updated timestamp to a file and upload it to an Amazon S3 bucket.

    This function takes a timestamp and a table name, and writes the timestamp
    to a file. The file is then uploaded to an Amazon S3 bucket with a
    specific key.

    Args:
        timestamp:
        The updated timestamp to be written and uploaded.

        table:
        Name of the table, used to construct the S3 key.

    Returns:
        dict:
        If the upload is successful, returns the response dictionary, else
        returns False.

    Raises:
        ClientError:
        If there's an issue while writing the updated timestamp to a txt file.
    """
    s3_resource = boto3.resource("s3")

    bucket_name = "kp-northcoders-ingestion-bucket"
    key = f"{table}/created_at.txt"

    updated_time = timestamp

    with open("/tmp//created_at.txt", "w") as f:
        f.write(updated_time)

    try:
        response = s3_resource.Object(bucket_name, key).put(
            Body=open("/tmp//created_at.txt", "rb")
        )

    except ClientError as e:
        logging.error(e)
        return False
    return response
