import boto3


def get_last_time(table_name):
    """
    Get the last time a table was updated or new records were created.

    This function retrieves the last time that records were created or updated
    in the specified table. It reads the content of a text file in an Amazon
    S3 bucket that contains the timestamp of the last update.

    Parameters:
    table_name:
    The name of the table for which to retrieve the last update time.

    Returns:
    string:
    A string representing the last update time in the format
    "YYYY-MM-DD HH:MM:SS.MS".
    """
    s3_client = boto3.client("s3")

    bucket_name = "kp-northcoders-ingestion-bucket"
    key = f"{table_name}/created_at.txt"

    file = s3_client.get_object(Bucket=bucket_name, Key=key)

    file_content = file["Body"].read().decode("utf-8")

    return file_content
