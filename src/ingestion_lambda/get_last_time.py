import boto3


def get_last_time(table_name):
    s3_client = boto3.client("s3")
    # add try block
    # Read file from bucket
    bucket_name = "kp-northcoders-ingestion-bucket"
    key = f"{table_name}/created_at.txt"

    file = s3_client.get_object(Bucket=bucket_name, Key=key)

    file_content = file["Body"].read().decode("utf-8")

    # returns

    return file_content
