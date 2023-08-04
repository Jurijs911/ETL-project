from datetime import datetime
import boto3


def write_timestamp(data, table_name):
    s3_client = boto3.client("s3")

    last_processed = "1900-1-25 15:20:49.962000"

    for row in data:
        for column in row:
            try:
                if datetime.strptime(
                    column, "%Y-%m-%d %H:%M:%S.%f"
                ) > datetime.strptime(last_processed, "%Y-%m-%d %H:%M:%S.%f"):
                    last_processed = column
            except Exception:
                pass

    with open("/tmp//last_processed.txt", "w") as f:
        f.write(last_processed)

    s3_client.upload_file(
        "last_processed.txt",
        "kp-northcoders-processed-bucket",
        f"/tmp//{table_name}/last_processed.txt",
    )