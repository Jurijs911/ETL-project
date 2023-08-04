from datetime import datetime
import boto3
import os


def write_timestamp(data, table_name):
    s3_client = boto3.client("s3")

    last_processed = (
        s3_client.get_object(
            Bucket="kp-northcoders-ingestion-bucket",
            Key=f"{table_name}/last_processed.txt",
        )["Body"]
        .read()
        .decode("utf-8")
    )

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
        "/tmp//last_processed.txt",
        "kp-northcoders-ingestion-bucket",
        f"{table_name}/last_processed.txt",
    )

    os.remove("/tmp//last_processed.txt")
