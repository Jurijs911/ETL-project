import csv
import boto3
import os
from botocore.exceptions import ClientError


def upload_csv(data, table_name, bucket_name):
    s3_client = boto3.client("s3")

    try:
        downloaded_csv = (
            s3_client.get_object(Bucket=bucket_name, Key=f"{table_name}.csv")[
                "Body"
            ]
            .read()
            .decode("utf-8")
            .split("\r\n")
        )
        with open(f"{table_name}.csv", "w", newline="") as csvfile:
            writer = csv.writer(csvfile)
            for row in downloaded_csv:
                writer.writerow([row])
    except ClientError:
        pass

    with open(f"{table_name}.csv", "a", newline="") as csvfile:
        if len(data) > 0:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for row in data:
                writer.writerow(row)
        else:
            pass

    s3_client.upload_file(
        f"{table_name}.csv", bucket_name, f"{table_name}.csv"
    )

    os.remove(f"{table_name}.csv")
