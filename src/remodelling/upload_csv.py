import csv
import boto3
import os
from botocore.exceptions import ClientError


def upload_csv(data, table_name, bucket_name):
    """
    Uploads CSV data to an S3 bucket.

    This function takes a list of dictionaries representing CSV data, converts
    it into a CSV file, and uploads it to the specified S3 bucket with the
    given table name.

    Note: The function checks if the table already exists in the S3 bucket,
    and if so, it appends the new data to the existing file.

    :param data:
    A list of dictionaries representing the CSV data.

    :param table_name:
    The name of the table corresponding to the CSV data.

    :param bucket_name:
    The name of the S3 bucket to which the CSV data will be uploaded.
    """

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

        if downloaded_csv != [""]:
            keys = list(data[0].keys())
            for idx, row in enumerate(downloaded_csv):
                if idx > 0:
                    data.append(
                        {
                            keys[idx]: item
                            for idx, item in enumerate(row.split("|"))
                        }
                    )

    except ClientError:
        pass

    with open(f"/tmp//{table_name}.csv", "w", newline="") as csvfile:
        if len(data) > 0:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(
                csvfile, fieldnames=fieldnames, delimiter="|"
            )

            writer.writeheader()

            for row in data:
                writer.writerow(row)
        else:
            pass

    s3_client.upload_file(
        f"/tmp//{table_name}.csv", bucket_name, f"{table_name}.csv"
    )

    os.remove(f"/tmp//{table_name}.csv")
