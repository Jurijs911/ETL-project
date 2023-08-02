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
            .split("\n")
        )

        if downloaded_csv[0] != "":
            for idx, row in enumerate(downloaded_csv):
                if idx > 0:
                    old_data = {
                        key: row.split(",")[i]
                        for dict in data
                        for i, key in enumerate(dict)
                    }
                    data.append(old_data)

    except ClientError:
        pass

    with open(f"{table_name}.csv", "w", newline="") as csvfile:
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
