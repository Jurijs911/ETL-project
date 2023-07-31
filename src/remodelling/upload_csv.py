import csv
import boto3
import os


def upload_csv(data, table_name, bucket_name):
    with open(f"{table_name}.csv", "w", newline="") as csvfile:
        if len(data) > 0:
            fieldnames = data[0].keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()

            for row in data:
                writer.writerow(row)
        else:
            pass

    s3_client = boto3.client("s3")
    s3_client.upload_file(
        f"{table_name}.csv", bucket_name, f"{table_name}.csv"
    )

    os.remove(f"{table_name}.csv")
