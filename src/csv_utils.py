import csv
import boto3
from moto import s3


def add_csv(data, table_name, bucket_name="kp-northcoder-ingestion-bucket"):
    with open("add.csv", "w", newline="") as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for row in data:
            writer.writerow(row)

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file("add.csv", bucket_name, "design/add.csv")
    except Exception as e:
        print("Error occured while uploading object: ", e)


def update_csv(data, table_name, bucket_name="kp-northcoder-ingestion-bucket"):
    with open("update.csv", "w", newline="") as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for row in data:
            writer.writerow(row)

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file("update.csv", bucket_name, "design/update.csv")
    except Exception as e:
        print("Error occured while uploading object: ", e)
