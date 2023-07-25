import csv
import boto3


def add_csv(data, table_name):
    with open("add.csv", "w", newline="") as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for row in data:
            writer.writerow(row)

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file("add.csv", "kp-northcoder-ingestion-bucket", "design/add.csv")
    except Exception as e:
        print("Error occured while uploading object: ", e)


def update_csv(data, table_name):
    with open("update.csv", "w", newline="") as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()

        for row in data:
            writer.writerow(row)

    try:
        s3_client = boto3.client("s3")
        s3_client.upload_file("update.csv", "kp-northcoder-ingestion-bucket", "design/update.csv")
    except Exception as e:
        print("Error occured while uploading object: ", e)
