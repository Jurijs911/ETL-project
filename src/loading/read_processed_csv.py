import csv
import boto3


def iterate_bucket_items(bucket):
    """
    Generator that iterates over all objects in a given s3 bucket

    Parameters:
    bucket: name of s3 bucket

    Returns:
    dict of metadata for an object
    """

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                yield item


def read_processed_csv(bucket_name="kp-northcoders-processed-bucket"):
    s3_client = boto3.client("s3")

    processed_data = {
        "fact_sales_order": [],
        "dim_date": [],
        "dim_staff": [],
        "dim_location": [],
        "dim_currency": [],
        "dim_design": [],
        "dim_counterparty": [],
    }

    for item in iterate_bucket_items(bucket=bucket_name):
        if "last_loaded.txt" not in item['Key']:
            response = (
                s3_client.get_object
                (Bucket=bucket_name, Key=item["Key"])["Body"]
                .read()
                .decode("utf-8")
                .splitlines()
            )
            records = csv.reader(response, delimiter="|")
            next(records)
            table_data = []
            for row in records:
                table_data.append(row)
            processed_data[item["Key"].split(".")[0]] = table_data

    return processed_data
