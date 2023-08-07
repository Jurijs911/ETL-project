"""
Retrieves data from csv file and returns it for use in manipulation utils

Iterates over each object in bucket.
For each object, it retrieves its value.
Stores the value in a dictionary under either 'update' or 'add'.
Grouped by the table name
{
    'design': [{'department_id': data, 'department_name': data etc.}]
    'currency': [{'currency_id': data, 'currency_code': data etc.}]
    etc.
}
"""
import csv
import boto3


def iterate_bucket_items(bucket):
    """
    Generator that iterates over all objects in a given s3 bucket

    :param bucket: name of s3 bucket
    :return: dict of metadata for an object
    """

    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                yield item


def read_ingestion_csv(bucket_name="kp-northcoders-ingestion-bucket"):
    """
    Retrieves data from CSV files in an S3 bucket and returns it for use in
    manipulation util functions.

    The function iterates over objects in the specified bucket and retrieves
    their data. The data is grouped by table name and stored in a dictionary.

    :param bucket_name:
    Name of the S3 bucket. Default is 'kp-northcoders-ingestion-bucket'.

    :return:
    Dictionary containing ingested data grouped by table name.
             e.g. {
                    'sales_order': [...],
                    'design': [...],
                    'currency': [...],
                    'staff': [...],
                    'counterparty': [...],
                    'address': [...],
                    'department': [...],
                }
    """
    s3_client = boto3.client("s3")

    ingested_data = {
        "sales_order": [],
        "design": [],
        "currency": [],
        "staff": [],
        "counterparty": [],
        "address": [],
        "department": [],
    }

    for item in iterate_bucket_items(bucket=bucket_name):
        if "last_processed" not in item["Key"]:
            response = (
                s3_client.get_object(Bucket=bucket_name, Key=item["Key"])[
                    "Body"
                ]
                .read()
                .decode("utf-8")
                .splitlines()
            )
            records = csv.reader(response, delimiter="|")
            next(records)
            table_data = []
            for row in records:
                table_data.append(row)
            ingested_data[item["Key"].split(".")[0]] = table_data
    print(ingested_data)
    return ingested_data
