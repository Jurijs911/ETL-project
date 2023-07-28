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


def copy_timestamps():
    s3 = boto3.resource("s3")

    for item in iterate_bucket_items("kp-northcoders-ingestion-bucket"):
        copy_source = {
            "Bucket": "kp-northcoders-ingestion-bucket",
            "Key": item["Key"],
        }
        bucket = s3.Bucket("kp-northcoders-processed-bucket")
        bucket.copy(copy_source, item["Key"])
