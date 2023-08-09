from src.ingestion_lambda.upload_csv import upload_csv
import boto3
from moto import mock_s3
import pytest


@mock_s3
def test_upload_csv():
    # Set up mock S3 bucket and object
    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    conn.Object(
        "kp-northcoders-ingestion-bucket", "address.csv"
    ).put(Body="test")

    # Call the function to test
    data = [{"1": "test"}]
    upload_csv(data, "address", "kp-northcoders-ingestion-bucket")

    # Assert that the object in S3 was updated with the new data
    body = (
        conn.Object("kp-northcoders-ingestion-bucket", "address.csv")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )

    assert body == '1\r\ntest\r\n'


def test_upload_csv_raises_exception():
    with pytest.raises(Exception):
        upload_csv("test_data", "currency", "test_bucket")
