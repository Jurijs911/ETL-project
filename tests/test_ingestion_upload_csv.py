from src.ingestion_lambda.upload_csv import upload_csv
import boto3
from moto import mock_s3
import pytest


@mock_s3
def test_upload_csv():
    """
    Test the upload_csv function to ensure it uploads CSV data to S3.

    This test uses the 'moto' library to mock AWS S3 service.
    It sets up a mock S3 bucket and object, then calls the upload_csv
    function with test data. Finally, it asserts that the object in S3
    was updated as expected.
    """
    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    conn.Object(
        "kp-northcoders-ingestion-bucket", "address.csv"
    ).put(Body="test")

    data = [{"1": "test"}]
    upload_csv(data, "address", "kp-northcoders-ingestion-bucket")

    body = (
        conn.Object("kp-northcoders-ingestion-bucket", "address.csv")
        .get()["Body"]
        .read()
        .decode("utf-8")
    )

    assert body == '1\r\ntest\r\n'


def test_upload_csv_raises_exception():
    """
    Test the upload_csv function's behavior when encountering an exception.

    This test checks whether calling upload_csv with invalid parameters raises an exception.
    """
    with pytest.raises(Exception):
        upload_csv("test_data", "currency", "test_bucket")
