from src.ingestion_lambda.write_updated_time import write_updated_time
from moto import mock_s3
import boto3


@mock_s3
def test_write_updated_time_returns_status_code_when_successfully_uploading():
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    write_response = write_updated_time("test_timestamp", "test_table")

    assert write_response["ResponseMetadata"]["HTTPStatusCode"] == 200
