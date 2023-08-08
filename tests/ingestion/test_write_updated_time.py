from src.ingestion_lambda.write_updated_time import write_updated_time
from moto import mock_s3
import boto3


@mock_s3
def test_write_updated_time_returns_status_code_when_successfully_uploading():
    """
    Test the write_updated_time function for successful uploading.

    This function tests the behavior of the write_updated_time function when
    it successfully uploads a timestamp to Amazon S3. It uses the moto library
    to mock the Amazon S3 service and then calls the write_updated_time
    function with test data. The HTTP status code of the response is asserted
    to check it was successful (200 OK).

    Raises:
        AssertionError: If the assertions fail.
    """
    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    write_response = write_updated_time("test_timestamp", "test_table")

    assert write_response["ResponseMetadata"]["HTTPStatusCode"] == 200
