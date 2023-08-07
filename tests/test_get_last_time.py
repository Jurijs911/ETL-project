from src.ingestion_lambda.get_last_time import get_last_time
from moto import mock_s3
import boto3


@mock_s3
def test_get_last_time_returns_time_can_access_object_in_bucket():
    """
    Test whether get_last_time function returns the correct timestamp when it
    can access the object in the bucket.

    This test function checks whether the get_last_time function correctly
    retrieves the timestamp from an Amazon S3 bucket when the object exists in
    the specified bucket. Test uses the moto library to mock the S3 service.

    The test creates a mock S3 bucket, puts a sample test_data into the bucket
    with the key 'test/created_at.txt', and then calls the get_last_time
    function with the table name. The test asserts that the returned value
    matches the test_data, indicating that the function successfully accessed
    and retrieved the timestamp from the bucket.
    """

    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket='kp-northcoders-ingestion-bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
    )

    s3_client.put_object(
        Body=b'test_data',
        Bucket='kp-northcoders-ingestion-bucket',
        Key='test/created_at.txt'
    )

    assert get_last_time('test') == 'test_data'
