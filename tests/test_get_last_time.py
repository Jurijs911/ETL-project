from src.ingestion_lambda.utils.get_last_time import get_last_time
from moto import mock_s3
import boto3


@mock_s3
def test_get_last_time_returns_time_can_access_object_in_bucket():

    s3_client = boto3.client("s3")
    s3_client.create_bucket(
        Bucket='kp-northcoder-ingestion-bucket',
        CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'},
    )

    s3_client.put_object(
        Body=b'test_data',
        Bucket='kp-northcoder-ingestion-bucket',
        Key='test/created_at.txt'
    )

    assert get_last_time('test') == 'test_data'
