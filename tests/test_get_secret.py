from src.ingestion_lambda.get_secret import get_secret
import boto3
from botocore.exceptions import ClientError
from moto import mock_secretsmanager
import json
import pytest


@mock_secretsmanager
def test_get_secret():
    """
    Test the get_secret function to ensure it retrieves secret values correctly.

    This test uses the moto library to mock AWS Secrets Manager service.
    It creates a mock secret and checks whether the retrieved secret matches
    the expected value.
    """
    secret_name = "totesys_DB_access"
    region_name = "eu-west-2"

    client = boto3.client(
        service_name="secretsmanager", region_name=region_name
    )

    secret_value = {"username": "testuser", "password": "testpass"}

    client.create_secret(Name=secret_name,
                         SecretString=json.dumps(secret_value))

    result = get_secret()

    assert result == secret_value


@mock_secretsmanager
def test_get_secret_raises_clienterror():
    """
    Test the get_secret function's behavior when encountering a ClientError.

    This test uses the `moto` library to mock AWS Secrets Manager service.
    It checks whether calling get_secret() raises a ClientError as expected.
    """
    with pytest.raises(ClientError):
        get_secret()
