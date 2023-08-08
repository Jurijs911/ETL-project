from src.ingestion_lambda.get_secret import get_secret
import boto3
from botocore.exceptions import ClientError
from moto import mock_secretsmanager
import json
import pytest


@mock_secretsmanager
def test_get_secret():
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

    with pytest.raises(ClientError):
        get_secret()
