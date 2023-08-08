import json
import boto3
from botocore.exceptions import ClientError


def get_secret():
    """
    Retrieve and decode a secret from AWS Secrets Manager.

    This function fetches a secret from AWS Secrets Manager using the provided
    'boto3' library. The secret value is returned in JSON format.

    Returns:
        dict:
        A dictionary containing the secret information.

    Raises:
        ClientError:
        If there's an issue while fetching the secret.
    """
    secret_name = "totesys_DB_access"
    region_name = "eu-west-2"

    client = boto3.client(
        service_name="secretsmanager", region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    secret = get_secret_value_response["SecretString"]

    return json.loads(secret)
