import json
import boto3
from botocore.exceptions import ClientError


def get_secret():
    secret_name = "arn:aws:secretsmanager:eu-west-2:124301163178:\
        secret:Target_DB_Access-CLApPX"
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
