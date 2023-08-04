from src.loading.loading import lambda_handler
from src.loading.loading_write_timestamp \
    import loading_write_timestamp
import os
from unittest.mock import patch
from moto import mock_s3, mock_logs
import boto3
from dotenv import load_dotenv
import pytest


load_dotenv()

test_user = os.environ.get("TEST_TARGET_USER")
test_database = os.environ.get("TEST_TARGET_DATABASE")
test_host = os.environ.get("TEST_TARGET_HOST")
test_port = os.environ.get("TEST_TARGET_PORT")
test_password = os.environ.get("TEST_TARGET_PASSWORD")


# Mock all individual loading utils, and assert that they
# are being called once

# Test that error handling is successfully carried out

@mock_logs
@mock_s3
def test_loading_lambda_calls_read_processed_csv():

    client = boto3.client("logs", region_name="eu-west-2")
    client.create_log_group(logGroupName="/aws/lambda/loading-lambda")
    client.create_log_stream(
        logGroupName="/aws/lambda/loading-lambda",
        logStreamName="lambda-log-stream",
    )

    with patch("src.loading.loading.read_processed_csv") as \
         mock_processed_csv:
        lambda_handler(
                    {},
                    {},
                    db_user=test_user,
                    db_database=test_database,
                    db_host=test_host,
                    db_port=test_port,
                    db_password=test_password,
                )
        mock_processed_csv.assert_called_once()
