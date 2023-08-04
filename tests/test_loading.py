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

# @mock_logs
# @mock_s3
# def test_loading_lambda_calls_read_processed_csv():
#     conn = boto3.resource("s3", region_name="eu-west-2")
#     conn.create_bucket(
#         Bucket="kp-northcoders-loading-bucket",
#         CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
#     )

#     client = boto3.client("logs", region_name="eu-west-2")
#     client.create_log_group(logGroupName="/aws/lambda/loading-lambda")
#     client.create_log_stream(
#         logGroupName="/aws/lambda/loading-lambda",
#         logStreamName="lambda-log-stream",
#     )

#     with patch("src.loading.loading.read_processed_csv") as \
#          mock_processed_csv:
#         lambda_handler(
#                     {},
#                     {},
#                     db_user=test_user,
#                     db_database=test_database,
#                     db_host=test_host,
#                     db_port=test_port,
#                     db_password=test_password,
#                 )
#         mock_processed_csv.assert_called_once()


@mock_s3
@mock_logs
def test_loading_lambda_calls_read_processed_csv(mocker):
    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    client = boto3.client("logs", region_name="eu-west-2")
    client.create_log_group(logGroupName="/aws/lambda/loading-lambda")
    client.create_log_stream(
        logGroupName="/aws/lambda/loading-lambda",
        logStreamName="lambda-log-stream",
    )
    conn.Object(
        "kp-northcoders-processed-bucket", "address/last_loaded.txt"
    ).put(Body="2023-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "counterparty/last_loaded.txt"
    ).put(Body="2023-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "currency/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "department/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "design/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "fact_sales_order/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "staff/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    mock_read_processed_csv = mocker.patch("src.loading.read_processed_csv.read_processed_csv")
    lambda_handler(
        {},
        {},
        db_user=test_user,
        db_database=test_database,
        db_host=test_host,
        db_port=test_port,
        db_password=test_password,
    )
    mock_read_processed_csv.assert_called()
