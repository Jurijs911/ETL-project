from src.loading.loading import lambda_handler
from src.loading import loading
import os
from unittest.mock import patch
from moto import mock_s3, mock_logs
import boto3
import pytest
from dotenv import load_dotenv


load_dotenv()

test_user = os.environ.get("TEST_TARGET_USER")
test_database = os.environ.get("TEST_TARGET_DATABASE")
test_host = os.environ.get("TEST_TARGET_HOST")
test_port = os.environ.get("TEST_TARGET_PORT")
test_password = os.environ.get("TEST_TARGET_PASSWORD")


@mock_s3
@mock_logs
def test_loading_lambda_calls_read_processed_csv(mocker):
    """
    Test the lambda_handler function to ensure it calls the read_processed_csv function.

    This test uses the moto library to mock AWS S3 and CloudWatch Logs services.
    It sets up mock S3 buckets and objects, then calls the lambda_handler function.
    The test asserts that the read_processed_csv function was called as expected.
    """
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
        "kp-northcoders-processed-bucket", "dim_location/last_loaded.txt"
    ).put(Body="2023-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_counterparty/last_loaded.txt"
    ).put(Body="2023-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_currency/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_date/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_design/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "fact_sales_order/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_staff/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    spy = mocker.spy(loading, "read_processed_csv")

    lambda_handler(
        {},
        {},
        db_user=test_user,
        db_database=test_database,
        db_host=test_host,
        db_port=test_port,
        db_password=test_password,
    )

    spy.assert_called_with("kp-northcoders-processed-bucket")


@mock_logs
@mock_s3
def test_loading_lambda_handler_logs_no_new_data(mocker):
    """
    Test the lambda_handler function's behavior when there is no new data.

    This test checks whether the lambda_handler function logs a message for each table
    indicating that no new data has been inserted into that table.
    """
    client = boto3.client("logs", region_name="eu-west-2")
    client.create_log_group(logGroupName="/aws/lambda/loading-lambda")
    client.create_log_stream(
        logGroupName="/aws/lambda/loading-lambda",
        logStreamName="lambda-log-stream",
    )

    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_location/last_loaded.txt"
    ).put(Body="2024-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_counterparty/last_loaded.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_currency/last_loaded.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_date/last_loaded.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_design/last_loaded.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "fact_sales_order/last_loaded.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_staff/last_loaded.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    spy = mocker.spy(loading, "log_to_cloudwatch")

    with patch(
        "src.loading.loading.read_processed_csv"
    ) as mock_read_processed_csv:
        mock_read_processed_csv.return_value = {
            "fact_sales_order": [],
            "dim_date": [],
            "dim_staff": [],
            "dim_location": [],
            "dim_currency": [],
            "dim_design": [],
            "dim_counterparty": [],
        }

        lambda_handler(
            {},
            {},
            test_user,
            test_database,
            test_host,
            test_port,
            test_password,
        )
        spy.assert_any_call(
            "No data has been inserted into the dim_design table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )

        spy.assert_any_call(
            "No data has been inserted into the dim_currency table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )

        spy.assert_any_call(
            "No data has been inserted into the dim_staff table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )

        spy.assert_any_call(
            "No data has been inserted into the dim_location table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )

        spy.assert_any_call(
            "No data has been inserted into the dim_date table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )

        spy.assert_any_call(
            "No data has been inserted into the dim_counterparty table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )

        spy.assert_any_call(
            "No data has been inserted into the fact_sales_order table.",
            "/aws/lambda/loading-lambda",
            "lambda-log-stream",
        )


@mock_logs
@mock_s3
def test_loading_lambda_handler_logs_to_cloudwatch(mocker):
    """
    Test the lambda_handler function's behavior when logging to CloudWatch.

    This test checks whether the lambda_handler function logs a message to CloudWatch
    when data has been inserted into a table.
    """
    client = boto3.client("logs", region_name="eu-west-2")
    client.create_log_group(logGroupName="/aws/lambda/loading-lambda")
    client.create_log_stream(
        logGroupName="/aws/lambda/loading-lambda",
        logStreamName="lambda-log-stream",
    )

    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_location/last_loaded.txt"
    ).put(Body="1900-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_counterparty/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_currency/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_date/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_design/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "fact_sales_order/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-processed-bucket", "dim_staff/last_loaded.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    spy = mocker.spy(loading, "log_to_cloudwatch")

    with patch(
        "src.loading.loading.read_processed_csv"
    ) as mock_read_processed_csv, patch(
        "src.loading.loading.insert_into_dim_design"
    ) as mocked_insert_into_dim_design, patch(
        "src.loading.loading.insert_into_dim_currency"
    ) as mocked_insert_into_dim_currency, patch(
        "src.loading.loading.insert_into_dim_staff"
    ) as mocked_insert_into_dim_staff, patch(
        "src.loading.loading.insert_into_dim_location"
    ) as mocked_insert_into_dim_location, patch(
        "src.loading.loading.insert_into_dim_date"
    ) as mocked_insert_into_dim_date, patch(
        "src.loading.loading.insert_into_dim_counterparty"
    ) as mocked_insert_into_dim_counterparty, patch(
        "src.loading.loading.insert_into_dim_fact_sales_order"
    ) as mocked_insert_into_dim_fact_sales_order:
        mocked_insert_into_dim_design.return_value = [1, 2, 3]
        mocked_insert_into_dim_currency.return_value = [1, 2, 3]
        mocked_insert_into_dim_staff.return_value = [1, 2, 3]
        mocked_insert_into_dim_location.return_value = [1, 2, 3]
        mocked_insert_into_dim_date.return_value = [1, 2, 3]
        mocked_insert_into_dim_counterparty.return_value = [1, 2, 3]
        mocked_insert_into_dim_fact_sales_order.return_value = [1, 2, 3]

        mock_read_processed_csv.return_value = {
            "fact_sales_order": [],
            "dim_date": [],
            "dim_staff": [],
            "dim_location": [],
            "dim_currency": [],
            "dim_design": [],
            "dim_counterparty": [],
        }
        lambda_handler(
            {},
            {},
            test_user,
            test_database,
            test_host,
            test_port,
            test_password,
        )

    spy.assert_any_call(
        "Data has been inserted into the fact_sales_order table.",
        "/aws/lambda/loading-lambda",
        "lambda-log-stream",
    )


@mock_logs
def test_loading_lambda_handler_raises_exception():
    """
    Test the lambda_handler function's behavior when encountering an exception.

    This test checks whether calling lambda_handler with invalid parameters
    raises an exception.
    """
    with pytest.raises(Exception):
        lambda_handler(
            {},
            {},
            test_user,
            test_database,
            test_host,
            test_port,
            test_password,
        )
