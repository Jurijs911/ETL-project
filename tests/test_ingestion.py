from src.ingestion_lambda.ingestion import lambda_handler
import os
from unittest.mock import patch
from moto import mock_s3, mock_logs
import boto3
from dotenv import load_dotenv
from src.ingestion_lambda import ingestion
import pytest


load_dotenv()

test_user = os.environ.get("TEST_SOURCE_USER")
test_database = os.environ.get("TEST_SOURCE_DATABASE")
test_host = os.environ.get("TEST_SOURCE_HOST")
test_port = os.environ.get("TEST_SOURCE_PORT")
test_password = os.environ.get("TEST_SOURCE_PASSWORD")


@mock_s3
@mock_logs
def test_lambda_handler_calls_utils():
    conn = boto3.resource("s3", region_name="eu-west-2")

    conn.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    conn.Object(
        "kp-northcoders-ingestion-bucket", "address/created_at.txt"
    ).put(Body="2023-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "counterparty/created_at.txt"
    ).put(Body="2023-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "currency/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "department/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "design/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "payment/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "purchase_order/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "sales_order/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object("kp-northcoders-ingestion-bucket", "staff/created_at.txt").put(
        Body="2020-07-30 15:20:49.962000"
    )

    conn.Object("kp-northcoders-ingestion-bucket", "address.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "counterparty.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "currency.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "department.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "design.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "payment.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "purchase_order.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "sales_order.csv").put()

    conn.Object("kp-northcoders-ingestion-bucket", "staff.csv").put()

    client = boto3.client("logs", region_name="eu-west-2")
    client.create_log_group(logGroupName="/aws/lambda/ingestion-lambda")
    client.create_log_stream(
        logGroupName="/aws/lambda/ingestion-lambda",
        logStreamName="lambda-log-stream",
    )
    with patch("src.ingestion_lambda.ingestion.log_to_cloudwatch"):
        lambda_handler(
            {},
            {},
            db_user=test_user,
            db_database=test_database,
            db_host=test_host,
            db_port=test_port,
            db_password=test_password,
        )

        s3_client = boto3.client("s3")

        address_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="address.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            "3|Bank of England|Threadneedle St||London|EC2R 8AH"
            in address_response
        )

        counterparty_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="counterparty.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            "2|Harris and Sons Ltd|2|Contract_2|Matt" in counterparty_response
        )

        currency_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="currency.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "2|USD|2023-07-28 15:09:53.424884" in currency_response

        department_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="department.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "2|Jeans|2nd Floor|Pete|2023-07-28" in department_response

        design_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="design.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "2023-07-28 14:56:58.558924|Glow in the dark" in design_response

        payment_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="payment.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "1|2023-08-01 12:39:34.942457|2023-08-01" in payment_response

        purchase_order_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="purchase_order.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "1|2023-08-01 12:36:40.948439" in purchase_order_response

        sales_order_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="sales_order.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "1|2023-07-28 15:09:58.335449" in sales_order_response

        staff_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="staff.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert "1|Paul|McCartney|1|paul@northcoders.com" in staff_response

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="address/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-07-30 14:07:32.362337"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="counterparty/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-07-31 11:35:14.976768"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="currency/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-07-31 12:01:49.175474"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="department/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-07-31 16:11:01.427541"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="design/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-08-01 12:23:35.315112"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="payment/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-08-01 12:39:34.942457"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="purchase_order/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-08-01 12:36:40.948439"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="sales_order/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-07-28 15:09:58.335449"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoders-ingestion-bucket",
                Key="staff/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert txt_response == "2023-07-28 15:02:21.393482"


@mock_s3
def test_lambda_handler_logs(mocker):
    # Set up the mocked S3 bucket and objects
    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    #
    # table.txt objects
    conn.Object(
        "kp-northcoders-ingestion-bucket", "address/created_at.txt"
    ).put(Body="2023-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "counterparty/created_at.txt"
    ).put(Body="2023-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "currency/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "department/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "design/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "payment/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "purchase_order/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "sales_order/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object("kp-northcoders-ingestion-bucket", "staff/created_at.txt").put(
        Body="2020-07-30 15:20:49.962000"
    )

    # Create a spy on the log_to_cloudwatch function
    spy = mocker.spy(ingestion, "log_to_cloudwatch")

    # Call the lambda_handler function
    lambda_handler(
        {}, {}, test_user, test_database, test_host, test_port, test_password
    )

    # Check if the log_to_cloudwatch function was
    # called with the expected arguments
    spy.assert_any_call(
        "New address data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New counterparty data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New currency data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New department data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New design data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New payment data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New purchase_order data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New sales_order data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "New staff data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )


@mock_s3
def test_lambda_handler_logs_no_data(mocker):
    # Set up the mocked S3 bucket and objects
    conn = boto3.resource("s3", region_name="eu-west-2")
    conn.create_bucket(
        Bucket="kp-northcoders-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    #
    # table.txt objects
    conn.Object(
        "kp-northcoders-ingestion-bucket", "address/created_at.txt"
    ).put(Body="2024-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "counterparty/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "currency/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "department/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "design/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "payment/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "purchase_order/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoders-ingestion-bucket", "sales_order/created_at.txt"
    ).put(Body="2024-07-30 15:20:49.962000")

    conn.Object("kp-northcoders-ingestion-bucket", "staff/created_at.txt").put(
        Body="2024-07-30 15:20:49.962000"
    )

    # Create a spy on the log_to_cloudwatch function
    spy = mocker.spy(ingestion, "log_to_cloudwatch")

    # Call the lambda_handler function
    lambda_handler(
        {}, {}, test_user, test_database, test_host, test_port, test_password
    )

    # Check if the log_to_cloudwatch function was
    # called with the expected arguments
    spy.assert_any_call(
        "No new address data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new counterparty data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new currency data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new department data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new design data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new payment data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new purchase_order data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new sales_order data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )
    spy.assert_any_call(
        "No new staff data returned",
        "/aws/lambda/ingestion-lambda",
        "lambda-log-stream",
    )


@mock_logs
def test_lambda_handler_raises_exception():
    with pytest.raises(Exception):
        lambda_handler(
                {}, {}, test_user, test_database,
                test_host, test_port, test_password)
