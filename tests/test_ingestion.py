from src.ingestion_lambda.ingestion import lambda_handler
import os
from unittest.mock import patch
from moto import mock_s3, mock_logs
import boto3
from dotenv import load_dotenv

load_dotenv()

test_user = os.environ.get("TEST_SOURCE_USER")
test_database = os.environ.get("TEST_SOURCE_DATABASE")
test_host = os.environ.get("TEST_SOURCE_HOST")
test_port = os.environ.get("TEST_SOURCE_PORT")
test_password = os.environ.get("TEST_SOURCE_PASSWORD")


@mock_s3
@mock_logs
def test_lambda_handler_calls_get_address_add():
    conn = boto3.resource("s3", region_name="eu-west-2")

    conn.create_bucket(
        Bucket="kp-northcoder-ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    conn.Object(
        "kp-northcoder-ingestion-bucket", "address/created_at.txt"
    ).put(Body="2023-07-29 15:20:49.962000")

    conn.Object(
        "kp-northcoder-ingestion-bucket", "counterparty/created_at.txt"
    ).put(Body="2023-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoder-ingestion-bucket", "currency/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoder-ingestion-bucket", "department/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    conn.Object(
        "kp-northcoder-ingestion-bucket", "design/created_at.txt"
    ).put(Body="2020-07-30 15:20:49.962000")

    # conn.Object(
    #     "kp-northcoder-ingestion-bucket", "payment/created_at.txt"
    # ).put(Body="2020-07-30 15:20:49.962000")

    # conn.Object(
    #     "kp-northcoder-ingestion-bucket", "purchase_order/created_at.txt"
    # ).put(Body="2020-07-30 15:20:49.962000")

    # conn.Object(
    #     "kp-northcoder-ingestion-bucket", "sales_order/created_at.txt"
    # ).put(Body="2020-07-30 15:20:49.962000")

    # conn.Object(
    #     "kp-northcoder-ingestion-bucket", "staff/created_at.txt"
    # ).put(Body="2020-07-30 15:20:49.962000")

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
            db_password=test_password
        )

        s3_client = boto3.client("s3")

        address_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="address.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert (
            "3,Bank of England,Threadneedle St,,London,EC2R 8AH"
            in address_response
        )

        counterparty_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="counterparty.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            "2,Harris and Sons Ltd,2,Contract_2,Matt" in counterparty_response
        )

        currency_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="currency.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            "2,USD,2022-11-03 14:20:49.962000" in currency_response
        )

        department_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="department.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            "5,Finance,Manchester,Jordan Belfort" in department_response
        )

        design_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="design.csv",
            )["Body"]
            .read()
            .decode("utf-8")
        )
        assert (
            "178,2023-07-20 09:29:09.957000,Granite" in design_response
        )

        # payment_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="payment.csv",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )
        # print(payment_response)
        # assert (
        #     "add data" in payment_response
        # )

        # purchase_order_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="purchase_order.csv",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )
        # assert (
        #     "put new data" in purchase_order_response
        # )

        # sales_order_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="sales_order.csv",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )
        # print(sales_order_response)
        # assert (
        #     "178,2023-07-20 09:29:09.957000,Granite" in sales_order_response
        # )

        # staff_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="staff.csv",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )
        # print(staff_response)
        # assert (
        #     "178,2023-07-20 09:29:09.957000,Granite" in staff_response
        # )

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="address/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert txt_response == "2023-07-30 14:07:32.362337"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="counterparty/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert txt_response == "2023-07-31 11:35:14.976768"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="currency/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert txt_response == "2022-11-03 14:20:49.962000"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="department/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert txt_response == "2022-11-03 14:20:49.962000"

        txt_response = (
            s3_client.get_object(
                Bucket="kp-northcoder-ingestion-bucket",
                Key="design/created_at.txt",
            )["Body"]
            .read()
            .decode("utf-8")
        )

        assert txt_response == "2023-07-27 13:00:09.857000"

        # txt_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="payment/created_at.txt",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )

        # assert txt_response == "2023-07-27 13:00:09.857000"

        # txt_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="purchase_order/created_at.txt",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )

        # assert txt_response == "2023-07-27 13:00:09.857000"

        # txt_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="sales_order/created_at.txt",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )

        # assert txt_response == "2023-07-27 13:00:09.857000"

        # txt_response = (
        #     s3_client.get_object(
        #         Bucket="kp-northcoder-ingestion-bucket",
        #         Key="staff/created_at.txt",
        #     )["Body"]
        #     .read()
        #     .decode("utf-8")
        # )

        # assert txt_response == "2023-07-27 13:00:09.857000"
